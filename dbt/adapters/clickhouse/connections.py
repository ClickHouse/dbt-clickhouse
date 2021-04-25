from typing import Optional, Any, Tuple

import agate
import time
import uuid
import dbt.exceptions

from decimal import Decimal
from dataclasses import dataclass
from contextlib import contextmanager

from sqlalchemy import create_engine, exc
from sqlalchemy.engine.url import URL
from requests import Session

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class ClickhouseCredentials(Credentials):
    host: str
    port: Optional[int] = None
    user: Optional[str] = 'default'
    database: Optional[str]
    password: str = ''
    protocol: str = 'native'

    @property
    def type(self):
        return 'clickhouse'

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'On Clickhouse, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None

    def _connection_keys(self):
        return ('host', 'port', 'user', 'schema')


class ClickhouseConnectionManager(SQLConnectionManager):
    TYPE = 'clickhouse'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except exc.DatabaseError as e:
            logger.debug('Clickhouse error: {}', str(e))

            try:
                # attempt to release the connection
                self.release()
            except exc.SQLAlchemyError:
                logger.debug('Failed to release connection!')
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug('Error running SQL: {}', sql)
            logger.debug('Rolling back transaction.')
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                raise

            raise dbt.exceptions.RuntimeException(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)

        url = URL(
            drivername=f'clickhouse+{credentials.protocol}',
            host=credentials.host,
            port=credentials.port,
            database='default',
            username=credentials.user,
            password=credentials.password,
            query={'session_id': str(uuid.uuid4())}
        )

        try:
            handle = create_engine(url, connect_args={'http_session': Session()}).connect()
            connection.handle = handle
            connection.state = 'open'
        except exc.DatabaseError as e:
            logger.debug(
                'Got an error when attempting to open a clickhouse connection: \'{}\'',
                str(e),
            )

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        connection_name = connection.name

        logger.debug('Cancelling query \'{}\'', connection_name)

        connection.handle.close()

        logger.debug('Cancel query \'{}\'', connection_name)

    @classmethod
    def get_table_from_response(cls, response, columns) -> agate.Table:
        column_names = columns

        data = []
        for row in response:
            data.append(dict(zip(column_names, row)))
        print(data)
        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[str, agate.Table]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        client = conn.handle

        with self.exception_handler(sql):
            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=conn.name,
                sql=f'{sql}...',
            )

            pre = time.time()

            response = client.execute(sql)
            columns = response._metadata.keys

            status = self.get_status(client)

            logger.debug(
                'SQL status: {status} in {elapsed:0.2f} seconds',
                status=status,
                elapsed=(time.time() - pre),
            )

            if fetch:
                table = self.get_table_from_response(response, columns)
            else:
                table = dbt.clients.agate_helper.empty_table()
            return status, table

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        sql = self._add_query_comment(sql)
        conn = self.get_thread_connection()
        credentials = cls.get_credentials(connection.credentials)
        client = conn.handle

        with self.exception_handler(sql):
            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=conn.name,
                sql=f'{sql}...',
            )

            # TODO: Convert to allow clickhouse type
            format_bindings = []
            for row in bindings.rows:
                format_row = []
                for v in row.values():
                    if isinstance(v, Decimal):
                        v = int(v)
                    format_row.append(v)
                format_bindings.append(format_row)

            pre = time.time()

            if credentials.protocol != 'native':
                format_bindings = []
                for row in bindings.rows:
                    d = dict(row)
                    d['id'] = int(d['id'])
                    format_bindings.append(d)
                ptr = ','.join([f'%({k})s' for k in bindings.column_names])
                sql += f' ({ptr})'

            client.execute(sql, format_bindings)

            status = self.get_status(client)

            logger.debug(
                'SQL status: {status} in {elapsed:0.2f} seconds',
                status=status,
                elapsed=(time.time() - pre),
            )

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    @classmethod
    def get_response(cls, cursor):
        return 'OK'

    def begin(self):
        pass

    def commit(self):
        pass
