from typing import Optional, Any, Tuple

import agate
import time
import dbt.exceptions

from decimal import Decimal
from dataclasses import dataclass
from contextlib import contextmanager

from clickhouse_driver import Client, errors

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.version import __version__ as dbt_version


@dataclass
class ClickhouseCredentials(Credentials):
    host: str
    port: Optional[int] = None
    user: Optional[str] = 'default'
    database: Optional[str]
    password: str = ''

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

        except errors.ServerException as e:
            logger.debug('Clickhouse error: {}', str(e))

            try:
                # attempt to release the connection
                self.release()
            except errors.Error:
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
        kwargs = {}

        try:
            handle = Client(
                host=credentials.host,
                port=credentials.port,
                database='default',
                user=credentials.user,
                password=credentials.password,
                client_name=f'dbt-{dbt_version}',
                connect_timeout=10,
                **kwargs,
            )
            connection.handle = handle
            connection.state = 'open'
        except errors.DatabaseError as e:
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

        connection.handle.disconnect()

        logger.debug('Cancel query \'{}\'', connection_name)

    @classmethod
    def get_table_from_response(cls, response, columns) -> agate.Table:
        column_names = [x[0] for x in columns]

        data = []
        for row in response:
            data.append(dict(zip(column_names, row)))

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

            response, columns = client.execute(sql, with_column_types=True)

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
