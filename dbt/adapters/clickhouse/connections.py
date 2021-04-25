from typing import List, Optional, Any, Tuple, Dict, Union, Iterable
import abc
import agate
import time
import uuid
import dbt.exceptions

from decimal import Decimal
from dataclasses import dataclass
from contextlib import contextmanager

from sqlalchemy import create_engine, exc
from clickhouse_sqlalchemy import make_session, exceptions
from sqlalchemy.engine.url import URL
from requests import Session

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class ClickhouseCredentials(Credentials):
    host: str
    port: Optional[int] = None
    user: Optional[str] = 'default'
    database: Optional[str]
    password: str = ''
    protocol: str = 'http'

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

        except exceptions.DatabaseException as e:
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

        if credentials.protocol=='http':
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
                connection.handle = make_session(handle)
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

    def add_query(
            self,
            sql: str,
            auto_begin: bool = True,
            bindings: Optional[Any] = None,
            abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            logger.debug(
                'On {connection_name}: {sql}',
                connection_name=connection.name,
                sql=log_sql,
            )
            pre = time.time()

            cursor = connection.handle
            cursor = cursor.execute(sql, bindings)
            logger.debug(
                "SQL status: {status} in {elapsed:0.2f} seconds",
                status=self.get_response(cursor),
                elapsed=(time.time() - pre)
            )

            return connection, cursor


    @abc.abstractclassmethod
    def get_response(cls, cursor: Any) -> Union[AdapterResponse, str]:
        """Get the status of the cursor."""
        raise dbt.exceptions.NotImplementedException(
            '`get_response` is not implemented for this adapter!'
        )

    @classmethod
    def process_results(
            cls,
            column_names: Iterable[str],
            rows: Iterable[Any]
    ) -> List[Dict[str, Any]]:
        unique_col_names = dict()
        for idx in range(len(column_names)):
            col_name = column_names[idx]
            if col_name in unique_col_names:
                unique_col_names[col_name] += 1
                column_names[idx] = f'{col_name}_{unique_col_names[col_name]}'
            else:
                unique_col_names[column_names[idx]] = 1
        return [dict(zip(column_names, row)) for row in rows]

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.keys() is not None:
            column_names = [col for col in cursor.keys()]
            rows = cursor.fetchall()
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(
            data,
            column_names
        )

    def execute(
            self, sql: str, auto_begin: bool = False, fetch: bool = False
    ) -> Tuple[Union[AdapterResponse, str], agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return response, table

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
