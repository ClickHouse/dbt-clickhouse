import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import agate
import dbt.exceptions

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.events import AdapterLogger
from dbt.version import __version__ as dbt_version

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

try:
    import clickhouse_driver
    import clickhouse_driver.errors
    from dbt.adapters.clickhouse.nativeadapter import ChNativeAdapter
except ImportError:
    clickhouse_driver = None
    ChNativeAdapter = None

logger = AdapterLogger('clickhouse')


@dataclass
class ClickhouseCredentials(Credentials):
    """
    ClickHouse connection credentials data class.
    """

    # pylint: disable=too-many-instance-attributes
    driver: Optional[str] = None
    host: str = 'localhost'
    port: Optional[int] = None
    user: Optional[str] = 'default'
    database: Optional[str] = None
    schema: Optional[str] = 'default'
    password: str = ''
    cluster: Optional[str] = None
    secure: bool = False
    verify: bool = True
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    sync_request_timeout: int = 5
    compress_block_size: int = 1048576
    compression: str = ''
    use_default_schema: bool = (
        False  # This is used in tests to make sure we connect always to the default database.
    )
    custom_settings: Optional[Dict[str, Any]] = None

    @property
    def type(self):
        return 'clickhouse'

    @property
    def unique_field(self):
        return self.host

    def __post_init__(self):
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'    cluster: {self.cluster} \n'
                f'On Clickhouse, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None

    def _connection_keys(self):
        return ('driver', 'host', 'port', 'user', 'schema', 'secure', 'verify',
                'connect_timeout', 'send_receive_timeout', 'sync_request_timeout',
                'compress_block_size', 'compression', 'use_default_schema', 'custom_settings')


class ClickhouseConnectionManager(SQLConnectionManager):
    """
    ClickHouse Connector connection manager.
    """

    TYPE = 'clickhouse'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as exp:
            self.release()
            if ((clickhouse_connect and isinstance(exp, clickhouse_connect.driver.exceptions.DatabaseError)) or
                    (clickhouse_driver and isinstance(exp, clickhouse_driver.errors.Error))):
                logger.debug('Clickhouse error: {}', str(exp))
                raise dbt.exceptions.DatabaseException(str(exp).strip()) from exp
            logger.debug('Error running SQL: {}', sql)
            if isinstance(exp, dbt.exceptions.RuntimeException):
                raise
            raise dbt.exceptions.RuntimeException(exp) from exp

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        driver = credentials.driver
        port = credentials.port
        if not driver:
            if clickhouse_connect and (not port or port in (8123, 443, 8443, 80)):
                driver = 'http'
            elif clickhouse_driver and port in (9000, 9440):
                driver = 'native'
            else:
                driver = 'unspecified'
        custom_settings = credentials.custom_settings or {}
        connection.state = 'fail'
        db_err = None
        if clickhouse_connect and driver == 'http':
            try:
                connection.handle = clickhouse_connect.get_client(
                    host=credentials.host,
                    port=credentials.port,
                    database='default' if credentials.use_default_schema else credentials.schema,
                    username=credentials.user,
                    password=credentials.password,
                    interface='https' if credentials.secure else 'http',
                    compress=False if credentials.compression == '' else bool(credentials.compression),
                    connect_timeout=credentials.connect_timeout,
                    send_receive_timeout=credentials.send_receive_timeout,
                    http_user_agent=f'cc-dbt-{dbt_version}',
                    verify=credentials.verify,
                    query_limit=0,
                    session_id='dbt::' + str(uuid.uuid4()),
                    **custom_settings)
            except clickhouse_connect.driver.exceptions.DatabaseError as db_err:
                pass
        elif clickhouse_driver and credentials.driver == 'native':
            try:
                client = clickhouse_driver.Client(
                    host = credentials.host,
                    port = credentials.port,
                    database = 'default',
                    user = credentials.user,
                    password = credentials.password,
                    client_name = f'dbt-{dbt_version}',
                    secure = credentials.secure,
                    verify = credentials.verify,
                    connect_timeout = credentials.connect_timeout,
                    send_receive_timeout = credentials.send_receive_timeout,
                    sync_request_timeout = credentials.sync_request_timeout,
                    compress_block_size = credentials.compress_block_size,
                    compression = False if credentials.compression == '' else credentials.compression,
                    **custom_settings)
                connection.handle = ChNativeAdapter(client)
            except clickhouse_driver.errors as db_err:
                pass
        else:
            raise dbt.exceptions.FailedToConnectException(f'Library for ClickHouse driver type {driver} not found')
        if db_err:
            logger.debug('Got an error when attempting to open a clickhouse connection: \'{}\'', str(db_err))
            raise dbt.exceptions.FailedToConnectException(str(db_err))
        connection.state = 'open'
        return connection

    def cancel(self, connection):
        connection_name = connection.name
        logger.debug('Cancelling query \'{}\'', connection_name)
        connection.handle.close()
        logger.debug('Cancel query \'{}\'', connection_name)

    @classmethod
    def get_table_from_response(cls, response, column_names) -> agate.Table:
        """
        Build agate tabel from response.
        :param response: ClickHouse query result
        :param column_names: Table column names
        """
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
            logger.debug(f'On {conn.name}: {sql}...')

            pre = time.time()

            if fetch:
                query_result = client.query(sql)
            else:
                query_result = client.command(sql)

            status = self.get_status(client)

            logger.debug(f'SQL status: {status} in {(time.time() - pre):.2f} seconds')

            if fetch:
                table = self.get_table_from_response(
                    query_result.result_set, query_result.column_names
                )
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
            logger.debug(f'On {conn.name}: {sql}...')

            pre = time.time()
            client.command(sql)

            status = self.get_status(client)

            logger.debug(f'SQL status: {status} in {(time.time() - pre):0.2f} seconds')

            return conn, None

    @classmethod
    def get_credentials(cls, credentials):
        """
        Returns ClickHouse credentials
        """
        return credentials

    @classmethod
    def get_status(cls, _):
        """
        Returns connection status
        """
        return 'OK'

    @classmethod
    def get_response(cls, _):
        return 'OK'

    def begin(self):
        pass

    def commit(self):
        pass
