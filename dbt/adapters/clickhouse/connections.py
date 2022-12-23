import re
import time
from contextlib import contextmanager
from typing import Any, Optional, Tuple

import agate
import dbt.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection

from dbt.adapters.clickhouse.dbclient import ChRetryableException, get_db_client
from dbt.adapters.clickhouse.logger import logger

retryable_exceptions = [ChRetryableException]
ddl_re = re.compile(r'^\s*(CREATE|DROP|ALTER)\s', re.IGNORECASE)


class ClickHouseConnectionManager(SQLConnectionManager):
    """
    ClickHouse Connector connection manager.
    """

    TYPE = 'clickhouse'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as exp:
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

        def connect():
            return get_db_client(credentials)

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection_name = connection.name
        logger.debug('Cancelling query \'{}\'', connection_name)
        connection.handle.close()
        logger.debug('Cancel query \'{}\'', connection_name)

    def release(self):
        pass  # There is no "release" type functionality in the existing ClickHouse connectors

    @classmethod
    def get_table_from_response(cls, response, column_names) -> agate.Table:
        """
        Build agate table from response.
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
        # Don't try to fetch result of clustered DDL responses, we don't know what to do with them
        if fetch and ddl_re.match(sql):
            fetch = False

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
