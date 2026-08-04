from typing import List

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from clickhouse_connect.driver.httputil import all_managers, check_env_proxy, get_pool_manager
from dbt.adapters.__about__ import version as dbt_adapters_version
from dbt.adapters.clickhouse import ClickHouseColumn
from dbt.adapters.clickhouse.__version__ import version as dbt_clickhouse_version
from dbt.adapters.clickhouse.dbclient import ChClientWrapper, ChRetryableException
from dbt.adapters.clickhouse.util import hide_stack_trace
from dbt_common.exceptions import DbtDatabaseError


class ChHttpClient(ChClientWrapper):
    _dedicated_pool = None

    @staticmethod
    def _inject_query_id(kwargs):
        query_id = kwargs.pop('query_id', None)
        if query_id:
            kwargs.setdefault('settings', {})['query_id'] = query_id

    def query(self, sql, **kwargs):
        try:
            self._inject_query_id(kwargs)
            return self._client.query(sql, **kwargs)
        except DatabaseError as ex:
            err_msg = hide_stack_trace(ex)
            raise DbtDatabaseError(err_msg) from ex

    def command(self, sql, **kwargs):
        try:
            self._inject_query_id(kwargs)
            return self._client.command(sql, **kwargs)
        except DatabaseError as ex:
            err_msg = hide_stack_trace(ex)
            raise DbtDatabaseError(err_msg) from ex

    def columns_in_query(self, sql: str, **kwargs) -> List[ClickHouseColumn]:
        try:
            query_result = self._client.query(
                f"SELECT * FROM ( \n{sql} \n) LIMIT 0",
                **kwargs,
            )
            return [
                ClickHouseColumn.create(name, ch_type.name)
                for name, ch_type in zip(
                    query_result.column_names, query_result.column_types, strict=True
                )
            ]
        except DatabaseError as ex:
            err_msg = hide_stack_trace(ex)
            raise DbtDatabaseError(err_msg) from ex

    def get_ch_setting(self, setting_name):
        setting = self._client.server_settings.get(setting_name)
        return (setting.value, setting.readonly) if setting else (None, 0)

    def database_dropped(self, database: str):
        super().database_dropped(database)
        # This is necessary for the http client to avoid exceptions when ClickHouse doesn't recognize the database
        # query parameter
        if self.database == database:
            self._client.database = None

    def close(self):
        try:
            self._client.close()
        finally:
            self._discard_dedicated_pool()

    def _discard_dedicated_pool(self):
        if self._dedicated_pool is not None:
            self._dedicated_pool.clear()
            # get_pool_manager registers every pool in clickhouse-connect's
            # module-level all_managers registry; without this pop each
            # per-model pool stays pinned there for the process lifetime.
            all_managers.pop(self._dedicated_pool, None)
            self._dedicated_pool = None

    def _create_dedicated_pool(self, credentials):
        # Passing pool_mgr to get_client bypasses clickhouse-connect's env
        # proxy handling, so replicate it here. Plain-HTTP pools need no
        # TLS options.
        proxy = check_env_proxy('http', credentials.host, credentials.port)
        if proxy:
            return get_pool_manager(http_proxy=proxy)
        return get_pool_manager()

    def _create_client(self, credentials):
        # When reuse_connections is False, each model needs a fresh TCP/TLS
        # connection so the ClickHouse Cloud load balancer can distribute
        # models across replicas. In clickhouse-connect, close() only tears
        # down the transport when the client owns its pool manager; in the
        # standard path clients share a process-wide pool singleton that
        # keeps sockets alive across close().
        server_host_name = credentials.server_host_name
        kwargs = {}
        if not credentials.reuse_connections:
            if credentials.secure:
                # Passing server_host_name flips clickhouse-connect onto an
                # internally built, client-owned pool (SNI, certs, and proxy
                # handling wired as usual), which close() then fully tears
                # down. Defaulting it to the connect host makes TLS a no-op:
                # SNI and hostname assertion match what the standard path
                # verifies anyway.
                server_host_name = server_host_name or credentials.host
            else:
                # clickhouse-connect only builds client-owned pools for
                # HTTPS, so over plain HTTP we must supply a dedicated pool
                # and discard it ourselves on close().
                self._dedicated_pool = self._create_dedicated_pool(credentials)
                kwargs['pool_mgr'] = self._dedicated_pool

        try:
            return clickhouse_connect.get_client(
                host=credentials.host,
                port=credentials.port,
                username=credentials.user,
                password=credentials.password,
                interface='https' if credentials.secure else 'http',
                compress=False if credentials.compression == '' else bool(credentials.compression),
                connect_timeout=credentials.connect_timeout,
                send_receive_timeout=credentials.send_receive_timeout,
                client_name=f'dbt-adapters/{dbt_adapters_version} dbt-clickhouse/{dbt_clickhouse_version}',
                verify=credentials.verify,
                server_host_name=server_host_name,
                client_cert=credentials.client_cert,
                client_cert_key=credentials.client_cert_key,
                query_limit=0,
                settings=self._conn_settings,
                **kwargs,
            )
        except OperationalError as ex:
            self._discard_dedicated_pool()
            raise ChRetryableException(str(ex)) from ex

    def _set_client_database(self):
        self._client.database = self.database

    def _server_version(self):
        return self._client.server_version
