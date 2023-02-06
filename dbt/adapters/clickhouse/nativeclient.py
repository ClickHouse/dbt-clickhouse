import clickhouse_driver
import pkg_resources
from clickhouse_driver.errors import NetworkError, SocketTimeoutError
from dbt.exceptions import DbtDatabaseError
from dbt.version import __version__ as dbt_version

from dbt.adapters.clickhouse import ClickHouseCredentials
from dbt.adapters.clickhouse.__version__ import version as dbt_clickhouse_version
from dbt.adapters.clickhouse.dbclient import ChClientWrapper, ChRetryableException
from dbt.adapters.clickhouse.logger import logger

try:
    driver_version = pkg_resources.get_distribution('clickhouse-driver').version
except pkg_resources.ResolutionError:
    driver_version = 'unknown'


class ChNativeClient(ChClientWrapper):
    def query(self, sql, **kwargs):
        try:
            return NativeClientResult(self._client.execute(sql, with_column_types=True, **kwargs))
        except clickhouse_driver.errors.Error as ex:
            raise DbtDatabaseError(str(ex).strip()) from ex

    def command(self, sql, **kwargs):
        try:
            result = self._client.execute(sql, **kwargs)
            if len(result) and len(result[0]):
                return result[0][0]
        except clickhouse_driver.errors.Error as ex:
            raise DbtDatabaseError(str(ex).strip()) from ex

    def get_ch_setting(self, setting_name):
        try:
            result = self._client.execute(
                f"SELECT value FROM system.settings WHERE name = '{setting_name}'"
            )
        except clickhouse_driver.errors.Error as ex:
            logger.warn('Unexpected error retrieving ClickHouse server setting', ex)
            return None
        return result[0][0] if result else None

    def close(self):
        self._client.disconnect()

    def _create_client(self, credentials: ClickHouseCredentials):
        client = clickhouse_driver.Client(
            host=credentials.host,
            port=credentials.port,
            user=credentials.user,
            password=credentials.password,
            client_name=f'dbt/{dbt_version} dbt-clickhouse/{dbt_clickhouse_version} clickhouse-driver/{driver_version}',
            secure=credentials.secure,
            verify=credentials.verify,
            connect_timeout=credentials.connect_timeout,
            send_receive_timeout=credentials.send_receive_timeout,
            sync_request_timeout=credentials.sync_request_timeout,
            compress_block_size=credentials.compress_block_size,
            compression=False if credentials.compression == '' else credentials.compression,
            settings=self._conn_settings,
        )
        try:
            client.connection.connect()
        except (SocketTimeoutError, NetworkError) as ex:
            raise ChRetryableException(str(ex)) from ex
        return client

    def _set_client_database(self):
        # After we know the database exists, reconnect to that database if appropriate
        if self._client.connection.database != self.database:
            self._client.connection.disconnect()
            self._client.connection.database = self.database
            self._client.connection.connect()

    def _server_version(self):
        server_info = self._client.connection.server_info
        return (
            f'{server_info.version_major}.{server_info.version_minor}.{server_info.version_patch}'
        )


class NativeClientResult:
    def __init__(self, native_result):
        self.result_set = native_result[0]
        self.column_names = [col[0] for col in native_result[1]]
