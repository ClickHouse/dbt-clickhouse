import clickhouse_driver
from dbt.exceptions import DatabaseException as DBTDatabaseException
from dbt.exceptions import FailedToConnectException
from dbt.version import __version__ as dbt_version

from dbt.adapters.clickhouse import ClickHouseCredentials
from dbt.adapters.clickhouse.dbclient import ChClientWrapper


class ChNativeClient(ChClientWrapper):
    def query(self, sql, **kwargs):
        try:
            return NativeClientResult(self.client.execute(sql, with_column_types=True, **kwargs))
        except clickhouse_driver.errors.Error as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def command(self, sql, **kwargs):
        try:
            result = self.client.execute(sql, **kwargs)
            if len(result) and len(result[0]):
                return result[0][0]
        except clickhouse_driver.errors.Error as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def close(self):
        self.client.disconnect()

    def _create_client(self, credentials: ClickHouseCredentials):
        try:
            return clickhouse_driver.Client(
                host=credentials.host,
                port=credentials.port,
                user=credentials.user,
                password=credentials.password,
                client_name=f'dbt-{dbt_version}',
                secure=credentials.secure,
                verify=credentials.verify,
                connect_timeout=credentials.connect_timeout,
                send_receive_timeout=credentials.send_receive_timeout,
                sync_request_timeout=credentials.sync_request_timeout,
                compress_block_size=credentials.compress_block_size,
                compression=False if credentials.compression == '' else credentials.compression,
                settings=credentials.custom_settings,
            )
        except clickhouse_driver.errors.Error as ex:
            raise FailedToConnectException(str(ex)) from ex

    def _set_client_database(self):
        # After we know the database exists, reconnect to that database if appropriate
        if self.client.connection.database != self.database:
            self.client.connection.disconnect()
            self.client.connection.database = self.database
            self.client.connection.connect()

    def _server_version(self):
        server_info = self.client.connection.server_info
        return (
            f'{server_info.version_major}.{server_info.version_minor}.{server_info.version_patch}'
        )


class NativeClientResult:
    def __init__(self, native_result):
        self.result_set = native_result[0]
        self.column_names = [col[0] for col in native_result[1]]
