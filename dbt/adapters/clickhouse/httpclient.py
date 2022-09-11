import uuid

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError
from dbt.exceptions import DatabaseException as DBTDatabaseException
from dbt.exceptions import FailedToConnectException
from dbt.version import __version__ as dbt_version

from dbt.adapters.clickhouse.dbclient import ChClientWrapper


class ChHttpClient(ChClientWrapper):
    def query(self, sql, **kwargs):
        try:
            return self.client.query(sql, **kwargs)
        except DatabaseError as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def command(self, sql, **kwargs):
        try:
            return self.client.command(sql, **kwargs)
        except DatabaseError as ex:
            raise DBTDatabaseException(str(ex).strip()) from ex

    def database_dropped(self, database: str):
        # This is necessary for the http client to avoid exceptions when ClickHouse doesn't recognize the database
        # query parameter
        if self.database == database:
            self.client.database = None

    def close(self):
        self.client.close()

    def _create_client(self, credentials):
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
                client_name=f'cc-dbt-{dbt_version}',
                verify=credentials.verify,
                query_limit=0,
                session_id='dbt::' + str(uuid.uuid4()),
                **(credentials.custom_settings or {}),
            )
        except clickhouse_connect.driver.exceptions.DatabaseError as ex:
            raise FailedToConnectException(str(ex)) from ex

    def _set_client_database(self):
        self.client.database = self.database

    def _server_version(self):
        return self.client.server_version
