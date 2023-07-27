import uuid
from abc import ABC, abstractmethod

from dbt.exceptions import DbtDatabaseError, FailedToConnectError

from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.logger import logger

LW_DELETE_SETTING = 'allow_experimental_lightweight_delete'
ND_MUTATION_SETTING = 'allow_nondeterministic_mutations'


def get_db_client(credentials: ClickHouseCredentials):
    driver = credentials.driver
    port = credentials.port
    if not driver:
        if port in (9000, 9440):
            driver = 'native'
        else:
            driver = 'http'
    if driver == 'http':
        if not port:
            port = 8443 if credentials.secure else 8123
    elif driver == 'native':
        if not port:
            port = 9440 if credentials.secure else 9000
    else:
        raise FailedToConnectError(f'Unrecognized ClickHouse driver {driver}')

    credentials.driver = driver
    credentials.port = port
    if driver == 'native':
        try:
            import clickhouse_driver  # noqa

            from dbt.adapters.clickhouse.nativeclient import ChNativeClient

            return ChNativeClient(credentials)
        except ImportError:
            raise FailedToConnectError(
                'Native adapter required but package clickhouse-driver is not installed'
            )
    try:
        import clickhouse_connect  # noqa

        from dbt.adapters.clickhouse.httpclient import ChHttpClient

        return ChHttpClient(credentials)
    except ImportError:
        raise FailedToConnectError(
            'HTTP adapter required but package clickhouse-connect is not installed'
        )


class ChRetryableException(Exception):
    pass


class ChClientWrapper(ABC):
    def __init__(self, credentials: ClickHouseCredentials):
        self.database = credentials.schema
        custom_settings = credentials.custom_settings or {}
        self._conn_settings = custom_settings.copy()
        self._conn_settings['session_id'] = f'dbt::{uuid.uuid4()}'
        if credentials.cluster_mode or credentials.database_engine == 'Replicated':
            self._conn_settings['database_replicated_enforce_synchronous_settings'] = '1'
            self._conn_settings['insert_quorum'] = 'auto'
        self._conn_settings['mutations_sync'] = '2'
        self._conn_settings['insert_distributed_sync'] = '1'
        self._client = self._create_client(credentials)
        check_exchange = credentials.check_exchange and not credentials.cluster_mode
        try:
            self._ensure_database(credentials.database_engine, credentials.cluster)
            self.server_version = self._server_version()
            self.has_lw_deletes, self.use_lw_deletes = self._check_lightweight_deletes(
                credentials.use_lw_deletes
            )
            self.atomic_exchange = not check_exchange or self._check_atomic_exchange()
        except Exception as ex:
            self.close()
            raise ex

    @abstractmethod
    def query(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def command(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def get_ch_setting(self, setting_name):
        pass

    def database_dropped(self, database: str):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def _create_client(self, credentials: ClickHouseCredentials):
        pass

    @abstractmethod
    def _set_client_database(self):
        pass

    @abstractmethod
    def _server_version(self):
        pass

    def _check_lightweight_deletes(self, requested: bool):
        lw_deletes = self.get_ch_setting(LW_DELETE_SETTING)
        nd_mutations = self.get_ch_setting(ND_MUTATION_SETTING)
        if lw_deletes is None or nd_mutations is None:
            if requested:
                logger.warning(
                    'use_lw_deletes requested but are not available on this ClickHouse server'
                )
            return False, False
        lw_deletes = int(lw_deletes) > 0
        if not lw_deletes:
            try:
                self.command(f'SET {LW_DELETE_SETTING} = 1')
                self._conn_settings[LW_DELETE_SETTING] = '1'
                lw_deletes = True
            except DbtDatabaseError:
                pass
        nd_mutations = int(nd_mutations) > 0
        if lw_deletes and not nd_mutations:
            try:
                self.command(f'SET {ND_MUTATION_SETTING} = 1')
                self._conn_settings[ND_MUTATION_SETTING] = '1'
                nd_mutations = True
            except DbtDatabaseError:
                pass
        if lw_deletes and nd_mutations:
            return True, requested
        if requested:
            logger.warning('use_lw_deletes requested but cannot enable on this ClickHouse server')
        return False, False

    def _ensure_database(self, database_engine, cluster_name) -> None:
        if not self.database:
            return
        check_db = f'EXISTS DATABASE {self.database}'
        try:
            db_exists = self.command(check_db)
            if not db_exists:
                engine_clause = f' ENGINE {database_engine} ' if database_engine else ''
                cluster_clause = f' ON CLUSTER {cluster_name} ' if cluster_name is not None else ''
                self.command(f'CREATE DATABASE {self.database}{cluster_clause}{engine_clause}')
                db_exists = self.command(check_db)
                if not db_exists:
                    raise FailedToConnectError(
                        f'Failed to create database {self.database} for unknown reason'
                    )
        except DbtDatabaseError as ex:
            raise FailedToConnectError(
                f'Failed to create {self.database} database due to ClickHouse exception'
            ) from ex
        self._set_client_database()

    def _check_atomic_exchange(self) -> bool:
        try:
            db_engine = self.command('SELECT engine FROM system.databases WHERE name = database()')
            if db_engine not in ('Atomic', 'Replicated'):
                return False
            create_cmd = (
                'CREATE TABLE IF NOT EXISTS {} (test String) ENGINE MergeTree() ORDER BY tuple()'
            )
            table_id = str(uuid.uuid1()).replace('-', '')
            swap_tables = [f'__dbt_exchange_test_{x}_{table_id}' for x in range(0, 2)]
            for table in swap_tables:
                self.command(create_cmd.format(table))
            try:
                self.command('EXCHANGE TABLES {} AND {}'.format(*swap_tables))
                return True
            except DbtDatabaseError:
                logger.info('ClickHouse server does not support the EXCHANGE TABLES command')
                logger.info(
                    'This can be caused by an obsolete ClickHouse version or by running ClickHouse on'
                )
                logger.info(
                    'an operating system that does not support the low level renameat2() system call.'
                )
                logger.info('Some DBT materializations will be slower and not atomic as a result.')
            finally:
                try:
                    for table in swap_tables:
                        self.command(f'DROP TABLE IF EXISTS {table}')
                except DbtDatabaseError:
                    logger.info('Unexpected server exception dropping table')
        except DbtDatabaseError:
            logger.warning('Failed to run exchange test')
        return False
