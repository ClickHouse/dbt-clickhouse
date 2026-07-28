import copy
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Optional

from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.errors import (
    nd_mutations_not_enabled_error,
    nd_mutations_not_enabled_warning,
)
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.adapters.clickhouse.util import compare_versions, engine_can_atomic_exchange
from dbt.adapters.exceptions import FailedToConnectError
from dbt_common.exceptions import DbtConfigError, DbtDatabaseError

_exchange_lock = threading.Lock()
_exchange_result: Optional[bool] = None

# Databases whose existence has already been ensured in this process. Guarded by
# `_database_lock`. With `reuse_connections: false` a client is created per model,
# so without this cache the `EXISTS DATABASE` probe would run on every model.
_database_lock = threading.Lock()
_ensured_databases: set = set()

# Server-side (value, readonly) of `allow_nondeterministic_mutations`, probed once
# per process and guarded by `_nd_mutation_lock`. With `reuse_connections: false` a
# client is created per model, so without this cache every model would re-probe.
_nd_mutation_lock = threading.Lock()
_nd_mutation_probe: Optional[tuple] = None

ND_MUTATION_SETTING = 'allow_nondeterministic_mutations'
DEDUP_WINDOW_SETTING = 'replicated_deduplication_window'
DEDUP_WINDOW_SETTING_SUPPORTED_MATERIALIZATION = [
    "table",
    "incremental",
    "ephemeral",
    "materialized_view",
]


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
        except ImportError as ex:
            raise FailedToConnectError(
                'Native adapter required but package clickhouse-driver is not installed'
            ) from ex
    try:
        import clickhouse_connect  # noqa
        from dbt.adapters.clickhouse.httpclient import ChHttpClient

        return ChHttpClient(credentials)
    except ImportError as ex:
        raise FailedToConnectError(
            'HTTP adapter required but package clickhouse-connect is not installed'
        ) from ex


class ChRetryableException(Exception):
    pass


class ChClientWrapper(ABC):
    def __init__(self, credentials: ClickHouseCredentials):
        self.database = credentials.schema
        custom_settings = credentials.custom_settings or {}
        self._conn_settings = custom_settings.copy()
        self._conn_settings['session_id'] = f'dbt::{uuid.uuid4()}'
        if credentials.cluster_mode or credentials.database_engine == 'Replicated':
            self._conn_settings.setdefault('database_replicated_enforce_synchronous_settings', '1')
            self._conn_settings.setdefault('insert_quorum', 'auto')
        if credentials.database_engine == 'Shared':
            self._conn_settings.setdefault('select_sequential_consistency', '1')
        self._conn_settings.setdefault('mutations_sync', '3')
        self._conn_settings.setdefault('alter_sync', '3')
        self._conn_settings.setdefault('insert_distributed_sync', '1')
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
        self._model_settings: Dict = {
            "table": {},
            "view": {},
            "incremental": {},
            "ephemeral": {},
            "materialized_view": {},
            "snapshot": {},
            "distributed_table": {},
            "distributed_incremental": {},
            "general": {},
        }
        if (
            not credentials.allow_automatic_deduplication
            and compare_versions(self._server_version(), '22.7.1.2484') >= 0
        ):
            for materialization in DEDUP_WINDOW_SETTING_SUPPORTED_MATERIALIZATION:
                self._model_settings[materialization][DEDUP_WINDOW_SETTING] = '0'

    @abstractmethod
    def query(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def command(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def columns_in_query(self, sql: str, **kwargs):
        pass

    @abstractmethod
    def get_ch_setting(self, setting_name):
        pass

    def database_dropped(self, database: str):
        # Forget the cached existence so a later model recreating the schema runs
        # the EXISTS/CREATE path again instead of trusting a stale cache entry.
        with _database_lock:
            _ensured_databases.discard(database)

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

    def update_model_settings(self, model_settings: Dict[str, str], materialization_type: str):
        settings = self._model_settings.get(materialization_type, {})
        model_settings_to_add = copy.deepcopy(settings)
        model_settings_to_add.update(self._model_settings['general'])
        for key, value in model_settings_to_add.items():
            if key not in model_settings:
                model_settings[key] = value

    def _check_lightweight_deletes(self, requested: bool):
        # Lightweight deletes have been generally available since ClickHouse 23.3,
        # which is older than every version this adapter supports, so only the
        # `allow_nondeterministic_mutations` setting they require still needs to be
        # checked (and enabled when the user has permission to change it).
        global _nd_mutation_probe
        with _nd_mutation_lock:
            if _nd_mutation_probe is None:
                _nd_mutation_probe = self.get_ch_setting(ND_MUTATION_SETTING)
            nd_mutations, nd_mutations_read_only = _nd_mutation_probe
        if nd_mutations is None:
            if requested:
                logger.warning(nd_mutations_not_enabled_warning)
            return False, False
        nd_mutations = int(nd_mutations) > 0
        if not nd_mutations:
            if nd_mutations_read_only:
                nd_mutations = False
                if requested:
                    raise DbtConfigError(nd_mutations_not_enabled_error)
                logger.warning(nd_mutations_not_enabled_warning)
            else:
                try:
                    self.command(f'SET {ND_MUTATION_SETTING} = 1')
                    self._conn_settings[ND_MUTATION_SETTING] = '1'
                    nd_mutations = True
                except DbtDatabaseError:
                    logger.warning(nd_mutations_not_enabled_warning)
        if nd_mutations:
            return True, requested
        return False, False

    def _ensure_database(self, database_engine, cluster_name) -> None:
        if not self.database:
            return
        # The existence check/create only needs to happen once per process; cache
        # the result so a per-model client (reuse_connections: false) doesn't probe
        # on every model. The cache is invalidated by `database_dropped`.
        with _database_lock:
            if self.database not in _ensured_databases:
                check_db = f'EXISTS DATABASE {quote_identifier(self.database)}'
                try:
                    db_exists = self.command(check_db)
                    if not db_exists:
                        engine_clause = f' ENGINE {database_engine} ' if database_engine else ''
                        cluster_clause = (
                            f' ON CLUSTER "{cluster_name}" '
                            if cluster_name is not None and cluster_name.strip() != ''
                            else ''
                        )
                        self.command(
                            f'CREATE DATABASE IF NOT EXISTS {quote_identifier(self.database)}{cluster_clause}{engine_clause}'
                        )
                        db_exists = self.command(check_db)
                        if not db_exists:
                            raise FailedToConnectError(
                                f'Failed to create database {self.database} for unknown reason'
                            )
                except DbtDatabaseError as ex:
                    raise FailedToConnectError(
                        f'Failed to create {self.database} database due to ClickHouse exception'
                    ) from ex
                _ensured_databases.add(self.database)
        self._set_client_database()

    def _check_atomic_exchange(self) -> bool:
        global _exchange_result
        with _exchange_lock:
            if _exchange_result is None:
                _exchange_result = self._run_exchange_test()
        return _exchange_result

    def _run_exchange_test(self) -> bool:
        try:
            db_engine = self.command('SELECT engine FROM system.databases WHERE name = database()')
            if not engine_can_atomic_exchange(db_engine):
                return False
            # Shared is only available on ClickHouse Cloud
            # EXCHANGE TABLES is guaranteed to work
            if db_engine == 'Shared':
                return True
            create_cmd = (
                'CREATE TABLE IF NOT EXISTS {} (test String) ENGINE MergeTree() ORDER BY tuple()'
            )
            table_id = str(uuid.uuid1()).replace('-', '')
            swap_tables = [f'__dbt_exchange_test_{x}_{table_id}' for x in range(0, 2)]
            for table in swap_tables:
                self.command(create_cmd.format(quote_identifier(table)))
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
