import importlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError


@dataclass
class ClickHouseCredentials(Credentials):
    """
    ClickHouse connection credentials data class.
    """

    driver: Optional[str] = None
    host: str = 'localhost'
    port: Optional[int] = None
    user: Optional[str] = 'default'
    retries: int = 1
    database: Optional[str] = ''
    schema: Optional[str] = 'default'
    password: str = ''
    # JWT access token (ClickHouse Cloud, http driver only). Replaces user/password.
    access_token: Optional[str] = None
    # Dotted path ('pkg.module:function' or 'pkg.module.function') to a zero-argument
    # callable returning a JWT. Passed to clickhouse-connect as token_provider, which
    # calls it on connect and again whenever the server rejects the current token.
    access_token_provider: Optional[str] = None
    cluster: Optional[str] = None
    database_engine: Optional[str] = None
    cluster_mode: bool = False
    secure: bool = False
    verify: bool = True
    client_cert: Optional[str] = None
    client_cert_key: Optional[str] = None
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    sync_request_timeout: int = 5
    compress_block_size: int = 1048576
    compression: str = ''
    check_exchange: bool = True
    custom_settings: Optional[Dict[str, Any]] = None
    use_lw_deletes: bool = False
    local_suffix: str = 'local'
    local_db_prefix: str = ''
    allow_automatic_deduplication: bool = False
    tcp_keepalive: Union[bool, tuple[int, int, int], list[int]] = False
    # When False, close the connection after each model so the next opens a
    # fresh TCP socket — lets a Cloud LB rebalance dbt across replicas.
    reuse_connections: bool = True
    server_host_name: Optional[str] = None

    @property
    def type(self):
        return 'clickhouse'

    @property
    def unique_field(self):
        return self.host

    def __post_init__(self):
        if self.database and self.database != self.schema:
            raise DbtRuntimeError(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'    cluster: {self.cluster} \n'
                f'On Clickhouse, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = ''

        if self.access_token and self.access_token_provider:
            raise DbtConfigError('access_token and access_token_provider cannot both be set.')
        if self.uses_token_auth and (self.password or self.user not in (None, 'default')):
            raise DbtConfigError(
                'JWT authentication (access_token or access_token_provider) cannot be combined '
                'with user/password authentication; remove user and password from the profile.'
            )

        # clickhouse_driver expects tcp_keepalive to be a tuple if it's not a boolean
        if isinstance(self.tcp_keepalive, list):
            self.tcp_keepalive = tuple(self.tcp_keepalive)

    @property
    def uses_token_auth(self) -> bool:
        return bool(self.access_token or self.access_token_provider)

    def resolve_token_provider(self) -> Optional[Callable[[], str]]:
        """Import the callable named by access_token_provider, or None if not configured."""
        path = self.access_token_provider
        if not path:
            return None
        module_name, _, attr = path.rpartition(':' if ':' in path else '.')
        if not module_name or not attr:
            raise DbtConfigError(
                f'access_token_provider must be "module:function" or "module.function", got {path!r}'
            )
        try:
            provider = getattr(importlib.import_module(module_name), attr)
        except (ImportError, AttributeError) as ex:
            raise DbtConfigError(f'Could not import access_token_provider {path!r}: {ex}') from ex
        if not callable(provider):
            raise DbtConfigError(f'access_token_provider {path!r} is not callable')
        return provider

    def _connection_keys(self):
        return (
            'driver',
            'host',
            'port',
            'user',
            'access_token_provider',
            'schema',
            'retries',
            'cluster',
            'database_engine',
            'cluster_mode',
            'secure',
            'verify',
            'client_cert',
            'client_cert_key',
            'connect_timeout',
            'send_receive_timeout',
            'sync_request_timeout',
            'compress_block_size',
            'compression',
            'check_exchange',
            'custom_settings',
            'use_lw_deletes',
            'allow_automatic_deduplication',
            'tcp_keepalive',
            'reuse_connections',
            'server_host_name',
        )
