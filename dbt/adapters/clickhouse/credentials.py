from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtRuntimeError


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
    cluster: Optional[str] = None
    database_engine: Optional[str] = None
    cluster_mode: bool = False
    secure: bool = False
    verify: bool = True
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

        # clickhouse_driver expects tcp_keepalive to be a tuple if it's not a boolean
        if isinstance(self.tcp_keepalive, list):
            self.tcp_keepalive = tuple(self.tcp_keepalive)

    def _connection_keys(self):
        return (
            'driver',
            'host',
            'port',
            'user',
            'schema',
            'retries',
            'database_engine',
            'cluster_mode',
            'secure',
            'verify',
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
        )
