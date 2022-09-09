from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.contracts.connection import Credentials

import dbt


@dataclass
class ClickHouseCredentials(Credentials):
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
        return (
            'driver',
            'host',
            'port',
            'user',
            'schema',
            'secure',
            'verify',
            'connect_timeout',
            'send_receive_timeout',
            'sync_request_timeout',
            'compress_block_size',
            'compression',
            'custom_settings',
        )
