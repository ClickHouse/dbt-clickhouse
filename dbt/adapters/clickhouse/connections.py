from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager


@dataclass
class ClickhouseCredentials(Credentials):
    # Add credentials members here, like:
    # host: str
    # port: int
    # username: str
    # password: str

    @property
    def type(self):
        return 'clickhouse'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        raise NotImplementedError


class ClickhouseConnectionManager(SQLConnectionManager):
    TYPE = 'clickhouse'
