from dbt.adapters.sql import SQLAdapter
from dbt.adapters.clickhouse import ClickhouseConnectionManager


class ClickhouseAdapter(SQLAdapter):
    ConnectionManager = ClickhouseConnectionManager
