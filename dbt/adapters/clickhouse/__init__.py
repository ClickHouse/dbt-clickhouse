from dbt.adapters.clickhouse.connections import ClickhouseConnectionManager  # noqa
from dbt.adapters.clickhouse.connections import ClickhouseCredentials
from dbt.adapters.clickhouse.relation import ClickhouseRelation  # noqa
from dbt.adapters.clickhouse.column import ClickhouseColumn  # noqa
from dbt.adapters.clickhouse.impl import ClickhouseAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import clickhouse


Plugin = AdapterPlugin(
    adapter=ClickhouseAdapter,
    credentials=ClickhouseCredentials,
    include_path=clickhouse.PACKAGE_PATH)
