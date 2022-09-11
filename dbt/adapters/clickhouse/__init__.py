from dbt.adapters.base import AdapterPlugin

from dbt.adapters.clickhouse.column import ClickHouseColumn  # noqa
from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager  # noqa
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.relation import ClickHouseRelation  # noqa
from dbt.include import clickhouse  # noqa

Plugin = AdapterPlugin(
    adapter=ClickHouseAdapter,
    credentials=ClickHouseCredentials,
    include_path=clickhouse.PACKAGE_PATH,
)
