from dbt.adapters.base import AdapterPlugin
from dbt.contracts.graph.nodes import BaseNode

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


def get_materialization(self):
    """
    Aliases `materialized` config `incremental` or `table` in combination with `is_distributed` model config set to true
    to `distributed_incremental` or `distributed_table` respectively. This is required for compatibility of dbt-core
    microbatch functionalities with distributed models.
    """
    materialized = self.config.materialized
    is_distributed = self.config.extra.get('is_distributed')
    if materialized in ('incremental', 'table') and is_distributed:
        materialized = f"distributed_{materialized}"
    return materialized


# patches a BaseNode method to allow setting `materialized` config overrides via dbt flags
BaseNode.get_materialization = get_materialization
