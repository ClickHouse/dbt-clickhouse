import os

import pytest
from dbt.tests.util import run_dbt

no_cluster = pytest.mark.skipif(
    os.environ.get("DBT_CH_TEST_CLUSTER", "").strip() == "", reason="Not on a cluster"
)

model_sql = """
{{ config(
        materialized='distributed_incremental',
        incremental_strategy='append',
        order_by='id',
        sharding_key='id',
) }}
select toUInt64(number) as id from numbers(3)
"""


@no_cluster
class TestDistributedIncrementalTempView:
    @pytest.fixture(scope="class")
    def models(self):
        return {"temp_view_cleanup.sql": model_sql}

    def test_temp_view_is_dropped(self, project):
        for _ in range(2):
            run_dbt(["run", "--select", "temp_view_cleanup"])
            left_over = project.run_sql(
                f"exists table {project.test_schema}.temp_view_cleanup__dbt_view_tmp",
                fetch="one",
            )[0]
            assert left_over == 0
