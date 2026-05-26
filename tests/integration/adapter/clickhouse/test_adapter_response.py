import os

import pytest
from clickhouse_connect import get_client
from dbt.tests.util import run_dbt

model_sql = """
select 1 as id, 'clickhouse' as name
"""


class TestAdapterResponseQueryId:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": model_sql}

    def test_query_id_round_trips_to_query_log(self, project, test_config):
        results = run_dbt(["run"])
        assert len(results.results) == 1

        query_id = results.results[0].adapter_response.get("query_id")
        assert query_id, "adapter_response did not contain a query_id"

        on_cloud = os.environ.get('DBT_CH_TEST_CLOUD', default='').lower() in ('1', 'true', 'yes')
        cluster = os.environ.get('DBT_CH_TEST_CLUSTER', '').strip()
        cluster = 'default' if not cluster and on_cloud else cluster

        cluster_clause = f'ON CLUSTER "{cluster}"' if cluster else ''
        project.run_sql(f"SYSTEM FLUSH LOGS {cluster_clause}", fetch="all")

        from_clause = (
            f"FROM clusterAllReplicas('{cluster}', system.query_log)"
            if on_cloud
            else "FROM system.query_log"
        )
        count = project.run_sql(
            f"SELECT count() {from_clause} WHERE query_id = '{query_id}'", fetch="one"
        )
        assert count[0] > 0, f"query_id {query_id!r} not found in system.query_log"
