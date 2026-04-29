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

        ch = get_client(
            host=test_config["host"],
            port=test_config.get("client_port", 8123),
            username=test_config["user"],
            password=test_config["password"],
            secure=test_config["secure"],
        )
        ch.command("SYSTEM FLUSH LOGS")

        count = ch.command(f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}'")
        assert count > 0, f"query_id {query_id!r} not found in system.query_log"
