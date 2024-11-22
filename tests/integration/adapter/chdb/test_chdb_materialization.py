from pathlib import Path

import pytest
from dbt.tests.util import run_dbt

amount_from_taxis_sql = """
{{
  config(
    materialized = "table"
  )
}}

select sum(total_amount) as total_cost from taxis.trips

"""

amount_from_taxis_schema = """
version: 2

models:
  - name: amount_from_taxis
    description: "sum of taxis trips amounts"
    columns:
      - name: total_cost
        description: "sum of taxis trips amounts"

"""


class TestChdbMaterialization:
    @pytest.fixture(scope="class")
    def profiles_config_update(self):
        return {
            "test": {
                "outputs": {
                    "chdb-test": {
                        "type": "clickhouse",
                        "driver": "chdb",
                        "chdb_state_dir": "chdb_state",
                        # This is a temporary measure to make sure it works for everyone running the test.
                        # Linked to the improvement needed on dbt/adapters/clickhouse/chdbclient.py L99
                        "chdb_dump_dir": str(
                            Path(__file__).resolve().parent.parent.parent.parent.parent
                            / "examples"
                            / "taxis"
                            / "dump"
                        ),
                        "threads": 1,
                        "schema": "taxis_dbt",
                    }
                },
                "target": "chdb-test",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": amount_from_taxis_schema,
            "amount_from_taxis.sql": amount_from_taxis_sql,
        }

    def test_chdb_base(self, project):
        res = run_dbt()
        assert len(res) > 0
