"""
Contract validation must apply model query_settings when introspecting the query
schema (e.g. join_use_nulls so LEFT JOIN nulls are Nullable).
"""

import pytest
from dbt.tests.util import run_dbt

CONTRACT_QUERY_SETTINGS_MODEL = """
{{
    config(
        order_by='id',
        query_settings={
            'join_use_nulls': 1
        }
    )
}}

select
    id,
    last_value
from (
    select 1 as id
) t1
left join (
    select
        1 as id,
        max(value) as last_value
    from (
        select 1 as value
    )
    where value = 2
) t2
    on t1.id = t2.id
"""

CONTRACT_QUERY_SETTINGS_SCHEMA = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: UInt8
      - name: last_value
        data_type: Nullable(UInt8)
"""


class TestContractRespectsQuerySettings:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": CONTRACT_QUERY_SETTINGS_MODEL,
            "schema.yml": CONTRACT_QUERY_SETTINGS_SCHEMA,
        }

    def test_contract_passes_with_join_use_nulls_query_setting(self, project):
        """Without query_settings on schema introspection, last_value would be UInt8
        (default join_use_nulls=0) and contract would fail against Nullable(UInt8)."""
        results = run_dbt(["run", "--select", "my_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Runtime data should also use Nullable for the left-join null
        result = project.run_sql(
            "select isNullable(last_value) from my_model",
            fetch="one",
        )
        assert result[0] == 1
