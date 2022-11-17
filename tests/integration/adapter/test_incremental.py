import pytest
from dbt.tests.util import run_dbt

uniq_schema = """
version: 2

models:
  - name: "unique_source_one"
    description: "Test table source"
    columns:
      - name: ts
      - name: impid
      - name: value1
"""

uniq_source_model = """
{{config(
        materialized='table',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid']
    )
}}
SELECT now() - toIntervalHour(number) as ts, toInt32(number) as impid, concat('value', toString(number)) as value1
  FROM numbers(100)
"""

uniq_incremental_model = """
{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid']
    )
}}
select ts, impid from unique_source_one
{% if is_incremental() %}
where ts >= now() - toIntervalHour(1)
{% endif %}
"""


class TestSimpleIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unique_source_one.sql": uniq_source_model,
            "unique_incremental_one.sql": uniq_incremental_model,
            "schema.yml": uniq_schema,
        }

    def test_simple_incremental(self, project):
        run_dbt(["run", "--select", "unique_source_one"])
        run_dbt(["run", "--select", "unique_incremental_one"])
