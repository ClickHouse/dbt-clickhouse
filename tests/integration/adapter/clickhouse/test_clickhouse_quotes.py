import pytest
from dbt.tests.util import run_dbt

schema_yaml = """
version: 2

sources:
  - name: t01
    schema: "a6bf4b3f-17ee-4ae3-b769-acc2ffe0d708"
    tables: &tables
      - name: the_table
      - name: the_other_table
    quoting: &quoting
      database: true
      schema: true
      identifier: true

  - name: t02
    schema: "6410659e-85c7-413b-b952-aca7fd639788"
    tables: *tables
    quoting: *quoting
"""

model_sql = """
{{
    config({
        "materialized": "table",
        "engine": "MergeTree()",
        "schema": "dev",
        "order_by": '(id)'
    })
}}
WITH cte AS (
    SELECT * FROM {{ source('t01', 'the_table') }} UNION ALL
    SELECT * FROM {{ source('t02', 'the_table') }}
)
SELECT * FROM cte
"""


class TestClickHouseIdentifiers:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sys_tables.sql": model_sql,
            "schema.yml": schema_yaml,
        }

    def test_read(self, project):
        run_dbt()
