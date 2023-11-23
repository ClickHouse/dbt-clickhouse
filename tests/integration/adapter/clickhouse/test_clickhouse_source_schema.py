import pytest
from dbt.tests.util import run_dbt

system_source_yml = """
version: 2
sources:
  - name: system_source
    database: system
    tables:
      - name: tables
        database: system
"""


class TestSourceSchema:
    @pytest.fixture(scope="class")
    def models(self):
        sys_tables_sql = """
                {{ config(order_by='(database, name)', engine='MergeTree()', materialized='table') }}

               select database, name, engine, total_rows from {{ source('system_source', 'tables') }}
              """
        return {
            "sys_tables.sql": sys_tables_sql,
            "sources.yml": system_source_yml,
        }

    def test_source_schema(self, project):
        results = run_dbt()
        assert len(results) > 0
