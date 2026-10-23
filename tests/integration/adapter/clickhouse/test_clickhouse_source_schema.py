import os

import pytest
from dbt.tests.util import run_dbt

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))

# dbt core v2 does not map a source `database` onto the ClickHouse database: there the key is `schema`.
system_source_yml = f"""
version: 2
sources:
  - name: system_source
    {'schema' if IS_CORE_V2 else 'database'}: system
    tables:
      - name: tables
"""


class TestSourceSchema:
    @pytest.fixture(scope="class")
    def models(self):
        sys_tables_sql = """
                {{ config(order_by='(database, name)',
                 engine='MergeTree()',
                  materialized='table',
                  settings={'allow_nullable_key': 1}) }}

               select database, name, engine, total_rows from {{ source('system_source', 'tables') }}
              """
        return {
            "sys_tables.sql": sys_tables_sql,
            "sources.yml": system_source_yml,
        }

    def test_source_schema(self, project):
        results = run_dbt()
        assert len(results) > 0
