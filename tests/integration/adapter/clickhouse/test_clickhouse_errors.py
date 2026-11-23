import pytest
from dbt.tests.util import run_dbt

# The limit must be high enough for creating the table and low enough for the insert to fail.
# A limit of 1 byte sometimes broke the HTTP response while creating the table on a cluster.
oom_table_sql = """
select uniqExact(number) as a from numbers(10000000) SETTINGS max_memory_usage = 50000000
"""

schema_yaml = """
version: 2

models:
  - name: oom_table
    description: Table that generates OOM
    config:
      materialized: table
      order_by: a
"""


class TestOOMError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yaml,
            "oom_table.sql": oom_table_sql,
        }

    def test_oom(self, project):
        res = run_dbt(["run"], expect_pass=False)
        assert 'exceeded' in res.results[0].message
