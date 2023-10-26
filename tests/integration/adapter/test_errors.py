import pytest
from dbt.tests.util import run_dbt

oom_table_sql = """
SELECT a FROM system.numbers_mt GROUP BY repeat(toString(number), 100000) as a
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
