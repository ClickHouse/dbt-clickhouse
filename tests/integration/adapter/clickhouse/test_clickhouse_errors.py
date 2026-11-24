import pytest
from dbt.tests.util import run_dbt

# throwIf depends on the row value, so the error is raised while executing the insert.
error_table_sql = """
select throwIf(number = 5, 'dbt test error') as a from numbers(10)
"""

schema_yaml = """
version: 2

models:
  - name: error_table
    description: Table that raises a server error
    config:
      materialized: table
      order_by: a
"""


class TestServerError:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_yaml,
            "error_table.sql": error_table_sql,
        }

    def test_server_error(self, project):
        res = run_dbt(["run"], expect_pass=False)
        assert 'dbt test error' in res.results[0].message
