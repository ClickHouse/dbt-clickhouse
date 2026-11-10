import pytest
from dbt.tests.util import run_dbt

# The limit is only checked when allocations are flushed to the memory tracker, so the query has
# to allocate far more than the 4 MiB untracked threshold. A small query could finish and trip
# the limit while the HTTP response was being finalised, which the server reports by breaking
# the chunked body (seen as IncompleteRead in CI).
oom_table_sql = """
select uniqExact(number) as a from numbers(10000000) SETTINGS max_memory_usage = 1
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
