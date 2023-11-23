import pytest
from dbt.tests.util import run_dbt_and_capture

my_model_sql_header_sql = """
{{
  config(
    materialized = "table",
  )
}}

{% call set_sql_header(config) %}
set log_comment = 'TEST_LOG_COMMENT';
{%- endcall %}
select getSettings('log_comment') as column_name
"""


class TestSQLHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_sql_header.sql": my_model_sql_header_sql,
        }

    def test__sql_header(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "my_model_sql_header"], expect_pass=False)

        assert 'Multi-statements' in log_output
