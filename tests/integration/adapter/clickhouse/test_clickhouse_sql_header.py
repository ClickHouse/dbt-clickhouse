import os

import pytest
from dbt.tests.util import run_dbt_and_capture

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))

my_model_sql_header_sql = """
{{
  config(
    materialized = "table",
  )
}}

{% call set_sql_header(config) %}
set log_comment = 'TEST_LOG_COMMENT';
{%- endcall %}
select getSetting('log_comment') as column_name
"""


class TestSQLHeader:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_sql_header.sql": my_model_sql_header_sql,
        }

    def test__sql_header(self, project):
        if IS_CORE_V2:
            # dbt core v2 splits the rendered SQL and runs the header as its own
            # statement, so the model builds instead of tripping the driver's
            # multi-statement guard.
            results, log_output = run_dbt_and_capture(["run", "-s", "my_model_sql_header"])
            assert len(results) == 1
            assert 'Multi-statements' not in log_output
            return

        _, log_output = run_dbt_and_capture(["run", "-s", "my_model_sql_header"], expect_pass=False)

        assert 'Multi-statements' in log_output
