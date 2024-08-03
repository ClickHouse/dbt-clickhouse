import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

schema_change_sql = """
{{
    config(
        materialized='incremental',
        unique_key='col_1' + var('extra_unique_keys',''),
        on_schema_change='%schema_change%'
    )
}}

{% if not is_incremental() %}
select
    number as col_1,
    number + 1 as col_2
from numbers(3)
{% else %}
select
    number as col_1,
    number + 1 as col_2,
    number + 2 as col_3
from numbers(2, 3)
{% endif %}
"""


class TestOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_change_ignore.sql": schema_change_sql.replace("%schema_change%", "ignore"),
            "schema_change_fail.sql": schema_change_sql.replace("%schema_change%", "fail"),
            "schema_change_append.sql": schema_change_sql.replace(
                "%schema_change%", "append_new_columns"
            ),
        }

    def test_ignore(self, project):
        run_dbt(["run", "--select", "schema_change_ignore"])
        result = project.run_sql("select * from schema_change_ignore order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["run", "--select", "schema_change_ignore"])
        result = project.run_sql("select * from schema_change_ignore", fetch="all")
        assert len(result) == 5

    def test_fail(self, project):
        run_dbt(["run", "--select", "schema_change_fail"])
        result = project.run_sql("select * from schema_change_fail order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        _, log_output = run_dbt_and_capture(
            [
                "run",
                "--select",
                "schema_change_fail",
            ],
            expect_pass=False,
        )
        assert 'out of sync' in log_output.lower()

    def test_append(self, project):
        run_dbt(["run", "--full-refresh", "--select", "schema_change_append"])
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["--debug", "run", "--select", "schema_change_append"])
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert result[0][2] == 0
        assert result[3][2] == 5

    def test_append_unique_key(self, project):
        run_dbt(["run", "--full-refresh", "--select", "schema_change_append"])
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(
            [
                "--debug",
                "run",
                "--select",
                "schema_change_append",
                "--vars",
                '{"extra_unique_keys": ",col_3"}',
            ]
        )
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert result[0][2] == 0
        assert result[3][2] == 5
