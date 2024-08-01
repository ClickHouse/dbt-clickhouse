import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

schema_change_sql = """
{{
    config(
        materialized='incremental',
        unique_key='col_1',
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
        run_dbt(["run", "--select", "schema_change_append"])
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["--debug", "run", "--select", "schema_change_append"])
        result = project.run_sql("select * from schema_change_append order by col_1", fetch="all")
        assert result[0][2] == 0
        assert result[3][2] == 5


# contains dropped, added, and (type) changed columns
complex_schema_change_sql = """
{{
    config(
        materialized='incremental',
        unique_key='col_1',
        on_schema_change='%schema_change%'
    )
}}

{% if not is_incremental() %}
select
    toUInt8(number) as col_1,
    number + 1 as col_2
from numbers(3)
{% else %}
select
    toFloat32(number) as col_1,
    number + 2 as col_3
from numbers(2, 3)
{% endif %}
"""


class TestComplexSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "complex_schema_change_fail.sql": complex_schema_change_sql.replace("%schema_change%", "fail"),
            "complex_schema_change_append.sql": complex_schema_change_sql.replace("%schema_change%",
                                                                                  "append_new_columns"),
            "complex_schema_change_sync.sql": complex_schema_change_sql.replace("%schema_change%", "sync_all_columns"),
        }

    def test_fail(self, project):
        run_dbt(["run", "--select", "complex_schema_change_fail"])
        result = project.run_sql("select * from complex_schema_change_fail order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        _, log_output = run_dbt_and_capture(
            [
                "run",
                "--select",
                "complex_schema_change_fail",
            ],
            expect_pass=False,
        )
        assert 'out of sync' in log_output.lower()

    def test_append(self, project):
        run_dbt(["run", "--select", "complex_schema_change_append"])
        result = project.run_sql("select * from complex_schema_change_append order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        _, log_output = run_dbt_and_capture(
            [
                "run",
                "--select",
                "complex_schema_change_append",
            ],
            expect_pass=False,
        )
        assert 'out of sync' in log_output.lower()

    def test_sync(self, project):
        run_dbt(["run", "--select", "complex_schema_change_sync"])
        result = project.run_sql("select * from complex_schema_change_sync order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["run", "--select", "complex_schema_change_sync"])
        result = project.run_sql("select * from complex_schema_change_sync order by col_1", fetch="all")
        result_types = project.run_sql("select toColumnTypeName(col_1) from complex_schema_change_sync", fetch="one")
        assert result_types[0] == 'Float32'
        assert result[0][1] == 0
        assert result[3][1] == 5
