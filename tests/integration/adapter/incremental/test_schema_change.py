from functools import reduce
import os

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

schema_change_sql = """
{{
    config(
        materialized='%s',
        unique_key='col_1',
        on_schema_change='%s'
    )
}}

{%% if not is_incremental() %%}
select
    number as col_1,
    number + 1 as col_2
from numbers(3)
{%% else %%}
select
    number as col_1,
    number + 1 as col_2,
    number + 2 as col_3
from numbers(2, 3)
{%% endif %%}
"""


class TestOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_change_ignore.sql": schema_change_sql % ("incremental", "ignore"),
            "schema_change_fail.sql": schema_change_sql % ("incremental", "fail"),
            "schema_change_append.sql": schema_change_sql % ("incremental", "append_new_columns"),
            "schema_change_distributed_ignore.sql": schema_change_sql
            % ("distributed_incremental", "ignore"),
            "schema_change_distributed_fail.sql": schema_change_sql
            % ("distributed_incremental", "fail"),
            "schema_change_distributed_append.sql": schema_change_sql
            % ("distributed_incremental", "append_new_columns"),
        }

    @pytest.mark.parametrize("model", ("schema_change_ignore", "schema_change_distributed_ignore"))
    def test_ignore(self, project, model):
        if (
            model == "schema_change_distributed_ignore"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model}", fetch="all")
        assert len(result) == 5

    @pytest.mark.parametrize("model", ("schema_change_fail", "schema_change_distributed_fail"))
    def test_fail(self, project, model):
        if (
            model == "schema_change_distributed_fail"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        _, log_output = run_dbt_and_capture(
            [
                "run",
                "--select",
                model,
            ],
            expect_pass=False,
        )
        assert 'out of sync' in log_output.lower()

    @pytest.mark.parametrize("model", ("schema_change_append", "schema_change_distributed_append"))
    def test_append(self, project, model):
        if (
            model == "schema_change_distributed_append"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["--debug", "run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert result[0][2] == 0
        assert result[3][2] == 5


# contains dropped, added, and changed (type) columns
complex_schema_change_sql = """
{{
    config(
        materialized='%s',
        unique_key='col_1',
        on_schema_change='%s'
    )
}}

{%% if not is_incremental() %%}
select
    toUInt8(number) as col_1,
    number + 1 as col_2
from numbers(3)
{%% else %%}
select
    toFloat32(number) as col_1,
    number + 2 as col_3
from numbers(2, 3)
{%% endif %%}
"""


class TestComplexSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "complex_schema_change_fail.sql": complex_schema_change_sql % ("incremental", "fail"),
            "complex_schema_change_append.sql": complex_schema_change_sql
            % ("incremental", "append_new_columns"),
            "complex_schema_change_sync.sql": complex_schema_change_sql
            % ("incremental", "sync_all_columns"),
            "complex_schema_change_distributed_fail.sql": complex_schema_change_sql
            % ("distributed_incremental", "fail"),
            "complex_schema_change_distributed_append.sql": complex_schema_change_sql
            % ("distributed_incremental", "append_new_columns"),
            "complex_schema_change_distributed_sync.sql": complex_schema_change_sql
            % ("distributed_incremental", "sync_all_columns"),
        }

    @pytest.mark.parametrize(
        "model",
        (
            "complex_schema_change_fail",
            "complex_schema_change_distributed_fail",
            "complex_schema_change_append",
            "complex_schema_change_distributed_append",
        ),
    )
    def test_fail(self, project, model):
        if "distributed" in model and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        _, log_output = run_dbt_and_capture(
            [
                "run",
                "--select",
                model,
            ],
            expect_pass=False,
        )
        assert 'out of sync' in log_output.lower()

    @pytest.mark.parametrize(
        "model", ("complex_schema_change_sync", "complex_schema_change_distributed_sync")
    )
    def test_sync(self, project, model):
        if (
            model == "complex_schema_change_distributed_sync"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 2 for row in result)
        assert result[0][1] == 0
        assert result[3][1] == 5
        result_types = project.run_sql(f"select toColumnTypeName(col_1) from {model}", fetch="one")
        assert result_types[0] == 'Float32'


out_of_order_columns_sql = """
{{
    config(
        materialized='%s',
        unique_key='col_1',
        on_schema_change='fail'
    )
}}

{%% if not is_incremental() %%}
select
    number as col_1,
    number + 1 as col_2
from numbers(3)
{%% else %%}
select
    number + 1 as col_2,
    number as col_1
from numbers(2, 3)
{%% endif %%}
"""


class TestReordering:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "out_of_order_columns.sql": out_of_order_columns_sql % "incremental",
            "out_of_order_columns_distributed.sql": out_of_order_columns_sql
            % "distributed_incremental",
        }

    @pytest.mark.parametrize("model", ("out_of_order_columns", "out_of_order_columns_distributed"))
    def test_reordering(self, project, model):
        if (
            model == "out_of_order_columns_distributed"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert result[0][1] == 1
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert result[0][1] == 1
        assert result[3][1] == 4
