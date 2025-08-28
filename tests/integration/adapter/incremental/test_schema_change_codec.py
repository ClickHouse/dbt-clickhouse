import os

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

schema_change_with_codec_sql = """
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


schema_change_with_codec_yml = """
version: 2
models:
  - name: schema_change_codec_append
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        codec: ZSTD
  - name: schema_change_codec_distributed_append
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        codec: LZ4
"""


class TestSchemaChangeWithCodec:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_change_codec_append.sql": schema_change_with_codec_sql
            % ("incremental", "append_new_columns"),
            "schema_change_codec_distributed_append.sql": schema_change_with_codec_sql
            % ("distributed_incremental", "append_new_columns"),
            "schema.yml": schema_change_with_codec_yml,
        }

    @pytest.mark.parametrize(
        "model", ("schema_change_codec_append", "schema_change_codec_distributed_append")
    )
    def test_append_with_codec(self, project, model):
        if (
            model == "schema_change_codec_distributed_append"
            and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == ''
        ):
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3
        assert result[0][1] == 1

        run_dbt(["--debug", "run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")

        assert all(len(row) == 3 for row in result)
        assert result[0][2] == 0
        assert result[3][2] == 5

        table_name = f"{project.test_schema}.{model}"
        create_table_sql = project.run_sql(f"SHOW CREATE TABLE {table_name}", fetch="one")[0]

        assert "CODEC" in create_table_sql
        if "distributed" in model:
            assert "LZ4" in create_table_sql
        else:
            assert "ZSTD" in create_table_sql


sync_all_columns_with_codec_sql = """
{{
    config(
        materialized='%s',
        unique_key='col_1',
        on_schema_change='sync_all_columns'
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

sync_all_columns_with_codec_yml = """
version: 2
models:
  - name: sync_codec_test
    columns:
      - name: col_1
        data_type: Float32
      - name: col_3
        data_type: UInt64
        codec: ZSTD
  - name: sync_codec_distributed_test
    columns:
      - name: col_1
        data_type: Float32
      - name: col_3
        data_type: UInt64
        codec: LZ4
"""


class TestSyncAllColumnsWithCodec:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sync_codec_test.sql": sync_all_columns_with_codec_sql % "incremental",
            "sync_codec_distributed_test.sql": sync_all_columns_with_codec_sql
            % "distributed_incremental",
            "schema.yml": sync_all_columns_with_codec_yml,
        }

    @pytest.mark.parametrize("model", ("sync_codec_test", "sync_codec_distributed_test"))
    def test_sync_all_columns_with_codec(self, project, model):
        if (
            model == "sync_codec_distributed_test"
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

        table_name = f"{project.test_schema}.{model}"
        create_table_sql = project.run_sql(f"SHOW CREATE TABLE {table_name}", fetch="one")[0]

        assert "CODEC" in create_table_sql
        if "distributed" in model:
            assert "LZ4" in create_table_sql
        else:
            assert "ZSTD" in create_table_sql

        result_types = project.run_sql(
            f"select toColumnTypeName(col_1) from {model} limit 1", fetch="one"
        )
        assert "Float32" in result_types[0]
