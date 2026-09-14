import os

import pytest
from dbt.tests.util import run_dbt


def assert_column_codec(project, model):
    is_distributed = "distributed" in model
    relation = f"{model}_local" if is_distributed else model
    ddl = project.run_sql(f"SHOW CREATE TABLE {project.test_schema}.{relation}", fetch="one")[0]
    assert "CODEC" in ddl
    assert ("LZ4" if is_distributed else "ZSTD") in ddl

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

        assert_column_codec(project, model)


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

        assert_column_codec(project, model)

        result_types = project.run_sql(
            f"select toColumnTypeName(col_1) from {model} limit 1", fetch="one"
        )
        assert "Float32" in result_types[0]


distributed_table_codec_sql = """
{{
    config(materialized='distributed_table')
}}
select
    number as col_1,
    number + 1 as col_2
from numbers(3)
"""

distributed_table_codec_yml = """
version: 2
models:
  - name: dist_table_rebuild_codec
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
        codec: LZ4
"""


class TestDistributedTableRebuildWithCodec:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_table_rebuild_codec.sql": distributed_table_codec_sql,
            "schema.yml": distributed_table_codec_yml,
        }

    def test_codec_survives_rebuild(self, project):
        if os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", "dist_table_rebuild_codec"])
        run_dbt(["run", "--select", "dist_table_rebuild_codec"])

        ddl = project.run_sql(
            f"SHOW CREATE TABLE {project.test_schema}.dist_table_rebuild_codec_local", fetch="one"
        )[0]
        assert "CODEC" in ddl
        assert "LZ4" in ddl
