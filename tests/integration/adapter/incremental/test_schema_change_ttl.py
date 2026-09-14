import os

import pytest
from dbt.tests.util import run_dbt


def assert_column_ttl(project, model):
    is_distributed = "distributed" in model
    relation = f"{model}_local" if is_distributed else model
    ddl = project.run_sql(f"SHOW CREATE TABLE {project.test_schema}.{relation}", fetch="one")[0]
    assert "TTL" in ddl
    assert ("toIntervalDay(60)" if is_distributed else "toIntervalDay(30)") in ddl

    if is_distributed:
        distributed_ddl = project.run_sql(
            f"SHOW CREATE TABLE {project.test_schema}.{model}", fetch="one"
        )[0]
        assert "TTL" not in distributed_ddl


schema_change_with_ttl_sql = """
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
    number + 1 as col_2,
    toDate('2020-01-01') as event_date
from numbers(3)
{%% else %%}
select
    number as col_1,
    number + 1 as col_2,
    number + 2 as col_3,
    toDate('2020-01-01') as event_date
from numbers(2, 3)
{%% endif %%}
"""


schema_change_with_ttl_yml = """
version: 2
models:
  - name: schema_change_ttl_append
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: event_date
        data_type: Date
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: schema_change_ttl_distributed_append
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: event_date
        data_type: Date
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
"""


class TestSchemaChangeWithTTL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema_change_ttl_append.sql": schema_change_with_ttl_sql
            % ("incremental", "append_new_columns"),
            "schema_change_ttl_distributed_append.sql": schema_change_with_ttl_sql
            % ("distributed_incremental", "append_new_columns"),
            "schema.yml": schema_change_with_ttl_yml,
        }

    @pytest.mark.parametrize(
        "model", ("schema_change_ttl_append", "schema_change_ttl_distributed_append")
    )
    def test_append_with_ttl(self, project, model):
        is_distributed = "distributed" in model
        if is_distributed and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3

        run_dbt(["--debug", "run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 4 for row in result)

        assert_column_ttl(project, model)


sync_all_columns_with_ttl_sql = """
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
    number + 1 as col_2,
    toDate('2020-01-01') as event_date
from numbers(3)
{%% else %%}
select
    toFloat32(number) as col_1,
    number + 2 as col_3,
    toDate('2020-01-01') as event_date
from numbers(2, 3)
{%% endif %%}
"""

sync_all_columns_with_ttl_yml = """
version: 2
models:
  - name: sync_ttl_test
    columns:
      - name: col_1
        data_type: Float32
      - name: event_date
        data_type: Date
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: sync_ttl_distributed_test
    columns:
      - name: col_1
        data_type: Float32
      - name: event_date
        data_type: Date
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
"""


class TestSyncAllColumnsWithTTL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sync_ttl_test.sql": sync_all_columns_with_ttl_sql % "incremental",
            "sync_ttl_distributed_test.sql": sync_all_columns_with_ttl_sql
            % "distributed_incremental",
            "schema.yml": sync_all_columns_with_ttl_yml,
        }

    @pytest.mark.parametrize("model", ("sync_ttl_test", "sync_ttl_distributed_test"))
    def test_sync_all_columns_with_ttl(self, project, model):
        is_distributed = "distributed" in model
        if is_distributed and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3

        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 3 for row in result)

        assert_column_ttl(project, model)

        result_types = project.run_sql(
            f"select toColumnTypeName(col_1) from {model} limit 1", fetch="one"
        )
        assert "Float32" in result_types[0]


distributed_table_ttl_sql = """
{{
    config(
        materialized='distributed_table',
        contract={'enforced': true},
    )
}}
select
    number as col_1,
    toDate('2020-01-01') as event_date
from numbers(3)
"""

distributed_table_ttl_yml = """
version: 2
models:
  - name: dist_table_rebuild_ttl
    columns:
      - name: col_1
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
      - name: event_date
        data_type: Date
"""


class TestDistributedTableRebuildWithTTL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_table_rebuild_ttl.sql": distributed_table_ttl_sql,
            "schema.yml": distributed_table_ttl_yml,
        }

    def test_ttl_survives_rebuild(self, project):
        if os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", "dist_table_rebuild_ttl"])
        run_dbt(["run", "--select", "dist_table_rebuild_ttl"])

        ddl = project.run_sql(
            f"SHOW CREATE TABLE {project.test_schema}.dist_table_rebuild_ttl_local", fetch="one"
        )[0]
        assert "TTL" in ddl
        assert "toIntervalDay(60)" in ddl
