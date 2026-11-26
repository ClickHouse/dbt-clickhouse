import os

import pytest
from dbt.tests.util import run_dbt, write_file


def assert_column_ttl(project, model, applied=True):
    is_distributed = "distributed" in model
    relation = f"{model}_local" if is_distributed else model
    ddl = project.run_sql(f"SHOW CREATE TABLE {project.test_schema}.{relation}", fetch="one")[0]
    if not applied:
        assert "TTL" not in ddl
        return
    assert "TTL" in ddl
    assert ("toIntervalDay(60)" if is_distributed else "toIntervalDay(30)") in ddl


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


schema_change_with_ttl_first_yml = """
version: 2
models:
  - name: schema_change_ttl_append
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: event_date
        data_type: Date
  - name: schema_change_ttl_distributed_append
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: event_date
        data_type: Date
  - name: schema_change_ttl_append_no_contract
    columns:
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: schema_change_ttl_distributed_append_no_contract
    columns:
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
"""

schema_change_with_ttl_second_yml = """
version: 2
models:
  - name: schema_change_ttl_append
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
      - name: event_date
        data_type: Date
  - name: schema_change_ttl_distributed_append
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_2
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
      - name: event_date
        data_type: Date
  - name: schema_change_ttl_append_no_contract
    columns:
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: schema_change_ttl_distributed_append_no_contract
    columns:
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
            "schema_change_ttl_append_no_contract.sql": schema_change_with_ttl_sql
            % ("incremental", "append_new_columns"),
            "schema_change_ttl_distributed_append_no_contract.sql": schema_change_with_ttl_sql
            % ("distributed_incremental", "append_new_columns"),
            "schema.yml": schema_change_with_ttl_first_yml,
        }

    @pytest.mark.parametrize(
        "model",
        (
            "schema_change_ttl_append",
            "schema_change_ttl_distributed_append",
            "schema_change_ttl_append_no_contract",
            "schema_change_ttl_distributed_append_no_contract",
        ),
    )
    def test_append_with_ttl(self, project, model):
        if "distributed" in model and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        write_file(schema_change_with_ttl_first_yml, project.project_root, "models", "schema.yml")
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3

        write_file(schema_change_with_ttl_second_yml, project.project_root, "models", "schema.yml")
        run_dbt(["--debug", "run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 4 for row in result)

        assert_column_ttl(project, model, applied="no_contract" not in model)


ttl_references_later_new_column_sql = """
{{
    config(
        materialized='%s',
        unique_key='col_1',
        on_schema_change='append_new_columns'
    )
}}

{%% if not is_incremental() %%}
select
    number as col_1
from numbers(3)
{%% else %%}
select
    number as col_1,
    number + 2 as col_3,
    toDate('2020-01-01') as event_date
from numbers(2, 3)
{%% endif %%}
"""

ttl_references_later_new_column_first_yml = """
version: 2
models:
  - name: ttl_later_column
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
  - name: ttl_later_column_distributed
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
"""

ttl_references_later_new_column_second_yml = """
version: 2
models:
  - name: ttl_later_column
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
      - name: event_date
        data_type: Date
  - name: ttl_later_column_distributed
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
      - name: event_date
        data_type: Date
"""


class TestAppendTTLReferencingLaterNewColumn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ttl_later_column.sql": ttl_references_later_new_column_sql % "incremental",
            "ttl_later_column_distributed.sql": ttl_references_later_new_column_sql
            % "distributed_incremental",
            "schema.yml": ttl_references_later_new_column_first_yml,
        }

    @pytest.mark.parametrize("model", ("ttl_later_column", "ttl_later_column_distributed"))
    def test_append_ttl_referencing_later_column(self, project, model):
        if "distributed" in model and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        write_file(
            ttl_references_later_new_column_first_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["run", "--select", model])

        write_file(
            ttl_references_later_new_column_second_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 3 for row in result)

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

sync_all_columns_with_ttl_first_yml = """
version: 2
models:
  - name: sync_ttl_test
    columns:
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: sync_ttl_distributed_test
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt8
      - name: col_2
        data_type: UInt64
      - name: event_date
        data_type: Date
"""

sync_all_columns_with_ttl_second_yml = """
version: 2
models:
  - name: sync_ttl_test
    columns:
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(30)
  - name: sync_ttl_distributed_test
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: Float32
      - name: col_3
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
      - name: event_date
        data_type: Date
"""


class TestSyncAllColumnsWithTTL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sync_ttl_test.sql": sync_all_columns_with_ttl_sql % "incremental",
            "sync_ttl_distributed_test.sql": sync_all_columns_with_ttl_sql
            % "distributed_incremental",
            "schema.yml": sync_all_columns_with_ttl_first_yml,
        }

    @pytest.mark.parametrize("model", ("sync_ttl_test", "sync_ttl_distributed_test"))
    def test_sync_all_columns_with_ttl(self, project, model):
        is_distributed = "distributed" in model
        if is_distributed and os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        write_file(
            sync_all_columns_with_ttl_first_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert len(result) == 3

        write_file(
            sync_all_columns_with_ttl_second_yml, project.project_root, "models", "schema.yml"
        )
        run_dbt(["run", "--select", model])
        result = project.run_sql(f"select * from {model} order by col_1", fetch="all")
        assert all(len(row) == 3 for row in result)

        assert_column_ttl(project, model, applied=is_distributed)

        result_types = project.run_sql(
            f"select toColumnTypeName(col_1) from {model} limit 1", fetch="one"
        )
        assert "Float32" in result_types[0]


distributed_table_ttl_sql = """
{{
    config(materialized='distributed_table')
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
    config:
      contract:
        enforced: true
    columns:
      - name: col_1
        data_type: UInt64
        ttl: event_date + toIntervalDay(60)
      - name: event_date
        data_type: Date
  - name: dist_table_ttl_no_contract
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
            "dist_table_ttl_no_contract.sql": distributed_table_ttl_sql,
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

    def test_ttl_not_applied_without_contract(self, project):
        if os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '':
            pytest.skip("Not on a cluster")

        run_dbt(["run", "--select", "dist_table_ttl_no_contract"])

        ddl = project.run_sql(
            f"SHOW CREATE TABLE {project.test_schema}.dist_table_ttl_no_contract_local",
            fetch="one",
        )[0]
        assert "TTL" not in ddl
