import os

import pytest
from dbt.tests.util import run_dbt


SHARD_DB = 'dbt_clickhouse_local_naming_shards'

naming_default_sql = """
{{ config(
       materialized='distributed_incremental',
       incremental_strategy='insert_overwrite',
       partition_by='part',
       order_by='id',
   )
}}
{% if is_incremental() %}
select 100 + number as id, 2 as part, 2 as run from numbers(2)
{% else %}
select number as id, if(number < 2, 1, 2) as part, 1 as run from numbers(4)
{% endif %}
"""

naming_local_db_sql = """
{{ config(
       materialized='distributed_incremental',
       incremental_strategy='insert_overwrite',
       partition_by='part',
       order_by='id',
       local_db='__SHARD_DB__',
       local_suffix='',
   )
}}
{% if is_incremental() %}
select 100 + number as id, 2 as part, 2 as run from numbers(2)
{% else %}
select number as id, if(number < 2, 1, 2) as part, 1 as run from numbers(4)
{% endif %}
"""

naming_local_db_table_sql = """
{{ config(
       materialized='distributed_table',
       order_by='id',
       local_db='__SHARD_DB__',
       local_suffix='',
   )
}}
select number as id from numbers(3)
"""

naming_schema_change_sql = """
{{ config(
       materialized='distributed_incremental',
       incremental_strategy='append',
       on_schema_change='append_new_columns',
       order_by='id',
       local_db='__SHARD_DB__',
       local_suffix='',
   )
}}
{% if is_incremental() %}
select number as id, 'added' as extra from numbers(3, 2)
{% else %}
select number as id from numbers(3)
{% endif %}"""

naming_collision_sql = """
{{ config(materialized='distributed_incremental', order_by='id', local_suffix='') }}
select number as id from numbers(3)
"""

no_cluster = pytest.mark.skipif(
    os.environ.get("DBT_CH_TEST_CLUSTER", "").strip() == "", reason="Not on a cluster"
)


def assert_distributed_over(project, model_schema, model, local_schema, local_table):
    ddl = project.run_sql(f"show create table {model_schema}.{model}", fetch="one")[0]
    engine = (
        f"ENGINE = Distributed('{project.test_config['cluster']}', "
        f"'{local_schema}', '{local_table}', rand())"
    )
    assert engine in ddl, f"expected {engine!r} in:\n{ddl}"


def table_exists(project, schema, name) -> bool:
    return project.run_sql(f"exists table {schema}.{name}", fetch="one")[0] == 1


def columns_of(project, schema, name) -> set:
    rows = project.run_sql(
        f"select name from system.columns where database = '{schema}' and table = '{name}'",
        fetch="all",
    )
    return {row[0] for row in rows}


def partitions_of(project, model) -> dict:
    rows = project.run_sql(f"select part, max(run) from {model} group by part", fetch="all")
    return {row[0]: row[1] for row in rows}


def apply_shard_db(sql: str) -> str:
    return sql.replace("__SHARD_DB__", SHARD_DB)


@no_cluster
class TestDistributedLocalNaming:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "naming_default.sql": naming_default_sql,
            "naming_local_db.sql": apply_shard_db(naming_local_db_sql),
            "naming_local_db_table.sql": apply_shard_db(naming_local_db_table_sql),
            "naming_schema_change.sql": apply_shard_db(naming_schema_change_sql),
            "naming_collision.sql": naming_collision_sql
        }

    @pytest.fixture(scope="class", autouse=True)
    def drop_shard_database(self, project):
        yield
        cluster = project.test_config['cluster']
        on_cluster = f" on cluster {cluster}" if cluster else ""
        project.run_sql(f"drop database if exists {SHARD_DB}{on_cluster} sync")

    def test_default_naming(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_default"])
        run_dbt(["run", "--select", "naming_default"])

        assert_distributed_over(project, schema, "naming_default", schema, "naming_default_local")
        assert partitions_of(project, "naming_default") == {1: 1, 2: 2}

    def test_local_db_moves_the_local_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_local_db"])
        run_dbt(["run", "--select", "naming_local_db"])

        assert_distributed_over(project, schema, "naming_local_db", SHARD_DB, "naming_local_db")
        assert not table_exists(project, schema, "naming_local_db_local")
        assert partitions_of(project, "naming_local_db") == {1: 1, 2: 2}

    def test_local_db_for_distributed_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_local_db_table"])

        assert_distributed_over(
            project, schema, "naming_local_db_table", SHARD_DB, "naming_local_db_table"
        )
        assert not table_exists(project, schema, "naming_local_db_table_local")
        assert project.run_sql("select count() from naming_local_db_table", fetch="one")[0] == 3

    def test_schema_change_reaches_the_moved_local_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_schema_change"])
        run_dbt(["run", "--select", "naming_schema_change"])

        assert "extra" in columns_of(project, SHARD_DB, "naming_schema_change")
        assert "extra" in columns_of(project, schema, "naming_schema_change")
        assert project.run_sql("select count() from naming_schema_change", fetch="one")[0] == 5

    def test_collision_fails_before_any_ddl(self, project):
        result = run_dbt(["run", "--select", "naming_collision"], expect_pass=False)

        assert "resolves to the Distributed table itself" in result[0].message
        assert not table_exists(project, project.test_schema, "naming_collision")
