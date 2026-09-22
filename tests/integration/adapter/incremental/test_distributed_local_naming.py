import os

import pytest
from dbt.tests.util import run_dbt

SHARD_DB = 'dbt_clickhouse_local_naming_shards'
OTHER_SHARD_DB = 'dbt_clickhouse_local_naming_shards_2'
GUARD_SHARD_DB = 'dbt_clickhouse_local_naming_shards_guard'

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
       local_db=var('shard_db'),
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
       local_db=var('shard_db'),
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
       local_db=var('shard_db'),
       local_suffix='',
   )
}}
{% if is_incremental() %}
select number as id, 'added' as extra from numbers(3, 2)
{% else %}
select number as id from numbers(3)
{% endif %}"""

naming_switch_sql = """
{{ config(
       materialized='distributed_incremental',
       incremental_strategy='append',
       order_by='id',
       local_db=var('shard_db'),
       local_suffix='',
   )
}}
select number as id, {{ var('run_no') }} as run from numbers(3)
"""

naming_mv_source_sql = """
{{ config(materialized='distributed_incremental', incremental_strategy='append', order_by='id') }}
select number as id from numbers(3)
"""

naming_incremental_switch_sql = """
{{ config(
       materialized='distributed_incremental',
       incremental_strategy='append',
       order_by='id',
       local_db=var('shard_db'),
       local_suffix='',
   )
}}
{% if is_incremental() %}
select 100 + number as id from numbers(2)
{% else %}
select number as id from numbers(3)
{% endif %}
"""

naming_table_switch_sql = """
{{ config(materialized='distributed_table', order_by='id', local_db=var('shard_db'), local_suffix='') }}
select number as id from numbers(3)
"""

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


def database_exists(project, name) -> bool:
    return project.run_sql(f"exists database {name}", fetch="one")[0] == 1


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


@no_cluster
class TestDistributedLocalNaming:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "naming_default.sql": naming_default_sql,
            "naming_local_db.sql": naming_local_db_sql,
            "naming_local_db_table.sql": naming_local_db_table_sql,
            "naming_schema_change.sql": naming_schema_change_sql,
            "naming_switch.sql": naming_switch_sql,
            "naming_mv_source.sql": naming_mv_source_sql,
            "naming_incremental_switch.sql": naming_incremental_switch_sql,
            "naming_table_switch.sql": naming_table_switch_sql,
            "naming_collision.sql": naming_collision_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def drop_shard_database(self, project):
        yield
        cluster = project.test_config['cluster']
        on_cluster = f" on cluster {cluster}" if cluster else ""
        for db in (SHARD_DB, OTHER_SHARD_DB, GUARD_SHARD_DB):
            project.run_sql(f"drop database if exists {db}{on_cluster} sync")

    def test_default_naming(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_default"])
        run_dbt(["run", "--select", "naming_default"])

        assert_distributed_over(project, schema, "naming_default", schema, "naming_default_local")
        assert partitions_of(project, "naming_default") == {1: 1, 2: 2}

    def test_local_db_moves_the_local_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_local_db", "--vars", f"{{shard_db: {SHARD_DB}}}"])
        run_dbt(["run", "--select", "naming_local_db", "--vars", f"{{shard_db: {SHARD_DB}}}"])

        assert_distributed_over(project, schema, "naming_local_db", SHARD_DB, "naming_local_db")
        assert not table_exists(project, schema, "naming_local_db_local")
        assert partitions_of(project, "naming_local_db") == {1: 1, 2: 2}

    def test_local_db_for_distributed_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_local_db_table", "--vars", f"{{shard_db: {SHARD_DB}}}"])

        assert_distributed_over(
            project, schema, "naming_local_db_table", SHARD_DB, "naming_local_db_table"
        )
        assert not table_exists(project, schema, "naming_local_db_table_local")
        assert project.run_sql("select count() from naming_local_db_table", fetch="one")[0] == 3

    def test_schema_change_reaches_the_moved_local_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_schema_change", "--vars", f"{{shard_db: {SHARD_DB}}}"])
        run_dbt(["run", "--select", "naming_schema_change", "--vars", f"{{shard_db: {SHARD_DB}}}"])

        assert "extra" in columns_of(project, SHARD_DB, "naming_schema_change")
        assert "extra" in columns_of(project, schema, "naming_schema_change")
        assert project.run_sql("select count() from naming_schema_change", fetch="one")[0] == 5

    def test_switching_local_db_back_rebuilds_the_distributed_table(self, project):
        schema = project.test_schema

        def run(db, run_no, full_refresh=False):
            args = [
                "run",
                "--select",
                "naming_switch",
                "--vars",
                f"{{shard_db: {db}, run_no: {run_no}}}",
            ]
            run_dbt(args + ["--full-refresh"] if full_refresh else args)

        run(SHARD_DB, 1)
        run(OTHER_SHARD_DB, 2, full_refresh=True)
        run(SHARD_DB, 3)

        assert_distributed_over(project, schema, "naming_switch", SHARD_DB, "naming_switch")
        runs = project.run_sql(
            "select arraySort(groupUniqArray(run)) from naming_switch", fetch="one"
        )[0]
        assert runs == [1, 3]
        assert table_exists(project, OTHER_SHARD_DB, "naming_switch")

    def test_materialized_view_over_the_distributed_table_survives_a_run(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_mv_source"])
        project.run_sql(
            f"create materialized view {schema}.mv_over_proxy engine = MergeTree order by id "
            f"as select id from {schema}.naming_mv_source"
        )

        run_dbt(["run", "--select", "naming_mv_source"])

        assert project.run_sql(f"select count() from {schema}.mv_over_proxy", fetch="one")[0] == 3

    def test_incremental_refuses_to_rebuild_a_missing_local_table(self, project):
        """Moving the local table leaves nothing at the new name, and the compiled sql is the
        incremental slice by then, so rebuilding from it would drop the rest of the data."""
        run_dbt(
            ["run", "--select", "naming_incremental_switch", "--vars", f"{{shard_db: {SHARD_DB}}}"]
        )

        result = run_dbt(
            [
                "run",
                "--select",
                "naming_incremental_switch",
                "--vars",
                f"{{shard_db: {GUARD_SHARD_DB}}}",
            ],
            expect_pass=False,
        )
        assert "--full-refresh" in result[0].message
        assert not database_exists(project, GUARD_SHARD_DB)

    def test_distributed_table_follows_a_moved_local_table(self, project):
        schema = project.test_schema
        run_dbt(["run", "--select", "naming_table_switch", "--vars", f"{{shard_db: {SHARD_DB}}}"])
        run_dbt(
            ["run", "--select", "naming_table_switch", "--vars", f"{{shard_db: {OTHER_SHARD_DB}}}"]
        )

        assert_distributed_over(
            project, schema, "naming_table_switch", OTHER_SHARD_DB, "naming_table_switch"
        )
        assert project.run_sql("select count() from naming_table_switch", fetch="one")[0] == 3

    def test_collision_fails_before_any_ddl(self, project):
        result = run_dbt(["run", "--select", "naming_collision"], expect_pass=False)

        assert "resolves to the Distributed table itself" in result[0].message
        assert not table_exists(project, project.test_schema, "naming_collision")
