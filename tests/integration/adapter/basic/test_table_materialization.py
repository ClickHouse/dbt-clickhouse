import pytest
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.util import run_dbt, run_dbt_and_capture


class TestTableMat(BaseTableMaterialization):
    pass


# Model that returns different data based on a variable
table_model_with_variable = """
{{ config(materialized='table') }}
select {{ var('row_value', 1) }} as id
"""


class TestTableRebuildOnRun:
    """
    Test that table materialization rebuilds the table on every run (without --full-refresh).

    This is the standard dbt behavior for table materializations. Tables should be
    dropped and recreated on each run, not preserved like incremental models.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": table_model_with_variable}

    def test_table_rebuilds_on_regular_run(self, project):
        """Verify that a second dbt run rebuilds the table with new data."""
        # First run - creates table with id=1
        run_dbt(["run", "--vars", '{"row_value": 1}'])
        result = project.run_sql("select id from table_model", fetch="all")
        assert result[0][0] == 1, "First run should create table with id=1"

        # Second run (no --full-refresh) - should rebuild table with id=2
        run_dbt(["run", "--vars", '{"row_value": 2}'])
        result = project.run_sql("select id from table_model", fetch="all")
        assert result[0][0] == 2, (
            "Second run should rebuild table with id=2. "
            "If this fails, the table materialization is not rebuilding on regular runs."
        )


# =============================================================================
# on_schema_change tests for TABLE materialization
#
# When on_schema_change is configured, the table is NOT rebuilt on regular runs.
# This is especially useful when the table has MVs writing to it and you only want to apply schema changes.
# Instead, schema changes are handled according to the strategy:
# - ignore: no schema changes applied
# - fail: fail if schema changed
# - append_new_columns: add new columns
# - sync_all_columns: fully sync schema (add/remove/modify columns)
# =============================================================================

# Base model - initial schema with 2 columns
table_schema_change_base = """
{{{{
    config(
        materialized='table',
        on_schema_change='{strategy}'
    )
}}}}
select
    number as col_1,
    number + 1 as col_2
from numbers(3)
"""

# Changed model - adds col_3
table_schema_change_add_column = """
{{{{
    config(
        materialized='table',
        on_schema_change='{strategy}'
    )
}}}}
select
    number as col_1,
    number + 1 as col_2,
    number + 2 as col_3
from numbers(3)
"""


class TestTableOnSchemaChangeIgnore:
    """
    Test on_schema_change='ignore' for table materialization.

    When set, the table is NOT rebuilt on subsequent runs and schema changes are ignored.
    The table keeps its original schema even if the model SQL changes.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_ignore.sql": table_schema_change_base.format(strategy="ignore"),
        }

    def test_ignore_schema_change(self, project):
        # First run - creates table with col_1, col_2
        run_dbt(["run"])
        result = project.run_sql("select * from table_ignore order by col_1", fetch="all")
        assert len(result) == 3
        assert len(result[0]) == 2  # 2 columns

        # Update the model file to add col_3
        model_path = project.project_root.join("models", "table_ignore.sql")
        model_path.write(table_schema_change_add_column.format(strategy="ignore"))

        # Second run - schema change should be ignored, table keeps 2 columns
        run_dbt(["run"])
        result = project.run_sql("select * from table_ignore order by col_1", fetch="all")
        assert len(result) == 3
        assert (
            len(result[0]) == 2
        ), "Table should still have 2 columns when on_schema_change='ignore'"


class TestTableOnSchemaChangeFail:
    """
    Test on_schema_change='fail' for table materialization.

    When set, the run should fail if the model schema differs from the existing table.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_fail.sql": table_schema_change_base.format(strategy="fail"),
        }

    def test_fail_on_schema_change(self, project):
        # First run - creates table with col_1, col_2
        run_dbt(["run"])
        result = project.run_sql("select * from table_fail order by col_1", fetch="all")
        assert len(result) == 3
        assert len(result[0]) == 2

        # Update the model file to add col_3
        model_path = project.project_root.join("models", "table_fail.sql")
        model_path.write(table_schema_change_add_column.format(strategy="fail"))

        # Second run - should fail because schema changed
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "out of sync" in log_output.lower()
        ), "Should fail with 'out of sync' error when on_schema_change='fail'"


class TestTableOnSchemaChangeAppend:
    """
    Test on_schema_change='append_new_columns' for table materialization.

    When set, new columns from the model are added to the existing table.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_append.sql": table_schema_change_base.format(strategy="append_new_columns"),
        }

    def test_append_new_columns(self, project):
        # First run - creates table with col_1, col_2
        run_dbt(["run"])
        result = project.run_sql("select * from table_append order by col_1", fetch="all")
        assert len(result) == 3
        assert len(result[0]) == 2

        # Update the model file to add col_3
        model_path = project.project_root.join("models", "table_append.sql")
        model_path.write(table_schema_change_add_column.format(strategy="append_new_columns"))

        # Second run - col_3 should be added
        run_dbt(["run"])
        result = project.run_sql("select * from table_append order by col_1", fetch="all")
        assert len(result) == 3
        assert (
            len(result[0]) == 3
        ), "Table should have 3 columns after on_schema_change='append_new_columns'"
        # New column should have default values (0) for existing rows
        assert result[0][2] == 0


class TestTableOnSchemaChangeSyncAll:
    """
    Test on_schema_change='sync_all_columns' for table materialization.

    When set, the table schema is fully synced with the model:
    - New columns are added
    - Removed columns are dropped
    - Column types can be modified
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_sync.sql": table_schema_change_base.format(strategy="sync_all_columns"),
        }

    def test_sync_all_columns(self, project):
        # First run - creates table with col_1, col_2
        run_dbt(["run"])
        result = project.run_sql("select * from table_sync order by col_1", fetch="all")
        assert len(result) == 3
        assert len(result[0]) == 2

        # Update model to: remove col_2, add col_3
        sync_model = """
{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}
select
    number as col_1,
    number + 2 as col_3
from numbers(3)
"""
        model_path = project.project_root.join("models", "table_sync.sql")
        model_path.write(sync_model)

        # Second run - should sync schema: drop col_2, add col_3
        run_dbt(["run"])

        # Verify col_2 is gone and col_3 exists
        columns = project.run_sql(
            "select name from system.columns where table = 'table_sync' "
            f"and database = '{project.test_schema}' order by position",
            fetch="all",
        )
        column_names = [c[0] for c in columns]
        assert "col_1" in column_names
        assert "col_2" not in column_names, "col_2 should be dropped with sync_all_columns"
        assert "col_3" in column_names, "col_3 should be added with sync_all_columns"


# =============================================================================
# Test: MV target table automatically defaults to on_schema_change='fail'
#
# When a table is the target of a dbt-managed materialized view, and the user
# has NOT explicitly configured on_schema_change, it should automatically
# default to 'fail' to prevent data loss.
# =============================================================================

# Seed data for source
MV_SOURCE_SEED_CSV = """col_1,col_2
1,2
3,4
5,6
"""

MV_SEED_SCHEMA_YML = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: mv_source_seed
"""

# Target table - NO on_schema_change configured (will be auto-set to 'fail' when MV points to it)
mv_target_table_base = """
{{ config(materialized='table') }}
select
    toInt64(0) as col_1,
    toInt64(0) as col_2
where 0  -- Creates empty table with correct schema
"""

mv_target_table_add_column = """
{{ config(materialized='table') }}
select
    toInt64(0) as col_1,
    toInt64(0) as col_2,
    toInt64(0) as col_3
where 0  -- Creates empty table with correct schema
"""

# Materialized view that writes TO the target table
mv_pointing_to_target = """
{{ config(materialized='materialized_view', catchup=False) }}

{{ materialization_target_table(ref('mv_target_table')) }}

select col_1, col_2 from {{ source('raw', 'mv_source_seed') }}
"""


class TestTableWithMVDefaultsToFail:
    """
    Test that a table targeted by a materialized view automatically defaults
    to on_schema_change='fail' even when the user doesn't configure it.

    This is a safety feature to prevent accidental data loss when schema changes
    would break the MV's ability to write to the target table.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "mv_source_seed.csv": MV_SOURCE_SEED_CSV,
            "schema.yml": MV_SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mv_target_table.sql": mv_target_table_base,
            "mv_pointing.sql": mv_pointing_to_target,
        }

    def test_mv_target_defaults_to_fail_on_schema_change(self, project):
        # Seed the source data
        run_dbt(["seed"])

        # First run - creates target table and MV
        run_dbt(["run"])

        # Verify table was created (MV writes to it asynchronously, so just check table exists)
        columns = project.run_sql(
            "select name from system.columns where table = 'mv_target_table' "
            f"and database = '{project.test_schema}' order by position",
            fetch="all",
        )
        column_names = [c[0] for c in columns]
        assert "col_1" in column_names
        assert "col_2" in column_names
        assert len(column_names) == 2

        # Update the target table model to add col_3 (schema change)
        # Note: on_schema_change is NOT set, so it should auto-default to 'fail'
        model_path = project.project_root.join("models", "mv_target_table.sql")
        model_path.write(mv_target_table_add_column)

        # Second run - should fail because:
        # 1. MV points to this table
        # 2. on_schema_change was not set by user
        # 3. Therefore it auto-defaults to 'fail'
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)

        assert "out of sync" in log_output.lower(), (
            "Table with MV pointing to it should auto-default to on_schema_change='fail' "
            "and fail when schema changes. Got log: " + log_output
        )
