import pytest
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.util import run_dbt


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


# Standalone tables ignore mv_on_schema_change and always rebuild on regular runs.

# Base model - initial schema with 2 columns
table_schema_change_base = """
{{{{
    config(
        materialized='table',
        mv_on_schema_change='{strategy}'
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
        mv_on_schema_change='{strategy}'
    )
}}}}
select
    number as col_1,
    number + 1 as col_2,
    number + 2 as col_3
from numbers(3)
"""


class TestTableOnSchemaChangeIgnore:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_ignore.sql": table_schema_change_base.format(strategy="ignore"),
        }

    def test_mv_on_schema_change_not_applied_if_no_mv_is_involved(self, project):
        # First run - creates table with col_1, col_2
        run_dbt(["run"])
        result = project.run_sql("select * from table_ignore order by col_1", fetch="all")
        assert len(result) == 3
        assert len(result[0]) == 2  # 2 columns

        # Update the model file to add col_3
        model_path = project.project_root.join("models", "table_ignore.sql")

        # Even when forcing this setting into the table, it should be ignored since this table is not targeted by an MV
        model_path.write(table_schema_change_add_column.format(strategy="fail"))

        # Second run - standalone table is always rebuilt, mv_on_schema_change is ignored
        run_dbt(["run"])
        result = project.run_sql("select * from table_ignore order by col_1", fetch="all")
        assert len(result) == 3
        actual_cols = len(result[0])
        assert (
            actual_cols == 3
        ), f"Standalone table should be rebuilt with 3 columns (mv_on_schema_change ignored), but has {actual_cols} columns"
