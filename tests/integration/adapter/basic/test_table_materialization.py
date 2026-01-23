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
