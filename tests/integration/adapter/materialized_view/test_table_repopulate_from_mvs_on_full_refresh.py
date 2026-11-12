"""
Tests repopulate_from_mvs_on_full_refresh feature for target tables with external MVs.

IMPORTANT: When using `repopulate_from_mvs_on_full_refresh=True` on a target table,
you MUST set `catchup=False` on the source MVs to avoid duplicate data as both will execute the same INSERT statement.
"""

import json

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

# repopulate_from_mvs_on_full_refresh controlled by var
TARGET_MODEL = """
{{ config(
       materialized='table'
) }}
{%- if var('enable_repopulate', false) %}
{{ config(repopulate_from_mvs_on_full_refresh=true) }}
{%- endif %}

SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS department
WHERE 0  -- Creates empty table with correct schema
"""

# MV 1 - engineering employees
# NOTE: catchup=False is required here because we're testing repopulate_from_mvs_on_full_refresh.
MV_MODEL_1 = """
{{ config(
       materialized='materialized_view',
       catchup=False
) }}

{{ materialization_target_table(ref('employees_target')) }}

select
    p.id,
    p.name,
    'engineering' as department
from {{ source('raw', 'people') }} p
where p.department = 'engineering'
"""

# MV 2 - sales employees
# NOTE: catchup=False is required here for the same reason as MV_MODEL_1
MV_MODEL_2 = """
{{ config(
       materialized='materialized_view',
       catchup=False
) }}

{{ materialization_target_table(ref('employees_target')) }}

select
    p.id,
    p.name,
    'sales' as department
from {{ source('raw', 'people') }} p
where p.department = 'sales'
"""


class TestFullRefreshRepopulate:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "employees_target.sql": TARGET_MODEL,
            "employees_mv_engineering.sql": MV_MODEL_1,
            "employees_mv_sales.sql": MV_MODEL_2,
        }

    def test_repopulate_from_mvs_on_full_refresh(self, project):
        """
        Test the repopulate_from_mvs_on_full_refresh feature:
        1. First run with catchup=False and repopulate disabled - target stays empty
        2. Full refresh with repopulate enabled - target gets data from MVs

        NOTE: MVs have catchup=False which is required when using repopulate_from_mvs_on_full_refresh
        to avoid inserting data twice (once by catchup, once by repopulate).
        """
        schema = quote_identifier(project.test_schema)

        results = run_dbt(["seed"])
        assert len(results) == 1

        # First run with repopulate_from_mvs_on_full_refresh=False (default from var)
        results = run_dbt(["run"])
        assert len(results) == 3  # 1 target table + 2 MVs

        # Verify target table is empty (catchup=False and no repopulation)
        result = project.run_sql(f"select count(*) from {schema}.employees_target", fetch="all")
        assert result[0][0] == 0, "Target table should be empty after first run with catchup=False"

        # Full refresh WITH repopulate_from_mvs_on_full_refresh=True
        run_vars = {"enable_repopulate": True}
        results = run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])
        assert len(results) == 3

        # Verify target table now has data from both MVs
        result = project.run_sql(
            f"select department, count(*) as cnt from {schema}.employees_target group by department order by department",
            fetch="all",
        )
        assert len(result) == 2
        assert result[0] == ("engineering", 3)
        assert result[1] == ("sales", 3)

        # Verify MVs still work after full refresh
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (4000,'Dave',40,'sales'), (5000,'Eve',35,'engineering');
            """
        )

        # Check that new data was captured by MVs
        result = project.run_sql(f"select count(*) from {schema}.employees_target", fetch="all")
        assert result[0][0] == 8, "Target should have 8 rows after new inserts (6 + 2 new)"


class TestFullRefreshRepopulateDisabled:
    """
    Test that with catchup=False and repopulate_from_mvs_on_full_refresh=False (both disabled),
    the target table remains empty even after full refresh.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "employees_target.sql": TARGET_MODEL,
            "employees_mv_engineering.sql": MV_MODEL_1,
        }

    def test_repopulate_disabled(self, project):
        """
        With repopulate_from_mvs_on_full_refresh=False and catchup=False, the target should
        remain empty even after a full refresh.
        """
        schema = quote_identifier(project.test_schema)
        # Seed and first run
        run_dbt(["seed"])
        run_dbt(["run"])

        # Target should be empty
        result = project.run_sql(f"select count(*) from {schema}.employees_target", fetch="all")
        assert result[0][0] == 0

        # Full refresh - should still be empty because repopulate is disabled
        run_dbt(["run", "--full-refresh"])

        result = project.run_sql(f"select count(*) from {schema}.employees_target", fetch="all")
        assert result[0][0] == 0, (
            "Target should remain empty with repopulate_from_mvs_on_full_refresh=False"
        )


# catchup and repopulate_from_mvs_on_full_refresh are both controlled by vars
WARNING_TARGET_MODEL = """
{{ config(
       materialized='table',
       repopulate_from_mvs_on_full_refresh=var('repopulate', false)
) }}

SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS department
WHERE 0  -- Creates empty table with correct schema
"""

WARNING_MV_MODEL = """
{{ config(
       materialized='materialized_view',
       catchup=var('catchup', true)
) }}

{{ materialization_target_table(ref('employees_target')) }}

select
    p.id,
    p.name,
    'engineering' as department
from {{ source('raw', 'people') }} p
where p.department = 'engineering'
"""

CATCHUP_REPOPULATE_WARNING = (
    "Materialized view employees_mv_engineering has catchup=True and writes to employees_target, "
    "which has repopulate_from_mvs_on_full_refresh=True"
)


class TestCatchupAndRepopulateWarning:
    """
    Enabling catchup on an MV and repopulate_from_mvs_on_full_refresh on its target table
    backfills the target twice on a full refresh, so the MV materialization warns about it.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "employees_target.sql": WARNING_TARGET_MODEL,
            "employees_mv_engineering.sql": WARNING_MV_MODEL,
        }

    @staticmethod
    def _run(run_vars, full_refresh=False):
        args = ["run", "--vars", json.dumps(run_vars)]
        if full_refresh:
            args.append("--full-refresh")
        results, log_output = run_dbt_and_capture(args)
        assert len(results) == 2
        return log_output

    def test_warns_when_both_enabled(self, project):
        run_dbt(["seed"])

        # catchup is left at its default (True)
        log_output = self._run({"repopulate": True})
        assert CATCHUP_REPOPULATE_WARNING in log_output

        log_output = self._run({"repopulate": True}, full_refresh=True)
        assert CATCHUP_REPOPULATE_WARNING in log_output

    def test_no_warning_with_only_catchup(self, project):
        run_dbt(["seed"])

        log_output = self._run({"repopulate": False}, full_refresh=True)
        assert CATCHUP_REPOPULATE_WARNING not in log_output

    def test_no_warning_with_only_repopulate(self, project):
        run_dbt(["seed"])

        log_output = self._run({"repopulate": True, "catchup": False}, full_refresh=True)
        assert CATCHUP_REPOPULATE_WARNING not in log_output
