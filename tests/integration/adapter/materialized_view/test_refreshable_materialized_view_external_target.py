"""
Test refreshable materialized view creation with external target table.
This tests the new implementation where a refreshable MV writes to an existing table
using the `materialization_target_table()` macro.
"""

import json
import os

import pytest
from dbt.tests.util import check_relation_types, run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

# Target table model - this creates the destination table that the MV will write to
TARGET_TABLE_MODEL = """
{{ config(
       materialized='table',
       engine='MergeTree()',
       order_by='(department)'
) }}

SELECT
    '' AS department,
    toFloat64(0) AS average
WHERE 0  -- Creates empty table with correct schema
"""

# Refreshable MV model that writes to the external target table
MV_MODEL = """
{{ config(
       materialized='materialized_view',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE",
               "depends_on": ['depend_on_model'],
               "depends_on_validation": True
           } if var('run_type', '') == 'validate_depends_on' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
) }}

{{ materialization_target_table(ref('hackers_target')) }}

select
    department,
    avg(age) as average
from {{ source('raw', 'people') }}
group by department
"""


class TestBasicExternalTargetRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        """
        we need a base table to pull from
        """
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": MV_MODEL,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a target table model
        3. create a model as a refreshable materialized view pointing to the target table
        4. check in system.view_refreshes for the MV existence
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the models (target table + refreshable MV)
        results = run_dbt()
        assert len(results) == 2

        # Check the target table structure
        columns = project.run_sql("DESCRIBE TABLE hackers_target", fetch="all")
        assert columns[0][1] == "String"

        # Check the MV exists
        columns = project.run_sql("DESCRIBE hackers", fetch="all")
        assert columns[0][1] == "String"

        check_relation_types(
            project.adapter,
            {
                "hackers": "materialized_view",
                "hackers_target": "table",
            },
        )

        # the catchup is the only initial populate (2 departments, inserted once) and EMPTY is a
        # creation-time keyword that must not end up in the stored DDL
        assert project.run_sql("select count() from hackers_target", fetch="one")[0] == 2
        ddl = project.run_sql(
            f"select create_table_query from system.tables"
            f" where database = '{project.test_schema}' and name = 'hackers'",
            fetch="one",
        )[0]
        assert 'EMPTY' not in ddl

        if os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'):
            result = project.run_sql(
                f"""
                        SELECT
                            hostName() as replica,
                            status,
                            last_refresh_time
                        FROM clusterAllReplicas('default', 'system', 'view_refreshes')
                        WHERE database = '{project.test_schema}'
                          AND view = 'hackers'
                    """,
                fetch="all",
            )
            statuses = [row[1] for row in result]
            assert 'Scheduled' in statuses or 'Running' in statuses
        else:
            result = project.run_sql(
                f"select database, view, status from system.view_refreshes where database= '{project.test_schema}' and view='hackers'",
                fetch="all",
            )
            mv_status = result[0][2]
            assert mv_status in ('Scheduled', 'Running')

        # --full-refresh drops the MV, runs the catchup, then recreates the MV with EMPTY
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 2
        assert project.run_sql("select count() from hackers_target", fetch="one")[0] == 2

    def test_validate_dependency(self, project):
        """
        1. create a base table via dbt seed
        2. create a refreshable mv model with non exist dependency and validation config
        3. make sure we get an error
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # re-run dbt but this time with the new MV SQL
        run_vars = {"run_type": "validate_depends_on"}
        result = run_dbt(["run", "--vars", json.dumps(run_vars)], False)
        # Find the result that has an error (might be hackers model)
        error_results = [r for r in result if r.status == 'error']
        assert len(error_results) > 0
        assert 'No existing MV found matching MV' in error_results[0].message


# Refresh parameters change between runs to test in-place updates via MODIFY REFRESH
MV_REFRESH_UPDATE_MODEL = """
{{ config(
       materialized='materialized_view',
       refreshable=(
           {
               "interval": "EVERY 5 MINUTE",
               "randomize": "30 SECOND"
           } if var('run_type', '') == 'update_refresh_params' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
) }}

{{ materialization_target_table(ref('hackers_target')) }}

select
    department,
    avg(age) as average
from {{ source('raw', 'people') }}
group by department
"""


class TestModifyRefreshParamsExternalTargetMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": MV_REFRESH_UPDATE_MODEL,
        }

    def test_update_refresh_params(self, project):
        """
        1. create a target table and a refreshable MV pointing to it
        2. re-run with changed refresh parameters (interval + randomize)
        3. verify the new schedule was applied via MODIFY REFRESH, without recreating the MV
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 2

        ddl, uuid_before = project.run_sql(
            f"select create_table_query, uuid from system.tables"
            f" where database = '{project.test_schema}' and name = 'hackers'",
            fetch="one",
        )
        assert 'REFRESH EVERY 2 MINUTE' in ddl

        run_vars = {"run_type": "update_refresh_params"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 2

        ddl, uuid_after = project.run_sql(
            f"select create_table_query, uuid from system.tables"
            f" where database = '{project.test_schema}' and name = 'hackers'",
            fetch="one",
        )
        assert 'REFRESH EVERY 5 MINUTE' in ddl
        assert 'RANDOMIZE FOR 30 SECOND' in ddl
        # the MV was altered in place, not dropped and recreated
        assert uuid_before == uuid_after


def refreshable_external_target_model(catchup=True, **refreshable):
    """
    Model for the catchup / initial_internal_refresh matrix. AFTER 1 HOUR counts from creation when
    the view is created EMPTY, so no scheduled refresh can fire while a test asserts row counts.
    """
    refreshable = {"interval": "AFTER 1 HOUR", **refreshable}
    return f"""
{{{{ config(
       materialized='materialized_view',
       catchup={catchup},
       refreshable={json.dumps(refreshable)}
) }}}}

{{{{ materialization_target_table(ref('hackers_target')) }}}}

select
    department,
    avg(age) as average
from {{{{ source('raw', 'people') }}}}
group by department
"""


def target_row_count(project):
    return project.run_sql("select count() from hackers_target", fetch="one")[0]


# The initial refresh fails until run_type switches to the corrected query
FAILING_THEN_FIXED_MODEL = """
{{ config(
       materialized='materialized_view',
       catchup=False,
       refreshable={"interval": "AFTER 1 HOUR", "initial_internal_refresh": True}
) }}

{{ materialization_target_table(ref('hackers_target')) }}

select
    department,
    avg(age) as average
from {{ source('raw', 'people') }}
{% if var('run_type', '') != 'fixed' %}
where throwIf(department != '', 'boom') = 0
{% endif %}
group by department
"""


def mv_refresh_state(project):
    return project.run_sql(
        f"select status, last_success_time from system.view_refreshes"
        f" where database = '{project.test_schema}' and view = 'hackers'",
        fetch="one",
    )


class TestAppendExternalTargetRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": refreshable_external_target_model(append=True),
        }

    def test_no_duplicate_rows(self, project):
        """
        An APPEND view created without EMPTY would insert the query result on top of the catchup.
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 2
        assert target_row_count(project) == 2


class TestNoCatchupExternalTargetRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": refreshable_external_target_model(catchup=False),
        }

    def test_target_stays_empty_until_first_scheduled_refresh(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 2
        assert target_row_count(project) == 0
        status, last_success_time = mv_refresh_state(project)
        assert status == 'Scheduled'
        assert last_success_time is None


class TestInitialInternalRefreshExternalTargetRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": refreshable_external_target_model(
                catchup=False, initial_internal_refresh=True
            ),
        }

    def test_initial_refresh_populates_target_before_run_ends(self, project):
        """
        ClickHouse runs the initial refresh and dbt waits for it with SYSTEM WAIT VIEW, so the
        target is populated by the time the model finishes.
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 2
        assert target_row_count(project) == 2
        _, last_success_time = mv_refresh_state(project)
        assert last_success_time is not None


class TestInitialInternalRefreshRequiresNoCatchup:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": refreshable_external_target_model(initial_internal_refresh=True),
        }

    def test_fails_before_any_ddl(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        result = run_dbt(["run"], False)
        errors = [r for r in result if r.status == 'error']
        assert len(errors) == 1
        assert 'initial_internal_refresh' in errors[0].message
        assert 'catchup=False' in errors[0].message
        mv_count = project.run_sql(
            f"select count() from system.tables where database = '{project.test_schema}' and name = 'hackers'",
            fetch="one",
        )[0]
        assert mv_count == 0


class TestInitialInternalRefreshWithDependsOnExternalTargetMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            # DEPENDS ON is only allowed with REFRESH EVERY; the dependency never refreshes, so
            # neither does this view and the hourly boundary cannot interfere
            "hackers.sql": refreshable_external_target_model(
                catchup=False,
                initial_internal_refresh=True,
                interval="EVERY 1 HOUR",
                depends_on=['default.hackers_dependency_that_does_not_exist'],
            ),
        }

    def test_run_does_not_wait_for_dependencies(self, project):
        """
        The first refresh of a view with DEPENDS ON only runs after its dependencies refresh, so
        dbt skips SYSTEM WAIT VIEW instead of blocking; the target is populated asynchronously.
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 2
        assert target_row_count(project) == 0


class TestInitialInternalRefreshFailureExternalTargetMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": FAILING_THEN_FIXED_MODEL,
        }

    def test_failed_initial_refresh_fails_the_model_and_full_refresh_recovers(self, project):
        """
        1. the first run fails with the ClickHouse error; the view stays and the message says so
        2. --full-refresh with a corrected query recreates the view and waits for its refresh
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        result = run_dbt(["run"], False)
        errors = [r for r in result if r.status == 'error']
        assert len(errors) == 1
        assert 'Refresh failed' in errors[0].message
        assert '--full-refresh' in errors[0].message
        assert target_row_count(project) == 0
        assert mv_refresh_state(project) is not None

        results = run_dbt(["run", "--full-refresh", "--vars", json.dumps({"run_type": "fixed"})])
        assert len(results) == 2
        assert target_row_count(project) == 2
        _, last_success_time = mv_refresh_state(project)
        assert last_success_time is not None
