"""
test refreshable materialized view creation. This is ClickHouse specific, which has a significantly different implementation
of materialized views from PostgreSQL or Oracle
"""

import json
import os

import pytest
from dbt.tests.util import check_relation_types, run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

# This model is parameterized, in a way, by the "run_type" dbt project variable
# This is to be able to switch between different model definitions within
# the same test run and allow us to test the evolution of a materialized view
MV_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE",
               "depends_on": ['depend_on_model'],
               "depends_on_validation": True
           } if var('run_type', '') == 'validate_depends_on' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""


# Refresh parameters change between runs to test in-place updates via MODIFY REFRESH
MV_REFRESH_UPDATE_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 5 MINUTE",
               "randomize": "30 SECOND"
           } if var('run_type', '') == 'update_refresh_params' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

# Switches between a regular MV and a refreshable MV to test conversion handling
PLAIN_TO_REFRESHABLE_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE"
           } if var('run_type', '') == 'make_refreshable' else none
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

REFRESHABLE_TO_PLAIN_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           none if var('run_type', '') == 'make_regular' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

# An APPEND MV whose interval changes between runs; MODIFY REFRESH must keep APPEND
APPEND_UPDATE_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 5 MINUTE",
               "append": True
           } if var('run_type', '') == 'update_refresh_params' else {
               "interval": "EVERY 2 MINUTE",
               "append": True
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

# Adds APPEND to an existing non-APPEND refreshable MV, which cannot be done in place
ADD_APPEND_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE",
               "append": True
           } if var('run_type', '') == 'add_append' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

# refreshable: false must behave exactly like omitting the config
REFRESHABLE_FALSE_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=false
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

# Toggles the APPEND setting, which cannot be changed via MODIFY REFRESH
APPEND_TOGGLE_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE"
           } if var('run_type', '') == 'drop_append' else {
               "interval": "EVERY 2 MINUTE",
               "append": True
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""


def get_mv_ddl_and_uuid(project, mv_name):
    return project.run_sql(
        f"select create_table_query, uuid from system.tables"
        f" where database = '{project.test_schema}' and name = '{mv_name}'",
        fetch="one",
    )


class TestBasicRefreshableMV:
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
            "hackers.sql": MV_MODEL,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a model as a refreshable materialized view, selecting from the table created in (1)
        3. check in system.view_refreshes for the table existence
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model
        results = run_dbt()
        assert len(results) == 1

        columns = project.run_sql("DESCRIBE TABLE hackers", fetch="all")
        assert columns[0][1] == "String"

        columns = project.run_sql("DESCRIBE hackers_mv", fetch="all")
        assert columns[0][1] == "String"

        check_relation_types(
            project.adapter,
            {
                "hackers_mv": "materialized_view",
                "hackers": "table",
            },
        )

        # the backfill is the only initial populate (2 departments, inserted once) and EMPTY is a
        # creation-time keyword that must not end up in the stored DDL
        assert project.run_sql("select count() from hackers", fetch="one")[0] == 2
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
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
                          AND view = 'hackers_mv'
                    """,
                fetch="all",
            )
            statuses = [row[1] for row in result]
            assert 'Scheduled' in statuses or 'Running' in statuses
        else:
            result = project.run_sql(
                f"select database, view, status from system.view_refreshes where database= '{project.test_schema}' and view='hackers_mv'",
                fetch="all",
            )
            mv_status = result[0][2]
            assert mv_status in ('Scheduled', 'Running')

    def test_validate_dependency(self, project):
        """
        1. create a base table via dbt seed
        2. create a refreshable mv model with non exist dependency and validation config, selecting from the table created in (1)
        3. make sure we get an error
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # re-run dbt but this time with the new MV SQL
        run_vars = {"run_type": "validate_depends_on"}
        result = run_dbt(["run", "--vars", json.dumps(run_vars)], False)
        assert result[0].status == 'error'
        assert 'No existing MV found matching MV' in result[0].message


class TestModifyRefreshParamsMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": MV_REFRESH_UPDATE_MODEL,
        }

    def test_update_refresh_params(self, project):
        """
        1. create a refreshable MV
        2. re-run with changed refresh parameters (interval + randomize)
        3. verify the new schedule was applied via MODIFY REFRESH, without recreating the MV
        4. re-run with the same parameters and verify the no-op is harmless
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        ddl, uuid_before = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 2 MINUTE' in ddl

        run_vars = {"run_type": "update_refresh_params"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        ddl, uuid_after = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 5 MINUTE' in ddl
        assert 'RANDOMIZE FOR 30 SECOND' in ddl
        # the MV was altered in place, not dropped and recreated
        assert uuid_before == uuid_after

        # re-running with unchanged parameters reissues MODIFY REFRESH and is a no-op
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1
        ddl, uuid_noop = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 5 MINUTE' in ddl
        assert uuid_noop == uuid_after


class TestRegularToRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": PLAIN_TO_REFRESHABLE_MODEL,
        }

    def test_regular_to_refreshable_requires_full_refresh(self, project):
        """
        1. create a regular (non-refreshable) MV
        2. re-run with a refreshable config and expect a clear error
        3. re-run with --full-refresh and verify the MV is now refreshable
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        run_vars = json.dumps({"run_type": "make_refreshable"})
        result = run_dbt(["run", "--vars", run_vars], False)
        assert result[0].status == 'error'
        assert 'is not refreshable' in result[0].message
        assert '--full-refresh' in result[0].message

        results = run_dbt(["run", "--full-refresh", "--vars", run_vars])
        assert len(results) == 1
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 2 MINUTE' in ddl


class TestRefreshableToRegularMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": REFRESHABLE_TO_PLAIN_MODEL,
        }

    def test_refreshable_to_regular_requires_full_refresh(self, project):
        """
        1. create a refreshable MV
        2. re-run without the refreshable config and expect a clear error
        3. re-run with --full-refresh and verify the MV is now a regular MV
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        run_vars = json.dumps({"run_type": "make_regular"})
        result = run_dbt(["run", "--vars", run_vars], False)
        assert result[0].status == 'error'
        assert 'is refreshable' in result[0].message
        assert '--full-refresh' in result[0].message

        results = run_dbt(["run", "--full-refresh", "--vars", run_vars])
        assert len(results) == 1
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' REFRESH ' not in ddl


class TestChangeAppendRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": APPEND_TOGGLE_MODEL,
        }

    def test_change_append_requires_full_refresh(self, project):
        """
        1. create a refreshable MV with APPEND
        2. re-run without append and expect a clear error (APPEND can't be changed in place)
        3. re-run with --full-refresh and verify the MV no longer has APPEND
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' APPEND ' in ddl

        run_vars = json.dumps({"run_type": "drop_append"})
        result = run_dbt(["run", "--vars", run_vars], False)
        assert result[0].status == 'error'
        assert 'APPEND' in result[0].message
        assert '--full-refresh' in result[0].message

        results = run_dbt(["run", "--full-refresh", "--vars", run_vars])
        assert len(results) == 1
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' APPEND ' not in ddl


class TestModifyRefreshParamsAppendMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": APPEND_UPDATE_MODEL,
        }

    def test_update_refresh_params_append(self, project):
        """
        1. create a refreshable MV with APPEND
        2. re-run with the same config and verify the no-op MODIFY REFRESH succeeds
        3. re-run with a changed interval and verify it was applied, keeping APPEND
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        ddl, uuid_before = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 2 MINUTE' in ddl
        assert ' APPEND ' in ddl

        # no-op re-run: MODIFY REFRESH must carry the APPEND keyword to be accepted
        results = run_dbt()
        assert len(results) == 1

        run_vars = {"run_type": "update_refresh_params"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        ddl, uuid_after = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert 'REFRESH EVERY 5 MINUTE' in ddl
        assert ' APPEND ' in ddl
        assert uuid_before == uuid_after


class TestAddAppendToRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": ADD_APPEND_MODEL,
        }

    def test_add_append_requires_full_refresh(self, project):
        """
        1. create a refreshable MV without APPEND
        2. re-run with append=True and expect a clear error
        3. re-run with --full-refresh and verify the MV now has APPEND
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        run_vars = json.dumps({"run_type": "add_append"})
        result = run_dbt(["run", "--vars", run_vars], False)
        assert result[0].status == 'error'
        assert 'APPEND' in result[0].message
        assert '--full-refresh' in result[0].message

        results = run_dbt(["run", "--full-refresh", "--vars", run_vars])
        assert len(results) == 1
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' APPEND ' in ddl


class TestRefreshableFalseMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": REFRESHABLE_FALSE_MODEL,
        }

    def test_refreshable_false_is_regular_mv(self, project):
        """
        refreshable: false must behave exactly like omitting the config:
        the MV is created as a regular MV and can be updated in place
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1

        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' REFRESH ' not in ddl

        # the update path must also treat refreshable: false as disabled
        results = run_dbt()
        assert len(results) == 1
        ddl, _ = get_mv_ddl_and_uuid(project, 'hackers_mv')
        assert ' REFRESH ' not in ddl


def refreshable_mv_model(catchup=True, **refreshable):
    """
    Model for the catchup / initial_internal_refresh matrix. AFTER 1 HOUR counts from creation when
    the view is created EMPTY, so no scheduled refresh can fire while a test asserts row counts.
    """
    refreshable = {"interval": "AFTER 1 HOUR", **refreshable}
    return f"""
{{{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       catchup={catchup},
       refreshable={json.dumps(refreshable)}
) }}}}
select
    department,
    avg(age) as average
    from {{{{ source('raw', 'people') }}}}
group by department
"""


def target_row_count(project):
    return project.run_sql("select count() from hackers", fetch="one")[0]


# Turns a plain table model into a refreshable MV with the same name, which takes the
# "target exists, view missing" path of clickhouse__update_mv
TABLE_TO_REFRESHABLE_MODEL = """
{{ config(
       materialized=('materialized_view' if var('run_type', '') == 'make_refreshable' else 'table'),
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {"interval": "AFTER 1 HOUR"} if var('run_type', '') == 'make_refreshable' else none
       )
       )
 }}
{% if var('run_type', '') == 'make_refreshable' %}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
{% else %}
select '' as department, toFloat64(0) as average where 0
{% endif %}
"""

# The initial refresh fails until run_type switches to the corrected query
FAILING_THEN_FIXED_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       catchup=False,
       refreshable={"interval": "AFTER 1 HOUR", "initial_internal_refresh": True}
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
{% if var('run_type', '') != 'fixed' %}
where throwIf(department != '', 'boom') = 0
{% endif %}
group by department
"""


class TestRefreshableMVOverExistingTable:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": TABLE_TO_REFRESHABLE_MODEL,
        }

    def test_existing_target_is_not_backfilled(self, project):
        """
        1. create an empty table model
        2. change it into a refreshable MV: the table is kept as the target and, as for regular
           MVs, only the view is created; the catchup runs only when dbt creates the target
        3. --full-refresh rebuilds the target with the catchup
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1
        assert target_row_count(project) == 0

        run_vars = json.dumps({"run_type": "make_refreshable"})
        results = run_dbt(["run", "--vars", run_vars])
        assert len(results) == 1
        check_relation_types(
            project.adapter, {"hackers_mv": "materialized_view", "hackers": "table"}
        )
        assert target_row_count(project) == 0

        results = run_dbt(["run", "--full-refresh", "--vars", run_vars])
        assert len(results) == 1
        assert target_row_count(project) == 2


class TestInitialInternalRefreshFailureMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": FAILING_THEN_FIXED_MODEL,
        }

    def test_failed_initial_refresh_fails_the_model_and_full_refresh_recovers(self, project):
        """
        1. the first run fails with the ClickHouse error; the view and the empty target stay
        2. --full-refresh with a corrected query recreates the view and waits for its refresh
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        result = run_dbt(["run"], False)
        assert result[0].status == 'error'
        assert 'Refresh failed' in result[0].message
        assert '--full-refresh' in result[0].message
        assert target_row_count(project) == 0
        check_relation_types(
            project.adapter, {"hackers_mv": "materialized_view", "hackers": "table"}
        )

        results = run_dbt(["run", "--full-refresh", "--vars", json.dumps({"run_type": "fixed"})])
        assert len(results) == 1
        assert target_row_count(project) == 2


class TestAppendRefreshableMVCatchup:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": refreshable_mv_model(append=True),
        }

    def test_no_duplicate_rows(self, project):
        """
        An APPEND view created without EMPTY would insert the query result on top of the backfill,
        both on first creation and when the MVs are recreated by --full-refresh.
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1
        assert target_row_count(project) == 2

        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1
        assert target_row_count(project) == 2


class TestInitialInternalRefreshRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": refreshable_mv_model(catchup=False, initial_internal_refresh=True),
        }

    def test_initial_refresh_populates_target_before_run_ends(self, project):
        """
        The target table is created empty, ClickHouse runs the initial refresh and dbt waits for
        it with SYSTEM WAIT VIEW, so the target is populated by the time the model finishes.
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 1
        assert target_row_count(project) == 2
