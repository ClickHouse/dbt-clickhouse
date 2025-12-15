"""
test materialized view creation.  This is ClickHouse specific, which has a significantly different implementation
of materialized views from PostgreSQL or Oracle
"""

import json

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import check_relation_types, run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
    VIEW_MODEL_HACKERS,
    query_table_type,
)

# This model is parameterized by three dbt project variables:
# - schema_name: the schema name to use (default: 'custom_schema')
# - catchup: whether to backfill existing data (default: True)
# - use_updated_schema: whether to use the extended schema with id2 column (default: False)
MV_MODEL = """
{{ config(
       materialized='materialized_view' if not var('use_view', False) else 'view',
       engine='MergeTree()',
       order_by='(id)',
       on_schema_change=var('on_schema_change', 'ignore'),
       schema=var('schema_name', 'custom_schema'),
       **({'catchup': False} if not var('catchup', True) else {})
) }}

{% if not var('use_updated_schema', False) %}
select
    id,
    name,
    case
        when name like 'Dade' then 'crash_override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}
where department = 'engineering'

{% else %}
select
    id,
    name,
    case
        -- Dade wasn't always known as 'crash override'!
        when name like 'Dade' and age = 11 then 'zero cool'
        when name like 'Dade' and age != 11 then 'crash override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias,
    id as id2
from {{ source('raw', 'people') }}
where department = 'engineering'

{% endif %}
"""


class TestBasicMV:
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
        2. create a model as a materialized view, selecting from the table created in (1)
        3. insert data into the base table and make sure it's there in the target table created in (2)
        """
        schema = quote_identifier(project.test_schema + "_custom_schema")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model
        results = run_dbt()
        assert len(results) == 1

        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv", fetch="all")
        assert columns[0][1] == "Int32"

        check_relation_types(
            project.adapter,
            {
                "hackers_mv": "view",
                "hackers": "table",
            },
        )

        # insert some data and make sure it reaches the target table
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 4


class TestUpdateMV:
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
            "hackers_mv.sql": MV_MODEL,
            "hackers.sql": VIEW_MODEL_HACKERS,
        }

    def test_update_incremental(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema")
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # re-run dbt but this time with the new MV SQL
        run_vars = {"use_updated_schema": True}
        run_dbt(["run", "--vars", json.dumps(run_vars)])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that we now have both of Dade's aliases in our hackers table
        result = project.run_sql(
            f"select distinct hacker_alias from {schema}.hackers_mv where name = 'Dade'",
            fetch="all",
        )
        assert len(result) == 2

        # As 'on_schema_change' is not defined, the new `id2` column will not be created in the destination table
        table_description_after_update = project.run_sql(
            f"DESCRIBE TABLE {schema}.hackers_mv", fetch="all"
        )
        assert not any(col[0] == "id2" for col in table_description_after_update)

    # Test to verify that updates with incremental materialized views also update its destination table
    def test_update_incremental_on_schema_change_sync_all_columns(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema")
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # re-run dbt but this time with the new MV SQL
        run_vars = {"use_updated_schema": True, "on_schema_change": "sync_all_columns"}
        run_dbt(["run", "--vars", json.dumps(run_vars)])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that the destination table is updated with the new column
        table_description_after_update = project.run_sql(
            f"DESCRIBE TABLE {schema}.hackers_mv", fetch="all"
        )
        assert any(col[0] == "id2" and col[1] == "Int32" for col in table_description_after_update)

        # run again without extended schema, to make sure table is updated back without the id2 column
        run_dbt(["run", "--vars", json.dumps({"on_schema_change": "sync_all_columns"})])
        table_description_after_revert_update = project.run_sql(
            f"DESCRIBE TABLE {schema}.hackers_mv", fetch="all"
        )
        assert not any(col[0] == "id2" for col in table_description_after_revert_update)

    def test_update_incremental_on_schema_change_fail(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema")
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # re-run dbt but this time with the new MV SQL
        run_vars = {"use_updated_schema": True, "on_schema_change": "fail"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)], expect_pass=False)

        result = next(r for r in results if r.status == "error")

        expected_messages = [
            'The source and target schemas on this materialized view model are out of sync',
            'Source columns not in target: []',
            "Target columns not in source: ['id2 Int32']",
            'New column types: []',
        ]
        for msg in expected_messages:
            assert msg in result.message

    def test_update_full_refresh(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema")
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # re-run dbt but this time with the new MV SQL
        run_vars = {"use_updated_schema": True}
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that we now have both of Dade's aliases in our hackers table
        result = project.run_sql(
            f"select distinct hacker_alias from {schema}.hackers_mv where name = 'Dade'",
            fetch="all",
        )
        assert len(result) == 2

    def test_mv_is_dropped_on_full_refresh(self, project):
        """
        1. create a base table via dbt seed
        2. create a model as a materialized view, selecting from the table created in (1)
        3. change the model to be a view and run with full refresh
        4. assert that the target table is now a view and the internal MV (_mv) no longer exists
        """
        schema_unquoted = project.test_schema + "_custom_schema"

        # Step 1: Create base table via dbt seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create the model as a materialized view
        results = run_dbt()
        assert len(results) == 2  # will include also a view for the other test.

        # Verify both tables were created correctly
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv')
        assert query_table_type(project, schema_unquoted, 'hackers_mv_mv') == "MaterializedView"

        # Step 3: Change model to view materialization and run with full refresh
        run_vars = {"use_view": True}
        results = run_dbt(
            ["run", "--full-refresh", "--log-level", "debug", "--vars", json.dumps(run_vars)]
        )
        assert len(results) == 2  # will include also a view for the other test.

        # Step 4: Assert that target table is now a view and internal MV no longer exists
        assert query_table_type(project, schema_unquoted, 'hackers_mv') == "View"
        # Verify that the internal materialized view (_mv) no longer exists
        assert query_table_type(project, schema_unquoted, 'hackers_mv_mv') is None

    def test_view_full_refresh_does_not_affect_existing_mv_with_mv_suffix(self, project):
        """
        1. create a base table via dbt seed
        2. create a regular view (hackers) and a materialized view (hackers_mv) with the same query
        4. force a full refresh on hackers (the view)
        5. verify that hackers still works and hackers_mv and hackers_mv_mv are still present
        """
        schema_unquoted = project.test_schema + "_custom_schema"

        # Step 1: Create base table via dbt seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create both models (view and materialized view)
        results = run_dbt()
        assert len(results) == 2

        # Verify both models were created correctly
        assert query_table_type(project, schema_unquoted, 'hackers') == "View"
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv')
        assert query_table_type(project, schema_unquoted, 'hackers_mv_mv') == "MaterializedView"

        # Verify data is present in both
        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers", fetch="all")
        assert result[0][0] == 3  # 3 engineering people in seed data

        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers_mv", fetch="all")
        assert result[0][0] == 3

        # Step 3: Force a full refresh on hackers (the view) only
        results = run_dbt(["run", "--full-refresh", "--select", "hackers"])
        assert len(results) == 1

        # Step 4: Verify that hackers still works
        assert query_table_type(project, schema_unquoted, 'hackers') == "View"
        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers", fetch="all")
        assert result[0][0] == 3

        # Verify that hackers_mv and hackers_mv_mv are still present and working
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv')
        assert query_table_type(project, schema_unquoted, 'hackers_mv_mv') == "MaterializedView"

        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers_mv", fetch="all")
        assert result[0][0] == 3

        # Insert new data and verify materialized view still captures it
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (7777,'Neo',30,'engineering');
        """
        )

        # Verify the new data appears in both view and materialized view
        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers", fetch="all")
        assert result[0][0] == 4

        result = project.run_sql(f"select count(*) from {schema_unquoted}.hackers_mv", fetch="all")
        assert result[0][0] == 4


class TestCatchup:
    """
    Comprehensive test suite for catchup flag behavior.
    Tests catchup functionality across initial creation, full refresh, and schema changes.
    Each test uses a unique schema to ensure proper isolation.
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
            "hackers.sql": MV_MODEL,
        }

    def test_initial_creation_catchup_disabled(self, project):
        """
        Test catchup=False on initial MV creation (original test from TestBasicMV).

        Scenario:
        1. Create seed data (3 engineering people)
        2. Create MV with catchup=False - table should be empty
        3. Insert new data (1 row)
        4. Verify table has only the 1 new row (not the 3 seed rows)
        """
        schema = quote_identifier(project.test_schema + "_catchup_initial")

        # Step 1: Create seed data
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # Step 2: Create the model with catchup disabled
        run_vars = {"schema_name": "catchup_initial", "catchup": False}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv", fetch="all")
        assert columns[0][1] == "Int32"

        check_relation_types(
            project.adapter,
            {
                "hackers_mv": "view",
                "hackers": "table",
            },
        )

        # Verify table is empty (no initial backfill)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 3: Insert new data
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
            """
        )

        # Step 4: Verify only the new row is present
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1

    def test_full_refresh_catchup_disabled(self, project):
        """
        Test that full refresh respects catchup=False flag (can_exchange=True path).

        Scenario:
        1. Create seed data (3 engineering people)
        2. Create MV with catchup=False initially - table should be empty
        3. Insert new data (1 row) - should have 1 row
        4. Full refresh with catchup=False and schema change
        5. Assert table is still empty (not backfilled during refresh)
        6. Insert new data after refresh
        7. Assert table only has the new post-refresh data
        """
        schema = quote_identifier(project.test_schema + "_catchup_full_refresh_disabled")

        # Step 1: Create seed data
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create MV with catchup=False
        run_vars = {"schema_name": "catchup_full_refresh_disabled", "catchup": False}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Verify table is empty (no initial backfill)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 3: Insert new data
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
            """
        )

        # Verify we now have 1 row (1 new engineering person, no seed data)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1

        # Step 4: Full refresh with catchup=False and schema change
        run_vars = {
            "schema_name": "catchup_full_refresh_disabled",
            "catchup": False,
            "use_updated_schema": True,
        }
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        # Step 5: Assert table was NOT backfilled (should be empty)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 6: Insert new data after refresh
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (5555,'Trinity',29,'engineering');
            """
        )

        # Step 7: Verify only the new post-refresh data is present
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1

    def test_full_refresh_catchup_enabled(self, project):
        """
        Control test: Verify default behavior still works (catchup=True on full refresh).

        Scenario:
        1. Create seed data (3 rows)
        2. Create MV with catchup=True (default)
        3. Insert new data (1 row) - should have 4 rows
        4. Full refresh with catchup=True (default) and schema change
        5. Assert table still has all historical data (backfilled during refresh)
        """
        schema = quote_identifier(project.test_schema + "_catchup_full_refresh_enabled")

        # Step 1: Create seed data
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create MV with default catchup=True
        run_vars = {"schema_name": "catchup_full_refresh_enabled"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Verify initial backfill worked
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 3

        # Step 3: Insert new data
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
            """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 4

        # Step 4: Full refresh with catchup=True (default) and schema change
        run_vars = {"schema_name": "catchup_full_refresh_enabled", "use_updated_schema": True}
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        # Step 5: Assert table was backfilled with all historical data
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 4  # All 4 rows should be present

    def test_catchup_toggle_between_runs(self, project):
        """
        Test switching catchup flag between deployments.

        Scenario:
        1. Create seed data (3 rows)
        2. Create MV with catchup=False - table should be empty
        3. Insert new data (1 row)
        4. Verify table has only the 1 new row
        5. Full refresh with catchup=True (by not specifying it) and schema change
        6. Assert table now has ALL historical data (3 seed + 1 previous insert = 4 total)
        """
        schema = quote_identifier(project.test_schema + "_catchup_toggle")

        # Step 1: Create seed data
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create MV with catchup=False
        run_vars = {"schema_name": "catchup_toggle", "catchup": False}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Verify table is empty (no initial backfill)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 3: Insert new data
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering');
            """
        )

        # Step 4: Verify only the new row is present
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1

        # Step 5: Full refresh with catchup=True (default) and schema change
        run_vars = {"schema_name": "catchup_toggle", "use_updated_schema": True}
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        # Step 6: Assert all historical data is now backfilled (3 seed + 1 insert)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 4

    def test_full_refresh_catchup_disabled_no_exchange(self, project):
        """
        Test that catchup=False works in the replace MV path (when can_exchange=False).
        This tests older ClickHouse versions or special database configurations.

        We mock can_exchange to force the replace path instead of atomic exchange.

        Scenario:
        1. Create seed data (3 rows)
        2. Create MV with catchup=False initially
        3. Insert data (1 row) - should have 1 row
        4. Mock can_exchange=False
        5. Full refresh with catchup=False and schema change
        6. Assert table was NOT backfilled (should be empty)
        7. Insert new data and verify MV still works
        """
        from unittest.mock import PropertyMock, patch

        schema = quote_identifier(project.test_schema + "_catchup_no_exchange")

        # Step 1: Create seed data
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create MV with catchup=False
        run_vars = {"schema_name": "catchup_no_exchange", "catchup": False}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Verify table is empty (no initial backfill)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 3: Insert data to verify behavior
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering');
            """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1

        # Step 4: Mock can_exchange to return False
        # This forces the code to use the clickhouse__replace_mv path instead of atomic exchange
        from dbt.adapters.clickhouse.relation import ClickHouseRelation

        # Step 5: Full refresh with catchup=False and schema change
        # The mock forces us into the replace MV path (line 274 in materialized_view.sql)
        run_vars = {
            "schema_name": "catchup_no_exchange",
            "catchup": False,
            "use_updated_schema": True,
        }
        with patch.object(
            ClickHouseRelation, 'can_exchange', new_callable=PropertyMock, return_value=False
        ):
            run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        # Step 6: Assert table was NOT backfilled (should be empty)
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 0

        # Step 7: Insert new data and verify MV still works
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (5555,'Trinity',29,'engineering');
            """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 1
