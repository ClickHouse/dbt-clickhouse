"""
test materialized view creation.  This is ClickHouse specific, which has a significantly different implementation
of materialized views from PostgreSQL or Oracle
"""

import json

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import check_relation_types, run_dbt

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
1000,Alfie,10,sales
2000,Bill,20,sales
3000,Charlie,30,sales
""".lstrip()

# This model is parameterized, in a way, by the "run_type" dbt project variable
# This is to be able to switch between different model definitions within
# the same test run and allow us to test the evolution of a materialized view
MV_MODEL = """
{%- set run_type = var('run_type', '') -%}
{{ config(
       materialized='materialized_view' if run_type != 'view_conversion' else 'view',
       engine='MergeTree()',
       order_by='(id)',
       on_schema_change=var('on_schema_change', 'ignore'),
       schema='catchup' if run_type == 'catchup' else ('catchup_on_full_refresh' if run_type == 'catchup_on_full_refresh' else 'custom_schema'),
       catchup=False if run_type == 'catchup' else True,
       catchup_on_full_refresh=False if run_type == 'catchup_on_full_refresh' else True
) }}

{% if var('run_type', '') in ['', 'catchup', 'catchup_on_full_refresh', 'view_conversion'] %}
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

{% elif var('run_type', '') == 'extended_schema' %}
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

SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
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

    def test_disabled_catchup(self, project):
        """
        1. create a base table via dbt seed
        2. create a model with catchup disabled as a materialized view, selecting from the table created in (1)
        3. insert data into the base table and make sure it's there in the target table created in (2)
        """
        schema = quote_identifier(project.test_schema + "_catchup")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model with catchup disabled
        run_vars = {"run_type": "catchup"}
        run_dbt(["run", "--vars", json.dumps(run_vars)])
        # check that we only have the new row, without the historical data
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
        assert result[0][0] == 1


# View model for testing view + MV coexistence
VIEW_MODEL_HACKERS = """
{{ config(
       materialized='view',
       schema='custom_schema'
) }}

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
"""


def query_table_type(project, schema, table):
    table_type = project.run_sql(
        f"""
        select engine from system.tables where database = '{schema}' and name = '{table}'
    """,
        fetch="all",
    )
    return table_type[0][0] if len(table_type) > 0 else None


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
        run_vars = {"run_type": "extended_schema"}
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
        run_vars = {"run_type": "extended_schema", "on_schema_change": "sync_all_columns"}
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
        run_vars = {"run_type": "extended_schema", "on_schema_change": "fail"}
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
        run_vars = {"run_type": "extended_schema"}
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
        run_vars = {"run_type": "view_conversion"}
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

    def test_catchup_on_full_refresh(self, project):
        """
        Test catchup_on_full_refresh=False skips backfilling on explicit full refresh:
        1. Create base table with seed data (3 engineering people)
        2. Create materialized view with catchup_on_full_refresh=False (should still backfill initially)
        3. Add new data to base table
        4. Run --full-refresh with catchup_on_full_refresh=False
        5. Verify table is empty after full refresh (no backfill)
        6. Verify MV continues to work for new data
        """
        schema = quote_identifier(project.test_schema + "_catchup_on_full_refresh")

        # Step 1: Create base table via dbt seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create the model with catchup_on_full_refresh=False
        # Note: Initial creation should still respect default catchup=True
        run_vars = {"run_type": "catchup_on_full_refresh"}
        results = run_dbt(["run", "--select", "hackers_mv", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Verify initial backfill happened (catchup_on_full_refresh only affects --full-refresh)
        result = project.run_sql(f"select count(*) from {schema}.hackers_mv", fetch="all")
        assert result[0][0] == 3  # Initial data should be there

        # Step 3: Add new data
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (1232,'Dade',16,'engineering'), (7777,'Neo',30,'engineering');
            """
        )

        # Verify MV captured new data
        result = project.run_sql(f"select count(*) from {schema}.hackers_mv", fetch="all")
        assert result[0][0] == 5

        # Step 4: Run full refresh with catchup_on_full_refresh=False
        run_vars = {"run_type": "catchup_on_full_refresh"}
        results = run_dbt(["run", "--select", "hackers_mv", "--full-refresh", "--vars", json.dumps(run_vars)])
        assert len(results) == 1

        # Step 5: Verify table is empty after full refresh (no backfill)
        result = project.run_sql(f"select count(*) from {schema}.hackers_mv", fetch="all")
        assert result[0][0] == 0  # No historical data should be backfilled

        # Step 6: Add new data and verify MV still works
        project.run_sql(
            f"""
            insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
                values (8888,'Trinity',28,'engineering');
            """
        )

        # Verify only new data is captured
        result = project.run_sql(f"select count(*) from {schema}.hackers_mv", fetch="all")
        assert result[0][0] == 1  # Only the new row after full refresh

