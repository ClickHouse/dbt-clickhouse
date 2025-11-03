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
{{ config(
       materialized='materialized_view' if var('run_type', '') != 'view_conversion' else 'view',
       engine='MergeTree()',
       order_by='(id)',
       schema='catchup' if var('run_type', '') == 'catchup' else 'custom_schema',
        **({'catchup': False} if var('run_type', '') == 'catchup' else {})
) }}

{% if var('run_type', '') in ['', 'catchup', 'view_conversion'] %}
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
    end as hacker_alias
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
