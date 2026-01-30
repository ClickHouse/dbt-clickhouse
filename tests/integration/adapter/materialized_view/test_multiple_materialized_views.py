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

MULTIPLE_MV_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(id)',
       on_schema_change=var('on_schema_change', 'ignore'),
       schema='custom_schema_for_multiple_mv',
) }}

{% if var('run_type', '') == '' %}

--mv1:begin
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
--mv1:end

union all

--mv2:begin
select
    id,
    name,
    -- sales people are not cool enough to have a hacker alias
    'N/A' as hacker_alias
from {{ source('raw', 'people') }}
where department = 'sales'
--mv2:end

{% elif var('run_type', '') == 'extended_schema' %}

--mv1:begin
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
--mv1:end

union all

--mv2:begin
select
    id,
    name,
    -- sales people are not cool enough to have a hacker alias
    'N/A' as hacker_alias,
    id as id2
from {{ source('raw', 'people') }}
where department = 'sales'
--mv2:end

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


class TestMultipleMV:
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
            "hackers.sql": MULTIPLE_MV_MODEL,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a model as a materialized view, selecting from the table created in (1)
        3. insert data into the base table and make sure it's there in the target table created in (2)
        """
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model
        run_dbt(["run"])
        assert len(results) == 1

        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv1", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv2", fetch="all")
        assert columns[0][1] == "Int32"

        with pytest.raises(Exception):
            columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv", fetch="all")

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
            values (4000,'Dave',40,'sales'), (9999,'Eugene',40,'engineering');
        """
        )

        result = project.run_sql(f"select * from {schema}.hackers order by id", fetch="all")
        assert result == [
            (1000, 'Alfie', 'N/A'),
            (1231, 'Dade', 'crash_override'),
            (2000, 'Bill', 'N/A'),
            (3000, 'Charlie', 'N/A'),
            (4000, 'Dave', 'N/A'),
            (6666, 'Ksenia', 'N/A'),
            (8888, 'Kate', 'acid burn'),
            (9999, 'Eugene', 'N/A'),
        ]


class TestUpdateMultipleMV:
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
            "hackers.sql": MULTIPLE_MV_MODEL,
        }

    def test_update_incremental(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
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
            f"select distinct hacker_alias from {schema}.hackers where name = 'Dade' order by hacker_alias",
            fetch="all",
        )
        assert len(result) == 2
        assert result[0][0] == "crash_override"
        assert result[1][0] == "zero cool"

        # As 'on_schema_change' is not defined, the new `id2` column will not be created in the destination table
        table_description_after_update = project.run_sql(
            f"DESCRIBE TABLE {schema}.hackers", fetch="all"
        )
        assert not any(col[0] == "id2" for col in table_description_after_update)

    # Test to verify that updates to multiple MVs also updates the destination table
    def test_update_incremental_on_schema_change_sync_all_columns(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
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
        table_description_after_update = project.run_sql(f"DESCRIBE {schema}.hackers", fetch="all")
        assert any(col[0] == "id2" and col[1] == "Int32" for col in table_description_after_update)

        # run again without extended schema, to make sure table is updated back without the id2 column
        run_dbt(["run", "--vars", json.dumps({"on_schema_change": "sync_all_columns"})])
        table_description_after_revert_update = project.run_sql(
            f"DESCRIBE TABLE {schema}.hackers", fetch="all"
        )
        assert not any(col[0] == "id2" for col in table_description_after_revert_update)

    def test_update_on_schema_change_fail(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
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
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
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
            f"select distinct hacker_alias from {schema}.hackers where name = 'Dade' order by hacker_alias",
            fetch="all",
        )
        print(result)
        assert len(result) == 2
        assert result[0][0] == "crash override"
        assert result[1][0] == "zero cool"
