"""
Test multiple materialized views with external target table.
This tests the new implementation where multiple MVs write to the same existing table
using the `materialization_target_table()` macro.
"""

import json

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import check_relation_types, run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
    query_table_type,
)

# Target table model - this creates the destination table that the MVs will write to
TARGET_TABLE_MODEL = """
{{ config(
       materialized='table',
       engine='MergeTree()',
       order_by='(id)',
       schema='custom_schema_for_multiple_mv'
) }}

SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias
WHERE 0  -- Creates empty table with correct schema
"""

# MV model 1 - engineering employees
MV_MODEL_1 = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema_for_multiple_mv'
) }}

{{ materialization_target_table(ref('hackers_target')) }}

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

# MV model 2 - sales employees
MV_MODEL_2 = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema_for_multiple_mv'
) }}

{{ materialization_target_table(ref('hackers_target')) }}

select
    id,
    name,
    -- sales people are not cool enough to have a hacker alias
    'N/A' as hacker_alias
from {{ source('raw', 'people') }}
where department = 'sales'
"""


class TestMultipleExternalTargetMV:
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
            "hackers_mv1.sql": MV_MODEL_1,
            "hackers_mv2.sql": MV_MODEL_2,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a target table model
        3. create two MVs pointing to the same target table
        4. insert data into the base table and make sure it reaches the target table
        """
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the models (target table + 2 MVs)
        results = run_dbt(["run"])
        assert len(results) == 3

        # Check the target table structure
        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers_target", fetch="all")
        assert columns[0][1] == "Int32"

        # Check both MVs exist
        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv1", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers_mv2", fetch="all")
        assert columns[0][1] == "Int32"

        check_relation_types(
            project.adapter,
            {
                "hackers_mv1": "materialized_view",
                "hackers_mv2": "materialized_view",
                "hackers_target": "table",
            },
        )

        # Verify catchup worked - all data from seed should be in target table
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 6  # 3 engineering + 3 sales

        # insert some data and make sure it reaches the target table
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (4000,'Dave',40,'sales'), (9999,'Eugene',40,'engineering');
        """
        )

        result = project.run_sql(f"select * from {schema}.hackers_target order by id", fetch="all")
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


class TestUpdateMultipleExternalTargetMVFullRefresh:
    """Test updating multiple MVs with external target using full refresh"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Use run_type variable to switch between normal and extended schema
        target_model = """
{{ config(
       materialized='table',
       engine='MergeTree()',
       order_by='(id)',
       schema='custom_schema_for_multiple_mv'
) }}

{% if var('run_type', '') == 'extended_schema' %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias,
    toInt32(0) AS id2
WHERE 0
{% else %}
SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias
WHERE 0
{% endif %}
"""
        mv_model_1 = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema_for_multiple_mv'
) }}

{{ materialization_target_table(ref('hackers_target')) }}

{% if var('run_type', '') == 'extended_schema' %}
select
    id,
    name,
    case
        when name like 'Dade' and age = 11 then 'zero cool'
        when name like 'Dade' and age != 11 then 'crash override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias,
    id as id2
from {{ source('raw', 'people') }}
where department = 'engineering'
{% else %}
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
{% endif %}
"""
        mv_model_2 = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema_for_multiple_mv'
) }}

{{ materialization_target_table(ref('hackers_target')) }}

{% if var('run_type', '') == 'extended_schema' %}
select
    id,
    name,
    'N/A' as hacker_alias,
    id as id2
from {{ source('raw', 'people') }}
where department = 'sales'
{% else %}
select
    id,
    name,
    'N/A' as hacker_alias
from {{ source('raw', 'people') }}
where department = 'sales'
{% endif %}
"""
        return {
            "hackers_target.sql": target_model,
            "hackers_mv1.sql": mv_model_1,
            "hackers_mv2.sql": mv_model_2,
        }

    def test_update_full_refresh(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
        # create our initial materialized views
        run_dbt(["seed"])
        run_dbt()

        # Verify initial setup
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 6

        # re-run dbt with full-refresh and extended schema
        run_vars = {"run_type": "extended_schema"}
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that we now have both of Dade's aliases in our target table
        result = project.run_sql(
            f"select distinct hacker_alias from {schema}.hackers_target where name = 'Dade' order by hacker_alias",
            fetch="all",
        )
        assert len(result) == 2
        assert result[0][0] == "crash override"
        assert result[1][0] == "zero cool"

        # Verify extended schema column exists
        table_description = project.run_sql(f"DESCRIBE TABLE {schema}.hackers_target", fetch="all")
        assert any(col[0] == "id2" and col[1] == "Int32" for col in table_description)


class TestUpdateMultipleExternalTargetMVQueryOnly:
    """Test updating MV query without schema changes"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Use run_type variable to switch between query logic (without schema change)
        mv_model_1 = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema_for_multiple_mv'
) }}

{{ materialization_target_table(ref('hackers_target')) }}

{% if var('run_type', '') == 'updated_query' %}
select
    id,
    name,
    case
        when name like 'Dade' and age = 11 then 'zero cool'
        when name like 'Dade' and age != 11 then 'crash override'
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
        when name like 'Dade' then 'crash_override'
        when name like 'Kate' then 'acid burn'
        else 'N/A'
    end as hacker_alias
from {{ source('raw', 'people') }}
where department = 'engineering'
{% endif %}
"""
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers_mv1.sql": mv_model_1,
            "hackers_mv2.sql": MV_MODEL_2,
        }

    def test_update_mv_query(self, project):
        """Test that MV query can be updated without schema changes (modify query)"""
        schema = quote_identifier(project.test_schema + "_custom_schema_for_multiple_mv")
        # create our initial materialized views
        run_dbt(["seed"])
        run_dbt()

        # Verify initial setup
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 6

        # re-run dbt with updated query (no schema change, should use modify query)
        run_vars = {"run_type": "updated_query"}
        run_dbt(["run", "--vars", json.dumps(run_vars)])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that we now have both of Dade's aliases in our target table
        # (the updated query differentiates based on age)
        result = project.run_sql(
            f"select distinct hacker_alias from {schema}.hackers_target where name = 'Dade' order by hacker_alias",
            fetch="all",
        )
        assert len(result) == 2
        assert result[0][0] == "crash_override"  # Note: original uses underscore
        assert result[1][0] == "zero cool"
