"""
Test materialized view creation with external target table.
This tests the new implementation where the MV writes to an existing table
using the `materialization_target_table()` macro.
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

# Target table model - this creates the destination table that the MV will write to
TARGET_TABLE_MODEL = """
{{ config(
       materialized='table',
       engine='MergeTree()',
       order_by='(id)',
       schema='custom_schema'
) }}

SELECT
    toInt32(0) AS id,
    '' AS name,
    '' AS hacker_alias
WHERE 0  -- Creates empty table with correct schema
"""

# MV model that writes to the external target table
MV_MODEL = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema'
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

# MV model without catchup
MV_MODEL_NO_CATCHUP = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema',
       catchup=False
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


class TestBasicExternalTargetMV:
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
        3. create a model as a materialized view pointing to the target table
        4. insert data into the base table and make sure it's there in the target table
        """
        schema = quote_identifier(project.test_schema + "_custom_schema")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the models (target table + MV)
        results = run_dbt()
        assert len(results) == 2

        # Check the target table structure
        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers_target", fetch="all")
        assert columns[0][1] == "Int32"

        # Check the MV exists (it's named after the model, not with _mv suffix in external target mode)
        columns = project.run_sql(f"DESCRIBE {schema}.hackers", fetch="all")
        assert columns[0][1] == "Int32"

        check_relation_types(
            project.adapter,
            {
                "hackers": "view",  # The MV appears as a view
                "hackers_target": "table",
            },
        )

        # Verify catchup worked - data from seed should be in target table
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 3  # 3 engineering people in seed data

        # insert some data and make sure it reaches the target table
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 4


class TestExternalTargetMVDisabledCatchup:
    """Separate class to test disabled catchup with clean schema"""

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
            "hackers.sql": MV_MODEL_NO_CATCHUP,
        }

    def test_disabled_catchup(self, project):
        """
        1. create a base table via dbt seed
        2. create a model with catchup disabled as a materialized view
        3. insert data into the base table and make sure only new data is in the target table
        """
        schema = quote_identifier(project.test_schema + "_custom_schema")
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql("DESCRIBE TABLE people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model with catchup disabled
        run_dbt()

        # Check the target table and MV exist
        columns = project.run_sql(f"DESCRIBE TABLE {schema}.hackers_target", fetch="all")
        assert columns[0][1] == "Int32"

        columns = project.run_sql(f"DESCRIBE {schema}.hackers", fetch="all")
        assert columns[0][1] == "Int32"

        check_relation_types(
            project.adapter,
            {
                "hackers": "view",
                "hackers_target": "table",
            },
        )

        # check that target table is empty (no catchup)
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 0

        # insert some data and make sure it reaches the target table
        project.run_sql(
            f"""
           insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
               values (1232,'Dade',16,'engineering'), (9999,'eugene',40,'malware');
           """
        )

        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 1


class TestUpdateExternalTargetMVFullRefresh:
    """Test full refresh behavior"""

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
            "hackers_mv.sql": MV_MODEL.replace("hackers_target", "hackers_mv_target"),
            "hackers_mv_target.sql": TARGET_TABLE_MODEL.replace(
                "hackers_target", "hackers_mv_target"
            ),
            "hackers.sql": VIEW_MODEL_HACKERS,
        }

    def test_update_full_refresh(self, project):
        schema = quote_identifier(project.test_schema + "_custom_schema")
        schema_unquoted = project.test_schema + "_custom_schema"
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # Verify MV was created correctly
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv_target')
        assert query_table_type(project, schema_unquoted, 'hackers_mv') == "MaterializedView"

        # re-run dbt with full-refresh
        run_dbt(["run", "--full-refresh"])

        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',11,'engineering'), (9999,'eugene',40,'malware');
        """
        )

        # assert that we have data in our target table
        result = project.run_sql(
            f"select count(*) from {schema}.hackers_mv_target",
            fetch="all",
        )
        assert result[0][0] == 4  # 3 from seed + 1 from insert (only 'engineering' department)


class TestViewRefreshDoesNotAffectExternalTargetMV:
    """Test that view full refresh doesn't affect existing MV with external target"""

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
            "hackers_mv.sql": MV_MODEL.replace("hackers_target", "hackers_mv_target"),
            "hackers_mv_target.sql": TARGET_TABLE_MODEL.replace(
                "hackers_target", "hackers_mv_target"
            ),
            "hackers.sql": VIEW_MODEL_HACKERS,
        }

    def test_view_full_refresh_does_not_affect_existing_external_target_mv(self, project):
        """
        1. create a base table via dbt seed
        2. create a regular view (hackers) and a materialized view (hackers_mv) with external target
        3. force a full refresh on hackers (the view)
        4. verify that hackers still works and hackers_mv and hackers_mv_target are still present
        """
        schema = quote_identifier(project.test_schema + "_custom_schema")
        schema_unquoted = project.test_schema + "_custom_schema"

        # Step 1: Create base table via dbt seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Step 2: Create both models (view and materialized view)
        results = run_dbt()
        assert len(results) == 4

        # Verify both models were created correctly
        assert query_table_type(project, schema_unquoted, 'hackers') == "View"
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv_target')
        assert query_table_type(project, schema_unquoted, 'hackers_mv') == "MaterializedView"

        # Verify data is present
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 3  # 3 engineering people in seed data

        result = project.run_sql(f"select count(*) from {schema}.hackers_mv_target", fetch="all")
        assert result[0][0] == 3

        # Step 3: Force a full refresh on hackers (the view) only
        results = run_dbt(["run", "--full-refresh", "--select", "hackers"])
        assert len(results) == 1

        # Step 4: Verify that hackers still works
        assert query_table_type(project, schema_unquoted, 'hackers') == "View"
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 3

        # Verify that hackers_mv and hackers_mv_target are still present and working
        assert "MergeTree" in query_table_type(project, schema_unquoted, 'hackers_mv_target')
        assert query_table_type(project, schema_unquoted, 'hackers_mv') == "MaterializedView"

        result = project.run_sql(f"select count(*) from {schema}.hackers_mv_target", fetch="all")
        assert result[0][0] == 3

        # Insert new data and verify materialized view still captures it
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (7777,'Neo',30,'engineering');
        """
        )

        # Verify the new data appears in both view and target table
        result = project.run_sql(f"select count(*) from {schema}.hackers", fetch="all")
        assert result[0][0] == 4

        result = project.run_sql(f"select count(*) from {schema}.hackers_mv_target", fetch="all")
        assert result[0][0] == 4


class TestUpdateExternalTargetMVWithSchemaChange:
    """Test full refresh with schema changes"""

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
       schema='custom_schema'
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
        mv_model = """
{{ config(
       materialized='materialized_view',
       schema='custom_schema'
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
        return {
            "hackers_target.sql": target_model,
            "hackers.sql": mv_model,
        }

    def test_update_full_refresh_with_schema_change(self, project):
        """Test full refresh when schema changes"""
        schema = quote_identifier(project.test_schema + "_custom_schema")
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # Verify initial setup
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 3

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
