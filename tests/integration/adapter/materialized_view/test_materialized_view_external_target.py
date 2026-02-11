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
    MV_MODEL,
    MV_MODEL_HACKERS,
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
    TARGET_MODEL,
    TARGET_MODEL_HACKERS,
)


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
            "hackers_target.sql": TARGET_MODEL_HACKERS,
            "hackers.sql": MV_MODEL_HACKERS,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a target table model
        3. create a model as a materialized view pointing to the target table
        4. insert data into the base table and make sure it's there in the target table
        """
        schema = quote_identifier(project.test_schema)
        run_vars = {"target_table": "hackers_target"}
        run_dbt(["seed", "--vars", json.dumps(run_vars)])
        run_dbt(["run", "--vars", json.dumps(run_vars)])

        check_relation_types(
            project.adapter,
            {
                "hackers": "materialized_view",  # The MV appears as a materialized_view
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
            "hackers_target.sql": TARGET_MODEL_HACKERS,
            "hackers.sql": MV_MODEL_HACKERS,
        }

    def test_disabled_catchup(self, project):
        """
        1. create a base table via dbt seed
        2. create a model with catchup disabled as a materialized view
        3. insert data into the base table and make sure only new data is in the target table
        """
        schema = quote_identifier(project.test_schema)
        run_vars = {"catchup": False, "target_table": "hackers_target"}
        run_dbt(["seed", "--vars", json.dumps(run_vars)])
        run_dbt(["run", "--vars", json.dumps(run_vars)])

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
        target_model = TARGET_MODEL_HACKERS
        mv_model = """
{{ config(
       materialized='materialized_view'
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
    id as extra_col
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
        schema = quote_identifier(project.test_schema)
        # create our initial materialized view
        run_dbt(["seed"])
        run_dbt()

        # Verify initial setup
        result = project.run_sql(f"select count(*) from {schema}.hackers_target", fetch="all")
        assert result[0][0] == 3

        # re-run dbt with full-refresh and extended schema
        # Disable repopulate_from_mvs_on_full_refresh to avoid double-insert:
        # repopulation uses old MV SQL while catchup uses the new SQL
        run_vars = {
            "run_type": "extended_schema",
            "enable_repopulate_from_mvs_on_full_refresh": False,
        }
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
        assert any(col[0] == "extra_col" and col[1] == "Int32" for col in table_description)


class TestExternalTargetMVTargetChange:
    """Test validation when target table changes without full-refresh"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target_a.sql": TARGET_MODEL_HACKERS,
            "hackers_target_b.sql": TARGET_MODEL_HACKERS,
            "hackers_mv.sql": MV_MODEL_HACKERS,
        }

    def test_target_change_validation(self, project):
        """
        Test that changing the target table without --full-refresh fails with appropriate error
        """
        schema = quote_identifier(project.test_schema)
        schema_unquoted = project.test_schema

        # Step 1: Create seed and initial MV pointing to target_a
        run_vars = {"target_table": "hackers_target_a"}
        run_dbt(["seed", "--vars", json.dumps(run_vars)])
        results = run_dbt(["run", "--vars", json.dumps(run_vars)])
        assert len(results) == 3  # Two target tables + MV

        # Verify initial MV is pointing to target_a
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_a", fetch="all")
        assert result[0][0] == 3  # 3 engineering people from seed

        # target_b should be empty since MV is not writing to it
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_b", fetch="all")
        assert result[0][0] == 0

        # Verify MV target from system.tables
        mv_target_query = f"""
            select replaceRegexpOne(create_table_query, '.*TO\\\\s+`?([^`\\\\s(]+)`?\\\\.`?([^`\\\\s(]+)`?.*', '\\\\1.\\\\2') as target_table
            from system.tables
            where database = '{schema_unquoted}'
              and name = 'hackers_mv'
              and engine = 'MaterializedView'
        """
        result = project.run_sql(mv_target_query, fetch="all")
        assert len(result) == 1
        assert result[0][0] == f"{schema_unquoted}.hackers_target_a"

        # Step 2: Change MV to point to target_b without --full-refresh
        # This should FAIL with validation error
        run_vars = {"target_table": "hackers_target_b"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)], expect_pass=False)

        # Verify the error message contains expected text
        assert len(results) == 3
        # Find the hackers_mv result
        mv_result = next((r for r in results if r.node.name == "hackers_mv"), None)
        assert mv_result.status == "error"
        assert (
            f'Current target is "{schema_unquoted}.hackers_target_a", but model references "{schema_unquoted}.hackers_target_b"'
            in mv_result.message
        )

        # Verify MV still points to target_a (unchanged)
        result = project.run_sql(mv_target_query, fetch="all")
        assert result[0][0] == f"{schema_unquoted}.hackers_target_a"

        # Step 3: Change MV to point to target_b WITH --full-refresh
        # This should SUCCEED (only refresh the MV, not the target tables)
        results = run_dbt(
            ["run", "--full-refresh", "--select", "hackers_mv", "--vars", json.dumps(run_vars)]
        )
        assert len(results) == 1

        # Verify MV now points to target_b
        result = project.run_sql(mv_target_query, fetch="all")
        assert len(result) == 1
        assert result[0][0] == f"{schema_unquoted}.hackers_target_b"

        # Verify target_b now has data (catchup should have backfilled)
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_b", fetch="all")
        assert result[0][0] == 3

        # target_a should still have old data (unchanged)
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_a", fetch="all")
        assert result[0][0] == 3

        # Step 4: Insert new data and verify it goes to target_b
        project.run_sql(
            f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values (1232,'Dade',16,'engineering');
        """
        )

        # New data should appear in target_b (the current target)
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_b", fetch="all")
        assert result[0][0] == 4

        # target_a should remain unchanged
        result = project.run_sql(f"select count(*) from {schema}.hackers_target_a", fetch="all")
        assert result[0][0] == 3


def _create_outside_mv(project, schema):
    """Create a materialized view directly in ClickHouse (outside dbt)."""
    project.run_sql(
        f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.outside_mv_sales
        TO {schema}.employees_target
        AS SELECT
            id,
            name,
            department
        FROM {schema}.people
        WHERE department = 'sales'
        """
    )


class TestOutsideMVsNotForceOnSchemaChangeFailInTable:
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
        }

    def test_outside_mv_does_not_force_on_schema_change_fail_in_table_models(self, project):
        """Outside-only MVs should NOT make on_schema_change='fail' default."""
        schema = quote_identifier(project.test_schema)

        run_dbt(["seed"])
        run_dbt(["run"])

        _create_outside_mv(project, project.test_schema)

        # Run with schema change — should succeed since outside MV is not in dbt's graph
        run_vars = {"run_type": "extended_schema"}
        run_dbt(["run", "--vars", json.dumps(run_vars)])

        # Table should have been rebuilt with the new column
        columns = project.run_sql(f"DESCRIBE TABLE {schema}.employees_target", fetch="all")
        column_names = [col[0] for col in columns]
        assert "extra_col" in column_names


class TestMVWithExplicitTargetForcesOnSchemaChangeFailInTable:
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
            "employees_mv_engineering.sql": MV_MODEL,
        }

    def test_mv_with_explicit_target_forces_on_schema_change_fail_in_table(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--vars", json.dumps({"target_table": "employees_target"})])

        _create_outside_mv(project, project.test_schema)

        run_vars = {"run_type": "extended_schema"}
        results = run_dbt(["run", "--vars", json.dumps(run_vars)], expect_pass=False)

        target_result = next(r for r in results if r.node.name == "employees_target")
        assert target_result.status == "error"
        assert (
            "The source and target schemas on this table model are out of sync"
            in target_result.message
        )


class TestRepopulateOnlyDbtMVsNotOutside:
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
            "employees_mv_engineering.sql": MV_MODEL,
        }

    def test_repopulate_excludes_outside_mv(self, project):
        schema = quote_identifier(project.test_schema)

        run_dbt(["seed"])
        run_dbt(
            ["run", "--vars", json.dumps({"target_table": "employees_target", "catchup": False})]
        )

        _create_outside_mv(project, project.test_schema)

        # Insert data captured by both MVs
        project.run_sql(
            f"""
            INSERT INTO {schema}.people ("id", "name", "age", "department")
                VALUES (4001, 'NewEngineer', 30, 'engineering')
            """
        )
        project.run_sql(
            f"""
            INSERT INTO {schema}.people ("id", "name", "age", "department")
                VALUES (4002, 'NewSales', 25, 'sales')
            """
        )

        # Full refresh with repopulation
        run_vars = {"enable_repopulate_from_mvs_on_full_refresh": True}
        results = run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])
        assert len(results) == 2

        # Only engineering (dbt MV) should be repopulated; sales (outside MV) should not
        result = project.run_sql(
            f"SELECT department, count(*) as cnt FROM {schema}.employees_target "
            f"GROUP BY department ORDER BY department",
            fetch="all",
        )
        assert len(result) == 1
        assert result[0][0] == "engineering"
        # 3 from seed + 1 inserted
        assert result[0][1] == 4


class TestRepopulateWithOnlyOutsideMV:
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
        }

    def test_repopulate_not_triggered_with_only_outside_mv(self, project):
        schema = quote_identifier(project.test_schema)

        run_dbt(["seed"])
        run_dbt(["run"])

        _create_outside_mv(project, project.test_schema)

        project.run_sql(
            f"""
            INSERT INTO {schema}.people ("id", "name", "age", "department")
                VALUES (4002, 'NewSales', 25, 'sales')
            """
        )

        # Full refresh — no dbt MVs means no repopulation
        run_vars = {"enable_repopulate_from_mvs_on_full_refresh": True}
        run_dbt(["run", "--full-refresh", "--vars", json.dumps(run_vars)])

        result = project.run_sql(f"SELECT count(*) FROM {schema}.employees_target", fetch="all")
        assert result[0][0] == 0


class TestAliasedMVRecognizedAsDbtManaged:
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
            "employees_mv_engineering.sql": MV_MODEL,
        }

    def test_aliased_mv_forces_on_schema_change_fail(self, project):
        """MV with custom alias should be recognized as dbt-managed and force on_schema_change='fail'."""
        run_dbt(["seed"])
        run_dbt(
            [
                "run",
                "--vars",
                json.dumps({"target_table": "employees_target", "alias": "engineering_mv_custom"}),
            ]
        )

        # Run with schema change — should fail because the aliased MV is recognized as dbt-managed
        results = run_dbt(
            [
                "run",
                "--vars",
                json.dumps(
                    {
                        "target_table": "employees_target",
                        "alias": "engineering_mv_custom",
                        "run_type": "extended_schema",
                    }
                ),
            ],
            expect_pass=False,
        )

        target_result = next(r for r in results if r.node.name == "employees_target")
        assert target_result.status == "error"
        assert (
            "The source and target schemas on this table model are out of sync"
            in target_result.message
        )
