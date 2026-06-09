import pytest
from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseClone,
    BaseCloneNotPossible,
    BaseClonePossible,
    BaseCloneSameSourceAndTarget,
    BaseCloneSameTargetAndState,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture


class TestBaseClonePossible(BaseClonePossible):
    pass


class TestCloneNotPossible(BaseCloneNotPossible):
    pass


class TestCloneSameTargetAndState(BaseCloneSameTargetAndState):
    pass


class TestCloneSameSourceAndTarget(BaseCloneSameSourceAndTarget):
    # The upstream base class declares `models`/`snapshots` as plain methods (the
    # `@pytest.fixture` decorator is missing), and its test uses Postgres-style
    # three-part names and an engine-less `CREATE TABLE ... AS SELECT`. Re-declare
    # the fixtures properly and override the test with ClickHouse-valid SQL.
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_based_model.sql": fixtures.source_based_model_sql,
            "source_schema.yml": fixtures.source_schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def test_clone_same_source_and_target(self, project, unique_schema):
        """Cloning a relation onto itself should be skipped, not executed."""
        project.run_sql(f"DROP TABLE IF EXISTS {unique_schema}.source_table")
        project.run_sql(
            f"""
            CREATE TABLE {unique_schema}.source_table
            ENGINE = MergeTree
            ORDER BY id
            AS
            SELECT 1 AS id, 'test_data' AS name
            UNION ALL
            SELECT 2 AS id, 'more_data' AS name
            """
        )

        run_dbt(["run"])

        # Save state, then clone with the default target (same schema as state)
        self.copy_state(project.project_root)
        clone_args = ["clone", "--state", "state", "--full-refresh", "--log-level", "debug"]

        _, output = run_dbt_and_capture(clone_args)

        assert "skipping clone for relation" in output


class TestCloneNonMergeTreeBecomesView(BaseClone):
    """A non-MergeTree table cannot be cloned with `CLONE AS`, so dbt-core should
    fall back to materializing it as a view in the target schema."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "memory_model.sql": "{{ config(materialized='table', engine='Memory') }}\n"
            "select 1 as id, 'a' as name",
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    def test_non_mergetree_not_cloned_as_table(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)

        results = run_dbt(["run"])
        assert len(results) == 1
        self.copy_state(project.project_root)

        clone_args = ["clone", "--state", "state", "--target", "otherschema"]
        results = run_dbt(clone_args)
        assert len(results) == 1

        schema_relations = project.adapter.list_relations(
            database=project.database, schema=other_schema
        )
        # Memory is not a MergeTree-family engine, so the clone falls back to a view
        # rather than being cloned as a table.
        assert len(schema_relations) == 1
        assert all(r.type == "view" for r in schema_relations)
