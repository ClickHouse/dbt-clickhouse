import os
from collections import Counter

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
from dbt_common.exceptions import DbtRuntimeError

from tests.integration.adapter.helpers import MigratedTestArgs

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))

# dbt core v2's `clone` has no --resource-type flag; the resource_type: selector
# narrows the selection to models the same way under both engines.
MODELS_ONLY_ARGS = (
    ["--select", "resource_type:model"] if IS_CORE_V2 else ["--resource-type", "model"]
)

# dbt core v2 words the missing-state error differently (dbt1007).
NO_STATE_ERROR = (
    "Use --state or --defer-state to provide a state manifest for cloning"
    if IS_CORE_V2
    else "--state or --defer-state are required for deferral, but neither was provided"
)


def run_clone_scenario(project, other_schema, expected_relation_types):
    """The upstream BaseClonePossible / BaseCloneNotPossible scenario, with the
    models-only step expressed through MODELS_ONLY_ARGS."""
    clone_args = ["clone", "--state", "state", "--target", "otherschema"]

    results = run_dbt(clone_args)
    assert len(results) == 4

    schema_relations = project.adapter.list_relations(
        database=project.database, schema=other_schema
    )
    assert Counter(r.type for r in schema_relations) == expected_relation_types

    # objects already exist, so this is a no-op
    results = run_dbt(clone_args)
    assert len(results) == 4
    assert all("no-op" in r.message.lower() for r in results)

    # recreate all objects
    results = run_dbt([*clone_args, "--full-refresh"])
    assert len(results) == 4

    # select only models this time
    results = run_dbt([*clone_args, *MODELS_ONLY_ARGS])
    assert len(results) == 2
    assert all("no-op" in r.message.lower() for r in results)


class TestBaseClonePossible(MigratedTestArgs, BaseClonePossible):
    def test_can_clone_true(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)
        run_clone_scenario(project, other_schema, Counter({"table": 3, "view": 1}))

    def test_clone_no_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)
        with pytest.raises(DbtRuntimeError, match=NO_STATE_ERROR):
            run_dbt(["clone", "--target", "otherschema"])


class TestCloneNotPossible(MigratedTestArgs, BaseCloneNotPossible):
    def test_can_clone_false(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)
        run_clone_scenario(project, other_schema, Counter({"view": 4}))


class TestCloneSameTargetAndState(MigratedTestArgs, BaseCloneSameTargetAndState):
    def test_clone_same_target_and_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root)
        clone_args = ["clone", "--defer", "--state", "target"]
        results, output = run_dbt_and_capture(clone_args, expect_pass=False)
        if IS_CORE_V2:
            # dbt core v2 compares the --state path as typed with the canonical target
            # directory, so its WarnStateTargetEqual never fires for `--state target`.
            # The run still fails the way Python's does: the never-run snapshot is
            # cloned onto itself through the view fallback.
            errors = [r.unique_id for r in results if r.status == "error"]
            assert errors == ["snapshot.test.my_cool_snapshot"]
        else:
            assert "The state and target directories are the same: 'target'" in output


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


class TestCloneNonMergeTreeBecomesView(MigratedTestArgs, BaseClone):
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
