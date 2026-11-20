import os

import pytest
from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    BaseCloneNotPossible,
    BaseClonePossible,
    BaseCloneSameSourceAndTarget,
    BaseCloneSameTargetAndState,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture


def assert_cloned_data(project, other_schema):
    for name in ("seed", "table_model", "my_cool_snapshot"):
        assert project.run_sql(f"select count() from {other_schema}.{name}", fetch="one")[0] == 2


class ClonePossible(BaseClonePossible):
    def test_can_clone_true(self, project, unique_schema, other_schema):
        super().test_can_clone_true(project, unique_schema, other_schema)
        assert_cloned_data(project, other_schema)


class TestBaseClonePossible(ClonePossible):
    # `CLONE AS`, see TestCloneOnCluster for the `ON CLUSTER` path
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {**test_config, "cluster": ""}


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
)
class TestCloneOnCluster(ClonePossible):
    pass


# the `{uuid}` macro in the database path needs ON CLUSTER
class TestCloneReplicatedDatabase(TestCloneOnCluster):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }


class TestCloneNotPossible(BaseCloneNotPossible):
    def test_can_clone_false(self, project, unique_schema, other_schema):
        super().test_can_clone_false(project, unique_schema, other_schema)
        assert_cloned_data(project, other_schema)


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
        # a view fallback would have replaced the table with a view of itself
        (engine,) = project.run_sql(
            f"select engine from system.tables where database = '{unique_schema}' and name = 'source_based_model'",
            fetch="one",
        )
        # Cloud stores MergeTree as SharedMergeTree
        assert engine.endswith("MergeTree")
        assert project.run_sql(
            f"select count() from {unique_schema}.source_based_model", fetch="one"
        ) == (2,)
