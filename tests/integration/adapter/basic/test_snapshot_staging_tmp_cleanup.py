from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import run_dbt


class TestSnapshotStagingTmpCleanup(BaseSnapshotTimestamp):
    """Regression test for https://github.com/ClickHouse/dbt-clickhouse/issues/691

    A snapshot run that dies after creating the ``__dbt_tmp`` staging table but
    before ``post_snapshot`` cleans it up leaves an orphaned ``__dbt_tmp``. The
    next snapshot run must drop it and succeed instead of failing with
    ``TABLE_ALREADY_EXISTS``.
    """

    def test_orphaned_staging_tmp_is_dropped_before_recreate(self, project):
        # Initial run: creates the snapshot target relation.
        run_dbt(["seed"])
        run_dbt(["snapshot"])

        # Simulate a crashed prior run that left `ts_snapshot__dbt_tmp` behind.
        orphan = f"{project.test_schema}.ts_snapshot__dbt_tmp"
        project.run_sql(f"DROP TABLE IF EXISTS {orphan}")
        project.run_sql(
            f"CREATE TABLE {orphan} ENGINE = MergeTree ORDER BY tuple() AS SELECT 1 AS id"
        )

        # Without the fix this raises TABLE_ALREADY_EXISTS; with the fix the
        # orphan is dropped before recreate and the run succeeds.
        results = run_dbt(["snapshot"])
        assert len(results) == 1
