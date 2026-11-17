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

    def test_leftover_snapshot_upsert_is_not_reused(self, project):
        """Regression test for https://github.com/ClickHouse/dbt-clickhouse/issues/600

        A snapshot run that dies after filling ``__snapshot_upsert`` leaves the
        table behind. The next run must not keep its rows, or every snapshot
        row ends up duplicated.
        """
        run_dbt(["seed"])
        run_dbt(["snapshot"])

        snapshot = f"{project.test_schema}.ts_snapshot"
        leftover = f"{snapshot}__snapshot_upsert"
        project.run_sql(f"CREATE TABLE {leftover} AS {snapshot}")
        project.run_sql(f"INSERT INTO {leftover} SELECT * FROM {snapshot}")

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        rows, scd_ids = project.run_sql(
            f"select count(), uniqExact(dbt_scd_id) from {snapshot}", fetch="one"
        )
        assert (rows, scd_ids) == (10, 10)
