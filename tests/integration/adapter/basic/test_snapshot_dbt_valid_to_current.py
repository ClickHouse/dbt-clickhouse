from datetime import datetime

import pytest

from dbt.tests.util import relation_from_name, run_dbt


seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

seeds_added_csv = (
    seeds_base_csv
    + """11,Mateo,2014-09-07T17:04:27
12,Julian,2000-02-04T11:48:30
""".lstrip()
)

# Seed with one row deleted (id=10 removed)
seeds_deleted_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
""".lstrip()

# Seed with one row updated (id=1 name changed, some_date changed for timestamp strategy)
seeds_updated_csv = """
id,name,some_date
1,Easton_updated,2020-01-01T00:00:00
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

# Timestamp strategy snapshot with dbt_valid_to_current configured
ts_snapshot_valid_to_current_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
        dbt_valid_to_current="toDateTime('2100-01-01 00:00:00')",
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

# Check strategy snapshot with dbt_valid_to_current configured
cc_snapshot_valid_to_current_sql = """
{% snapshot cc_snapshot %}
    {{ config(
        strategy='check',
        unique_key='id',
        check_cols='all',
        target_database=database,
        target_schema=schema,
        dbt_valid_to_current="toDateTime('2100-01-01 00:00:00')",
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()

# Check strategy snapshot with dbt_valid_to_current AND hard_deletes='invalidate'
cc_snapshot_valid_to_current_hard_deletes_sql = """
{% snapshot cc_snapshot_hd %}
    {{ config(
        strategy='check',
        unique_key='id',
        check_cols='all',
        target_database=database,
        target_schema=schema,
        dbt_valid_to_current="toDateTime('2100-01-01 00:00:00')",
        hard_deletes='invalidate',
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


def get_row_count(project, snapshot_name):
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select count(*) from {relation}", fetch="one")
    return result[0]


def get_valid_to_values(project, snapshot_name):
    """Return list of dbt_valid_to values for all rows, ordered by id and dbt_valid_from."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select id, dbt_valid_to from {relation} order by id, dbt_valid_from",
        fetch="all",
    )
    return result


def get_current_row_count(project, snapshot_name):
    """Count rows where dbt_valid_to equals the configured current value."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select count(*) from {relation} where dbt_valid_to = toDateTime('2100-01-01 00:00:00')",
        fetch="one",
    )
    return result[0]


def get_expired_row_count(project, snapshot_name):
    """Count rows where dbt_valid_to is a real timestamp (not the current sentinel)."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select count(*) from {relation} where dbt_valid_to != toDateTime('2100-01-01 00:00:00')",
        fetch="one",
    )
    return result[0]


def get_deleted_row_count(project, snapshot_name):
    """Count rows for id=10 (the deleted row) to verify it was invalidated."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select count(*) from {relation} where id = 10",
        fetch="one",
    )
    return result[0]


def get_deleted_row_valid_to(project, snapshot_name):
    """Get dbt_valid_to for the deleted row (id=10) to verify it's not the sentinel."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select dbt_valid_to from {relation} where id = 10",
        fetch="one",
    )
    return result[0] if result else None


def get_rows_for_id(project, snapshot_name, row_id):
    """Return all rows for a specific id, ordered by dbt_valid_from (oldest first)."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select id, name, dbt_valid_from, dbt_valid_to from {relation} where id = {row_id} order by dbt_valid_from",
        fetch="all",
    )
    return result


def get_row_count_for_id(project, snapshot_name, row_id):
    """Count rows for a specific id."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select count(*) from {relation} where id = {row_id}",
        fetch="one",
    )
    return result[0]


class TestSnapshotTimestampDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "updated.csv": seeds_updated_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": ts_snapshot_valid_to_current_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_valid_to_current_timestamp"}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_snapshot_valid_to_current_timestamp(self, project):
        # Seed the base data (10 rows)
        results = run_dbt(["seed"])
        assert len(results) == 3

        # --- First snapshot run ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should have 10 rows, all with dbt_valid_to = '2100-01-01' (not NULL)
        assert get_row_count(project, "ts_snapshot") == 10
        assert get_current_row_count(project, "ts_snapshot") == 10
        assert get_expired_row_count(project, "ts_snapshot") == 0

        # --- Second snapshot run (no changes) ---
        # This is the critical test: without the fix, the second run would
        # fail to find current records (because it looks for dbt_valid_to IS NULL
        # but they have '2100-01-01'), causing duplicate inserts.
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should still have exactly 10 rows - no duplicates
        assert get_row_count(project, "ts_snapshot") == 10
        assert get_current_row_count(project, "ts_snapshot") == 10
        assert get_expired_row_count(project, "ts_snapshot") == 0

        # --- Third snapshot run with new data ---
        # Point at the "added" seed so the snapshot sees 2 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        assert len(results) == 1

        # Should now have 12 rows (10 original + 2 new), all current
        assert get_row_count(project, "ts_snapshot") == 12
        assert get_current_row_count(project, "ts_snapshot") == 12
        assert get_expired_row_count(project, "ts_snapshot") == 0

        # --- Fourth snapshot run with updated data ---
        # Point at the "updated" seed so id=1 has a new name and later some_date
        results = run_dbt(
            ["--no-partial-parse", "snapshot", "--vars", "seed_name: updated"]
        )
        assert len(results) == 1

        # Should now have 13 rows (12 + 1 new version for updated id=1)
        assert get_row_count(project, "ts_snapshot") == 13

        # 12 rows should still be current (dbt_valid_to = sentinel)
        assert get_current_row_count(project, "ts_snapshot") == 12

        # 1 row should be expired (the old version of id=1)
        assert get_expired_row_count(project, "ts_snapshot") == 1

        # Verify id=1 has exactly 2 rows: old expired + new current
        id_1_rows = get_rows_for_id(project, "ts_snapshot", 1)
        assert len(id_1_rows) == 2, f"Expected 2 rows for id=1, got {len(id_1_rows)}"

        # First row (oldest) should be the original version, now expired
        old_row = id_1_rows[0]
        assert old_row[1] == "Easton"  # original name
        # dbt_valid_to should be a real timestamp (not the sentinel)
        assert old_row[3] != datetime(2100, 1, 1, 0, 0)

        # Second row (newest) should be the updated version, still current
        new_row = id_1_rows[1]
        assert new_row[1] == "Easton_updated"  # updated name
        assert new_row[3] == datetime(2100, 1, 1, 0, 0)  # dbt_valid_to = sentinel


class TestSnapshotCheckDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "updated.csv": seeds_updated_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_snapshot.sql": cc_snapshot_valid_to_current_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_valid_to_current_check"}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_snapshot_valid_to_current_check(self, project):
        # Seed the base data (10 rows)
        results = run_dbt(["seed"])
        assert len(results) == 3

        # --- First snapshot run ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should have 10 rows, all with dbt_valid_to = '2100-01-01' (not NULL)
        assert get_row_count(project, "cc_snapshot") == 10
        assert get_current_row_count(project, "cc_snapshot") == 10
        assert get_expired_row_count(project, "cc_snapshot") == 0

        # --- Second snapshot run (no changes) ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should still have exactly 10 rows - no duplicates
        assert get_row_count(project, "cc_snapshot") == 10
        assert get_current_row_count(project, "cc_snapshot") == 10
        assert get_expired_row_count(project, "cc_snapshot") == 0

        # --- Third snapshot run with new data ---
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        assert len(results) == 1

        # Should now have 12 rows (10 original + 2 new), all current
        assert get_row_count(project, "cc_snapshot") == 12
        assert get_current_row_count(project, "cc_snapshot") == 12
        assert get_expired_row_count(project, "cc_snapshot") == 0

        # --- Fourth snapshot run with updated data ---
        # Point at the "updated" seed so id=1 has a changed name
        results = run_dbt(
            ["--no-partial-parse", "snapshot", "--vars", "seed_name: updated"]
        )
        assert len(results) == 1

        # Should now have 13 rows (12 + 1 new version for updated id=1)
        assert get_row_count(project, "cc_snapshot") == 13

        # 12 rows should still be current (dbt_valid_to = sentinel)
        assert get_current_row_count(project, "cc_snapshot") == 12

        # 1 row should be expired (the old version of id=1)
        assert get_expired_row_count(project, "cc_snapshot") == 1

        # Verify id=1 has exactly 2 rows: old expired + new current
        id_1_rows = get_rows_for_id(project, "cc_snapshot", 1)
        assert len(id_1_rows) == 2, f"Expected 2 rows for id=1, got {len(id_1_rows)}"

        # First row (oldest) should be the original version, now expired
        old_row = id_1_rows[0]
        assert old_row[1] == "Easton"  # original name
        # dbt_valid_to should be a real timestamp (not the sentinel)
        assert old_row[3] != datetime(2100, 1, 1, 0, 0)

        # Second row (newest) should be the updated version, still current
        new_row = id_1_rows[1]
        assert new_row[1] == "Easton_updated"  # updated name
        assert new_row[3] == datetime(2100, 1, 1, 0, 0)  # dbt_valid_to = sentinel


class TestSnapshotCheckDbtValidToCurrentWithHardDeletes:
    """Test hard_deletes='invalidate' with dbt_valid_to_current.

    This tests the combination from issue #481 where hard_deletes: 'invalidate'
    is used together with dbt_valid_to_current.

    NOTE: This test is skipped by default because it requires
    join_use_nulls=1 to pass. ClickHouse's default join_use_nulls=0 causes
    LEFT JOINs to return default values (0, '') instead of NULL for missing
    keys. The snapshot deletes CTE uses 'WHERE x IS NULL' to detect deleted
    rows, which never matches when default values are returned instead of
    NULL. This is a known ClickHouse behavior that affects all snapshots
    using hard_deletes='invalidate', not specific to the dbt_valid_to_current
    fix (see issues #271, #291).

    To run this test locally, add 'join_use_nulls': 1 to custom_settings in
    your dbt profile connection config.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "deleted.csv": seeds_deleted_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_snapshot_hd.sql": cc_snapshot_valid_to_current_hard_deletes_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_valid_to_current_hard_deletes"}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    @pytest.mark.skip(
        reason=(
            "Requires join_use_nulls=1 to pass (see class docstring). "
            "ClickHouse's default join_use_nulls=0 returns default values "
            "(0, '') instead of NULL for missing LEFT JOIN keys, preventing "
            "the snapshot deletes CTE from detecting deleted rows. "
            "Not specific to dbt_valid_to_current - affects all snapshots "
            "using hard_deletes='invalidate' (issues #271, #291)."
        )
    )
    def test_snapshot_valid_to_current_with_hard_deletes(self, project):
        # Seed the base data (10 rows)
        results = run_dbt(["seed"])
        assert len(results) == 2

        # --- First snapshot run ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should have 10 rows, all with dbt_valid_to = '2100-01-01'
        assert get_row_count(project, "cc_snapshot_hd") == 10
        assert get_current_row_count(project, "cc_snapshot_hd") == 10
        assert get_expired_row_count(project, "cc_snapshot_hd") == 0

        # --- Second snapshot run with deleted data ---
        # Point at the "deleted" seed so id=10 is now missing from source
        results = run_dbt(["snapshot", "--vars", "seed_name: deleted"])
        assert len(results) == 1

        # Should still have 10 rows (9 current + 1 invalidated)
        assert get_row_count(project, "cc_snapshot_hd") == 10

        # 9 rows should still be current (dbt_valid_to = sentinel)
        assert get_current_row_count(project, "cc_snapshot_hd") == 9

        # 1 row should be expired (the deleted row id=10)
        assert get_expired_row_count(project, "cc_snapshot_hd") == 1

        # Verify the deleted row (id=10) exists but is invalidated
        assert get_deleted_row_count(project, "cc_snapshot_hd") == 1
        deleted_valid_to = get_deleted_row_valid_to(project, "cc_snapshot_hd")
        # The deleted row should have a real timestamp, not the sentinel
        assert deleted_valid_to != "2100-01-01 00:00:00"
