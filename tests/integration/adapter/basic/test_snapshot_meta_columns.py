"""
ClickHouse-native coverage for two snapshot features unlocked by rebasing
``clickhouse__snapshot_staging_table`` on dbt-core's default:

* ``snapshot_meta_column_names`` — renaming the dbt-managed meta columns;
* multi-column ``unique_key``.

dbt-tests-adapter does cover these (``BaseSnapshotColumnNames`` /
``BaseSnapshotMultiUniqueKey``), but those base tests embed PostgreSQL DDL/DML
directly in the test body (``create table … VARCHAR``, ``update … set``,
``interval '1 hour'``, ``md5(… || …)``) rather than exposing it through
overridable fixtures, so they cannot be inherited for ClickHouse. This test
reproduces the same behaviour with ClickHouse-native SQL.
"""

import datetime

import pytest
from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture, write_file

_SEED_CSV = """id1,id2,first_name,updated_at
1,100,Judith,2016-08-20 10:00:00
2,200,Arthur,2016-08-20 10:00:00
3,300,Rachel,2016-08-20 10:00:00
4,400,Ralph,2016-08-20 10:00:00
""".lstrip()

_SEED_YML = """
seeds:
  - name: seed
    config:
      column_types:
        id1: Int32
        id2: Int32
        first_name: String
        updated_at: DateTime
"""

# Multi-column unique_key + renamed meta columns, defined entirely in YAML.
_SNAPSHOT_YML = """
snapshots:
  - name: snap
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key:
        - id1
        - id2
      snapshot_meta_column_names:
        dbt_valid_to: valid_to
        dbt_valid_from: valid_from
        dbt_scd_id: scd_id
        dbt_updated_at: updated_ts
"""


class TestSnapshotMetaColumnsMultiKey:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEED_CSV, "seed.yml": _SEED_YML}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.yml": _SNAPSHOT_YML}

    def test_meta_columns_and_multi_key(self, project):
        run_dbt(["seed"])

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        snap = relation_from_name(project.adapter, "snap")

        # The renamed meta columns must exist and the dbt defaults must not.
        cols = {
            c[0]
            for c in project.run_sql(
                f"select name from system.columns "
                f"where database = currentDatabase() and table = '{snap.identifier}'",
                fetch="all",
            )
        }
        assert {"valid_to", "valid_from", "scd_id", "updated_ts"}.issubset(cols)
        assert "dbt_valid_to" not in cols
        assert "dbt_scd_id" not in cols

        # All four rows are current after the first snapshot.
        open_rows = project.run_sql(
            f"select count() from {snap} where valid_to is null", fetch="all"
        )
        assert open_rows[0][0] == 4

        # Update one composite key on a tracked column; snapshot must close out the
        # old version (valid_to set) and open a new one — proving the multi-column
        # unique_key join works.
        project.run_sql(
            f"alter table {relation_from_name(project.adapter, 'seed')} "
            f"update updated_at = updated_at + interval 1 day where id1 = 1 and id2 = 100 "
            f"settings mutations_sync = 2"
        )

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        rows = project.run_sql(
            f"select valid_to is null as is_current, count() from {snap} "
            f"where id1 = 1 and id2 = 100 group by is_current order by is_current",
            fetch="all",
        )
        # one closed-out record (is_current = 0) and one current record (is_current = 1)
        assert sorted((int(r[0]), int(r[1])) for r in rows) == [(0, 1), (1, 1)]

        # Untouched keys still have exactly one current record each.
        still_open = project.run_sql(
            f"select count() from {snap} where valid_to is null", fetch="all"
        )
        assert still_open[0][0] == 4


_NR_SEED_CSV = """id1,id2,val,updated_at
1,100,a,2024-01-01 10:00:00
2,200,b,2024-01-01 10:00:00
3,300,c,2024-01-01 10:00:00
4,400,d,2024-01-01 10:00:00
""".lstrip()

_NR_SEED_YML = """
seeds:
  - name: seed
    config:
      column_types:
        id1: Int32
        id2: Int32
        val: String
        updated_at: DateTime
"""

# multi-column unique_key combined with hard_deletes: new_record
_NR_SNAPSHOT_YML = """
snapshots:
  - name: snap
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key:
        - id1
        - id2
      hard_deletes: new_record
"""


class TestSnapshotMultiKeyNewRecord:
    """Multi-column ``unique_key`` together with ``hard_deletes: new_record``."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _NR_SEED_CSV, "seed.yml": _NR_SEED_YML}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.yml": _NR_SNAPSHOT_YML}

    def test_multi_key_new_record(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["snapshot"])) == 1

        snap = relation_from_name(project.adapter, "snap")

        def scalar(sql):
            return project.run_sql(sql, fetch="all")[0][0]

        # all four composite keys current, none flagged deleted
        assert scalar(f"select count() from {snap} where dbt_valid_to is null") == 4
        assert scalar(f"select count() from {snap} where dbt_is_deleted = 'True'") == 0

        # hard-delete one composite key from the source
        project.run_sql(
            f"alter table {relation_from_name(project.adapter, 'seed')} "
            f"delete where id1 = 1 and id2 = 100 settings mutations_sync = 2"
        )
        assert len(run_dbt(["snapshot"])) == 1

        # the deleted key now has two rows: the original (closed out) and a
        # new_record deletion marker (dbt_is_deleted = 'True', current)
        rows = project.run_sql(
            f"select dbt_is_deleted, dbt_valid_to is null as is_current from {snap} "
            f"where id1 = 1 and id2 = 100",
            fetch="all",
        )
        assert len(rows) == 2

        # exactly one deletion marker across the whole snapshot, and it is the
        # current row for the deleted key
        assert scalar(f"select count() from {snap} where dbt_is_deleted = 'True'") == 1
        assert (
            scalar(
                f"select count() from {snap} where id1 = 1 and id2 = 100 "
                f"and dbt_is_deleted = 'True' and dbt_valid_to is null"
            )
            == 1
        )
        # the three untouched keys remain current and not deleted
        assert scalar(f"select count() from {snap} where dbt_valid_to is null") == 4


# ---------------------------------------------------------------------------
# BaseSnapshotInvalidColumnNames / BaseSnapshotDbtValidToCurrent
#
# Like the classes above, the dbt-tests-adapter versions embed PostgreSQL DDL/DML
# directly in the test body, so they are reproduced natively here.
# ---------------------------------------------------------------------------

_CFG_SEED_CSV = """id,name,updated_at
1,Alice,2024-01-01 10:00:00
2,Bob,2024-01-01 10:00:00
3,Carol,2024-01-01 10:00:00
""".lstrip()

_CFG_SEED_YML = """
seeds:
  - name: seed
    config:
      column_types:
        id: Int32
        name: String
        updated_at: DateTime
"""

# Full meta-column rename.
_INVALID_SNAPSHOT_YML = """
snapshots:
  - name: snap
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key: id
      snapshot_meta_column_names:
        dbt_valid_to: test_valid_to
        dbt_valid_from: test_valid_from
        dbt_scd_id: test_scd_id
        dbt_updated_at: test_updated_at
"""

# Mismatched mapping: only two columns renamed. The existing snapshot target was
# built with all four renamed, so it is now missing the (default-named)
# dbt_valid_from / dbt_scd_id the new config expects.
_INVALID_SNAPSHOT_YML_CHANGED = """
snapshots:
  - name: snap
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key: id
      snapshot_meta_column_names:
        dbt_valid_to: test_valid_to
        dbt_updated_at: test_updated_at
"""


class TestSnapshotInvalidColumnNames:
    """Changing snapshot_meta_column_names against an existing snapshot errors out."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _CFG_SEED_CSV, "seed.yml": _CFG_SEED_YML}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.yml": _INVALID_SNAPSHOT_YML}

    def test_invalid_column_names(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["snapshot"])) == 1

        snap = relation_from_name(project.adapter, "snap")
        cols = {
            c[0]
            for c in project.run_sql(
                f"select name from system.columns "
                f"where database = currentDatabase() and table = '{snap.identifier}'",
                fetch="all",
            )
        }
        # the renamed meta columns exist, the dbt defaults do not
        assert {"test_valid_to", "test_valid_from", "test_scd_id", "test_updated_at"}.issubset(cols)
        assert "dbt_valid_to" not in cols and "dbt_scd_id" not in cols

        # repoint two of the meta columns at their default names; the target is now
        # missing the configured dbt_valid_from / dbt_scd_id columns
        write_file(_INVALID_SNAPSHOT_YML_CHANGED, project.project_root, "snapshots", "snap.yml")

        results, log_output = run_dbt_and_capture(
            ["--no-partial-parse", "snapshot"], expect_pass=False
        )
        assert len(results) == 1
        assert "Snapshot target is missing configured columns" in log_output


# Far-future sentinel for current rows. Kept within ClickHouse's DateTime range
# (max 2106-02-07) so it does not silently clamp.
_VTC_SENTINEL = datetime.datetime(2105, 1, 1, 0, 0)

_VTC_SNAPSHOT_YML = """
snapshots:
  - name: snap
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key: id
      dbt_valid_to_current: "toDateTime('2105-01-01 00:00:00')"
      snapshot_meta_column_names:
        dbt_valid_to: test_valid_to
        dbt_valid_from: test_valid_from
        dbt_scd_id: test_scd_id
        dbt_updated_at: test_updated_at
"""


class TestSnapshotDbtValidToCurrentTimestamp:
    """dbt_valid_to_current (current rows carry a sentinel valid_to) with the
    timestamp strategy and renamed meta columns."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _CFG_SEED_CSV, "seed.yml": _CFG_SEED_YML}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.yml": _VTC_SNAPSHOT_YML}

    def test_valid_to_current(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["snapshot"])) == 1

        snap = relation_from_name(project.adapter, "snap")

        # every current record carries the sentinel as test_valid_to
        valid_tos = [
            r[0] for r in project.run_sql(f"select test_valid_to from {snap}", fetch="all")
        ]
        assert len(valid_tos) == 3
        assert all(v == _VTC_SENTINEL for v in valid_tos)

        # change one row in the source -> timestamp strategy detects it
        project.run_sql(
            f"alter table {relation_from_name(project.adapter, 'seed')} "
            f"update updated_at = updated_at + interval 1 day where id = 1 "
            f"settings mutations_sync = 2"
        )
        assert len(run_dbt(["snapshot"])) == 1

        # id 1 now has two rows: the closed-out original (test_valid_to set to the
        # new updated_at, i.e. no longer the sentinel) and the new current row (sentinel)
        rows = sorted(
            r[0]
            for r in project.run_sql(f"select test_valid_to from {snap} where id = 1", fetch="all")
        )
        assert len(rows) == 2
        assert rows[1] == _VTC_SENTINEL  # current version
        assert rows[0] == datetime.datetime(2024, 1, 2, 10, 0)  # closed version
        assert rows[0] != _VTC_SENTINEL

        # untouched ids keep a single current (sentinel) row
        assert (
            project.run_sql(
                f"select count() from {snap} where test_valid_to = toDateTime('2105-01-01 00:00:00')",
                fetch="all",
            )[0][0]
            == 3
        )
