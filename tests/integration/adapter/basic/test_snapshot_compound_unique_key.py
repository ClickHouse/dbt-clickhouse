import pytest
from dbt.tests.util import run_dbt

# `id` alone is not unique: the same `id` appears once per `team_id`.
SEED_CSV = """id,team_id,name,updated_at
1,10,alice,2024-01-01 00:00:00
1,20,bob,2024-01-01 00:00:00
2,10,carol,2024-01-01 00:00:00
2,20,dave,2024-01-01 00:00:00
""".lstrip()

TIMESTAMP_SNAPSHOT = """
{% snapshot compound_snapshot %}
    {{ config(
        unique_key=['id', 'team_id'],
        strategy='timestamp',
        updated_at='updated_at',
    ) }}
    select * from {{ ref('seed') }}
{% endsnapshot %}
"""

CHECK_SNAPSHOT = """
{% snapshot compound_snapshot %}
    {{ config(
        unique_key=['id', 'team_id'],
        strategy='check',
        check_cols=['name'],
    ) }}
    select * from {{ ref('seed') }}
{% endsnapshot %}
"""


class CompoundUniqueKeySnapshotBase:
    """Regression test for https://github.com/ClickHouse/dbt-clickhouse/issues/544

    A list ``unique_key`` used to be rendered verbatim into the staging SQL,
    producing an ``Array(String)`` literal that is identical for every row.
    Joining ``snapshotted_data`` to the source on that literal then produced a
    Cartesian product and inserted spurious rows on every snapshot run.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": SEED_CSV}

    def _snapshot_rows(self, project):
        rows = project.run_sql(
            f"select id, team_id, name, dbt_valid_to is null as is_current "
            f"from {project.test_schema}.compound_snapshot",
            fetch="all",
        )
        return sorted((row[0], row[1], row[2], bool(row[3])) for row in rows)

    def test_compound_unique_key(self, project):
        run_dbt(["seed"])

        run_dbt(["snapshot"])
        rows = self._snapshot_rows(project)
        assert rows == [
            (1, 10, 'alice', True),
            (1, 20, 'bob', True),
            (2, 10, 'carol', True),
            (2, 20, 'dave', True),
        ]

        # Nothing changed: a second run must not insert or invalidate anything.
        run_dbt(["snapshot"])
        assert self._snapshot_rows(project) == rows

        # Change one row identified by both key columns: only that row may be
        # invalidated, and only one new row may be inserted.
        project.run_sql(
            f"alter table {project.test_schema}.seed "
            f"update name = 'alice2', updated_at = toDateTime('2024-01-02 00:00:00') "
            f"where id = 1 and team_id = 10 settings mutations_sync = 2"
        )
        run_dbt(["snapshot"])

        rows = self._snapshot_rows(project)
        assert rows == [
            (1, 10, 'alice', False),
            (1, 10, 'alice2', True),
            (1, 20, 'bob', True),
            (2, 10, 'carol', True),
            (2, 20, 'dave', True),
        ]

        # Stable again after the changes have been captured.
        run_dbt(["snapshot"])
        assert self._snapshot_rows(project) == rows


class TestSnapshotCompoundUniqueKeyTimestamp(CompoundUniqueKeySnapshotBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"compound_snapshot.sql": TIMESTAMP_SNAPSHOT}


class TestSnapshotCompoundUniqueKeyCheck(CompoundUniqueKeySnapshotBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"compound_snapshot.sql": CHECK_SNAPSHOT}
