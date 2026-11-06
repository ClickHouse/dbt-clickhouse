import pytest
from dbt.tests.util import run_dbt

SEED_CSV = """id,team_id,name,updated_at
1,10,alice,2024-01-01 00:00:00
2,10,bob,2024-01-01 00:00:00
3,20,carol,2024-01-01 00:00:00
""".lstrip()

# Nullable `name`/`updated_at` with a non-nullable key, as is common in real sources. With
# ClickHouse's default `join_use_nulls = 0`, an unmatched LEFT JOIN row is filled with type
# defaults: `0` for the key (so `key is null` is false) and NULL for the Nullable
# `dbt_valid_from`/`name` (so `row_changed` is false too). New keys and removed keys were then
# silently skipped. Non-nullable seed columns masked this in the existing snapshot tests: the
# `1970-01-01` default `dbt_valid_from` made `row_changed` true for every new key.
SEED_COLUMN_TYPES = {
    "id": "Int32",
    "team_id": "Int32",
    "name": "Nullable(String)",
    "updated_at": "Nullable(DateTime)",
}

SNAPSHOT_TEMPLATE = """
{{% snapshot key_snapshot %}}
    {{{{ config(
        unique_key={unique_key},
        strategy='{strategy}',
        {strategy_config}
        hard_deletes='invalidate',
    ) }}}}
    select * from {{{{ ref('seed') }}}}
{{% endsnapshot %}}
"""


def _snapshot_sql(unique_key, strategy):
    strategy_config = (
        "updated_at='updated_at'," if strategy == "timestamp" else "check_cols=['name'],"
    )
    return SNAPSHOT_TEMPLATE.format(
        unique_key=unique_key, strategy=strategy, strategy_config=strategy_config
    )


class SnapshotNewAndDeletedKeysBase:
    """Regression test for https://github.com/ClickHouse/dbt-clickhouse/issues/784

    After the first build, a snapshot must insert keys that are new in the source and, with
    ``hard_deletes='invalidate'``, close the current version of keys removed from the source.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": SEED_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+column_types": SEED_COLUMN_TYPES}}

    def _snapshot_rows(self, project):
        rows = project.run_sql(
            f"select id, team_id, name, dbt_valid_to is null as is_current "
            f"from {project.test_schema}.key_snapshot",
            fetch="all",
        )
        return sorted((row[0], row[1], row[2], bool(row[3])) for row in rows)

    def test_new_and_deleted_keys(self, project):
        run_dbt(["seed"])
        run_dbt(["snapshot"])
        assert self._snapshot_rows(project) == [
            (1, 10, 'alice', True),
            (2, 10, 'bob', True),
            (3, 20, 'carol', True),
        ]

        # Add two keys (one with a NULL check column) and remove one.
        project.run_sql(
            f"insert into {project.test_schema}.seed values "
            f"(4, 20, 'dave', '2024-01-02 00:00:00'), (5, 20, NULL, '2024-01-02 00:00:00')"
        )
        project.run_sql(
            f"alter table {project.test_schema}.seed delete where id = 1 "
            f"settings mutations_sync = 2"
        )
        run_dbt(["snapshot"])

        expected = [
            (1, 10, 'alice', False),
            (2, 10, 'bob', True),
            (3, 20, 'carol', True),
            (4, 20, 'dave', True),
            (5, 20, None, True),
        ]
        assert self._snapshot_rows(project) == expected

        # Stable once the changes have been captured.
        run_dbt(["snapshot"])
        assert self._snapshot_rows(project) == expected


class TestSnapshotNewAndDeletedKeysTimestamp(SnapshotNewAndDeletedKeysBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"key_snapshot.sql": _snapshot_sql("'id'", "timestamp")}


class TestSnapshotNewAndDeletedKeysCheck(SnapshotNewAndDeletedKeysBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"key_snapshot.sql": _snapshot_sql("'id'", "check")}


class TestSnapshotNewAndDeletedKeysCompoundTimestamp(SnapshotNewAndDeletedKeysBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"key_snapshot.sql": _snapshot_sql("['id', 'team_id']", "timestamp")}


class TestSnapshotNewAndDeletedKeysCompoundCheck(SnapshotNewAndDeletedKeysBase):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"key_snapshot.sql": _snapshot_sql("['id', 'team_id']", "check")}
