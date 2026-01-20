import pytest
from dbt.tests.util import relation_from_name, run_dbt


SEED_CSV = """id,name,email,updated_at
1,Alice,alice@example.com,2024-01-01 10:00:00
2,Bob,bob@example.com,2024-01-02 11:00:00
3,Charlie,charlie@example.com,2024-01-03 12:00:00
"""

TABLE_MODEL_SQL = """
{{ config(materialized='table', order_by='id', engine='MergeTree()') }}
select * from {{ ref('base') }}
"""

SNAPSHOT_TEMPLATE = """
{{% snapshot {name} %}}
{{{{ config(strategy='check', check_cols=['name', 'email'], unique_key='id'{extra_config}) }}}}
select * from {{{{ ref('table_model') }}}}
{{% endsnapshot %}}
"""


def make_snapshot(name, **config):
    extra = "".join(f", {k}={repr(v)}" for k, v in config.items()) if config else ""
    return SNAPSHOT_TEMPLATE.format(name=name, extra_config=extra)


class TestClickHouseSnapshotOrderBy:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"table_model.sql": TABLE_MODEL_SQL}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snap_both.sql": make_snapshot("snap_both", order_by=["dbt_scd_id", "dbt_valid_from"], primary_key="dbt_scd_id"),
            "snap_order_only.sql": make_snapshot("snap_order_only", order_by="dbt_scd_id"),
            "snap_pk_only.sql": make_snapshot("snap_pk_only", primary_key="dbt_scd_id"),
            "snap_none.sql": make_snapshot("snap_none"),
            "snap_replacing.sql": make_snapshot("snap_replacing", engine="ReplacingMergeTree()", order_by="dbt_scd_id"),
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_models(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

    def _get_metadata(self, project, table):
        rel = relation_from_name(project.adapter, table)
        row = project.run_sql(
            f"SELECT sorting_key, primary_key, engine FROM system.tables WHERE database='{rel.schema}' AND name='{rel.identifier}'",
            fetch="one",
        )
        return {"sorting_key": row[0], "primary_key": row[1], "engine": row[2]}

    def _get_snapshot_counts(self, project, table):
        rel = relation_from_name(project.adapter, table)
        row = project.run_sql(
            f"""SELECT 
                count() as total,
                countIf(dbt_valid_to IS NULL) as active,
                countIf(dbt_valid_to IS NOT NULL) as closed
            FROM {rel}""",
            fetch="one",
        )
        return {"total": row[0], "active": row[1], "closed": row[2]}

    def _reset_source_data(self, project):
        run_dbt(["run", "--full-refresh"])

    def _update_source_data(self, project):
        rel = relation_from_name(project.adapter, "table_model")
        project.run_sql(f"ALTER TABLE {rel} UPDATE email = 'alice.new@example.com' WHERE id = 1 SETTINGS mutations_sync = 2")
        project.run_sql(f"INSERT INTO {rel} VALUES (4, 'David', 'david@example.com', '2024-01-04 13:00:00')")

    def _drop_snapshot(self, project, snapshot):
        rel = relation_from_name(project.adapter, snapshot)
        project.run_sql(f"DROP TABLE IF EXISTS {rel}")

    @pytest.mark.parametrize("snapshot,expect_order,expect_pk", [
        ("snap_both", ["dbt_scd_id", "dbt_valid_from"], ["dbt_scd_id"]),
        ("snap_order_only", ["dbt_scd_id"], None),
        ("snap_pk_only", ["dbt_scd_id"], ["dbt_scd_id"]),
        ("snap_none", None, None),
    ])
    def test_snapshot_config(self, project, snapshot, expect_order, expect_pk):
        self._reset_source_data(project)
        self._drop_snapshot(project, snapshot)

        run_dbt(["snapshot", "--select", snapshot])
        meta = self._get_metadata(project, snapshot)
        counts = self._get_snapshot_counts(project, snapshot)

        if expect_order:
            assert all(col in meta["sorting_key"] for col in expect_order)
        if expect_pk:
            assert all(col in meta["primary_key"] for col in expect_pk)

        assert counts["total"] == 3
        assert counts["active"] == 3
        assert counts["closed"] == 0

        self._update_source_data(project)

        run_dbt(["snapshot", "--select", snapshot])
        counts2 = self._get_snapshot_counts(project, snapshot)
        meta2 = self._get_metadata(project, snapshot)

        assert counts2["total"] == 5
        assert counts2["active"] == 4
        assert counts2["closed"] == 1
        assert meta["sorting_key"] == meta2["sorting_key"]
        assert meta["primary_key"] == meta2["primary_key"]

    def test_snapshot_engine_replacing(self, project):
        self._reset_source_data(project)
        self._drop_snapshot(project, "snap_replacing")

        run_dbt(["snapshot", "--select", "snap_replacing"])
        meta = self._get_metadata(project, "snap_replacing")

        assert "ReplacingMergeTree" in meta["engine"]
        assert "dbt_scd_id" in meta["sorting_key"]
