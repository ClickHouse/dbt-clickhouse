import pytest
from dbt.tests.util import (
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)


SEED_CSV = """id,name,email,updated_at
1,Alice,alice@example.com,2024-01-01 10:00:00
2,Bob,bob@example.com,2024-01-02 11:00:00
3,Charlie,charlie@example.com,2024-01-03 12:00:00
"""

TABLE_MODEL_SQL = """
{{ config(
    materialized='table',
    order_by='id',
    engine='MergeTree()'
) }}

select * from {{ ref('base') }}
"""

SNAPSHOT_WITH_ORDER_BY_AND_PRIMARY_KEY_SQL = """
{% snapshot users_snapshot %}

{{
    config(
        strategy='check',
        check_cols=['name', 'email'],
        unique_key='id',
        order_by=['dbt_scd_id', 'dbt_valid_from'],
        primary_key='dbt_scd_id'
    )
}}

select * from {{ ref('table_model') }}

{% endsnapshot %}
"""

SNAPSHOT_ORDER_BY_ONLY_SQL = """
{% snapshot users_snapshot_order_by_only %}

{{
    config(
        strategy='check',
        check_cols=['name', 'email'],
        unique_key='id',
        order_by='dbt_scd_id'
    )
}}

select * from {{ ref('table_model') }}

{% endsnapshot %}
"""

SNAPSHOT_PRIMARY_KEY_ONLY_SQL = """
{% snapshot users_snapshot_primary_key_only %}

{{
    config(
        strategy='check',
        check_cols=['name', 'email'],
        unique_key='id',
        primary_key='dbt_scd_id'
    )
}}

select * from {{ ref('table_model') }}

{% endsnapshot %}
"""

SNAPSHOT_NO_CONFIG_SQL = """
{% snapshot users_snapshot_no_config %}

{{
    config(
        strategy='check',
        check_cols=['name', 'email'],
        unique_key='id'
    )
}}

select * from {{ ref('table_model') }}

{% endsnapshot %}
"""

SNAPSHOT_LIST_ORDER_BY_SQL = """
{% snapshot users_snapshot_list_order_by %}

{{
    config(
        strategy='check',
        check_cols=['name', 'email'],
        unique_key='id',
        order_by=['dbt_scd_id', 'dbt_valid_from', 'dbt_valid_to'],
        primary_key=['dbt_scd_id', 'dbt_valid_from']
    )
}}

select * from {{ ref('table_model') }}

{% endsnapshot %}
"""


class TestClickHouseSnapshotOrderBy:
    """Test that snapshot tables respect order_by and primary_key configurations"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": SEED_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": TABLE_MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "users_snapshot.sql": SNAPSHOT_WITH_ORDER_BY_AND_PRIMARY_KEY_SQL,
            "users_snapshot_order_by_only.sql": SNAPSHOT_ORDER_BY_ONLY_SQL,
            "users_snapshot_primary_key_only.sql": SNAPSHOT_PRIMARY_KEY_ONLY_SQL,
            "users_snapshot_no_config.sql": SNAPSHOT_NO_CONFIG_SQL,
            "users_snapshot_list_order_by.sql": SNAPSHOT_LIST_ORDER_BY_SQL,
        }

    def _get_table_metadata(self, project, table_name):
        relation = relation_from_name(project.adapter, table_name)
        result = project.run_sql(
            f"""
            SELECT 
                sorting_key,
                primary_key
            FROM system.tables
            WHERE database = '{relation.schema}' AND name = '{relation.identifier}'
            """,
            fetch="one",
        )
        return {
            "sorting_key": result[0],
            "primary_key": result[1],
        }

    def _get_create_table_sql(self, project, table_name):
        relation = relation_from_name(project.adapter, table_name)
        result = project.run_sql(
            f"SHOW CREATE TABLE {relation}",
            fetch="one",
        )
        return result[0] if result else None

    def test_snapshot_with_order_by_and_primary_key(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        snapshot_results = run_dbt(["snapshot", "--select", "users_snapshot"])
        assert len(snapshot_results) == 1
        check_result_nodes_by_name(snapshot_results, ["users_snapshot"])

        metadata = self._get_table_metadata(project, "users_snapshot")
        create_sql = self._get_create_table_sql(project, "users_snapshot")

        assert metadata["sorting_key"] is not None
        assert "dbt_scd_id" in metadata["sorting_key"]
        assert "dbt_valid_from" in metadata["sorting_key"]

        assert metadata["primary_key"] is not None
        assert "dbt_scd_id" in metadata["primary_key"]

        assert "ORDER BY" in create_sql.upper()
        assert "PRIMARY KEY" in create_sql.upper()

        snapshot_results_2 = run_dbt(["snapshot", "--select", "users_snapshot"])
        assert len(snapshot_results_2) == 1

        metadata_after = self._get_table_metadata(project, "users_snapshot")
        assert metadata_after["sorting_key"] == metadata["sorting_key"]
        assert metadata_after["primary_key"] == metadata["primary_key"]

    def test_snapshot_with_order_by_only(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        snapshot_results = run_dbt(["snapshot", "--select", "users_snapshot_order_by_only"])
        assert len(snapshot_results) == 1
        check_result_nodes_by_name(snapshot_results, ["users_snapshot_order_by_only"])

        metadata = self._get_table_metadata(project, "users_snapshot_order_by_only")
        create_sql = self._get_create_table_sql(project, "users_snapshot_order_by_only")

        assert metadata["sorting_key"] is not None
        assert "dbt_scd_id" in metadata["sorting_key"]

        assert "ORDER BY" in create_sql.upper()

        snapshot_results_2 = run_dbt(["snapshot", "--select", "users_snapshot_order_by_only"])
        assert len(snapshot_results_2) == 1

        metadata_after = self._get_table_metadata(project, "users_snapshot_order_by_only")
        assert metadata_after["sorting_key"] == metadata["sorting_key"]

    def test_snapshot_with_primary_key_only(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        snapshot_results = run_dbt(["snapshot", "--select", "users_snapshot_primary_key_only"])
        assert len(snapshot_results) == 1
        check_result_nodes_by_name(snapshot_results, ["users_snapshot_primary_key_only"])

        metadata = self._get_table_metadata(project, "users_snapshot_primary_key_only")
        create_sql = self._get_create_table_sql(project, "users_snapshot_primary_key_only")

        assert metadata["primary_key"] is not None
        assert "dbt_scd_id" in metadata["primary_key"]

        assert "PRIMARY KEY" in create_sql.upper()

        snapshot_results_2 = run_dbt(["snapshot", "--select", "users_snapshot_primary_key_only"])
        assert len(snapshot_results_2) == 1

        metadata_after = self._get_table_metadata(project, "users_snapshot_primary_key_only")
        assert metadata_after["primary_key"] == metadata["primary_key"]

    def test_snapshot_with_no_config(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        snapshot_results = run_dbt(["snapshot", "--select", "users_snapshot_no_config"])
        assert len(snapshot_results) == 1
        check_result_nodes_by_name(snapshot_results, ["users_snapshot_no_config"])

        metadata = self._get_table_metadata(project, "users_snapshot_no_config")

        assert metadata["sorting_key"] is not None
        assert metadata["primary_key"] is not None

        snapshot_results_2 = run_dbt(["snapshot", "--select", "users_snapshot_no_config"])
        assert len(snapshot_results_2) == 1

        metadata_after = self._get_table_metadata(project, "users_snapshot_no_config")
        assert metadata_after["sorting_key"] == metadata["sorting_key"]
        assert metadata_after["primary_key"] == metadata["primary_key"]

    def test_snapshot_with_list_order_by(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        snapshot_results = run_dbt(["snapshot", "--select", "users_snapshot_list_order_by"])
        assert len(snapshot_results) == 1
        check_result_nodes_by_name(snapshot_results, ["users_snapshot_list_order_by"])

        metadata = self._get_table_metadata(project, "users_snapshot_list_order_by")
        create_sql = self._get_create_table_sql(project, "users_snapshot_list_order_by")

        assert metadata["sorting_key"] is not None
        assert "dbt_scd_id" in metadata["sorting_key"]
        assert "dbt_valid_from" in metadata["sorting_key"]
        assert "dbt_valid_to" in metadata["sorting_key"]

        assert metadata["primary_key"] is not None
        assert "dbt_scd_id" in metadata["primary_key"]
        assert "dbt_valid_from" in metadata["primary_key"]

        assert "ORDER BY" in create_sql.upper()
        assert "PRIMARY KEY" in create_sql.upper()

        snapshot_results_2 = run_dbt(["snapshot", "--select", "users_snapshot_list_order_by"])
        assert len(snapshot_results_2) == 1

        metadata_after = self._get_table_metadata(project, "users_snapshot_list_order_by")
        assert metadata_after["sorting_key"] == metadata["sorting_key"]
        assert metadata_after["primary_key"] == metadata["primary_key"]
