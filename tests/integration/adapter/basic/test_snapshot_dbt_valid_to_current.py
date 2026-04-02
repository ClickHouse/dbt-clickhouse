import pytest

from dbt.tests.util import run_dbt, relation_from_name


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

# Timestamp strategy snapshot with dbt_valid_to_current configured
ts_snapshot_valid_to_current_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
        dbt_valid_to_current="toDateTime('9999-12-31 00:00:00')",
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
        dbt_valid_to_current="toDateTime('9999-12-31 00:00:00')",
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
        f"select count(*) from {relation} where dbt_valid_to = toDateTime('9999-12-31 00:00:00')",
        fetch="one",
    )
    return result[0]


def get_expired_row_count(project, snapshot_name):
    """Count rows where dbt_valid_to is a real timestamp (not the current sentinel)."""
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(
        f"select count(*) from {relation} where dbt_valid_to != toDateTime('9999-12-31 00:00:00')",
        fetch="one",
    )
    return result[0]


class TestSnapshotTimestampDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
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
        assert len(results) == 2

        # --- First snapshot run ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should have 10 rows, all with dbt_valid_to = '9999-12-31' (not NULL)
        assert get_row_count(project, "ts_snapshot") == 10
        assert get_current_row_count(project, "ts_snapshot") == 10
        assert get_expired_row_count(project, "ts_snapshot") == 0

        # --- Second snapshot run (no changes) ---
        # This is the critical test: without the fix, the second run would
        # fail to find current records (because it looks for dbt_valid_to IS NULL
        # but they have '9999-12-31'), causing duplicate inserts.
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


class TestSnapshotCheckDbtValidToCurrent:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
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
        assert len(results) == 2

        # --- First snapshot run ---
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Should have 10 rows, all with dbt_valid_to = '9999-12-31' (not NULL)
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
