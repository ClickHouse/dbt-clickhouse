"""
ClickHouse ports of the dbt-tests-adapter ``simple_snapshot/test_various_configs.py``
suite: renamed meta columns (``snapshot_meta_column_names``), ``dbt_valid_to_current``
and multi-column ``unique_key``.

The upstream classes embed PostgreSQL DDL/DML directly in their test bodies
(``VARCHAR``, ``update ... set``, ``interval '1 hour'``, ``null::timestamp``,
``md5(... || ...)``) instead of exposing it through overridable fixtures, so they
cannot be inherited. Each port below replays the upstream flow with ClickHouse SQL:
20-row seed, change rows 10-20, compare ``snapshot_actual`` against a hand-built
``snapshot_expected``.

Two comparison details differ from upstream:

* ``dbt.tests.util.check_relations_equal`` only skips ``dbt_``-prefixed columns, and
  ``ClickHouseAdapter.get_rows_different_sql`` joins on every remaining column, so the
  ``NULL`` ``TEST_VALID_TO`` of current rows would never match once the meta columns
  are renamed. ``_assert_relations_equal`` uses ``EXCEPT`` instead, where ClickHouse
  treats ``NULL`` as equal.
* ``snapshot_expected.test_scd_id`` mirrors ``clickhouse__snapshot_hash_arguments``
  (``halfMD5`` over the ``'|'``-joined key and ``updated_at``), not upstream's ``md5``.
"""

import datetime

import pytest
from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture, update_config_file

# --- shared 20-row data set (upstream ``fixtures.py``) ------------------------

# fmt: off
_PEOPLE = [
    (1, "Judith", "Kennedy", "(not provided)", "Female", "54.60.24.128", "2015-12-24 12:19:28"),
    (2, "Arthur", "Kelly", "(not provided)", "Male", "62.56.24.215", "2015-10-28 16:22:15"),
    (3, "Rachel", "Moreno", "rmoreno2@msu.edu", "Female", "31.222.249.23", "2016-04-05 02:05:30"),
    (4, "Ralph", "Turner", "rturner3@hp.com", "Male", "157.83.76.114", "2016-08-08 00:06:51"),
    (5, "Laura", "Gonzales", "lgonzales4@howstuffworks.com", "Female", "30.54.105.168", "2016-09-01 08:25:38"),
    (6, "Katherine", "Lopez", "klopez5@yahoo.co.jp", "Female", "169.138.46.89", "2016-08-30 18:52:11"),
    (7, "Jeremy", "Hamilton", "jhamilton6@mozilla.org", "Male", "231.189.13.133", "2016-07-17 02:09:46"),
    (8, "Heather", "Rose", "hrose7@goodreads.com", "Female", "87.165.201.65", "2015-12-29 22:03:56"),
    (9, "Gregory", "Kelly", "gkelly8@trellian.com", "Male", "154.209.99.7", "2016-03-24 21:18:16"),
    (10, "Rachel", "Lopez", "rlopez9@themeforest.net", "Female", "237.165.82.71", "2016-08-20 15:44:49"),
    (11, "Donna", "Welch", "dwelcha@shutterfly.com", "Female", "103.33.110.138", "2016-02-27 01:41:48"),
    (12, "Russell", "Lawrence", "rlawrenceb@qq.com", "Male", "189.115.73.4", "2016-06-11 03:07:09"),
    (13, "Michelle", "Montgomery", "mmontgomeryc@scientificamerican.com", "Female", "243.220.95.82", "2016-06-18 16:27:19"),
    (14, "Walter", "Castillo", "wcastillod@pagesperso-orange.fr", "Male", "71.159.238.196", "2016-10-06 01:55:44"),
    (15, "Robin", "Mills", "rmillse@vkontakte.ru", "Female", "172.190.5.50", "2016-10-31 11:41:21"),
    (16, "Raymond", "Holmes", "rholmesf@usgs.gov", "Male", "148.153.166.95", "2016-10-03 08:16:38"),
    (17, "Gary", "Bishop", "gbishopg@plala.or.jp", "Male", "161.108.182.13", "2016-08-29 19:35:20"),
    (18, "Anna", "Riley", "arileyh@nasa.gov", "Female", "253.31.108.22", "2015-12-11 04:34:27"),
    (19, "Sarah", "Knight", "sknighti@foxnews.com", "Female", "222.220.3.177", "2016-09-26 00:49:06"),
    (20, "Phyllis", "Fox", None, "Female", "163.191.232.95", "2016-08-21 10:35:19"),
]
# fmt: on

_BIZ_COLS = ["first_name", "last_name", "email", "gender", "ip_address", "updated_at"]
_BIZ_DDL = (
    "first_name String, last_name String, email Nullable(String), gender String, "
    "ip_address String, updated_at DateTime"
)
_META_DDL = (
    "test_valid_from DateTime, TEST_VALID_TO Nullable(DateTime), "
    "test_scd_id UInt64, test_updated_at DateTime"
)

_META_COLUMN_NAMES = {
    "dbt_valid_to": "TEST_VALID_TO",
    "dbt_valid_from": "test_valid_from",
    "dbt_scd_id": "test_scd_id",
    "dbt_updated_at": "test_updated_at",
}

_VALID_TO_CURRENT = "date('2099-12-31')"


def _keys(multi_key):
    return ["id1", "id2"] if multi_key else ["id"]


def _scd_id(multi_key):
    # same arguments as api.Relation.scd_args(unique_key, updated_at)
    if multi_key:
        return "halfMD5(concat(toString(id1), '|', toString(id2), '|', toString(updated_at)))"
    return "halfMD5(concat(toString(id), '-', first_name, '|', toString(updated_at)))"


def _seed_values(multi_key):
    rows = []
    for id_, first, last, email, gender, ip, ts in _PEOPLE:
        key = f"{id_}, {id_ * 100}" if multi_key else str(id_)
        email_sql = "null" if email is None else f"'{email}'"
        rows.append(f"({key}, '{first}', '{last}', {email_sql}, '{gender}', '{ip}', '{ts}')")
    return ",\n".join(rows)


def _populate_expected_sql(schema, multi_key, valid_to="null", where="1 = 1"):
    keys = ", ".join(_keys(multi_key))
    biz = ", ".join(_BIZ_COLS)
    return (
        f"insert into {schema}.snapshot_expected "
        f"({keys}, {biz}, test_valid_from, TEST_VALID_TO, test_updated_at, test_scd_id) "
        f"select {keys}, {biz}, updated_at, {valid_to}, updated_at, {_scd_id(multi_key)} "
        f"from {schema}.seed where {where}"
    )


def _setup_tables(project, multi_key=False, valid_to="null"):
    """create_seed_sql / create_snapshot_expected_sql / seed_insert_sql / populate_*."""
    schema = project.test_schema
    keys = _keys(multi_key)
    key_ddl = ", ".join(f"{k} Int32" for k in keys)
    order_by = ", ".join(keys)
    project.run_sql(
        f"create table {schema}.seed ({key_ddl}, {_BIZ_DDL}) "
        f"engine = MergeTree order by ({order_by})"
    )
    project.run_sql(
        f"create table {schema}.snapshot_expected ({key_ddl}, {_BIZ_DDL}, {_META_DDL}) "
        f"engine = MergeTree order by ({order_by})"
    )
    project.run_sql(
        f"insert into {schema}.seed ({', '.join(keys)}, {', '.join(_BIZ_COLS)}) "
        f"values {_seed_values(multi_key)}"
    )
    project.run_sql(_populate_expected_sql(schema, multi_key, valid_to))


def _change_rows_10_to_20(project, multi_key=False, valid_to="null"):
    """invalidate_sql + update_sql: bump updated_at/email in the source, close the old
    expected rows and append their v2."""
    schema = project.test_schema
    key = _keys(multi_key)[0]
    project.run_sql(
        f"alter table {schema}.seed update "
        f"updated_at = updated_at + interval 1 hour, "
        f"email = if({key} = 20, 'pfoxj@creativecommons.org', concat('new_', email)) "
        f"where {key} between 10 and 20 settings mutations_sync = 2"
    )
    project.run_sql(
        f"alter table {schema}.snapshot_expected update "
        f"TEST_VALID_TO = updated_at + interval 1 hour "
        f"where {key} between 10 and 20 settings mutations_sync = 2"
    )
    project.run_sql(
        _populate_expected_sql(schema, multi_key, valid_to, where=f"{key} between 10 and 20")
    )


def _scalar(project, sql):
    return project.run_sql(sql, fetch="one")[0]


def _assert_relations_equal(project, actual="snapshot_actual", expected="snapshot_expected"):
    a = relation_from_name(project.adapter, actual)
    b = relation_from_name(project.adapter, expected)
    cols = project.run_sql(
        f"select name from system.columns where database = '{project.test_schema}' "
        f"and table = '{actual}' order by position",
        fetch="all",
    )
    col_csv = ", ".join(f"`{c[0]}`" for c in cols)
    assert _scalar(project, f"select count() from {a}") == _scalar(
        project, f"select count() from {b}"
    )
    for left, right in ((a, b), (b, a)):
        missing = _scalar(
            project,
            f"select count() from (select {col_csv} from {left} except select {col_csv} from {right})",
        )
        assert missing == 0, f"{missing} rows of {left} are missing from {right}"


# --- snapshot definitions (upstream ``fixtures.py``) --------------------------

# {{ target.database }} is empty on ClickHouse, so only the schema is used.
_SNAPSHOT_ACTUAL_SQL = """
{% snapshot snapshot_actual %}
    {{ config(unique_key='id || ' ~ "'-'" ~ ' || first_name') }}
    select * from {{ target.schema }}.seed
{% endsnapshot %}
"""

_REF_SNAPSHOT_SQL = "select * from {{ ref('snapshot_actual') }}"

_SNAPSHOTS_YML = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      snapshot_meta_column_names:
        dbt_valid_to: TEST_VALID_TO
        dbt_valid_from: test_valid_from
        dbt_scd_id: test_scd_id
        dbt_updated_at: test_updated_at
"""

_SNAPSHOTS_NO_COLUMN_NAMES_YML = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
"""

_SNAPSHOTS_VALID_TO_CURRENT_YML = f"""
snapshots:
  - name: snapshot_actual
    config:
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "{_VALID_TO_CURRENT}"
      snapshot_meta_column_names:
        dbt_valid_to: TEST_VALID_TO
        dbt_valid_from: test_valid_from
        dbt_scd_id: test_scd_id
        dbt_updated_at: test_updated_at
"""

# Never materialized (only `dbt snapshot` runs); it exists so that `ref('seed')` in the
# YAML-only snapshot below resolves to the raw `seed` table of the same name.
_MODEL_SEED_SQL = "select * from {{ target.schema }}.seed"

_SNAPSHOTS_MULTI_KEY_YML = """
snapshots:
  - name: snapshot_actual
    relation: "ref('seed')"
    config:
      strategy: timestamp
      updated_at: updated_at
      unique_key:
        - id1
        - id2
      snapshot_meta_column_names:
        dbt_valid_to: TEST_VALID_TO
        dbt_valid_from: test_valid_from
        dbt_scd_id: test_scd_id
        dbt_updated_at: test_updated_at
"""


class _SingleKeySqlSnapshot:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _SNAPSHOT_ACTUAL_SQL}


class TestSnapshotColumnNames(_SingleKeySqlSnapshot):
    """ClickHouse port of ``BaseSnapshotColumnNames``."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"snapshots.yml": _SNAPSHOTS_YML, "ref_snapshot.sql": _REF_SNAPSHOT_SQL}

    def test_snapshot_column_names(self, project):
        _setup_tables(project)
        assert len(run_dbt(["snapshot"])) == 1

        _change_rows_10_to_20(project)
        assert len(run_dbt(["snapshot"])) == 1

        _assert_relations_equal(project)


class TestSnapshotColumnNamesFromDbtProject(_SingleKeySqlSnapshot):
    """ClickHouse port of ``BaseSnapshotColumnNamesFromDbtProject``."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _SNAPSHOTS_NO_COLUMN_NAMES_YML,
            "ref_snapshot.sql": _REF_SNAPSHOT_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"test": {"+snapshot_meta_column_names": _META_COLUMN_NAMES}}}

    def test_snapshot_column_names_from_project(self, project):
        _setup_tables(project)
        assert len(run_dbt(["snapshot"])) == 1

        _change_rows_10_to_20(project)
        assert len(run_dbt(["snapshot"])) == 1

        _assert_relations_equal(project)


class TestSnapshotInvalidColumnNames(_SingleKeySqlSnapshot):
    """ClickHouse port of ``BaseSnapshotInvalidColumnNames``."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _SNAPSHOTS_NO_COLUMN_NAMES_YML,
            "ref_snapshot.sql": _REF_SNAPSHOT_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"test": {"+snapshot_meta_column_names": _META_COLUMN_NAMES}}}

    def test_snapshot_invalid_column_names(self, project):
        _setup_tables(project)
        assert len(run_dbt(["snapshot"])) == 1

        _change_rows_10_to_20(project)

        # Repoint two meta columns at their default names: the existing target has
        # neither dbt_valid_from nor dbt_scd_id, so the run must refuse to continue.
        different_columns = {
            "snapshots": {
                "test": {
                    "+snapshot_meta_column_names": {
                        "dbt_valid_to": "TEST_VALID_TO",
                        "dbt_updated_at": "test_updated_at",
                    }
                }
            }
        }
        update_config_file(different_columns, "dbt_project.yml")

        results, log_output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert len(results) == 1
        assert "Compilation Error in snapshot snapshot_actual" in log_output
        assert "Snapshot target is missing configured columns" in log_output


class TestSnapshotDbtValidToCurrent(_SingleKeySqlSnapshot):
    """ClickHouse port of ``BaseSnapshotDbtValidToCurrent``."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _SNAPSHOTS_VALID_TO_CURRENT_YML,
            "ref_snapshot.sql": _REF_SNAPSHOT_SQL,
        }

    def test_valid_to_current(self, project):
        current = datetime.datetime(2099, 12, 31, 0, 0)
        _setup_tables(project, valid_to=_VALID_TO_CURRENT)
        assert len(run_dbt(["snapshot"])) == 1

        snap = relation_from_name(project.adapter, "snapshot_actual")

        def valid_to_of(id_):
            rows = project.run_sql(
                f"select TEST_VALID_TO from {snap} where id = {id_} order by test_valid_from",
                fetch="all",
            )
            return [r[0] for r in rows]

        assert valid_to_of(1) == [current]
        assert valid_to_of(10) == [current]

        _change_rows_10_to_20(project, valid_to=_VALID_TO_CURRENT)
        assert len(run_dbt(["snapshot"])) == 1

        assert valid_to_of(1) == [current]
        # the closed-out original of id 10 carries the new updated_at, its v2 is current
        assert valid_to_of(10) == [datetime.datetime(2016, 8, 20, 16, 44, 49), current]

        _assert_relations_equal(project)


class TestSnapshotMultiUniqueKey:
    """ClickHouse port of ``BaseSnapshotMultiUniqueKey``."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "seed.sql": _MODEL_SEED_SQL,
            "snapshots.yml": _SNAPSHOTS_MULTI_KEY_YML,
            "ref_snapshot.sql": _REF_SNAPSHOT_SQL,
        }

    def test_multi_column_unique_key(self, project):
        _setup_tables(project, multi_key=True)
        assert len(run_dbt(["snapshot"])) == 1

        _change_rows_10_to_20(project, multi_key=True)
        assert len(run_dbt(["snapshot"])) == 1

        _assert_relations_equal(project)


# --- multi-column unique_key + hard_deletes: new_record -----------------------

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
    """No upstream counterpart: the upstream new_record tests only use a scalar key."""

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

        # all four composite keys current, none flagged deleted
        assert _scalar(project, f"select count() from {snap} where dbt_valid_to is null") == 4
        assert _scalar(project, f"select count() from {snap} where dbt_is_deleted = 'True'") == 0

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
        assert _scalar(project, f"select count() from {snap} where dbt_is_deleted = 'True'") == 1
        assert (
            _scalar(
                project,
                f"select count() from {snap} where id1 = 1 and id2 = 100 "
                f"and dbt_is_deleted = 'True' and dbt_valid_to is null",
            )
            == 1
        )
        # the three untouched keys remain current and not deleted
        assert _scalar(project, f"select count() from {snap} where dbt_valid_to is null") == 4
