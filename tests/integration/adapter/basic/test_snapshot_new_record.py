"""
``hard_deletes: new_record`` snapshot support, which the ClickHouse snapshot
macros gained once ``clickhouse__snapshot_staging_table`` was rebased on
dbt-core's ``default__snapshot_staging_table``.

The dbt-tests-adapter base classes target PostgreSQL, so we override the
ClickHouse-incompatible SQL:

* the source ``seed`` (and, for the relation-equality tests, ``snapshot_expected``)
  tables need an explicit engine;
* row mutations use ``alter table ... update`` / ``alter table ... delete`` with
  ``mutations_sync`` instead of ``update ... set`` / ``delete from``;
* ``interval '1 hour'`` and string concat use ClickHouse syntax;
* the snapshot model references ``{{ target.database }}.{{ target.schema }}``,
  but ClickHouse has no database part, so it renders as an invalid
  ``"".schema.seed`` — we point it at ``{{ target.schema }}`` instead.

``check_relations_equal`` ignores ``dbt_``-prefixed columns, so ``snapshot_expected``
only has to carry the business columns (no need to mirror the ``halfMD5`` scd_id
or the ``dbt_valid_*`` types). ClickHouse's row-difference query joins on every
compared column, and ``NULL = NULL`` is false there, so the seed avoids NULL
business values (the base data has a NULL email for id 20).
"""

import pytest
from dbt.tests.adapter.simple_snapshot.new_record_check_mode import (
    BaseSnapshotNewRecordCheckMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)

# ---------------------------------------------------------------------------
# BaseSnapshotNewRecordDbtValidToCurrent  (check strategy + dbt_valid_to_current)
# ---------------------------------------------------------------------------

_CH_VTC_SEED = [
    "create table {schema}.seed (id Int32, first_name String) engine = MergeTree order by id;",
    "insert into {schema}.seed (id, first_name) values (1, 'Judith'), (2, 'Arthur');",
]

_CH_VTC_DELETE = "alter table {schema}.seed delete where id = 1 settings mutations_sync = 2"

_CH_VTC_SNAPSHOT = """
{% snapshot snapshot_actual %}
    select * from {{ target.schema }}.seed
{% endsnapshot %}
"""


class TestSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _CH_VTC_SNAPSHOT}

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _CH_VTC_SEED

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _CH_VTC_DELETE


# ---------------------------------------------------------------------------
# BaseSnapshotNewRecordTimestampMode / BaseSnapshotNewRecordCheckMode
# (these compare snapshot_actual to a hand-built snapshot_expected)
# ---------------------------------------------------------------------------

_COLS = (
    "id Int32, first_name String, last_name String, email String, "
    "gender String, ip_address String, updated_at DateTime"
)
_BIZ = "id, first_name, last_name, email, gender, ip_address, updated_at"

# 20 rows; unlike the PostgreSQL base data, id 20 has a non-NULL email so the
# row-difference join (which treats NULL = NULL as false) stays well-defined.
_SEED_ROWS = """
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 'Phyllis', 'Fox', 'pfox0@example.com', 'Female', '163.191.232.95', '2016-08-21 10:35:19')
""".strip()

_CH_NR_SEED = [
    f"create table {{schema}}.seed ({_COLS}) engine = MergeTree order by id;",
    f"create table {{schema}}.snapshot_expected ({_COLS}) engine = MergeTree order by id;",
    f"insert into {{schema}}.seed ({_BIZ}) values {_SEED_ROWS};",
    f"insert into {{schema}}.snapshot_expected ({_BIZ}) select {_BIZ} from {{schema}}.seed;",
]

# Update the source for ids 10-20 (changed email + updated_at). No statement for
# snapshot_expected here: its closed-out row keeps the original business values,
# and the new version is appended by update_sql below.
_CH_NR_INVALIDATE = [
    "alter table {schema}.seed update "
    "updated_at = updated_at + interval 1 hour, "
    "email = case when id = 20 then 'pfoxj@creativecommons.org' else concat('new_', email) end "
    "where id >= 10 and id <= 20 settings mutations_sync = 2;",
]

# Append v2 of the changed rows to snapshot_expected (business columns only).
_CH_NR_UPDATE = (
    f"insert into {{schema}}.snapshot_expected ({_BIZ}) "
    f"select {_BIZ} from {{schema}}.seed where id >= 10 and id <= 20;"
)

_CH_NR_DELETE = "alter table {schema}.seed delete where id = 1 settings mutations_sync = 2;"

_CH_NR_REINSERT = (
    "insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) "
    "values (1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', "
    "'2030-01-01 12:00:00');"  # base uses 2200 which is out of ClickHouse DateTime range
)

_CH_NR_SNAPSHOT = """
{% snapshot snapshot_actual %}
    {{ config(unique_key="id || '-' || first_name") }}
    select * from {{ target.schema }}.seed
{% endsnapshot %}
"""


class _ClickHouseNewRecordSQL:
    """ClickHouse SQL overrides shared by the timestamp- and check-mode tests."""

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _CH_NR_SNAPSHOT}

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return _CH_NR_SEED

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return _CH_NR_INVALIDATE

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _CH_NR_UPDATE

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _CH_NR_DELETE

    @pytest.fixture(scope="class")
    def reinsert_sql(self):
        return _CH_NR_REINSERT

    @pytest.fixture(scope="class")
    def reinsert_check_sql(self):
        # base value has a trailing ';' which clickhouse-connect rejects as a
        # multi-statement once it appends `FORMAT Native` for the fetch.
        return (
            "select dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_is_deleted "
            "from {schema}.snapshot_actual where id = 1"
        )


class TestSnapshotNewRecordTimestampMode(
    _ClickHouseNewRecordSQL, BaseSnapshotNewRecordTimestampMode
):
    pass


class TestSnapshotNewRecordCheckMode(_ClickHouseNewRecordSQL, BaseSnapshotNewRecordCheckMode):
    pass
