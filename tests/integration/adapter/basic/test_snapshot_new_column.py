"""
Snapshotting an ephemeral model with ``hard_deletes: new_record``, plus
schema-evolution cases (a new source column appearing between snapshots). These
exercise the `new_record` deletion-records path and the materialization's
missing-column handling.

The dbt-tests-adapter base classes expose all their source SQL through
overridable fixtures, so only that SQL needs ClickHouse adaptation: an explicit
table engine, `String` instead of `VARCHAR`, `alter table ... delete` for hard
deletes, and `{schema}` only (ClickHouse has no database part, so the base's
`{database}.{schema}` renders as an invalid `.schema.table`). The models
(`_sources.yml`, the ephemeral model, the snapshot YAML) are already
ClickHouse-compatible and are inherited unchanged.
"""

import pytest
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
    BaseSnapshotNewColumnSpecificCheckCols,
    BaseSnapshotNewColumnTimestampStrategy,
    BaseSnapshotNewColumnWithDeletes,
)

_CH_SOURCE_CREATE = (
    "create table {schema}.src_customers "
    "(id Int32, first_name String, last_name String, email String, updated_at DateTime) "
    "engine = MergeTree order by id;"
)

_CH_SOURCE_INSERT = (
    "insert into {schema}.src_customers (id, first_name, last_name, email, updated_at) values "
    "(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 10:00:00'),"
    "(2, 'Jane', 'Smith', 'jane.smith@example.com', '2023-01-02 11:00:00'),"
    "(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2023-01-03 12:00:00');"
)

_CH_SOURCE_ALTER = (
    "alter table {schema}.src_customers add column dummy_column String default 'dummy_value';"
)

_CH_SOURCE_DELETE = (
    "alter table {schema}.src_customers delete where id = 3 settings mutations_sync = 2;"
)


class _ClickHouseSourceSQL:
    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return _CH_SOURCE_CREATE

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return _CH_SOURCE_INSERT

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return _CH_SOURCE_ALTER

    @pytest.fixture(scope="class")
    def source_delete_sql(self):
        return _CH_SOURCE_DELETE


class TestSnapshotEphemeralHardDeletes(_ClickHouseSourceSQL, BaseSnapshotEphemeralHardDeletes):
    pass


class TestSnapshotNewColumnTimestampStrategy(
    _ClickHouseSourceSQL, BaseSnapshotNewColumnTimestampStrategy
):
    pass


class TestSnapshotNewColumnSpecificCheckCols(
    _ClickHouseSourceSQL, BaseSnapshotNewColumnSpecificCheckCols
):
    pass


class TestSnapshotNewColumnWithDeletes(_ClickHouseSourceSQL, BaseSnapshotNewColumnWithDeletes):
    pass
