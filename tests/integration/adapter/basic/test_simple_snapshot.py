"""
Inherits the generic ``simple_snapshot`` suite from dbt-tests-adapter.

The base classes (``BaseSimpleSnapshotBase`` and friends) drive their assertions
by mutating the source ``fact`` table with raw SQL through a set of helper
methods. That SQL is written for PostgreSQL, so it is incompatible with
ClickHouse in a few ways:

* ``create table ... as select`` requires an explicit engine in ClickHouse.
* row mutations use ``update ... set`` / ``delete from`` instead of
  ``alter table ... update`` / ``alter table ... delete`` and need
  ``mutations_sync`` to be observable synchronously.
* ``interval '1 day'`` literals and ``varchar(n)`` column definitions are not
  valid ClickHouse syntax.

The ``ClickHouseSnapshotHelpers`` mixin overrides the helper methods so the
inherited test bodies run unchanged against ClickHouse.
"""

from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)
from dbt.tests.util import relation_from_name


class ClickHouseSnapshotHelpers:
    def _rel(self, table):
        return relation_from_name(self.project.adapter, table)

    @staticmethod
    def _translate_expr(expr):
        # PostgreSQL interval literals -> ClickHouse interval syntax.
        return expr.replace("interval '1 day'", "interval 1 day").replace(
            "interval '1 hour'", "interval 1 hour"
        )

    @staticmethod
    def _translate_type(definition):
        if definition and "varchar" in definition.lower():
            return "Nullable(String)"
        return definition

    def create_fact_from_seed(self, where=None):
        fact = self._rel("fact")
        seed = self._rel("seed")
        where_clause = where or "1 = 1"
        self.project.run_sql(f"drop table if exists {fact}")
        self.project.run_sql(
            f"create table {fact} engine = MergeTree order by id "
            f"as select * from {seed} where {where_clause}"
        )

    def update_fact_records(self, updates, where=None):
        fact = self._rel("fact")
        set_clause = ", ".join(
            f"{field} = {self._translate_expr(expr)}" for field, expr in updates.items()
        )
        where_clause = where or "1 = 1"
        self.project.run_sql(
            f"alter table {fact} update {set_clause} where {where_clause} "
            f"settings mutations_sync = 2"
        )

    def delete_fact_records(self, where=None):
        fact = self._rel("fact")
        where_clause = where or "1 = 1"
        self.project.run_sql(
            f"alter table {fact} delete where {where_clause} settings mutations_sync = 2"
        )

    def insert_fact_records(self, where=None):
        fact = self._rel("fact")
        seed = self._rel("seed")
        where_clause = where or "1 = 1"
        self.project.run_sql(f"insert into {fact} select * from {seed} where {where_clause}")

    def add_fact_column(self, column=None, definition=None):
        fact = self._rel("fact")
        definition = self._translate_type(definition)
        self.project.run_sql(
            f"alter table {fact} add column {column} {definition} settings mutations_sync = 2"
        )


class TestSimpleSnapshot(ClickHouseSnapshotHelpers, BaseSimpleSnapshot):
    pass


class TestSnapshotCheck(ClickHouseSnapshotHelpers, BaseSnapshotCheck):
    pass
