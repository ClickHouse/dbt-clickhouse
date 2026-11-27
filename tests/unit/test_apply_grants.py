from __future__ import annotations

from typing import Any, Dict, Optional

from tests.unit.macro_harness import SandboxSafeMock


class _Config:
    def __init__(self, values: Optional[Dict[str, Any]] = None) -> None:
        self.values = values or {}

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


def _relation(schema: str = 'db', name: str = 't', should_on_cluster: bool = True) -> SandboxSafeMock:
    relation = SandboxSafeMock(schema=schema, name=name, should_on_cluster=should_on_cluster)
    relation.__str__.return_value = f'`{schema}`.`{name}`'
    return relation


def _adapter(cluster: Optional[str] = None) -> SandboxSafeMock:
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = cluster
    adapter.quote.side_effect = lambda identifier: f'`{identifier}`'
    return adapter


def test_grant_and_revoke_quote_grantees(macros):
    relation = _relation()
    adapter = _adapter(cluster=None)

    grant_sql = macros.call(
        'clickhouse__get_grant_sql',
        relation,
        'SELECT',
        ['etl-user', 'read only'],
        context={'adapter': adapter, 'config': _Config()},
    )
    revoke_sql = macros.call(
        'clickhouse__get_revoke_sql',
        relation,
        'SELECT',
        ['etl-user'],
        context={'adapter': adapter, 'config': _Config()},
    )

    assert ' '.join(grant_sql.split()) == 'grant SELECT on `db`.`t` to `etl-user`, `read only`'
    assert ' '.join(revoke_sql.split()) == 'revoke SELECT on `db`.`t` from `etl-user`'


def test_grant_and_revoke_emit_on_cluster_even_for_replicated_relation(macros):
    # Replicated databases set relation.should_on_cluster = False for table DDL,
    # but DCL must still emit ON CLUSTER so access grants propagate to all nodes.
    relation = _relation(schema='repl', name='t', should_on_cluster=False)
    adapter = _adapter(cluster='"test_shard"')

    grant_sql = macros.call(
        'clickhouse__get_grant_sql',
        relation,
        'SELECT',
        ['user_1'],
        context={'adapter': adapter, 'config': _Config()},
    )
    assert (
        ' '.join(grant_sql.split())
        == 'grant ON CLUSTER "test_shard" SELECT on `repl`.`t` to `user_1`'
    )


def test_grant_honors_disable_on_cluster(macros):
    relation = _relation(schema='db', name='t', should_on_cluster=True)
    adapter = _adapter(cluster='"test_shard"')

    grant_sql = macros.call(
        'clickhouse__get_grant_sql',
        relation,
        'SELECT',
        ['user_1'],
        context={'adapter': adapter, 'config': _Config({'disable_on_cluster': True})},
    )
    assert ' '.join(grant_sql.split()) == 'grant SELECT on `db`.`t` to `user_1`'


def test_show_grant_sql_filters_column_grants_partial_revokes_and_escapes(macros):
    relation = _relation(schema="my'db", name=r"tbl\1")
    sql = macros.call('clickhouse__get_show_grant_sql', relation)
    normalized = ' '.join(sql.split())

    assert r"WHERE table = 'tbl\\1' AND database = 'my\'db'" in normalized
    assert 'GROUP BY privilege_type, grantee' in normalized
    assert 'HAVING countIf(column IS NULL AND is_partial_revoke = 0) > 0' in normalized
    assert 'AND countIf(is_partial_revoke = 1) = 0' in normalized
