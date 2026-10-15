from typing import Any, Dict, Optional

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.relation import ClickHouseRelation

from tests.unit.macro_harness import SandboxSafeMock


def _normalize(rendered: str) -> str:
    return ' '.join(rendered.split())


def _context(
    cluster: Optional[str] = None, model_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = f'"{cluster}"' if cluster else None
    adapter.quote.side_effect = ClickHouseAdapter.quote
    config = SandboxSafeMock()
    config.get.side_effect = lambda key, default=None: (model_config or {}).get(key, default)
    return {'adapter': adapter, 'config': config}


def _relation(schema: str = 'db', identifier: str = 't', can_on_cluster: bool = False):
    # can_on_cluster=False is what a relation in a Replicated database gets, which is also
    # the default when no cluster is configured
    return ClickHouseRelation.create(
        schema=schema, identifier=identifier, can_on_cluster=can_on_cluster
    )


@pytest.mark.parametrize(
    'grantees, expected',
    [
        (['etl-user'], '"etl-user"'),
        (['user_1', 'Role With Space'], '"user_1", "Role With Space"'),
        (['"already_quoted"', '`backticked-role`'], '"already_quoted", `backticked-role`'),
    ],
)
def test_grant_and_revoke_quote_grantees(macros, grantees, expected):
    relation = _relation()
    grant = macros.call(
        'clickhouse__get_grant_sql', relation, 'select', grantees, context=_context()
    )
    revoke = macros.call(
        'clickhouse__get_revoke_sql', relation, 'select', grantees, context=_context()
    )
    assert _normalize(grant) == f'grant select on `db`.`t` to {expected}'
    assert _normalize(revoke) == f'revoke select on `db`.`t` from {expected}'


@pytest.mark.parametrize('can_on_cluster', [True, False])
def test_grant_and_revoke_use_on_cluster_whenever_cluster_is_set(macros, can_on_cluster):
    # can_on_cluster=False covers database_engine: Replicated, where table DDL skips
    # ON CLUSTER but access entities are not replicated
    relation = _relation(can_on_cluster=can_on_cluster)
    context = _context(cluster='test_shard')
    grant = macros.call('clickhouse__get_grant_sql', relation, 'insert', ['u1'], context=context)
    revoke = macros.call('clickhouse__get_revoke_sql', relation, 'insert', ['u1'], context=context)
    assert _normalize(grant) == 'grant ON CLUSTER "test_shard" insert on `db`.`t` to "u1"'
    assert _normalize(revoke) == 'revoke ON CLUSTER "test_shard" insert on `db`.`t` from "u1"'


@pytest.mark.parametrize('disable_on_cluster', [True, 'true', 'True'])
def test_grant_honours_disable_on_cluster(macros, disable_on_cluster):
    context = _context(
        cluster='test_shard', model_config={'disable_on_cluster': disable_on_cluster}
    )
    grant = macros.call('clickhouse__get_grant_sql', _relation(), 'select', ['u1'], context=context)
    assert _normalize(grant) == 'grant select on `db`.`t` to "u1"'


def test_grant_without_cluster_has_no_on_cluster(macros):
    grant = macros.call(
        'clickhouse__get_grant_sql', _relation(True), 'select', ['u1'], context=_context()
    )
    assert 'ON CLUSTER' not in grant


def test_show_grant_sql_ignores_column_grants_and_partial_revokes(macros):
    sql = _normalize(macros.call('clickhouse__get_show_grant_sql', _relation()))
    assert "WHERE database = 'db' AND table = 't'" in sql
    assert 'GROUP BY privilege_type, grantee' in sql
    assert (
        'HAVING countIf(column IS NULL AND NOT is_partial_revoke) > 0 '
        'AND countIf(is_partial_revoke) = 0'
    ) in sql
    # dbt's grants tests detect REVOKE statements by looking for "revoke " in the logs
    assert 'revoke ' not in sql.lower()


def test_show_grant_sql_escapes_relation_names(macros):
    relation = _relation(schema="it's", identifier='back\\slash')
    sql = _normalize(macros.call('clickhouse__get_show_grant_sql', relation))
    assert "database = 'it\\'s'" in sql
    assert "table = 'back\\\\slash'" in sql


def test_quote_passes_through_quoted_identifiers():
    assert ClickHouseAdapter.quote('etl-user') == '"etl-user"'
    assert ClickHouseAdapter.quote('"etl-user"') == '"etl-user"'
    assert ClickHouseAdapter.quote('`etl-user`') == '`etl-user`'
    # a lone or unbalanced quote character is not a quoted identifier
    assert ClickHouseAdapter.quote('"') == '"""'
    assert ClickHouseAdapter.quote('"abc`') == '""abc`"'
