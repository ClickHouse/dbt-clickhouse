from __future__ import annotations

from typing import Optional

import pytest

from tests.unit.macro_harness import SandboxSafeMock


def _relation(identifier: Optional[str], should_on_cluster: bool) -> SandboxSafeMock:
    relation = SandboxSafeMock(identifier=identifier, should_on_cluster=should_on_cluster)
    return relation


def _adapter(cluster: Optional[str]) -> SandboxSafeMock:
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = cluster
    adapter.should_on_cluster.side_effect = (
        lambda relation=None, **_: bool(relation.should_on_cluster) if relation is not None else False
    )
    return adapter


def test_on_cluster_clause_respects_this_disable_on_cluster_for_catalog_relation(macros):
    """When rebuilding a model with disable_on_cluster=true, catalog relations carry
    can_on_cluster=True from the profile; `this` must govern table-level DDL."""
    existing_relation = _relation(identifier='my_model', should_on_cluster=True)
    this_relation = _relation(identifier='my_model', should_on_cluster=False)
    adapter = _adapter('"test_shard"')

    result = macros.call(
        'on_cluster_clause',
        existing_relation,
        False,
        context={'adapter': adapter, 'this': this_relation},
    )

    assert result.strip() == ''
    adapter.should_on_cluster.assert_called_once_with(relation=this_relation)


def test_on_cluster_clause_emits_clause_when_this_allows_cluster(macros):
    existing_relation = _relation(identifier='my_model', should_on_cluster=True)
    this_relation = _relation(identifier='my_model', should_on_cluster=True)
    adapter = _adapter('"test_shard"')

    result = macros.call(
        'on_cluster_clause',
        existing_relation,
        True,
        context={'adapter': adapter, 'this': this_relation},
    )

    assert ' '.join(result.split()) == 'ON CLUSTER "test_shard" SYNC'
    adapter.should_on_cluster.assert_called_once_with(relation=this_relation)


def test_on_cluster_clause_uses_relation_for_schema_ddl_even_when_this_defined(macros):
    schema_relation = _relation(identifier=None, should_on_cluster=True)
    this_relation = _relation(identifier='my_model', should_on_cluster=False)
    adapter = _adapter('"test_shard"')

    result = macros.call(
        'on_cluster_clause',
        schema_relation,
        False,
        context={'adapter': adapter, 'this': this_relation},
    )

    assert result.strip() == 'ON CLUSTER "test_shard"'
    adapter.should_on_cluster.assert_called_once_with(relation=schema_relation)


@pytest.mark.parametrize(
    'cluster,should_on_cluster,expected',
    [
        ('"test_shard"', True, 'ON CLUSTER "test_shard"'),
        ('"test_shard"', False, ''),
        (None, True, ''),
    ],
    ids=['cluster-enabled', 'relation-disabled', 'no-cluster'],
)
def test_on_cluster_clause_outside_node_context(macros, cluster, should_on_cluster, expected):
    relation = _relation(identifier='my_model', should_on_cluster=should_on_cluster)
    adapter = _adapter(cluster)

    result = macros.call(
        'on_cluster_clause',
        relation,
        False,
        context={'adapter': adapter},
    )

    assert result.strip() == expected
