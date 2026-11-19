from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest

from tests.unit.macro_harness import SandboxSafeMock


class _Config:
    def __init__(self, values: Dict[str, Any]) -> None:
        self.values = values

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


PROD_MODEL = SimpleNamespace(schema='prod', identifier='my_model')


def _can_clone_table(
    macros,
    config: Optional[Dict[str, Any]] = None,
    cluster: Optional[str] = None,
    on_cluster: bool = True,
    database_engine: str = 'Atomic',
    defer_relation: Any = PROD_MODEL,
) -> bool:
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = cluster
    adapter.should_on_cluster.return_value = on_cluster
    adapter.get_ch_database.return_value = (
        SimpleNamespace(engine=database_engine) if database_engine else None
    )
    return macros.call(
        'clickhouse__can_clone_table',
        context={
            'adapter': adapter,
            'config': _Config(config or {}),
            'this': SimpleNamespace(schema='dev', identifier='my_model'),
            'defer_relation': defer_relation,
        },
    )


@pytest.mark.parametrize(
    'kwargs,expected',
    [
        ({}, True),
        ({'config': {'engine': 'ReplacingMergeTree(version)'}}, True),
        ({'config': {'engine': 'Memory()'}}, False),
        ({'config': {'materialized': 'distributed_table'}}, False),
        ({'cluster': '"my_cluster"'}, False),
        ({'cluster': '"my_cluster"', 'on_cluster': False}, True),
        ({'database_engine': 'Replicated'}, False),
        ({'database_engine': ''}, True),
        ({'defer_relation': None}, True),
    ],
    ids=[
        'merge-tree',
        'merge-tree-family',
        'other-engine',
        'distributed',
        'on-cluster',
        'disable-on-cluster',
        'replicated-database',
        'missing-database',
        'no-defer-relation',
    ],
)
def test_can_clone_table(macros, kwargs, expected):
    assert _can_clone_table(macros, **kwargs) is expected


# dbt only skips cloning a table onto itself when it can clone
@pytest.mark.parametrize(
    'kwargs',
    [
        {'config': {'engine': 'Memory()'}},
        {'cluster': '"my_cluster"'},
        {'database_engine': 'Replicated'},
    ],
    ids=['other-engine', 'on-cluster', 'replicated-database'],
)
def test_can_clone_table_onto_itself(macros, kwargs):
    itself = SimpleNamespace(schema='dev', identifier='my_model')
    assert _can_clone_table(macros, defer_relation=itself, **kwargs) is True
