from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from tests.unit.macro_harness import SandboxSafeMock


class _Config:
    def __init__(self, values: Dict[str, Any]) -> None:
        self.values = values

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


def _can_clone_table(macros, config: Optional[Dict[str, Any]] = None) -> bool:
    return macros.call('clickhouse__can_clone_table', context={'config': _Config(config or {})})


@pytest.mark.parametrize(
    'kwargs,expected',
    [
        ({}, True),
        ({'config': {'engine': 'ReplacingMergeTree(version)'}}, True),
        ({'config': {'engine': 'Memory()'}}, False),
        ({'config': {'materialized': 'distributed_table'}}, False),
    ],
    ids=['merge-tree', 'merge-tree-family', 'other-engine', 'distributed'],
)
def test_can_clone_table(macros, kwargs, expected):
    assert _can_clone_table(macros, **kwargs) is expected


def _relation(schema: str) -> SandboxSafeMock:
    relation = SandboxSafeMock(schema=schema, identifier='my_model')
    relation.__str__.return_value = f'`{schema}`.`my_model`'
    return relation


def _create_or_replace_clone(
    macros, cluster: Optional[str] = None, on_cluster: bool = True, database_engine: str = 'Atomic'
) -> List[str]:
    """Returns the statements the macro runs, the last one is dbt's `main` statement."""
    statements: List[str] = []

    def statement(name: str, fetch_result: bool = False, caller: Any = None) -> str:
        statements.append(' '.join(caller().split()))
        return ''

    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = cluster
    adapter.should_on_cluster.return_value = on_cluster
    adapter.get_ch_database.return_value = (
        SimpleNamespace(engine=database_engine) if database_engine else None
    )
    main = macros.call(
        'clickhouse__create_or_replace_clone',
        _relation('dev'),
        _relation('prod'),
        context={'adapter': adapter, 'statement': statement},
    )
    return [*statements, ' '.join(main.split())]


@pytest.mark.parametrize(
    'kwargs',
    [
        {},
        {'cluster': '"my_cluster"', 'on_cluster': False},
        {'database_engine': ''},
    ],
    ids=['no-cluster', 'disable-on-cluster', 'missing-database'],
)
def test_create_or_replace_clone(macros, kwargs):
    assert _create_or_replace_clone(macros, **kwargs) == [
        'CREATE OR REPLACE TABLE `dev`.`my_model` CLONE AS `prod`.`my_model`'
    ]


# `CLONE AS` drops the source `ON CLUSTER` and Replicated databases reject it
@pytest.mark.parametrize(
    'kwargs,on_cluster',
    [
        ({'cluster': '"my_cluster"'}, ' ON CLUSTER "my_cluster"'),
        ({'database_engine': 'Replicated'}, ''),
    ],
    ids=['on-cluster', 'replicated-database'],
)
def test_create_or_replace_clone_without_clone_as(macros, kwargs, on_cluster):
    assert _create_or_replace_clone(macros, **kwargs) == [
        f'CREATE OR REPLACE TABLE `dev`.`my_model`{on_cluster} AS `prod`.`my_model`',
        f'ALTER TABLE `dev`.`my_model`{on_cluster} ATTACH PARTITION ALL FROM `prod`.`my_model`',
    ]
