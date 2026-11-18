from types import SimpleNamespace

import pytest
from dbt.adapters.clickhouse.relation import ClickHouseRelation


@pytest.mark.parametrize(
    'cluster,database_engine,expected',
    [
        ('', None, False),
        ('cluster', None, True),
        ('cluster', '', True),
        ('cluster', 'Atomic', True),
        ('cluster', 'Replicated', False),
        ('cluster', 'ReplicatedDatabase', False),
    ],
)
def test_get_on_cluster(cluster, database_engine, expected):
    assert ClickHouseRelation.get_on_cluster(cluster, database_engine) is expected


def _relation_config(resource_type, schema='my_schema', **kwargs):
    defaults = {
        'resource_type': resource_type,
        'schema': schema,
        'identifier': 'my_node',
        'database': None,
        'source_name': None,
        'quoting_dict': {},
        'config': {},
    }
    return SimpleNamespace(**{**defaults, **kwargs})


def _runtime_config(cluster='my_cluster', database_engine=''):
    return SimpleNamespace(
        quoting={}, credentials=SimpleNamespace(cluster=cluster, database_engine=database_engine)
    )


# what dbt passes when resolving source() and function() calls
_QUOTING_ONLY_CONFIG = SimpleNamespace(quoting={})


@pytest.mark.parametrize(
    'quoting,resource_type,config,expected',
    [
        (_runtime_config(), 'model', {}, True),
        (_runtime_config(), 'function', {}, True),
        (_runtime_config(), 'source', {}, False),
        (_runtime_config(cluster=''), 'model', {}, False),
        (_runtime_config(database_engine='Replicated'), 'model', {}, False),
        (_runtime_config(database_engine='Replicated'), 'function', {}, True),
        (_runtime_config(), 'model', {'disable_on_cluster': True}, False),
        (_runtime_config(), 'function', {'disable_on_cluster': True}, False),
        (_QUOTING_ONLY_CONFIG, 'function', {}, False),
        (_QUOTING_ONLY_CONFIG, 'source', {}, False),
    ],
    ids=[
        'model',
        'function',
        'source',
        'no-cluster',
        'replicated-model',
        'replicated-function',
        'disabled-model',
        'disabled-function',
        'quoting-only-function',
        'quoting-only-source',
    ],
)
def test_create_from_on_cluster(quoting, resource_type, config, expected):
    relation_config = _relation_config(resource_type, config=config)

    relation = ClickHouseRelation.create_from(quoting, relation_config)

    assert bool(relation.should_on_cluster) is expected


@pytest.mark.parametrize(
    'resource_type,kwargs,rendered',
    [
        ('model', {}, '`my_schema`.`my_node`'),
        ('function', {}, '`my_node`'),
        ('source', {'schema': 'src', 'source_name': 'src', 'database': 'db'}, '`db`.`my_node`'),
    ],
    ids=['model', 'function', 'source-database-as-schema'],
)
def test_create_from_renders(resource_type, kwargs, rendered):
    relation_config = _relation_config(resource_type, **kwargs)

    relation = ClickHouseRelation.create_from(_runtime_config(), relation_config)

    assert relation.render() == rendered
