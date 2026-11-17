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


def _runtime_config(cluster='my_cluster'):
    return SimpleNamespace(
        quoting={}, credentials=SimpleNamespace(cluster=cluster, database_engine='')
    )


# what dbt passes when resolving source() and function() calls
_QUOTING_ONLY_CONFIG = SimpleNamespace(quoting={})


def test_create_from_model_keeps_schema_and_cluster():
    relation = ClickHouseRelation.create_from(_runtime_config(), _relation_config('model'))

    assert relation.render() == '`my_schema`.`my_node`'
    assert relation.should_on_cluster


def test_create_from_function_drops_schema_and_keeps_cluster():
    relation = ClickHouseRelation.create_from(_runtime_config(), _relation_config('function'))

    assert relation.render() == '`my_node`'
    assert relation.should_on_cluster


def test_create_from_function_with_quoting_only_config():
    relation = ClickHouseRelation.create_from(_QUOTING_ONLY_CONFIG, _relation_config('function'))

    assert relation.render() == '`my_node`'
    assert not relation.should_on_cluster


def test_create_from_source_with_quoting_only_config_uses_database_as_schema():
    relation_config = _relation_config(
        'source', schema='my_source', source_name='my_source', database='my_database'
    )

    relation = ClickHouseRelation.create_from(_QUOTING_ONLY_CONFIG, relation_config)

    assert relation.render() == '`my_database`.`my_node`'
    assert not relation.should_on_cluster
