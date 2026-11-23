from datetime import datetime
from types import SimpleNamespace

import pytest
from dbt.adapters.base.relation import EventTimeFilter
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


@pytest.mark.parametrize(
    'start,end,expected',
    [
        (
            datetime(2020, 1, 1),
            datetime(2020, 1, 2),
            "event_time >= toDateTime64('2020-01-01 00:00:00', 0)"
            " and event_time < toDateTime64('2020-01-02 00:00:00', 0)",
        ),
        (
            datetime(2020, 1, 1, 10),
            None,
            "event_time >= toDateTime64('2020-01-01 10:00:00', 0)",
        ),
        (None, datetime(2020, 1, 2), "event_time < toDateTime64('2020-01-02 00:00:00', 0)"),
        (None, None, ""),
        # Outside the DateTime range (1970-2106): the literal must still carry the real boundary,
        # because toDateTime() would clamp it to 1970-01-01.
        (
            datetime(1960, 1, 1),
            datetime(1960, 1, 2),
            "event_time >= toDateTime64('1960-01-01 00:00:00', 0)"
            " and event_time < toDateTime64('1960-01-02 00:00:00', 0)",
        ),
        (None, datetime(2200, 1, 1), "event_time < toDateTime64('2200-01-01 00:00:00', 0)"),
    ],
)
def test_render_event_time_filtered(start, end, expected):
    relation = ClickHouseRelation.create(schema='db', identifier='events')
    event_time_filter = EventTimeFilter(field_name='event_time', start=start, end=end)
    assert relation._render_event_time_filtered(event_time_filter) == expected
