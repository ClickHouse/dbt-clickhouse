import threading
from unittest.mock import patch

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.relation import ClickHouseRelation
from dbt.adapters.sql import SQLAdapter


@pytest.fixture
def adapter():
    # Skip __init__, which needs a dbt project config
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter._staging = threading.local()
    return adapter


def staging_relation(name='model__dbt_new_data_1'):
    return ClickHouseRelation.create(schema='db', identifier=name, type='table')


def test_post_model_hook_drops_relation_left_by_failed_model(adapter):
    context = adapter.pre_model_hook({})
    adapter.register_staging_relation(staging_relation())

    with patch.object(SQLAdapter, 'drop_relation') as drop:
        adapter.post_model_hook({}, context)

    drop.assert_called_once_with(staging_relation())


def test_post_model_hook_does_nothing_after_materialization_dropped_relation(adapter):
    context = adapter.pre_model_hook({})
    adapter.register_staging_relation(staging_relation())
    adapter.unregister_staging_relation(staging_relation())

    with patch.object(SQLAdapter, 'drop_relation') as drop:
        adapter.post_model_hook({}, context)

    drop.assert_not_called()


def test_drop_before_create_keeps_registration(adapter):
    # create_distributed_local_table drops the shard relation before creating it
    context = adapter.pre_model_hook({})
    adapter.register_staging_relation(staging_relation())

    with patch.object(SQLAdapter, 'drop_relation') as drop:
        adapter.drop_relation(staging_relation())
        adapter.post_model_hook({}, context)

    assert drop.call_count == 2


def test_post_model_hook_does_not_raise_when_drop_fails(adapter):
    context = adapter.pre_model_hook({})
    adapter.register_staging_relation(staging_relation('a__dbt_new_data_1'))
    adapter.register_staging_relation(staging_relation('b__dbt_new_data_1'))

    with patch.object(SQLAdapter, 'drop_relation', side_effect=RuntimeError('lost')) as drop:
        adapter.post_model_hook({}, context)

    assert drop.call_count == 2


def test_register_outside_model_is_ignored(adapter):
    adapter.register_staging_relation(staging_relation())

    assert not hasattr(adapter._staging, 'relations')


def test_relations_are_tracked_per_thread(adapter):
    context = adapter.pre_model_hook({})
    other = []

    def other_thread():
        other.append(adapter.pre_model_hook({}))
        adapter.register_staging_relation(staging_relation('other__dbt_new_data_1'))

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()

    assert context == []
    assert other == [[staging_relation('other__dbt_new_data_1')]]
