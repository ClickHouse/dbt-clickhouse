import os

import pytest
from dbt.tests.adapter.basic.files import model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import BaseIncremental


def _change_db_engine_to_replicated(original_config: dict) -> dict:
    # mutating it in place leaks the Replicated db_engine into every test
    # that runs after this module in the same session.
    config = dict(original_config)
    config["db_engine"] = "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')"
    return config


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseSimpleMaterialization(BaseSimpleMaterializations):
    """Contains tests for table, view and swappable view materialization."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        return _change_db_engine_to_replicated(test_config)


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseIncremental(BaseIncremental):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        return _change_db_engine_to_replicated(test_config)

    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_incremental = """
          {{ config(order_by='(some_date, id, name)', inserts_only=True, materialized='incremental', unique_key='id') }}
        """
        incremental_sql = config_materialized_incremental + model_incremental
        return {
            "incremental.sql": incremental_sql,
            "schema.yml": schema_base_yml,
        }
