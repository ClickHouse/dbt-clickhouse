import os
import uuid

import pytest
from dbt.tests.adapter.basic.files import model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import BaseIncremental

from tests.integration.adapter.helpers import below_version


def get_uuid_macro_value() -> str:
    if below_version(25):
        # Code: 36. DB::Exception: There was an error on [ch0:9000]: Code: 36. DB::Exception: Macro 'uuid' and empty arguments of ReplicatedMergeTree are supported only for ON CLUSTER queries with Atomic database engine. (BAD_ARGUMENTS) (version 24.3.18.7
        return str(uuid.uuid4())
    else:
        return '{uuid}'


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseSimpleMaterialization(BaseSimpleMaterializations):
    """Contains tests for table, view and swappable view materialization."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):

        test_config["db_engine"] = (
            "Replicated('/clickhouse/databases/"
            + get_uuid_macro_value()
            + "', '{shard}', '{replica}')"
        )
        return test_config


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseIncremental(BaseIncremental):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        test_config["db_engine"] = (
            "Replicated('/clickhouse/databases/"
            + get_uuid_macro_value()
            + "', '{shard}', '{replica}')"
        )
        return test_config

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
