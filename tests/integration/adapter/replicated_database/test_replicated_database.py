import pytest
from dbt.tests.adapter.basic.files import model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import BaseIncremental


class TestReplicatedDatabaseSimpleMaterialization(BaseSimpleMaterializations):
    """Contains tests for table, view and swappable view materialization."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        test_config["db_engine"] = (
            "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')"
        )
        return test_config


class TestReplicatedDatabaseIncremental(BaseIncremental):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        test_config["db_engine"] = (
            "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')"
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
