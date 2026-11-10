import os

import pytest
from dbt.tests.adapter.basic.files import model_incremental, schema_base_yml
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.util import relation_from_name, run_dbt


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseSimpleMaterialization(BaseSimpleMaterializations):
    """Contains tests for table, view and swappable view materialization."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseIncremental(BaseIncremental):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }

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


distributed_table_model = """
{{ config(materialized='distributed_table', engine='MergeTree()', order_by='id', sharding_key='id') }}
select number as id, toString(number) as name from numbers(10)
"""

distributed_incremental_model = """
{{ config(materialized='distributed_incremental', engine='MergeTree()', order_by='id', unique_key='id', sharding_key='id') }}
select number as id, toString(number) as name from numbers(20)
{% if is_incremental() %}
where id >= 5
{% else %}
where id < 10
{% endif %}
"""


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
)
class TestReplicatedDatabaseDistributed:
    """Distributed materializations on a Replicated database.

    The database engine broadcasts the DDL itself and rejects ON CLUSTER (code 80), so no
    ON CLUSTER clause is rendered; the cluster from the profile still has to be accepted,
    since it names the shards of the Distributed table."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        # copy, since the session-scoped config is shared with every other test module
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dist_table.sql": distributed_table_model,
            "dist_incremental.sql": distributed_incremental_model,
        }

    def _count_on_all_shards(self, project, relation_name):
        cluster = project.test_config['cluster']
        relation = relation_from_name(project.adapter, relation_name)
        return project.run_sql(
            f"select count() from cluster('{cluster}', {relation})", fetch="one"
        )[0]

    def _nodes_with_table(self, project, table_name):
        cluster = project.test_config['cluster']
        return project.run_sql(
            f"select count() from clusterAllReplicas('{cluster}', system.tables) "
            f"where database = '{project.test_schema}' and name = '{table_name}'",
            fetch="one",
        )[0]

    def test_distributed_materializations(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert project.run_sql("select count() from dist_table", fetch="one")[0] == 10
        assert project.run_sql("select count() from dist_incremental", fetch="one")[0] == 10
        # the Replicated database created the local tables on every node of the cluster
        cluster = project.test_config['cluster']
        nodes = project.run_sql(
            f"select count() from system.clusters where cluster = '{cluster}'", fetch="one"
        )[0]
        assert nodes > 1
        assert self._nodes_with_table(project, "dist_table_local") == nodes
        assert self._nodes_with_table(project, "dist_incremental_local") == nodes
        assert self._count_on_all_shards(project, "dist_table_local") == 10
        assert self._count_on_all_shards(project, "dist_incremental_local") == 10

        # a second run rebuilds the distributed table and runs the incremental path
        results = run_dbt(["run"])
        assert len(results) == 2
        assert project.run_sql("select count() from dist_table", fetch="one")[0] == 10
        assert project.run_sql("select count() from dist_incremental", fetch="one")[0] == 20
        assert self._count_on_all_shards(project, "dist_incremental_local") == 20
