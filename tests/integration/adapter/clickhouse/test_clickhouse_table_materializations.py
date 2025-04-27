import os

import pytest
from dbt.tests.adapter.basic.files import model_base, schema_base_yml, seeds_base_csv
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    relation_from_name,
    run_dbt,
)

from tests.integration.adapter.basic.test_basic import base_seeds_schema_yml


class TestMergeTreeTableMaterialization(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_table = """
          {{ config(
            order_by='(some_date, id, name)',
            engine='MergeTree()',
            materialized='table',
            settings={'allow_nullable_key': 1},
            query_settings={'allow_nondeterministic_mutations': 1})
        }}
        """
        base_table_sql = config_materialized_table + model_base
        return {
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        check_relation_types(project.adapter, {"table_model": "table"})

        # base table rowcount
        relation = relation_from_name(project.adapter, "table_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10


class TestDistributedMaterializations(BaseSimpleMaterializations):
    '''Test distributed materializations and check if data is properly distributed/replicated'''

    @pytest.fixture(scope="class")
    def models(self):
        config_distributed_table = """
            {{ config(
                order_by='(some_date, id, name)',
                engine='MergeTree()',
                materialized='distributed_table',
                settings={'allow_nullable_key': 1})
            }}
        """
        return {
            "distributed.sql": config_distributed_table + model_base,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "schema.yml": base_seeds_schema_yml,
            "base.csv": seeds_base_csv,
        }

    def assert_total_count_correct(self, project):
        # Check if data is properly distributed
        cluster = project.test_config['cluster']
        table_relation = relation_from_name(project.adapter, "distributed_local")
        cluster_info = project.run_sql(
            f"select shard_num,max(host_name) as host_name, count(distinct replica_num) as replica_counts "
            f"from system.clusters where cluster='{cluster}' group by shard_num",
            fetch="all",
        )
        sum_count = project.run_sql(
            f"select count() From clusterAllReplicas('{cluster}',{table_relation})",
            fetch="one",
        )
        total_count = 0
        # total count should be equal to sum(count of each shard * replica_counts)
        for shard_num, host_name, replica_counts in cluster_info:
            count = project.run_sql(
                f"select count() From remote('{host_name}',{table_relation})",
                fetch="one",
            )
            total_count += count[0] * replica_counts
        assert total_count == sum_count[0]

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_base(self, project):
        # cluster setting must exist
        cluster = project.test_config['cluster']
        assert cluster

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        # names exist in result nodes
        check_result_nodes_by_name(results, ["distributed"])

        # check relation types
        expected = {
            "base": "table",
            "distributed": "table",
        }
        check_relation_types(project.adapter, expected)

        relation = relation_from_name(project.adapter, "base")
        # table rowcount
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "distributed"])

        # check result
        self.assert_total_count_correct(project)

        # run full-refresh
        results = run_dbt(['run', '--full-refresh'])
        # run result length
        assert len(results) == 1
        # check result
        self.assert_total_count_correct(project)

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 2
        assert len(catalog.sources) == 1

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() != '', reason='Not on a cluster'
    )
    def test_no_cluster_setting(self, project):
        result = run_dbt(['run', '--select', 'distributed'], False)
        assert result[0].status == 'error'
        assert 'Compilation Error' in result[0].message


class TestReplicatedTableMaterialization(BaseSimpleMaterializations):
    '''Test ReplicatedMergeTree table with table materialization'''

    @pytest.fixture(scope="class")
    def models(self):
        config_replicated_table = """
            {{ config(
                order_by='(some_date, id, name)',
                engine="ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}' )",
                materialized='table',
                settings={'allow_nullable_key': 1})
            }}
        """

        return {
            "replicated.sql": config_replicated_table + model_base,
            "schema.yml": schema_base_yml,
        }

    def assert_total_count_correct(self, project):
        '''Check if table is created on cluster and data is properly replicated'''
        cluster = project.test_config['cluster']
        # check if data is properly distributed/replicated
        table_relation = relation_from_name(project.adapter, "replicated")
        # ClickHouse cluster in the docker-compose file
        # under tests/integration is configured with 3 nodes
        host_count = project.run_sql(
            f"select count(host_name) as host_count from system.clusters where cluster='{cluster}'",
            fetch="one",
        )
        assert host_count[0] > 1

        table_count = project.run_sql(
            f"select count() From clusterAllReplicas('{cluster}', system.tables) "
            f"where database='{table_relation.schema}' and name='{table_relation.identifier}'",
            fetch="one",
        )
        assert table_count[0] == host_count[0]

        sum_count = project.run_sql(
            f"select count() From clusterAllReplicas('{cluster}',{table_relation})",
            fetch="one",
        )

        assert sum_count[0] >= 20

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_base(self, project):
        # cluster setting must exist
        cluster = project.test_config['cluster']
        assert cluster

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 1

        # names exist in result nodes
        check_result_nodes_by_name(results, ["replicated"])

        # check relation types
        expected = {
            "base": "table",
            "replicated": "table",
        }
        check_relation_types(project.adapter, expected)

        relation = relation_from_name(project.adapter, "base")
        # table rowcount
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "replicated"])

        self.assert_total_count_correct(project)

        # run full refresh
        results = run_dbt(['--debug', 'run', '--full-refresh'])
        # run result length
        assert len(results) == 1

        self.assert_total_count_correct(project)


default_cluster_materialized_config = """
            {{ config(
                engine='MergeTree',
                materialized='materialized_view'
            ) }}
        """
models_disable_on_cluster_config = """
    {{ config(
                engine='MergeTree',
                materialized='materialized_view',
                disable_on_cluster='true'
            ) }}
"""


class TestMergeTreeDisableClusterMaterialization:
    '''Test MergeTree materialized view optionally created across cluster using disable_on_cluster config'''

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "schema.yml": base_seeds_schema_yml,
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "default_cluster_materialized.sql": default_cluster_materialized_config + model_base,
            "disable_cluster_materialized.sql": models_disable_on_cluster_config + model_base,
            "schema.yml": schema_base_yml,
        }

    def assert_object_count_on_cluster(self, project, model_name, expected_count: int):
        cluster = project.test_config['cluster']
        table_relation = relation_from_name(project.adapter, model_name)

        table_count = project.run_sql(
            f"select count() From clusterAllReplicas('{cluster}', system.tables) "
            f"where database='{table_relation.schema}' and name='{table_relation.identifier}'",
            fetch="one",
        )
        assert table_count[0] == expected_count

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_create_on_cluster_by_default(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "default_cluster_materialized.sql"])
        assert len(results) == 1
        self.assert_object_count_on_cluster(project, "default_cluster_materialized", 3)

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_disable_on_cluster(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--select", "disable_cluster_materialized.sql"])
        assert len(results) == 1
        self.assert_object_count_on_cluster(project, "disable_cluster_materialized", 1)
