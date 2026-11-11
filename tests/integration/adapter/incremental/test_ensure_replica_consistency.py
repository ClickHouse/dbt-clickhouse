import os

import pytest
from dbt.tests.util import run_dbt

REPLICATED_LW_DELETE_MODEL = """
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='key',
    order_by='key',
    engine="ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}')"
) }}
{% if is_incremental() %}
select number as key, 'second' as val from numbers(5, 10)
{% else %}
select number as key, 'first' as val from numbers(10)
{% endif %}
"""


def sync_replica_statements(project, table_pattern):
    """Count the SYSTEM SYNC REPLICA statements dbt issued for tables matching
    table_pattern in this test's schema. The pattern is a SQL LIKE fragment."""
    project.run_sql(f"SYSTEM FLUSH LOGS ON CLUSTER \"{project.test_config['cluster']}\"")
    return project.run_sql(
        "select count() from system.query_log "
        "where type = 'QueryFinish' and is_initial_query "
        "and query not like '%system.query_log%' "
        "and query like '%SYSTEM SYNC REPLICA%' "
        f"and query like '%`{project.test_schema}`.`{table_pattern}%' "
        "and query like '%LIGHTWEIGHT%'",
        fetch='one',
    )[0]


def staging_table_syncs(project):
    return sync_replica_statements(project, 'replicated_lw__dbt_new_data_')


def rows_per_replica(project):
    """Wait for every replica to fetch the final insert, then count what each one holds."""
    cluster = project.test_config['cluster']
    project.run_sql(
        f'SYSTEM SYNC REPLICA ON CLUSTER "{cluster}" {project.test_schema}.replicated_lw LIGHTWEIGHT'
    )
    return project.run_sql(
        "select hostName(), count(), uniqExact(key) "
        f"from clusterAllReplicas('{cluster}', {project.test_schema}.replicated_lw) group by 1",
        fetch='all',
    )


class TestEnsureReplicaConsistencyDeleteInsert:
    @pytest.fixture(scope='class')
    def models(self):
        return {'replicated_lw.sql': REPLICATED_LW_DELETE_MODEL}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_staging_table_is_synced_and_no_duplicates(self, project):
        run_dbt()
        run_dbt()

        assert staging_table_syncs(project) >= 1

        per_host = rows_per_replica(project)
        assert per_host, 'no replicas answered'
        for host, rows, distinct_keys in per_host:
            assert (rows, distinct_keys) == (15, 15), f'{host} holds {rows} rows'


class TestEnsureReplicaConsistencyDisabled:
    @pytest.fixture(scope='class')
    def models(self):
        return {'replicated_lw.sql': REPLICATED_LW_DELETE_MODEL}

    @pytest.fixture(scope='class')
    def dbt_profile_target(self, dbt_profile_target):
        return {**dbt_profile_target, 'ensure_replica_consistency': False}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_no_sync_when_disabled(self, project):
        run_dbt()
        run_dbt()

        assert staging_table_syncs(project) == 0
