import os

import pytest
from dbt.tests.adapter.basic.files import (
    model_incremental,
    schema_base_yml,
    seeds_added_csv,
    seeds_base_csv,
)
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange
from dbt.tests.util import run_dbt

from tests.integration.adapter.incremental.test_base_incremental import uniq_schema

uniq_source_model = """
{{config(
        materialized='distributed_table',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid']
    )
}}
SELECT now() - toIntervalHour(number) as ts, toInt32(number) as impid, concat('value', toString(number)) as value1
  FROM numbers(100)
"""

uniq_incremental_model = """
{{
    config(
        materialized='distributed_incremental',
        engine='MergeTree()',
        order_by=['ts'],
        unique_key=['impid']
    )
}}
select ts, impid from unique_source_one
{% if is_incremental() %}
where ts >= now() - toIntervalHour(1)
{% endif %}
"""


class TestSimpleDistributedIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unique_source_one.sql": uniq_source_model,
            "unique_incremental_one.sql": uniq_incremental_model,
            "schema.yml": uniq_schema,
        }

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_simple_incremental(self, project):
        run_dbt(["run", "--select", "unique_source_one"])
        run_dbt(["run", "--select", "unique_incremental_one"])


lw_delete_schema = """
version: 2

models:
  - name: "lw_delete_inc"
    description: "Incremental table"
"""

lw_delete_inc = """
{{ config(
        materialized='distributed_incremental',
        order_by=['key1'],
        unique_key='key1',
        incremental_strategy='delete+insert'
    )
}}
{% if is_incremental() %}
   WITH (SELECT max(key1) - 20 FROM lw_delete_inc) as old_max
   SELECT assumeNotNull(toUInt64(number + old_max + 1)) as key1, toInt64(-(number + old_max)) as key2, toString(number + 30) as value FROM numbers(100)
{% else %}
   SELECT toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value FROM numbers(100)
{% endif %}
"""


class TestLWDeleteDistributedIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"lw_delete_inc.sql": lw_delete_inc}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_lw_delete(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from lw_delete_inc", fetch="one")
        assert result[0] == 100
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from lw_delete_inc", fetch="one")
        assert result[0] == 180


compound_key_schema = """
version: 2

models:
  - name: "compound_key_inc"
    description: "Incremental table"
"""

compound_key_inc = """
{{ config(
        materialized='distributed_incremental',
        order_by=['key1', 'key2'],
        unique_key='key1, key2',
        incremental_strategy='delete+insert'
    )
}}
{% if is_incremental() %}
   WITH (SELECT max(key1) - 20 FROM compound_key_inc) as old_max
   SELECT assumeNotNull(toUInt64(number + old_max + 1)) as key1, toInt64(-key1) as key2, toString(number + 30) as value FROM numbers(100)
{% else %}
   SELECT toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value FROM numbers(100)
{% endif %}
"""


class TestDistributedIncrementalCompoundKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"compound_key_inc.sql": compound_key_inc}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_compound_key(self, project):
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from compound_key_inc", fetch="one")
        assert result[0] == 100
        run_dbt()
        result = project.run_sql("select count(*) as num_rows from compound_key_inc", fetch="one")
        assert result[0] == 180


replicated_seed_schema_yml = """
version: 2

seeds:
  - name: base
    config:
      engine: ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}' )
  - name: added
    config:
      engine: ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}' )
"""


class TestInsertsOnlyDistributedIncrementalMaterialization(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_incremental = """
          {{ config(order_by='(some_date, id, name)', inserts_only=True, materialized='distributed_incremental', unique_key='id') }}
        """
        incremental_sql = config_materialized_incremental + model_incremental
        return {
            "incremental.sql": incremental_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "schema.yml": replicated_seed_schema_yml,
        }

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_incremental(self, project):
        super().test_incremental(project)


incremental_not_schema_change_sql = """
{{ config(materialized="distributed_incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    toString(1) || '-' || toString(now64()) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestDistributedIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_incremental_not_schema_change(self, project):
        super().test_incremental_not_schema_change(project)


insert_overwrite_dist_inc = """
{{ config(
        materialized='distributed_incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['partitionKey'],
        order_by=['orderKey'],
        sharding_key='shardingKey'
    )
}}
{% if not is_incremental() %}
    SELECT shardingKey, partitionKey, orderKey, value
    FROM VALUES(
        'shardingKey UInt8, partitionKey String, orderKey UInt8, value String',
        (1, 'p1', 1, 'a'), (1, 'p1', 2, 'b'), (2, 'p1', 3, 'c'), (2, 'p2', 4, 'd')
    )
{% else %}
    SELECT shardingKey, partitionKey, orderKey, value
    FROM VALUES(
        'shardingKey UInt8, partitionKey String, orderKey UInt8, value String',
        (1, 'p1', 2, 'e'), (3, 'p1', 2, 'f')
    )
{% endif %}
"""


class TestInsertOverwriteDistributedIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"insert_overwrite_dist_inc.sql": insert_overwrite_dist_inc}

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_insert_overwrite_distributed_incremental(self, project):
        run_dbt()
        result = project.run_sql(
            "select * from insert_overwrite_dist_inc order by shardingKey, partitionKey, orderKey",
            fetch="all",
        )
        assert result == [
            (1, 'p1', 1, 'a'),
            (1, 'p1', 2, 'b'),
            (2, 'p1', 3, 'c'),
            (2, 'p2', 4, 'd'),
        ]
        run_dbt()
        result = project.run_sql(
            "select * from insert_overwrite_dist_inc order by shardingKey, partitionKey, orderKey",
            fetch="all",
        )
        assert result == [
            (1, 'p1', 2, 'e'),
            (2, 'p2', 4, 'd'),
            (3, 'p1', 2, 'f'),
        ]
