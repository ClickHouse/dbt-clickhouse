import os
import time
from datetime import datetime

import pytest
from dbt.tests.adapter.basic.files import model_base, schema_base_yml
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.util import relation_from_name, run_dbt


class TestTableTTL(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        config_materialized_table = """
          {{ config(
            order_by='(some_date, id, name)',
            engine='MergeTree()',
            materialized='table',
            settings={'allow_nullable_key': 1},
            ttl='some_date + INTERVAL 5 SECONDS',
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

        # base table rowcount
        relation = relation_from_name(project.adapter, "table_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        # the dates from the seed are too old, so those are expired
        assert result[0] == 0

        # insert new data
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        project.run_sql(f"insert into {relation} (*) values (11, 'Elian', '{now}')")

        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        # the dates from the seed are too old, so those are expired
        assert result[0] == 1

        # wait for TTL to expire
        time.sleep(6)

        # optimize table
        project.run_sql(f"OPTIMIZE TABLE {relation} FINAL")

        # make sure is empty
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 0


DISTRIBUTED_TABLE_TTL_MODEL = """
{{
    config(
        order_by='(id)',
        engine='MergeTree()',
        materialized='distributed_table',
        incremental_strategy='append',
        ttl='expiration_date + interval 5 seconds',
    )
}}
SELECT 1 AS id, toDateTime('2010-05-20 06:46:51') AS expiration_date
UNION ALL
SELECT 2, toDateTime('2007-09-03 12:31:55')
UNION ALL
SELECT 3, toDateTime('2005-01-01 09:23:15')
"""


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
)
class TestDistributedTableTTL:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "id_expire.sql": DISTRIBUTED_TABLE_TTL_MODEL,
        }

    def test_base(self, project):
        results = run_dbt()
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "id_expire")
        relation_local = relation_from_name(project.adapter, "id_expire_local")

        # wait for TTL to expire
        time.sleep(6)

        project.run_sql(f"OPTIMIZE TABLE {relation_local} FINAL")

        # make sure is empty
        cnt = project.run_sql(f"select count(*) from {relation}", fetch="all")
        assert cnt[0][0] == 0
