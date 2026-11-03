import os

import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.integration.adapter.incremental import test_incremental_unique_key

on_cluster = pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
)

insert_delete_inc = """
{{ config(
        materialized='incremental',
        engine=var('engine', 'MergeTree()'),
        order_by=['key1'],
        partition_by='length(value)',
        unique_key='key1',
        incremental_strategy='insert+delete',
        incremental_predicates=['no_such_column = 1' if var('fail_delete', 0) else '1 = 1'],
    )
}}
{% if is_incremental() %}
   select toUInt64(2) as key1, toInt64(500) as key2, '{{ var('value', 'test') }}' as value
   union all
   select toUInt64(102) as key1, toInt64(400) as key2, 'test2' as value
{% else %}
   select toUInt64(number) as key1, toInt64(-number) as key2, toString(number) as value from numbers(100)
{% endif %}
"""


class TestInsertDeleteIncremental:
    engine_vars: str = ''

    @pytest.fixture(scope="class")
    def models(self):
        return {"insert_delete_inc.sql": insert_delete_inc}

    def _run(self, extra_vars: str = '', expect_pass: bool = True):
        all_vars = ', '.join(v for v in (self.engine_vars, extra_vars) if v)
        return run_dbt(["run", "--vars", "{" + all_vars + "}"], expect_pass=expect_pass)

    def test_insert_delete(self, project):
        self._run()
        assert self._rows(project) == 100
        assert self._block_number_column_enabled(project)

        # key 2 moves from partition '1', which this run does not write to, to '4', which is new
        assert '4' not in self._partitions(project)
        self._run()
        assert self._rows(project) == 101
        assert self._versions(project, 2) == [(500, 'test', '4')]

        self._run('value: shifted')
        assert self._rows(project) == 101
        assert self._versions(project, 2) == [(500, 'shifted', '7')]

    def test_failure_after_insert_keeps_every_key(self, project):
        run_dbt(["run", "--full-refresh", "--vars", "{" + self.engine_vars + "}"])
        self._run()

        # an unknown column fails the delete before a mutation is queued that would block later runs
        self._run('fail_delete: 1, value: retried', expect_pass=False)
        assert self._keys(project) == 101
        assert sorted(self._versions(project, 2)) == [(500, 'retried', '7'), (500, 'test', '4')]

        self._run('value: final')
        assert self._rows(project) == 101
        assert self._versions(project, 2) == [(500, 'final', '5')]

    @staticmethod
    def _rows(project):
        return project.run_sql("select count() from insert_delete_inc", fetch="one")[0]

    @staticmethod
    def _keys(project):
        return project.run_sql("select uniqExact(key1) from insert_delete_inc", fetch="one")[0]

    @staticmethod
    def _partitions(project):
        rows = project.run_sql("select distinct _partition_id from insert_delete_inc", fetch="all")
        return {row[0] for row in rows}

    @staticmethod
    def _versions(project, key):
        return project.run_sql(
            f"select key2, value, _partition_id from insert_delete_inc where key1 = {key}",
            fetch="all",
        )

    @staticmethod
    def _block_number_column_enabled(project):
        engine_full = project.run_sql(
            "select engine_full from system.tables "
            f"where database = '{project.test_schema}' and name = 'insert_delete_inc'",
            fetch="one",
        )[0]
        return 'enable_block_number_column = 1' in engine_full


@on_cluster
class TestInsertDeleteReplicatedIncremental(TestInsertDeleteIncremental):
    # a new ReplicatedMergeTree partition starts at block number 0
    engine_vars = (
        "engine: \"ReplicatedMergeTree('/clickhouse/tables/{uuid}/one_shard', '{server_index}')\""
    )


insert_delete_existing_table = """
{{ config(
        materialized='incremental',
        order_by=['key1'],
        unique_key='key1',
        incremental_strategy=var('strategy', 'append'),
        on_schema_change='append_new_columns',
    )
}}
select toUInt64(number) as key1, toString(number) as value
{%- if var('extra', 0) %}, 'x' as extra{% endif %} from numbers(10)
"""


class TestInsertDeleteRequiresBlockNumberColumn:
    @pytest.fixture(scope="class")
    def models(self):
        return {"insert_delete_existing_table.sql": insert_delete_existing_table}

    def test_existing_table_without_block_number_column(self, project):
        run_dbt()
        _, log_output = run_dbt_and_capture(
            ["run", "--vars", "{strategy: insert+delete, extra: 1}"], expect_pass=False
        )
        assert 'enable_block_number_column' in log_output
        # the check runs before on_schema_change, so the table is not altered either
        assert self._columns(project) == ['key1', 'value']
        result = project.run_sql("select count() from insert_delete_existing_table", fetch="one")
        assert result[0] == 10

        # the fix the error suggests, quoted the way the adapter renders string settings
        project.run_sql(
            "alter table insert_delete_existing_table modify setting enable_block_number_column = '1'"
        )
        run_dbt(["run", "--vars", "{strategy: insert+delete, extra: 1}"])
        assert self._columns(project) == ['key1', 'value', 'extra']
        result = project.run_sql(
            "select count(), countIf(extra = 'x') from insert_delete_existing_table", fetch="one"
        )
        assert result == (10, 10)

    @staticmethod
    def _columns(project):
        rows = project.run_sql(
            "select name from system.columns "
            f"where database = '{project.test_schema}' and table = 'insert_delete_existing_table' "
            "order by position",
            fetch="all",
        )
        return [row[0] for row in rows]


insert_delete_distributed = """
{{ config(
        materialized='distributed_incremental',
        order_by=['key1'],
        unique_key='key1',
        incremental_strategy='insert+delete',
    )
}}
select toUInt64(number) as key1 from numbers(10)
"""


@on_cluster
class TestInsertDeleteDistributedIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {"insert_delete_distributed.sql": insert_delete_distributed}

    def test_rejected(self, project):
        run_dbt()
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert 'not supported by distributed_incremental' in log_output


class TestIncrementalPredicatesInsertDelete(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["id != 2"],
                "+incremental_strategy": "insert+delete",
            }
        }


class TestIncrementalUniqueKeyInsertDelete(test_incremental_unique_key.TestIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": "insert+delete"}}
