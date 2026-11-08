from __future__ import annotations

from typing import Any, Dict, List

from tests.unit.macro_harness import SandboxSafeMock

STAGING = '`test_db`.`target_table__dbt_new_data_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee`'


class _Rows:
    def __init__(self, partition_ids: List[str]) -> None:
        self.rows = [{'partition_id': p} for p in partition_ids]


def _relation(schema: str, identifier: str) -> SandboxSafeMock:
    rel = SandboxSafeMock(schema=schema, identifier=identifier, should_on_cluster=False)
    rel.__str__.return_value = f'`{schema}`.`{identifier}`'
    rel.incorporate.side_effect = lambda path: _relation(
        path.get('schema', schema), path.get('identifier', identifier)
    )
    return rel


class _Run:
    """The dbt context for one insert_overwrite run. Records every query and statement."""

    def __init__(self, partition_ids: List[str], database_engine: str = '') -> None:
        self.queries: List[str] = []
        self.statements: Dict[str, str] = {}
        self.partition_ids = partition_ids
        target = _relation('test_db', 'target_table')
        adapter = SandboxSafeMock()
        adapter.get_clickhouse_cluster_name.return_value = None
        adapter.is_at_or_after_version.return_value = False
        self.context: Dict[str, Any] = {
            'adapter': adapter,
            'target': SandboxSafeMock(database_engine=database_engine),
            'this': target,
            'existing_relation': target,
            'sql': 'select 1',
            'execute': True,
            'invocation_id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            'run_query': self.run_query,
            'statement': self.statement,
            'drop_relation_if_exists': lambda relation: '',
            'get_create_table_as_sql': lambda temporary, relation, sql: (
                f'create table {relation} as {sql}'
            ),
        }

    def run_query(self, sql: str) -> _Rows:
        self.queries.append(sql)
        return _Rows(self.partition_ids)

    def statement(self, name: str, fetch_result: bool = False, caller: Any = None) -> str:
        self.statements[name] = caller()
        return ''

    def call(self, macros) -> None:
        macros.call(
            'clickhouse__incremental_insert_overwrite',
            self.context['existing_relation'],
            ['toYYYYMM(ts)'],
            False,
            context=self.context,
        )


class TestInsertOverwritePartitions:
    def test_partitions_are_read_from_the_staging_table(self, macros):
        """system.parts is per replica and can lag the staging insert on another replica (#418)."""
        run = _Run(['202611'])
        run.call(macros)

        assert len(run.queries) == 1
        assert '_partition_id' in run.queries[0]
        assert f'FROM {STAGING}' in run.queries[0]
        assert 'system.parts' not in run.queries[0]

    def test_shared_engine_embeds_sequential_consistency(self, macros):
        run = _Run(['202611'], database_engine='Shared')
        run.call(macros)

        assert 'settings select_sequential_consistency = 1' in run.queries[0]

    def test_other_engines_do_not_embed_sequential_consistency(self, macros):
        run = _Run(['202611'], database_engine='')
        run.call(macros)

        assert 'select_sequential_consistency' not in run.queries[0]

    def test_every_returned_partition_is_replaced(self, macros):
        run = _Run(['202610', '202611'])
        run.call(macros)

        swap = run.statements['replace_partitions']
        assert "replace partition id '202610'" in swap
        assert "replace partition id '202611'" in swap
        assert f'from {STAGING}' in swap

    def test_empty_staging_issues_no_swap(self, macros):
        run = _Run([])
        run.call(macros)

        assert 'replace_partitions' not in run.statements
