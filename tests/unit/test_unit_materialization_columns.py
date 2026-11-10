from __future__ import annotations

from typing import Any, Dict, List

from tests.unit.macro_harness import SandboxSafeMock

MODEL_SQL = 'select id, name from people'


class _Column:
    def __init__(self, name: str, data_type: str) -> None:
        self.name = name
        self.data_type = data_type
        self.quoted = f'`{name}`'


def _relation(identifier: str) -> SandboxSafeMock:
    rel = SandboxSafeMock(schema='test_db', identifier=identifier)
    rel.__str__.return_value = f'`test_db`.`{identifier}`'
    rel.incorporate.side_effect = lambda **kwargs: _relation(identifier)
    return rel


class _Run:
    """The dbt context for one unit test run on a two-replica service.

    The session-scoped temporary table the stock materialization creates only exists on
    the replica that ran the CREATE. ``get_columns_in_relation`` answers for a replica
    that never saw it, which is what happens when the next request is routed elsewhere.
    """

    def __init__(self, columns: List[_Column]) -> None:
        self.statements: Dict[str, str] = {}
        self.temp_tables: List[str] = []
        self.column_types: Dict[str, str] | None = None
        adapter = SandboxSafeMock()
        adapter.get_columns_in_relation.return_value = []
        adapter.get_column_schema_from_query.return_value = columns
        self.adapter = adapter
        config = SandboxSafeMock()
        config.get.side_effect = lambda key, default=None: {
            'expected_rows': [{'id': 1, 'name': 'a'}],
            'expected_sql': None,
        }.get(key, default)
        self.context: Dict[str, Any] = {
            'adapter': adapter,
            'config': config,
            'this': _relation('people_unit_test'),
            'sql': MODEL_SQL,
            'execute': True,
            'statement': self.statement,
            'make_temp_relation': lambda relation, suffix='__dbt_tmp': _relation(
                relation.identifier + suffix
            ),
            'get_create_table_as_sql': self.get_create_table_as_sql,
            'get_empty_subquery_sql': lambda sql, header=None: (
                f'select * from ({sql}) as __dbt_sbq where false limit 0'
            ),
            'get_columns_in_query': lambda sql: ['id', 'name'],
            'get_expected_sql': self.get_expected_sql,
            'get_unit_test_sql': lambda sql, expected_sql, columns: (
                f'unit test over ({sql}) expecting ({expected_sql}) on {columns}'
            ),
        }

    def statement(self, name: str, fetch_result: bool = False, caller: Any = None) -> str:
        self.statements[name] = caller()
        return ''

    def get_create_table_as_sql(self, temporary: bool, relation: Any, sql: str) -> str:
        self.temp_tables.append(f'create {"temporary " if temporary else ""}table {relation}')
        return ''

    def get_expected_sql(
        self, expected_rows: Any, types: Dict[str, str], quoted: Dict[str, str]
    ) -> str:
        self.column_types = dict(types)
        return 'select 1 as id, \'a\' as name'

    def call(self, macros) -> None:
        macros.call('materialization_unit_clickhouse', context=self.context)


class TestUnitMaterializationColumns:
    def test_columns_do_not_depend_on_a_session_scoped_table(self, macros):
        """The lookup must not go through a temporary table another replica cannot see (#737)."""
        run = _Run([_Column('id', 'UInt64'), _Column('name', 'String')])
        run.call(macros)

        assert run.column_types == {'id': 'UInt64', 'name': 'String'}
        assert run.temp_tables == []
        run.adapter.get_columns_in_relation.assert_not_called()
        run.adapter.drop_relation.assert_not_called()

    def test_schema_comes_from_the_empty_subquery_of_the_model(self, macros):
        run = _Run([_Column('id', 'UInt64'), _Column('name', 'String')])
        run.call(macros)

        run.adapter.get_column_schema_from_query.assert_called_once_with(
            f'select * from ({MODEL_SQL}) as __dbt_sbq where false limit 0'
        )

    def test_main_statement_runs_the_unit_test_sql(self, macros):
        run = _Run([_Column('id', 'UInt64'), _Column('name', 'String')])
        run.call(macros)

        assert run.statements['main'].strip() == (
            f"unit test over ({MODEL_SQL}) expecting (select 1 as id, 'a' as name) on ['`id`', '`name`']"
        )
