import pytest
from dbt_common.exceptions import CompilationError

from tests.unit.macro_harness import SandboxSafeMock

COLUMNS = {
    'id': {'name': 'id', 'data_type': 'UInt64'},
    'name': {'name': 'name', 'data_type': 'Nullable(String)'},
}


def _context(enforced, columns=COLUMNS):
    config = SandboxSafeMock()
    config.get.return_value = SandboxSafeMock(enforced=enforced)
    return {'config': config, 'model': {'name': 'people_dict', 'columns': columns}}


class TestDictionaryFieldsFromColumns:
    def test_contracted_model_takes_its_yaml_columns_in_order(self, macros):
        fields = macros.call('clickhouse_dictionary_fields_from_columns', context=_context(True))

        assert fields == [('id', 'UInt64'), ('name', 'Nullable(String)')]

    def test_model_without_contract_fails(self, macros):
        with pytest.raises(CompilationError, match='needs a `fields` config'):
            macros.call('clickhouse_dictionary_fields_from_columns', context=_context(False))

    def test_column_without_data_type_fails(self, macros):
        columns = {**COLUMNS, 'age': {'name': 'age'}}

        with pytest.raises(CompilationError, match='age'):
            macros.call(
                'clickhouse_dictionary_fields_from_columns', context=_context(True, columns)
            )
