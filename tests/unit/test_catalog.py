from unittest.mock import MagicMock

from dbt.adapters.capability import Capability
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.relation import ClickHouseRelation, ClickHouseRelationType


class FakeCatalog:
    def __init__(self):
        self.predicate = None

    def __bool__(self):
        return True

    def where(self, predicate):
        self.predicate = predicate
        return self


class FakeRow(dict):
    pass


def test_schema_metadata_by_relations_is_supported():
    assert ClickHouseAdapter.supports(Capability.SchemaMetadataByRelations)


def test_get_filtered_catalog_uses_relation_scoped_fetch_for_small_relation_sets():
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    catalog = FakeCatalog()
    relations = {ClickHouseRelation.create(schema='analytics', identifier='orders')}
    used_schemas = frozenset()

    adapter.get_catalog = MagicMock(return_value=('schema-catalog', []))
    adapter.get_catalog_by_relations = MagicMock(return_value=(catalog, []))

    result, exceptions = adapter.get_filtered_catalog([], used_schemas, relations=relations)

    assert result is catalog
    assert exceptions == []
    adapter.get_catalog.assert_not_called()
    adapter.get_catalog_by_relations.assert_called_once_with(used_schemas, relations)
    assert catalog.predicate is not None
    assert catalog.predicate(FakeRow(table_schema='analytics', table_name='orders'))
    assert not catalog.predicate(FakeRow(table_schema='analytics', table_name='customers'))


def test_catalog_sql_reports_dictionaries_with_the_relation_type(macros):
    """`dbt docs generate` must report the type the adapter already uses for the same relation."""
    sql = macros.call('clickhouse__get_catalog_results_sql', "where columns.database = 'analytics'")
    table_type = ' '.join(sql.split())

    dictionary = ClickHouseRelationType.Dictionary.value
    assert f"tables.engine = 'Dictionary', '{dictionary}'," in table_type
    assert "tables.engine in ('MaterializedView', 'View'), 'view'," in table_type
    assert "'table' ) as table_type" in table_type
