import pytest
from clickhouse_connect.datatypes.registry import get_from_name
from dbt.adapters.clickhouse.column import ClickHouseColumn, comparable_type_name
from dbt.adapters.clickhouse.impl import ClickHouseAdapter


@pytest.mark.parametrize(
    'server_spelling',
    [
        'Array(Tuple(id UInt64, name String, request_count UInt64))',
        'Tuple(a Int32, b Tuple(c String, d Array(UInt8)))',
        'Map(String, Tuple(k UInt64, v String))',
        # Names ClickHouse 24.3-25.8 writes bare but newer servers quote
        'Tuple(from UInt8, select String, values UInt8)',
        'Tuple(`from` UInt8, `select` String, `values` UInt8)',
        'Tuple(`b c` String, `1x` UInt8)',
        "Tuple(`it's` String)",
        'JSON(max_dynamic_paths=10)',
        'JSON(`a.b` UInt32, SKIP c)',
        "Enum8('a' = 1, 'b' = 2)",
        "DateTime64(3, 'UTC')",
        'Decimal(18, 4)',
    ],
)
def test_http_spelling_compares_equal_to_server_spelling(server_spelling):
    http_spelling = get_from_name(server_spelling).name
    assert comparable_type_name(http_spelling) == comparable_type_name(server_spelling)


@pytest.mark.parametrize(
    'left,right',
    [
        ('Tuple(id UInt64)', 'Tuple(id String)'),
        ('Tuple(id UInt64)', 'Tuple(uid UInt64)'),
        ('Tuple(`a b` UInt8)', 'Tuple(a_b UInt8)'),
        ('Tuple(`a,b` UInt8)', 'Tuple(a UInt8, b UInt8)'),
        ("Enum8('a' = 1)", "Enum8('a ' = 1)"),
        ("Enum8('`a`' = 1)", "Enum8('a' = 1)"),
        ('Tuple(UInt64, String)', 'Tuple(a UInt64, b String)'),
    ],
)
def test_different_types_still_compare_different(left, right):
    assert comparable_type_name(left) != comparable_type_name(right)


def _adapter_with_columns(source, target) -> ClickHouseAdapter:
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.get_columns_in_relation = lambda relation: [
        ClickHouseColumn(name, dtype) for name, dtype in source
    ]
    adapter.get_column_schema_from_query = lambda sql, **kwargs: [
        ClickHouseColumn(name, dtype) for name, dtype in target
    ]
    return adapter


def test_schema_change_check_ignores_http_spelling():
    server_spelling = 'Array(Tuple(id UInt64, from String))'
    adapter = _adapter_with_columns(
        source=[('tags', server_spelling)],
        target=[('tags', get_from_name(server_spelling).name)],
    )

    changes = adapter.check_incremental_schema_changes('fail', None, 'select 1')

    assert not changes


def test_schema_change_check_detects_tuple_element_type_change():
    adapter = _adapter_with_columns(
        source=[('tags', 'Array(Tuple(id UInt64, name String))')],
        target=[('tags', 'Array(Tuple(`id` UInt64, `name` LowCardinality(String)))')],
    )

    changes = adapter.check_incremental_schema_changes('sync_all_columns', None, 'select 1')

    assert [column.name for column in changes.columns_to_modify] == ['tags']


@pytest.mark.parametrize(
    'left,right,expected',
    [
        ('Tuple(id UInt64)', 'Tuple(`id` UInt64)', True),
        ('Tuple(id UInt64)', 'Tuple(id String)', False),
        ('String', None, False),
        (None, None, True),
    ],
)
def test_data_types_match(left, right, expected):
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    assert adapter.data_types_match(left, right) is expected
