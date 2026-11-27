from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from clickhouse_connect.datatypes.registry import get_from_name
from dbt.adapters.clickhouse.httpclient import ChHttpClient, server_type_name


@pytest.mark.parametrize(
    'type_name',
    [
        'Array(Tuple(id UInt64, name String, request_count UInt64))',
        'Tuple(a Int32, b Tuple(c String, d Array(UInt8)))',
        'Map(String, Tuple(k UInt64, v String))',
        'Tuple(UInt64, String)',
        'Tuple(`b c` String, `1x` UInt8)',
        "Enum8('a' = 1, 'b' = 2)",
        "DateTime64(3, 'UTC')",
        'LowCardinality(Nullable(String))',
    ],
)
def test_server_type_name_matches_server_spelling(type_name):
    # The server's own spelling, which system.columns reports for the existing table
    assert server_type_name(get_from_name(type_name).name) == type_name


@pytest.mark.parametrize(
    'type_name,expected',
    [
        (
            'Array(Tuple(`id` UInt64, `name` String, `request_count` UInt64))',
            'Array(Tuple(id UInt64, name String, request_count UInt64))',
        ),
        ('Tuple(`_a1` UInt8, `A_b` UInt8)', 'Tuple(_a1 UInt8, A_b UInt8)'),
        # The server quotes these too
        (
            'Tuple(`b c` String, `1x` UInt8, `a.b` UInt8)',
            'Tuple(`b c` String, `1x` UInt8, `a.b` UInt8)',
        ),
        ('Tuple(`null` UInt8, `NULL` UInt8)', 'Tuple(`null` UInt8, `NULL` UInt8)'),
        ('Tuple(`x\\`y` String, `z` String)', 'Tuple(`x\\`y` String, z String)'),
        # Backquotes inside string literals are left alone
        (
            "Enum8('`a`' = 1, 'it''s' = 2, '\\'`b`' = 3)",
            "Enum8('`a`' = 1, 'it''s' = 2, '\\'`b`' = 3)",
        ),
    ],
)
def test_server_type_name(type_name, expected):
    assert server_type_name(type_name) == expected


def test_columns_in_query_uses_server_type_names():
    client = ChHttpClient.__new__(ChHttpClient)
    client._client = MagicMock()
    type_name = 'Array(Tuple(id UInt64, name String, request_count UInt64))'
    client._client.query.return_value = SimpleNamespace(
        column_names=('tags', 'name'),
        column_types=(get_from_name(type_name), get_from_name('Nullable(String)')),
    )

    columns = client.columns_in_query('select 1')

    assert [(column.name, column.dtype) for column in columns] == [
        ('tags', type_name),
        ('name', 'String'),
    ]
    assert columns[1].is_nullable
