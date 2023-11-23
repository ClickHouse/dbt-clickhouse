from dbt.adapters.clickhouse import ClickHouseColumn


class TestColumn:
    def test_base_types(self):
        verify_column('name', 'UInt8', False, False, False, True)
        verify_column('name', 'UInt16', False, False, False, True)
        verify_column('name', 'UInt32', False, False, False, True)
        verify_column('name', 'UInt64', False, False, False, True)
        verify_column('name', 'UInt128', False, False, False, True)
        verify_column('name', 'UInt256', False, False, False, True)
        verify_column('name', 'Int8', False, False, False, True)
        verify_column('name', 'Int16', False, False, False, True)
        verify_column('name', 'Int32', False, False, False, True)
        verify_column('name', 'Int64', False, False, False, True)
        verify_column('name', 'Int128', False, False, False, True)
        verify_column('name', 'Int256', False, False, False, True)
        str_col = verify_column('name', 'String', True, False, False, False)
        assert str_col.string_size() == 256
        fixed_str_col = verify_column('name', 'FixedString', True, False, False, False)
        assert fixed_str_col.string_size() == 256
        fixed_str_col = verify_column('name', 'FixedString(16)', True, False, False, False)
        assert fixed_str_col.string_size() == 16
        verify_column('name', 'Decimal(6, 6)', False, True, False, False)
        verify_column('name', 'Float32', False, False, True, False)
        verify_column('name', 'Float64', False, False, True, False)
        verify_column('name', 'Float64', False, False, True, False)
        verify_column('name', 'Date', False, False, False, False)
        verify_column('name', 'Date32', False, False, False, False)
        verify_column('name', "DateTime('Asia/Istanbul')", False, False, False, False)
        verify_column('name', "UUID", False, False, False, False)

    def test_array_type(self):
        # Test Array of Strings type
        col = ClickHouseColumn(column='name', dtype='Array(String)')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (Array(String), is nullable: False)>'

        # Test Array of Nullable Strings type
        col = ClickHouseColumn(column='name', dtype='Array(Nullable(String))')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (Array(Nullable(String)), is nullable: False)>'

        # Test Array of Nullable FixedStrings type
        col = ClickHouseColumn(column='name', dtype='Array(Nullable(FixedString(16)))')
        verify_column_types(col, False, False, False, False)
        assert (
            repr(col)
            == '<ClickhouseColumn name (Array(Nullable(FixedString(16))), is nullable: False)>'
        )

    def test_low_cardinality_nullable_type(self):
        col = ClickHouseColumn(column='name', dtype='LowCardinality(Nullable(String))')
        verify_column_types(col, True, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (Nullable(String), is nullable: True)>'
        col = ClickHouseColumn(column='name', dtype='LowCardinality(Nullable(FixedString(16)))')
        verify_column_types(col, True, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (Nullable(String), is nullable: True)>'

    def test_map_type(self):
        col = ClickHouseColumn(column='name', dtype='Map(String, UInt64)')
        verify_column_types(col, False, False, False, False)
        assert repr(col) == '<ClickhouseColumn name (Map(String, UInt64), is nullable: False)>'
        col = ClickHouseColumn(column='name', dtype='Map(String, Decimal(6, 6))')
        verify_column_types(col, False, False, False, False)
        assert (
            repr(col) == '<ClickhouseColumn name (Map(String, Decimal(6, 6)), is nullable: False)>'
        )


def verify_column(
    name: str, dtype: str, is_string: bool, is_numeric: bool, is_float: bool, is_int: bool
) -> ClickHouseColumn:
    data_type = 'String' if is_string else dtype
    col = ClickHouseColumn(column=name, dtype=dtype)
    verify_column_types(col, is_string, is_numeric, is_float, is_int)
    assert repr(col) == f'<ClickhouseColumn {name} ({data_type}, is nullable: False)>'

    # Test Nullable dtype.
    nullable_col = ClickHouseColumn(column=name, dtype=f'Nullable({dtype})')
    verify_column_types(nullable_col, is_string, is_numeric, is_float, is_int)
    assert (
        repr(nullable_col)
        == f'<ClickhouseColumn {name} (Nullable({data_type}), is nullable: True)>'
    )

    # Test low cardinality dtype
    low_cardinality_col = ClickHouseColumn(column=name, dtype=f'LowCardinality({dtype})')
    verify_column_types(low_cardinality_col, is_string, is_numeric, is_float, is_int)
    assert (
        repr(low_cardinality_col) == f'<ClickhouseColumn {name} ({data_type}, is nullable: False)>'
    )
    return col


def verify_column_types(
    col: ClickHouseColumn, is_string: bool, is_numeric: bool, is_float: bool, is_int: bool
):
    assert col.is_string() == is_string
    assert col.is_numeric() == is_numeric
    assert col.is_float() == is_float
    assert col.is_integer() == is_int
