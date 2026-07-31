from unittest.mock import MagicMock

from dbt.adapters.clickhouse.impl import ClickHouseAdapter


def _adapter_with_mock_handle() -> tuple[ClickHouseAdapter, MagicMock]:
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    handle = MagicMock()
    handle.columns_in_query.return_value = []
    conn = MagicMock()
    conn.handle = handle
    adapter.connections = MagicMock()
    adapter.connections.get_if_exists.return_value = conn
    return adapter, handle


def test_get_column_schema_from_query_without_settings():
    adapter, handle = _adapter_with_mock_handle()
    sql = "select 1 as id"

    adapter.get_column_schema_from_query(sql)

    handle.columns_in_query.assert_called_once_with(sql)


def test_get_column_schema_from_query_with_keyword_settings():
    adapter, handle = _adapter_with_mock_handle()
    sql = "select 1 as id"
    settings = {"join_use_nulls": 1}

    adapter.get_column_schema_from_query(sql, query_settings=settings)

    handle.columns_in_query.assert_called_once_with(sql, settings=settings)


def test_get_column_schema_from_query_with_positional_settings_dict():
    adapter, handle = _adapter_with_mock_handle()
    sql = "select 1 as id"
    settings = {"join_use_nulls": 1}

    adapter.get_column_schema_from_query(sql, settings)

    handle.columns_in_query.assert_called_once_with(sql, settings=settings)


def test_get_column_schema_from_query_ignores_non_dict_positional_arg():
    """Base adapter may pass RelationConfig as second arg; ignore non-dicts."""
    adapter, handle = _adapter_with_mock_handle()
    sql = "select 1 as id"

    adapter.get_column_schema_from_query(sql, object())

    handle.columns_in_query.assert_called_once_with(sql)
