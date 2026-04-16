from unittest.mock import MagicMock, patch

from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager


def _make_manager():
    """Return a ClickHouseConnectionManager with a mocked thread connection."""
    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    return manager


def _mock_connection(query_result):
    """Build a fake thread connection whose handle returns query_result."""
    client = MagicMock()
    client.query.return_value = query_result
    client.command.return_value = query_result

    conn = MagicMock()
    conn.handle = client

    return conn


def test_execute_fetch_populates_query_id():
    """HTTP client: fetch=True reads query_id from QueryResult.query_id."""
    query_result = MagicMock()
    query_result.query_id = 'abc-123'
    query_result.result_set = []
    query_result.column_names = []

    manager = _make_manager()
    conn = _mock_connection(query_result)

    with (
        patch.object(manager, 'get_thread_connection', return_value=conn),
        patch.object(manager, '_add_query_comment', side_effect=lambda sql: sql),
    ):
        response, _ = manager.execute('SELECT 1', fetch=True)

    assert response.query_id == 'abc-123'


def test_execute_command_populates_query_id():
    """HTTP client: fetch=False reads query_id from QuerySummary.summary dict."""
    query_result = MagicMock()
    query_result.summary = {'query_id': 'def-456', 'read_rows': '1'}

    manager = _make_manager()
    conn = _mock_connection(query_result)

    with (
        patch.object(manager, 'get_thread_connection', return_value=conn),
        patch.object(manager, '_add_query_comment', side_effect=lambda sql: sql),
    ):
        response, _ = manager.execute('CREATE TABLE t (x Int32) ENGINE=Memory', fetch=False)

    assert response.query_id == 'def-456'


def test_execute_native_client_fetch_returns_empty_query_id():
    """Native client: no query_id attribute on result — query_id falls back to ''."""
    query_result = MagicMock(spec=[])  # no attributes at all
    query_result.result_set = []
    query_result.column_names = []

    manager = _make_manager()
    conn = _mock_connection(query_result)

    with (
        patch.object(manager, 'get_thread_connection', return_value=conn),
        patch.object(manager, '_add_query_comment', side_effect=lambda sql: sql),
    ):
        response, _ = manager.execute('SELECT 1', fetch=True)

    assert response.query_id == ''


def test_execute_native_client_command_returns_empty_query_id():
    """Native client: no summary dict on result — query_id falls back to ''."""
    query_result = MagicMock(spec=[])  # no attributes at all

    manager = _make_manager()
    conn = _mock_connection(query_result)

    with (
        patch.object(manager, 'get_thread_connection', return_value=conn),
        patch.object(manager, '_add_query_comment', side_effect=lambda sql: sql),
    ):
        response, _ = manager.execute('DROP TABLE IF EXISTS t', fetch=False)

    assert response.query_id == ''
