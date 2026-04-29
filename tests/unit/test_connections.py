import uuid
from unittest.mock import MagicMock, patch

from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager


def _make_manager_with_client(mock_client):
    conn = MagicMock()
    conn.name = 'test'
    conn.handle = mock_client

    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    manager.get_thread_connection = MagicMock(return_value=conn)
    manager._add_query_comment = lambda s: s
    return manager


class TestAdapterResponseQueryId:
    def test_query_id_passed_to_client_on_fetch(self):
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_set = [('val',)]
        mock_result.column_names = ['col']
        mock_client.query.return_value = mock_result

        response, _ = _make_manager_with_client(mock_client).execute('SELECT 1', fetch=True)

        _, kwargs = mock_client.query.call_args
        assert 'query_id' in kwargs
        assert kwargs['query_id'] == response.query_id

    def test_query_id_passed_to_client_on_command(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        response, _ = _make_manager_with_client(mock_client).execute('CREATE TABLE t (x Int32) ENGINE=Memory')

        _, kwargs = mock_client.command.call_args
        assert 'query_id' in kwargs
        assert kwargs['query_id'] == response.query_id

    def test_query_id_is_valid_uuid(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        response, _ = _make_manager_with_client(mock_client).execute('SELECT 1')

        # raises ValueError if not a valid UUID
        uuid.UUID(response.query_id)

    def test_query_id_unique_per_call(self):
        mock_client = MagicMock()
        mock_client.command.return_value = None

        manager = _make_manager_with_client(mock_client)
        r1, _ = manager.execute('SELECT 1')
        r2, _ = manager.execute('SELECT 1')

        assert r1.query_id != r2.query_id
