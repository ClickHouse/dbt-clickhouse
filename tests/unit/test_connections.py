import uuid
from unittest.mock import MagicMock, patch

from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt.adapters.sql import SQLConnectionManager


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

        response, _ = _make_manager_with_client(mock_client).execute(
            'CREATE TABLE t (x Int32) ENGINE=Memory'
        )

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


class TestGetTableFromResponse:
    def test_returns_empty_table_when_no_column_metadata(self):
        # shape of a GRANT ... ON CLUSTER response from clickhouse-connect
        response = [['ch0', '9000', '0', '', '1', '0\nch1', '9000', '0', '', '0', '0']]

        table = ClickHouseConnectionManager.get_table_from_response(response, ())

        assert len(table.rows) == 0

    def test_fetch_on_cluster_grant_does_not_raise(self):
        mock_client = MagicMock()
        mock_client.query.return_value.result_set = [['ch0', '9000', '0', '', '0', '0']]
        mock_client.query.return_value.column_names = ()

        _, table = _make_manager_with_client(mock_client).execute(
            'GRANT ON CLUSTER c some_role TO some_user', fetch=True
        )

        assert len(table.rows) == 0


def _fired_events(mock_fire_event):
    return [c.args[0] for c in mock_fire_event.call_args_list]


@patch('dbt.adapters.clickhouse.connections.fire_event')
class TestSqlQueryEvents:
    def test_execute_fires_typed_events(self, mock_fire_event):
        response, _ = _make_manager_with_client(MagicMock()).execute('SELECT 1')

        query_event, status_event = _fired_events(mock_fire_event)
        assert isinstance(query_event, SQLQuery)
        assert query_event.conn_name == 'test'
        assert query_event.sql == 'SELECT 1'
        assert isinstance(status_event, SQLQueryStatus)
        assert status_event.status == 'OK'
        assert status_event.query_id == response.query_id

    def test_add_query_fires_typed_events_with_query_id(self, mock_fire_event):
        mock_client = MagicMock()

        _make_manager_with_client(mock_client).add_query('INSERT INTO t VALUES (1)')

        query_event, status_event = _fired_events(mock_fire_event)
        assert isinstance(query_event, SQLQuery)
        assert query_event.sql == 'INSERT INTO t VALUES (1)'
        assert isinstance(status_event, SQLQueryStatus)
        assert status_event.query_id == mock_client.command.call_args.kwargs['query_id']

    def test_add_query_abridges_logged_sql(self, mock_fire_event):
        mock_client = MagicMock()
        sql = 'x' * 1000

        _make_manager_with_client(mock_client).add_query(sql, abridge_sql_log=True)

        assert _fired_events(mock_fire_event)[0].sql == 'x' * 512 + '...'
        # the full statement is still executed
        assert mock_client.command.call_args.args[0] == sql

    def test_add_query_logs_full_sql_by_default(self, mock_fire_event):
        sql = 'x' * 1000

        _make_manager_with_client(MagicMock()).add_query(sql)

        assert _fired_events(mock_fire_event)[0].sql == sql


def test_reuse_connections_defaults_to_true():
    from dbt.adapters.clickhouse.credentials import ClickHouseCredentials

    creds = ClickHouseCredentials(host='localhost', schema='default')
    assert creds.reuse_connections is True


def _make_manager_with_reuse(reuse_connections: bool) -> ClickHouseConnectionManager:
    manager = ClickHouseConnectionManager.__new__(ClickHouseConnectionManager)
    manager.profile = MagicMock()
    manager.profile.credentials.reuse_connections = reuse_connections
    return manager


class TestReleaseRespectsReuseConnections:
    def test_release_is_a_noop_when_reuse_connections_is_true(self):
        manager = _make_manager_with_reuse(reuse_connections=True)
        with patch.object(SQLConnectionManager, 'release') as mock_super_release:
            manager.release()
        mock_super_release.assert_not_called()

    def test_release_calls_super_when_reuse_connections_is_false(self):
        manager = _make_manager_with_reuse(reuse_connections=False)
        with patch.object(SQLConnectionManager, 'release') as mock_super_release:
            manager.release()
        mock_super_release.assert_called_once()
