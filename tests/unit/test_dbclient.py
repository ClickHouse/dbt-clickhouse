from unittest.mock import MagicMock, patch

import dbt.adapters.clickhouse.dbclient as dbclient_module
import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.dbclient import ND_MUTATION_SETTING
from dbt.adapters.clickhouse.httpclient import ChHttpClient


@pytest.fixture
def mock_ch_client():
    with patch('clickhouse_connect.get_client') as mock_get_client:
        mock_get_client.return_value = MagicMock()
        yield mock_get_client


def _get_settings(mock_get_client):
    return mock_get_client.call_args.kwargs['settings']


def test_shared_engine_default_settings(mock_ch_client):
    """Shared engine sets select_sequential_consistency=1 plus common defaults."""
    credentials = ClickHouseCredentials(
        host='localhost',
        port=8123,
        user='default',
        password='',
        schema='default',
        database_engine='Shared',
    )
    ChHttpClient(credentials)
    settings = _get_settings(mock_ch_client)

    assert settings['select_sequential_consistency'] == '1'
    assert settings['mutations_sync'] == '3'
    assert settings['alter_sync'] == '3'


def test_shared_engine_custom_settings_override(mock_ch_client):
    """User's custom_settings override Shared engine defaults."""
    credentials = ClickHouseCredentials(
        host='localhost',
        port=8123,
        user='default',
        password='',
        schema='default',
        database_engine='Shared',
        custom_settings={
            'select_sequential_consistency': '0',
            'mutations_sync': '1',
            'alter_sync': '0',
        },
    )
    ChHttpClient(credentials)
    settings = _get_settings(mock_ch_client)

    assert settings['select_sequential_consistency'] == '0'
    assert settings['mutations_sync'] == '1'
    assert settings['alter_sync'] == '0'


def test_default_engine_settings(mock_ch_client):
    """Default engine has no select_sequential_consistency."""
    credentials = ClickHouseCredentials(
        host='localhost',
        port=8123,
        user='default',
        password='',
        schema='default',
    )
    ChHttpClient(credentials)
    settings = _get_settings(mock_ch_client)

    assert settings['mutations_sync'] == '3'
    assert settings['alter_sync'] == '3'
    assert 'select_sequential_consistency' not in settings


def _lw_credentials(use_lw_deletes, schema='default'):
    return ClickHouseCredentials(
        host='localhost',
        port=8123,
        user='default',
        password='',
        schema=schema,
        use_lw_deletes=use_lw_deletes,
        check_exchange=False,
    )


def test_nd_mutation_setting_injected_when_lw_deletes_enabled(mock_ch_client):
    """allow_nondeterministic_mutations is set on the client when lw deletes are requested."""
    ChHttpClient(_lw_credentials(use_lw_deletes=True))
    settings = _get_settings(mock_ch_client)

    assert settings[ND_MUTATION_SETTING] == '1'


def test_nd_mutation_setting_absent_when_lw_deletes_disabled(mock_ch_client):
    """No nondeterministic mutation setting is added by default."""
    ChHttpClient(_lw_credentials(use_lw_deletes=False))
    settings = _get_settings(mock_ch_client)

    assert ND_MUTATION_SETTING not in settings


def test_check_lightweight_deletes_always_available(mock_ch_client):
    """Lightweight deletes are assumed available; use_lw_deletes mirrors the request."""
    client = ChHttpClient(_lw_credentials(use_lw_deletes=True))
    assert client.has_lw_deletes is True
    assert client.use_lw_deletes is True


def _exists_calls(mock_ch_client):
    client = mock_ch_client.return_value
    return [
        call
        for call in client.command.call_args_list
        if str(call.args[0]).startswith('EXISTS DATABASE')
    ]


def test_ensure_database_probes_only_once_per_process(mock_ch_client):
    """The EXISTS DATABASE probe is cached process-wide across client creations."""
    dbclient_module._ensured_databases.clear()

    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))

    assert len(_exists_calls(mock_ch_client)) == 1
    assert 'cache_test_db' in dbclient_module._ensured_databases


def test_database_dropped_invalidates_existence_cache(mock_ch_client):
    """Dropping a schema forces the next client to probe again."""
    dbclient_module._ensured_databases.clear()

    client = ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    assert len(_exists_calls(mock_ch_client)) == 1

    client.database_dropped('cache_test_db')
    assert 'cache_test_db' not in dbclient_module._ensured_databases

    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    assert len(_exists_calls(mock_ch_client)) == 2
