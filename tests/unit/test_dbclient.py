from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dbt.adapters.clickhouse.dbclient as dbclient_module
import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.dbclient import ND_MUTATION_SETTING
from dbt.adapters.clickhouse.httpclient import ChHttpClient
from dbt_common.exceptions import DbtConfigError, DbtDatabaseError


@pytest.fixture
def mock_ch_client():
    with patch('clickhouse_connect.get_client') as mock_get_client:
        client = MagicMock()
        client.server_settings = {}
        mock_get_client.return_value = client
        yield mock_get_client


@pytest.fixture(autouse=True)
def reset_process_caches():
    dbclient_module._ensured_databases.clear()
    dbclient_module._nd_mutation_probe = None
    yield


def _set_nd_mutation_server_setting(mock_ch_client, value, readonly=0):
    mock_ch_client.return_value.server_settings = {
        ND_MUTATION_SETTING: SimpleNamespace(value=value, readonly=readonly)
    }


def _set_commands(mock_ch_client):
    return [
        call
        for call in mock_ch_client.return_value.command.call_args_list
        if str(call.args[0]).startswith(f'SET {ND_MUTATION_SETTING}')
    ]


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


def test_nd_mutation_already_enabled_server_side(mock_ch_client):
    """When the server already has the setting enabled nothing is injected or SET."""
    _set_nd_mutation_server_setting(mock_ch_client, value='1')
    client = ChHttpClient(_lw_credentials(use_lw_deletes=True))

    assert ND_MUTATION_SETTING not in _get_settings(mock_ch_client)
    assert not _set_commands(mock_ch_client)
    assert client.has_lw_deletes is True
    assert client.use_lw_deletes is True


def test_nd_mutation_enabled_via_set(mock_ch_client):
    """Disabled but writable: the client enables the setting with a SET command."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=0)
    client = ChHttpClient(_lw_credentials(use_lw_deletes=True))

    assert len(_set_commands(mock_ch_client)) == 1
    assert client.has_lw_deletes is True
    assert client.use_lw_deletes is True


def test_nd_mutation_probe_cached_across_clients(mock_ch_client):
    """The server probe runs once per process; each client still enables the
    setting for its own session with SET."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=0)
    ChHttpClient(_lw_credentials(use_lw_deletes=True))

    # A second client must not depend on the probe, even if server_settings
    # were to become unavailable.
    mock_ch_client.return_value.server_settings = {}
    client = ChHttpClient(_lw_credentials(use_lw_deletes=True))

    assert len(_set_commands(mock_ch_client)) == 2
    assert client.has_lw_deletes is True
    assert client.use_lw_deletes is True


def test_nd_mutation_enabled_even_when_lw_deletes_not_requested(mock_ch_client):
    """Old behavior: the setting is enabled whenever possible, independent of
    use_lw_deletes, so model-level delete_insert works without the profile flag."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=0)
    client = ChHttpClient(_lw_credentials(use_lw_deletes=False))

    assert len(_set_commands(mock_ch_client)) == 1
    assert client.has_lw_deletes is True
    assert client.use_lw_deletes is False


def test_nd_mutation_readonly_raises_when_lw_deletes_requested(mock_ch_client):
    """Disabled and read-only for the user: a clean config error at connect time."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=1)
    with pytest.raises(DbtConfigError, match=ND_MUTATION_SETTING):
        ChHttpClient(_lw_credentials(use_lw_deletes=True))


def test_nd_mutation_readonly_warns_when_lw_deletes_not_requested(mock_ch_client):
    """Disabled and read-only without use_lw_deletes: no error, capability reported off."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=1)
    client = ChHttpClient(_lw_credentials(use_lw_deletes=False))

    assert ND_MUTATION_SETTING not in _get_settings(mock_ch_client)
    assert not _set_commands(mock_ch_client)
    assert client.has_lw_deletes is False
    assert client.use_lw_deletes is False


def test_nd_mutation_set_failure_downgrades_gracefully(mock_ch_client):
    """If the SET command fails the run continues with lw deletes reported unavailable."""
    _set_nd_mutation_server_setting(mock_ch_client, value='0', readonly=0)

    def fail_on_set(sql, *args, **kwargs):
        if str(sql).startswith(f'SET {ND_MUTATION_SETTING}'):
            raise DbtDatabaseError('not allowed')
        return MagicMock()

    with patch.object(ChHttpClient, 'command', side_effect=fail_on_set):
        client = ChHttpClient(_lw_credentials(use_lw_deletes=True))

    assert client.has_lw_deletes is False
    assert client.use_lw_deletes is False


def _exists_calls(mock_ch_client):
    client = mock_ch_client.return_value
    return [
        call
        for call in client.command.call_args_list
        if str(call.args[0]).startswith('EXISTS DATABASE')
    ]


def test_ensure_database_probes_only_once_per_process(mock_ch_client):
    """The EXISTS DATABASE probe is cached process-wide across client creations."""
    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))

    assert len(_exists_calls(mock_ch_client)) == 1
    assert 'cache_test_db' in dbclient_module._ensured_databases


def test_database_dropped_invalidates_existence_cache(mock_ch_client):
    """Dropping a schema forces the next client to probe again."""
    client = ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    assert len(_exists_calls(mock_ch_client)) == 1

    client.database_dropped('cache_test_db')
    assert 'cache_test_db' not in dbclient_module._ensured_databases

    ChHttpClient(_lw_credentials(use_lw_deletes=False, schema='cache_test_db'))
    assert len(_exists_calls(mock_ch_client)) == 2
