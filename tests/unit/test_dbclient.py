from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
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
