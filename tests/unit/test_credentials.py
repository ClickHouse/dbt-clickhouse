from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.nativeclient import ChNativeClient


def _credentials(**kwargs):
    return ClickHouseCredentials(
        host='localhost',
        port=9000,
        user='default',
        password='',
        schema='default',
        **kwargs,
    )


@pytest.mark.parametrize('tcp_keepalive', [False, True, [60, 30, 3]])
def test_tcp_keepalive_validates(tcp_keepalive):
    credentials = _credentials(tcp_keepalive=tcp_keepalive)

    ClickHouseCredentials.validate(credentials.to_dict(omit_none=True))

    assert credentials.tcp_keepalive == tcp_keepalive


@pytest.mark.parametrize(
    'tcp_keepalive,expected',
    [(False, False), (True, True), ([60, 30, 3], (60, 30, 3))],
)
def test_tcp_keepalive_passed_to_driver(tcp_keepalive, expected):
    client = ChNativeClient.__new__(ChNativeClient)
    client._conn_settings = {}

    with patch('clickhouse_driver.Client', MagicMock()) as mock_client:
        client._create_client(_credentials(tcp_keepalive=tcp_keepalive))

    assert mock_client.call_args.kwargs['tcp_keepalive'] == expected
