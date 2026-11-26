from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.nativeclient import ChNativeClient
from dbt_common.exceptions import DbtConfigError


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


@pytest.mark.parametrize(
    'given, expected',
    [
        ({}, ('http', 8123)),
        ({'secure': True}, ('http', 8443)),
        ({'port': 9000}, ('native', 9000)),
        ({'port': 9440}, ('native', 9440)),
        ({'port': 8124}, ('http', 8124)),
        ({'driver': 'native'}, ('native', 9000)),
        ({'driver': 'native', 'secure': True}, ('native', 9440)),
        ({'driver': 'http', 'port': 9000}, ('http', 9000)),
    ],
)
def test_driver_and_port_defaults(given, expected):
    credentials = ClickHouseCredentials(host='localhost', schema='default', **given)
    assert (credentials.driver, credentials.port) == expected


def test_unknown_driver_rejected():
    with pytest.raises(DbtConfigError, match='Unrecognized ClickHouse driver grpc'):
        ClickHouseCredentials(host='localhost', schema='default', driver='grpc')
