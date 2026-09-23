"""JWT authentication via `access_token` and `access_token_provider`.

ClickHouse only validates JWTs in ClickHouse Cloud, so the end-to-end runs need
`DBT_CH_TEST_CLOUD=true` and a valid token in `DBT_CH_TEST_ACCESS_TOKEN`. Against OSS
ClickHouse we can still verify the wiring: the server must see a `Bearer` header
(rather than a Basic auth fallback) and reject it as an unsupported scheme, and the
provider must be called a second time after that rejection.
"""

import os

import pytest
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.httpclient import ChHttpClient
from dbt.tests.util import run_dbt

from tests.integration.adapter.clickhouse import token_provider_stub

_CLOUD = os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes')
_TOKEN = os.environ.get('DBT_CH_TEST_ACCESS_TOKEN')
_PROVIDER = f'{token_provider_stub.__name__}:rejected_then_real'
_BEARER_REJECTED = "'Bearer' HTTP Authorization scheme is not supported"


def _credentials(test_config, **auth):
    return ClickHouseCredentials(
        driver='http',
        host=test_config['host'],
        port=test_config['port'],
        secure=test_config['secure'],
        schema='default',
        **auth,
    )


@pytest.mark.skipif(_CLOUD, reason='OSS ClickHouse only; Cloud validates JWTs')
class TestAccessTokenRejectedByOSS:
    @pytest.fixture(autouse=True)
    def _http_only(self, test_config):
        if test_config['driver'] != 'http':
            pytest.skip('JWT authentication is only supported by the http driver')

    def test_token_sent_as_bearer(self, test_config):
        with pytest.raises(Exception, match=_BEARER_REJECTED):
            ChHttpClient(_credentials(test_config, access_token='not-a-real-jwt'))

    def test_provider_called_again_after_rejection(self, test_config):
        token_provider_stub.calls.clear()
        with pytest.raises(Exception, match=_BEARER_REJECTED):
            ChHttpClient(_credentials(test_config, access_token_provider=_PROVIDER))
        # initial token, then one refresh after the server rejected it
        assert len(token_provider_stub.calls) == 2


def _jwt_target(dbt_profile_target, **auth):
    if not (_CLOUD and _TOKEN):
        pytest.skip('requires DBT_CH_TEST_CLOUD=true and DBT_CH_TEST_ACCESS_TOKEN')
    if dbt_profile_target['driver'] != 'http':
        pytest.skip('JWT authentication is only supported by the http driver')
    target = {k: v for k, v in dbt_profile_target.items() if k not in ('user', 'password')}
    target.update(auth)
    return target


class TestAccessTokenCloud:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        return _jwt_target(dbt_profile_target, access_token=_TOKEN)

    @pytest.fixture(scope="class")
    def models(self):
        return {'jwt_model.sql': 'select 1 as id'}

    def test_run_with_access_token(self, project):
        run_dbt(['debug'])
        run_dbt(['run'])
        assert project.run_sql('select id from jwt_model', fetch='one')[0] == 1
        assert project.run_sql('select currentUser()', fetch='one')[0].startswith('JWT::')


class TestAccessTokenProviderCloud:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        token_provider_stub.calls.clear()
        return _jwt_target(dbt_profile_target, access_token_provider=_PROVIDER)

    @pytest.fixture(scope="class")
    def models(self):
        return {'jwt_model.sql': 'select 1 as id'}

    def test_run_with_refreshed_token(self, project):
        run_dbt(['run'])
        assert project.run_sql('select id from jwt_model', fetch='one')[0] == 1
        assert project.run_sql('select currentUser()', fetch='one')[0].startswith('JWT::')
        # The first token was rejected, so the provider must have been asked again.
        assert len(token_provider_stub.calls) >= 2
