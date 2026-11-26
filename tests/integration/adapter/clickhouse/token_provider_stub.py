"""Token provider used by test_access_token_auth.py.

Referenced from the dbt profile by dotted path, exactly as a user would reference their
own function, so this module must be importable from the repo root (pytest.ini sets
`pythonpath = .`).
"""

import base64
import json
import os

calls: list = []


def _b64url(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b'=').decode()


# Well-formed but unverifiable JWT: valid structure, all claims ClickHouse requires, an
# expiry in the past and a garbage signature. ClickHouse Cloud decodes it and then fails
# verification with AUTHENTICATION_FAILED (516), the same code an expired real token gets
# and the only code clickhouse-connect refreshes on. A random string would instead be
# rejected earlier as undecodable (BAD_ARGUMENTS, 36) and never trigger a refresh.
EXPIRED_TOKEN = '.'.join(
    [
        _b64url({'alg': 'RS256', 'typ': 'JWT', 'kid': 'dbt-test-stub'}),
        _b64url(
            {'iss': 'ClickHouse', 'sub': 'dbt-test-stub', 'aud': 'dbt-test', 'iat': 1, 'exp': 2}
        ),
        'bm90LWEtcmVhbC1zaWduYXR1cmU',
    ]
)


def rejected_then_real() -> str:
    """Return an expired token on the first call and the real one afterwards.

    The server rejects the expired token, clickhouse-connect calls this provider again
    and retries with the fresh token. That exercises the refresh path end to end.
    """
    calls.append(1)
    if len(calls) == 1:
        return EXPIRED_TOKEN
    return os.environ.get('DBT_CH_TEST_ACCESS_TOKEN', EXPIRED_TOKEN)
