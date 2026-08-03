from multiprocessing.context import SpawnContext
from unittest.mock import MagicMock, Mock

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt_common.exceptions import DbtRuntimeError

ROLE_ARN = 'arn:aws:iam::111111111111:role/ClickHouseAccessRole-001'


def test_aws_credentials_from_config():
    mock_config = MagicMock()
    mock_vars = MagicMock()
    mock_vars.vars = {
        'test_s3': {
            'bucket': 'test-bucket.s3.amazonaws.com',
            'path': '/test/path',
            'fmt': 'Parquet',
            'aws_access_key_id': 'test_key_123',
            'aws_secret_access_key': 'test_secret_456',
        }
    }
    mock_config.vars = mock_vars

    adapter = ClickHouseAdapter(mock_config, Mock(spec=SpawnContext))
    adapter.config = mock_config

    result = adapter.s3source_clause(
        config_name='test_s3',
        s3_model_config={},
        bucket='',
        path='',
        fmt='',
        structure='',
        aws_access_key_id='',
        aws_secret_access_key='',
        role_arn='',
        compression='',
    )

    assert 'test_key_123' in result
    assert 'test_secret_456' in result
    assert (
        "s3('https://test-bucket.s3.amazonaws.com/test/path', 'test_key_123', 'test_secret_456', 'Parquet')"
        == result
    )


def test_model_config_does_not_mutate_global_config():
    mock_config = MagicMock()
    mock_vars = MagicMock()
    s3_config = {
        'bucket': 'test-bucket.s3.amazonaws.com',
        'path': '/global/path',
        'fmt': 'Parquet',
    }
    mock_vars.vars = {'test_s3': s3_config}
    mock_config.vars = mock_vars

    adapter = ClickHouseAdapter(mock_config, Mock(spec=SpawnContext))
    adapter.config = mock_config

    model_result = adapter.s3source_clause(
        config_name='test_s3',
        s3_model_config={'path': '/model/path'},
        bucket='',
        path='',
        fmt='',
        structure='',
        aws_access_key_id='',
        aws_secret_access_key='',
        role_arn='',
        compression='',
    )
    global_result = adapter.s3source_clause(
        config_name='test_s3',
        s3_model_config={},
        bucket='',
        path='',
        fmt='',
        structure='',
        aws_access_key_id='',
        aws_secret_access_key='',
        role_arn='',
        compression='',
    )

    assert s3_config['path'] == '/global/path'
    assert '/model/path' in model_result
    assert '/global/path' in global_result


def role_based_clause(s3_vars: dict, **kwargs) -> str:
    """Build an s3() clause for a bucket configured without access keys."""
    mock_config = MagicMock()
    mock_vars = MagicMock()
    mock_vars.vars = {
        'test_s3': {
            'bucket': 'test-bucket.s3.amazonaws.com',
            'path': '/test/path',
            'fmt': 'Parquet',
            **s3_vars,
        }
    }
    mock_config.vars = mock_vars

    adapter = ClickHouseAdapter(mock_config, Mock(spec=SpawnContext))
    adapter.config = mock_config

    kwargs.setdefault('role_arn', '')
    return adapter.s3source_clause(
        config_name='test_s3',
        s3_model_config={},
        bucket='',
        path='',
        fmt='',
        structure='',
        aws_access_key_id='',
        aws_secret_access_key='',
        compression='',
        **kwargs,
    )


def test_role_arn_without_external_id():
    result = role_based_clause({}, role_arn=ROLE_ARN)

    assert (
        "s3('https://test-bucket.s3.amazonaws.com/test/path', 'Parquet', "
        f"extra_credentials(role_arn='{ROLE_ARN}'))" == result
    )


def test_role_arn_with_external_id():
    result = role_based_clause({}, role_arn=ROLE_ARN, external_id='my-external-id')

    assert (
        "s3('https://test-bucket.s3.amazonaws.com/test/path', 'Parquet', "
        f"extra_credentials(role_arn='{ROLE_ARN}', external_id='my-external-id'))" == result
    )


def test_role_arn_and_external_id_from_config():
    result = role_based_clause({'role_arn': ROLE_ARN, 'external_id': 'my-external-id'})

    assert (
        "s3('https://test-bucket.s3.amazonaws.com/test/path', 'Parquet', "
        f"extra_credentials(role_arn='{ROLE_ARN}', external_id='my-external-id'))" == result
    )


def test_external_id_without_role_arn_raises():
    with pytest.raises(DbtRuntimeError, match='external_id specified without role_arn'):
        role_based_clause({}, external_id='my-external-id')
