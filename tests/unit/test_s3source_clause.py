from multiprocessing.context import SpawnContext
from unittest.mock import MagicMock, Mock

from dbt.adapters.clickhouse.impl import ClickHouseAdapter


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


def test_compression_argument_without_structure():
    mock_config = MagicMock()
    mock_vars = MagicMock()
    mock_vars.vars = {}
    mock_config.vars = mock_vars

    adapter = ClickHouseAdapter(mock_config, Mock(spec=SpawnContext))
    adapter.config = mock_config

    result = adapter.s3source_clause(
        config_name='',
        s3_model_config={},
        bucket='test-bucket.s3.amazonaws.com',
        path='/test/path',
        fmt='Parquet',
        structure='',
        aws_access_key_id='',
        aws_secret_access_key='',
        role_arn='',
        compression='gzip',
    )

    assert "s3('https://test-bucket.s3.amazonaws.com/test/path', 'Parquet', '', 'gzip')" == result


def test_compression_argument_with_structure():
    mock_config = MagicMock()
    mock_vars = MagicMock()
    mock_vars.vars = {}
    mock_config.vars = mock_vars

    adapter = ClickHouseAdapter(mock_config, Mock(spec=SpawnContext))
    adapter.config = mock_config

    result = adapter.s3source_clause(
        config_name='',
        s3_model_config={},
        bucket='test-bucket.s3.amazonaws.com',
        path='/test/path',
        fmt='Parquet',
        structure='id UInt64',
        aws_access_key_id='',
        aws_secret_access_key='',
        role_arn='',
        compression='gzip',
    )

    assert (
        "s3('https://test-bucket.s3.amazonaws.com/test/path', 'Parquet','id UInt64', 'gzip')"
        == result
    )
