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
