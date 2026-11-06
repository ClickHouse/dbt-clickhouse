import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt_common.exceptions import DbtRuntimeError


def _adapter() -> ClickHouseAdapter:
    return ClickHouseAdapter.__new__(ClickHouseAdapter)


@pytest.mark.parametrize(
    'model_config, expected',
    [
        ({'materialized': 'table', 'is_distributed': True}, True),
        ({'materialized': 'incremental', 'is_distributed': True}, True),
        ({'materialized': 'table'}, False),
        ({'materialized': 'incremental', 'is_distributed': False}, False),
        # quoted YAML values and rendered env vars
        ({'materialized': 'table', 'is_distributed': 'true'}, True),
        ({'materialized': 'table', 'is_distributed': ' True '}, True),
        ({'materialized': 'table', 'is_distributed': 'false'}, False),
        ({'materialized': 'table', 'is_distributed': 'FALSE'}, False),
        # deprecated materialization names
        ({'materialized': 'distributed_table'}, True),
        ({'materialized': 'distributed_incremental'}, True),
        # other materializations ignore the flag
        ({'materialized': 'view', 'is_distributed': True}, False),
        ({'materialized': 'materialized_view', 'is_distributed': True}, False),
        ({}, False),
    ],
)
def test_is_distributed(model_config, expected):
    assert _adapter().is_distributed(model_config) is expected


@pytest.mark.parametrize('value', ['yes', '', 1, 0])
def test_is_distributed_rejects_ambiguous_values(value):
    with pytest.raises(DbtRuntimeError, match='Invalid value for is_distributed'):
        _adapter().is_distributed({'materialized': 'table', 'is_distributed': value})
