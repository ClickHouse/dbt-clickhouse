from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter


@pytest.mark.parametrize('database', [None, 'analytics', 'logical_database'])
@pytest.mark.parametrize('count', [0, 1])
def test_check_schema_exists_uses_clickhouse_schema(database, count):
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.config = SimpleNamespace(quoting={})
    adapter.execute_macro = Mock(return_value=[(count,)])

    assert adapter.check_schema_exists(database, 'analytics') is bool(count)

    args, kwargs = adapter.execute_macro.call_args
    assert args == ('check_schema_exists',)
    assert kwargs['kwargs']['schema'] == 'analytics'
