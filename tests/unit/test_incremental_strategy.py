from unittest.mock import MagicMock

import pytest
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt_common.exceptions import DbtRuntimeError


def _make_adapter(has_lw_deletes: bool) -> ClickHouseAdapter:
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.connections = MagicMock()
    mock_conn = MagicMock()
    mock_conn.handle.has_lw_deletes = has_lw_deletes
    mock_conn.handle.use_lw_deletes = has_lw_deletes
    adapter.connections.get_if_exists.return_value = mock_conn
    return adapter


class TestValidateIncrementalStrategy:
    def test_delete_insert_raises_when_lw_deletes_unavailable(self):
        """delete_insert requires lightweight deletes to be usable on the server
        (has_lw_deletes reflects whether allow_nondeterministic_mutations could be enabled)."""
        adapter = _make_adapter(has_lw_deletes=False)
        with pytest.raises(DbtRuntimeError, match="allow_nondeterministic_mutations"):
            adapter.validate_incremental_strategy('delete_insert', [], 'id', None)

    def test_microbatch_raises_when_lw_deletes_unavailable(self):
        adapter = _make_adapter(has_lw_deletes=False)
        with pytest.raises(DbtRuntimeError, match="allow_nondeterministic_mutations"):
            adapter.validate_incremental_strategy('microbatch', [], 'id', None)

    def test_delete_insert_passes_when_lw_deletes_available(self):
        adapter = _make_adapter(has_lw_deletes=True)
        adapter.validate_incremental_strategy('delete_insert', [], 'id', None)

    def test_microbatch_passes_when_lw_deletes_available(self):
        adapter = _make_adapter(has_lw_deletes=True)
        adapter.validate_incremental_strategy('microbatch', [], 'id', None)

    def test_legacy_does_not_require_lw_deletes(self):
        adapter = _make_adapter(has_lw_deletes=False)
        adapter.validate_incremental_strategy('legacy', [], None, None)

    def test_append_does_not_require_lw_deletes(self):
        adapter = _make_adapter(has_lw_deletes=False)
        adapter.validate_incremental_strategy('append', [], None, None)

    def test_insert_overwrite_does_not_require_lw_deletes(self):
        adapter = _make_adapter(has_lw_deletes=False)
        adapter.validate_incremental_strategy('insert_overwrite', [], None, 'date')

    def test_delete_insert_raises_without_unique_key(self):
        adapter = _make_adapter(has_lw_deletes=True)
        with pytest.raises(DbtRuntimeError, match="unique_key"):
            adapter.validate_incremental_strategy('delete_insert', [], None, None)

    def test_unknown_strategy_raises(self):
        adapter = _make_adapter(has_lw_deletes=True)
        with pytest.raises(DbtRuntimeError, match="not valid"):
            adapter.validate_incremental_strategy('invalid_strategy', [], 'id', None)
