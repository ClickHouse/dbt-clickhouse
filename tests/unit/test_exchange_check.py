import threading
from unittest.mock import MagicMock

import dbt.adapters.clickhouse.dbclient as dbclient_module
import pytest
from dbt.adapters.clickhouse.dbclient import ChClientWrapper


@pytest.fixture(autouse=True)
def reset_exchange_result():
    dbclient_module._exchange_result = None
    yield
    dbclient_module._exchange_result = None


def test_run_exchange_test_shared_returns_true_without_ddl():
    """Shared engine short-circuits before any DDL."""
    client = MagicMock(spec=ChClientWrapper)
    client.command.return_value = 'Shared'

    result = ChClientWrapper._run_exchange_test(client)

    assert result is True
    assert client.command.call_count == 1  # only SELECT engine, no CREATE/DROP


def test_run_exchange_test_non_atomic_engine_returns_false():
    """Engines that don't support atomic exchange return False without DDL."""
    client = MagicMock(spec=ChClientWrapper)
    client.command.return_value = 'Memory'

    result = ChClientWrapper._run_exchange_test(client)

    assert result is False
    assert client.command.call_count == 1  # only SELECT engine


def test_check_atomic_exchange_caches_result():
    """Second call returns cached result without re-running the test."""
    client = MagicMock(spec=ChClientWrapper)
    client._run_exchange_test.return_value = True

    first = ChClientWrapper._check_atomic_exchange(client)
    second = ChClientWrapper._check_atomic_exchange(client)

    assert first is True
    assert second is True
    client._run_exchange_test.assert_called_once()


def test_check_atomic_exchange_runs_once_across_threads():
    """Exchange test runs exactly once regardless of how many threads call it."""
    client = MagicMock(spec=ChClientWrapper)
    client._run_exchange_test.return_value = True

    results = []

    def call_check():
        results.append(ChClientWrapper._check_atomic_exchange(client))

    threads = [threading.Thread(target=call_check) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r is True for r in results)
    client._run_exchange_test.assert_called_once()
