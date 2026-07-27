import atexit
import threading

from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.dbclient import reset_process_caches
from dbt.adapters.clickhouse.httpclient import ChHttpClient
from dbt.adapters.exceptions import FailedToConnectError

_shared_lock = threading.Lock()
_shared_client: dict = {'path': None, 'client': None}


def _release_shared_client():
    client = _shared_client['client']
    if client is not None:
        try:
            client.close()
        except Exception:
            pass
        # Server-state caches describe the engine we just closed; drop them so a
        # replacement engine (different path) isn't judged by the old one's state.
        reset_process_caches()
    _shared_client['path'] = None
    _shared_client['client'] = None


@atexit.register
def _close_shared_client():
    with _shared_lock:
        _release_shared_client()


class ChDbClient(ChHttpClient):
    """dbt client for embedded chDB via the clickhouse-connect chdb backend.

    The backend returns a client with the same surface as the HTTP one, so only
    client construction (a data directory, and the shared engine below) differs.
    """

    def _create_client(self, credentials: ClickHouseCredentials):
        import clickhouse_connect

        key = credentials.chdb_path or ':memory:'
        with _shared_lock:
            if _shared_client['client'] is not None and _shared_client['path'] != key:
                # Only one engine path per process; release the old one first.
                _release_shared_client()
            if _shared_client['client'] is None:
                try:
                    _shared_client['client'] = clickhouse_connect.get_client(
                        interface='chdb',
                        path=key,
                        settings=self._conn_settings,
                    )
                except ImportError as ex:
                    raise FailedToConnectError(
                        'driver "chdb" requires the chdb package: '
                        'pip install "clickhouse-connect[chdb]"'
                    ) from ex
                _shared_client['path'] = key
            return _shared_client['client']

    def close(self):
        # Shared, process-owned client; closed at interpreter exit, not here.
        pass
