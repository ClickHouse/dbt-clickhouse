"""
Comparison harness for the `reuse_connections` profile option (PR #670 / issue #669).

Runs the example dbt project twice against a local ClickHouse HTTP endpoint:

  1. reuse_connections = True  (default, historical behavior)
  2. reuse_connections = False (new option)

A monkeypatch on dbt.adapters.clickhouse.dbclient.get_db_client records every client
creation, tagged with the creating thread id. The number of distinct clients created
is the observable that distinguishes the two modes:

  - True  -> one client per dbt thread, reused across all models on that thread
  - False -> one client per model (closed after every model, reopened for the next)

Also records the number of distinct `session_id`s the adapter stamps onto clients
(one uuid4 per client) as a cross-check.

Run:  python run_comparison.py
"""

from __future__ import annotations

import os
import shutil
import threading
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import clickhouse_connect
from dbt.adapters.clickhouse import dbclient as dbclient_module
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.dbclient import get_db_client as _real_get_db_client
from dbt.cli.main import dbtRunner

HERE = Path(__file__).resolve().parent
HOST = os.environ.get("CH_HOST", "localhost")
PORT = int(os.environ.get("CH_PORT", "8123"))
USER = os.environ.get("CH_USER", "default")
PASSWORD = os.environ.get("CH_PASSWORD", "")
SCHEMA = os.environ.get("CH_SCHEMA", "reuse_conn_test")
THREADS = int(os.environ.get("DBT_THREADS", "8"))


@dataclass
class ClientTracker:
    created: list[tuple[int, str]] = field(default_factory=list)  # (thread_id, session_id)

    def reset(self) -> None:
        self.created.clear()

    def record(self, session_id: str) -> None:
        self.created.append((threading.get_ident(), session_id))

    @property
    def total_creations(self) -> int:
        return len(self.created)

    @property
    def distinct_clients(self) -> int:
        return len({sid for _, sid in self.created})

    @property
    def distinct_threads(self) -> int:
        return len({tid for tid, _ in self.created})

    def creations_per_thread(self) -> Counter:
        return Counter(tid for tid, _ in self.created)


TRACKER = ClientTracker()


def _tracking_get_db_client(credentials: ClickHouseCredentials):
    """Wrapper around the real get_db_client that records each client creation."""
    client = _real_get_db_client(credentials)
    # The adapter sets session_id = f"dbt::{uuid4()}" in _conn_settings; read it
    # back off the constructed client's connection settings if available.
    session_id = getattr(client, "_conn_settings", {}).get("session_id", "<unknown>")
    TRACKER.record(session_id)
    return client


def _install_tracker() -> None:
    dbclient_module.get_db_client = _tracking_get_db_client
    # connections.py imported the name into its module namespace, so patch there too
    from dbt.adapters.clickhouse import connections as ch_connections

    ch_connections.get_db_client = _tracking_get_db_client


def _uninstall_tracker() -> None:
    dbclient_module.get_db_client = _real_get_db_client
    from dbt.adapters.clickhouse import connections as ch_connections

    ch_connections.get_db_client = _real_get_db_client


def _write_profile(reuse_connections: bool) -> Path:
    profiles_dir = HERE / ".profiles"
    profiles_dir.mkdir(exist_ok=True)
    profile_text = f"""
reuse_connections:
  target: dev
  outputs:
    dev:
      type: clickhouse
      driver: http
      host: {HOST}
      port: {PORT}
      user: {USER}
      password: '{PASSWORD}'
      schema: {SCHEMA}
      threads: {THREADS}
      reuse_connections: {str(reuse_connections).lower()}
      use_lw_deletes: true
      check_exchange: false
"""
    (profiles_dir / "profiles.yml").write_text(profile_text)
    return profiles_dir


def _drop_schema() -> None:
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USER, password=PASSWORD)
    try:
        client.command(f"DROP DATABASE IF EXISTS {SCHEMA}")
        client.command(f"CREATE DATABASE {SCHEMA}")
    finally:
        client.close()


def _clean_targets() -> None:
    for sub in ("target", "logs"):
        p = HERE / sub
        if p.exists():
            shutil.rmtree(p)
    # pp = HERE / ".profiles" / "profiles.yml"
    # keep profiles dir, will be rewritten


def run_dbt(reuse_connections: bool) -> dict:
    """Run seed + run for the project with the given reuse_connections setting."""
    _drop_schema()
    _clean_targets()
    profiles_dir = _write_profile(reuse_connections)
    TRACKER.reset()
    _install_tracker()
    try:
        runner = dbtRunner()
        # seed first
        seed_result = runner.invoke(
            ["seed", "--project-dir", str(HERE), "--profiles-dir", str(profiles_dir)]
        )
        # then run models (parallel across THREADS)
        run_result = runner.invoke(
            [
                "run",
                "--project-dir",
                str(HERE),
                "--profiles-dir",
                str(profiles_dir),
                "--select",
                "staging marts mvs",
            ],
        )
    finally:
        _uninstall_tracker()

    seed_ok = bool(seed_result.success)
    run_ok = bool(run_result.success)
    summary = {
        "reuse_connections": reuse_connections,
        "seed_success": seed_ok,
        "run_success": run_ok,
        "total_client_creations": TRACKER.total_creations,
        "distinct_clients": TRACKER.distinct_clients,
        "distinct_threads": TRACKER.distinct_threads,
        "creations_per_thread": dict(TRACKER.creations_per_thread()),
    }
    return summary


def _model_count() -> int:
    """Count the SQL models we expect to run (staging + marts + mvs)."""
    n = 0
    for sub in ("staging", "marts", "mvs"):
        n += sum(1 for _ in (HERE / "models" / sub).glob("*.sql"))
    return n


def main() -> None:
    expected_models = _model_count()
    print("=" * 72)
    print(f"reuse_connections example — HTTP driver, threads={THREADS}")
    print(f"models to run (staging+marts+mvs): {expected_models}")
    print("=" * 72)

    results = {}
    for mode in (True, False):
        print(f"\n>>> Running with reuse_connections={mode} ...")
        results[mode] = run_dbt(mode)
        r = results[mode]
        print(
            f"    seed_success={r['seed_success']} run_success={r['run_success']}\n"
            f"    total_client_creations={r['total_client_creations']}\n"
            f"    distinct_clients      ={r['distinct_clients']}\n"
            f"    distinct_threads      ={r['distinct_threads']}\n"
            f"    creations_per_thread  ={r['creations_per_thread']}"
        )

    print("\n" + "=" * 72)
    print("VERDICT")
    print("=" * 72)
    r_true, r_false = results[True], results[False]
    print(
        f"reuse_connections=True   -> distinct_clients={r_true['distinct_clients']} "
        f"(expect ~= distinct_threads={r_true['distinct_threads']})"
    )
    print(
        f"reuse_connections=False  -> distinct_clients={r_false['distinct_clients']} "
        f"(expect ~= models run={expected_models}, or more due to retries)"
    )

    ok_true = r_true["run_success"] and r_true["distinct_clients"] <= r_true["distinct_threads"] + 1
    ok_false = r_false["run_success"] and r_false["distinct_clients"] >= expected_models
    # The key behavioral check: False must create strictly more clients than True.
    ok_ratio = r_false["distinct_clients"] > r_true["distinct_clients"]

    print(f"\n  run_success both modes      : {r_true['run_success'] and r_false['run_success']}")
    print(f"  True reuses (<= threads+1)  : {ok_true}")
    print(f"  False opens many (>= models): {ok_false}")
    print(f"  False > True distinct clients: {ok_ratio}")

    if ok_true and ok_false and ok_ratio:
        print("\n  RESULT: PASS  — reuse_connections behaves as designed.")
    else:
        print("\n  RESULT: FAIL  — behavior does not match expectations.")


if __name__ == "__main__":
    main()
