"""
Regression test for replacing a view on filesystems without renameat2(RENAME_EXCHANGE)
support (NFS, EFS, CephFS, etc.). ClickHouse implements CREATE OR REPLACE over an
existing object with that syscall, so the view materialization must fall back to a
non-atomic drop + create when the server reports exchange is unsupported.

The filesystem limitation is emulated with a seccomp profile that makes renameat2
fail with ENOSYS inside a dedicated ClickHouse container.
"""

import json
import os
import shutil
import subprocess
import time
import uuid

import dbt.adapters.clickhouse.dbclient as dbclient
import pytest
import requests
from dbt.tests.util import run_dbt

# renameat2 returns ENOSYS (38), exactly as on a filesystem without RENAME_EXCHANGE
SECCOMP_NO_RENAMEAT2 = json.dumps(
    {
        "defaultAction": "SCMP_ACT_ALLOW",
        "syscalls": [{"names": ["renameat2"], "action": "SCMP_ACT_ERRNO", "errnoRet": 38}],
    }
)

HOST_PORT = 10825

SIMPLE_VIEW_MODEL = """
{{ config(materialized='view') }}
select {{ var('col_value', 1) }} as id
"""


@pytest.fixture(scope="class")
def seccomp_clickhouse(tmp_path_factory):
    if not shutil.which('docker'):
        pytest.skip('docker is not available')
    profile_path = tmp_path_factory.mktemp('seccomp') / 'no-renameat2.json'
    profile_path.write_text(SECCOMP_NO_RENAMEAT2)
    ch_version = os.environ.get('DBT_CH_TEST_CH_VERSION', 'latest')
    container_name = f'dbt-ch-no-renameat2-{uuid.uuid4().hex[:8]}'
    run_result = subprocess.run(
        [
            'docker',
            'run',
            '-d',
            '--name',
            container_name,
            '--security-opt',
            f'seccomp={profile_path}',
            '-e',
            'CLICKHOUSE_SKIP_USER_SETUP=1',
            '-p',
            f'{HOST_PORT}:8123',
            f'clickhouse/clickhouse-server:{ch_version}',
        ],
        capture_output=True,
        text=True,
    )
    if run_result.returncode != 0:
        pytest.skip(f'failed to start seccomp ClickHouse container: {run_result.stderr}')
    try:
        deadline = time.time() + 60
        while True:
            try:
                if requests.get(f'http://localhost:{HOST_PORT}/ping').status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass
            if time.time() > deadline:
                raise Exception('Timeout waiting for seccomp ClickHouse container')
            time.sleep(0.5)
        yield
    finally:
        subprocess.run(['docker', 'rm', '-f', container_name], capture_output=True)


class TestClickHouseViewNoRenameat2:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_view.sql": SIMPLE_VIEW_MODEL}

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, seccomp_clickhouse):
        return {
            'type': 'clickhouse',
            'threads': 1,
            'driver': 'http',
            'host': 'localhost',
            'port': HOST_PORT,
            'user': 'default',
            'password': '',
            'secure': False,
            # probe the server so supports_atomic_exchange() reflects the seccomp limits
            'check_exchange': True,
        }

    def test_replace_view_without_renameat2(self, project):
        # the exchange probe result is cached process-wide; force a fresh probe
        # against the seccomp container and clean up after ourselves
        dbclient._exchange_result = None
        try:
            # first run: fresh create works even without renameat2
            results = run_dbt()
            assert len(results) == 1
            result = project.run_sql("select id from simple_view", fetch="one")
            assert result[0] == 1

            # second run replaces the existing view; without the drop + create
            # fallback this fails with UNSUPPORTED_METHOD (renameat2 not supported)
            results = run_dbt(["run", "--vars", json.dumps({"col_value": 2})])
            assert len(results) == 1
            result = project.run_sql("select id from simple_view", fetch="one")
            assert result[0] == 2
        finally:
            dbclient._exchange_result = None
