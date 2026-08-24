import json
import os
import subprocess
import sys
import time
import timeit
import uuid
from pathlib import Path
from subprocess import PIPE, Popen

import pytest
import requests
from clickhouse_connect import get_client

# ---------------------------------------------------------------------------
# dbt core v2 (Rust) support
#
# When DBT_CH_TEST_CORE_V2_BINARY is set to the path of a dbt core v2 binary
# (e.g. dbt-core-v2/target/debug/dbt-sa-cli), `dbt.tests.util.run_dbt` is
# replaced with a subprocess wrapper that shells out to that binary instead of
# invoking the Python dbtRunner in-process. Everything else in the test
# framework (project scaffolding, schema setup, `project.run_sql` assertions)
# still runs through the Python adapter, so tests exercise the dbt core v2
# engine for dbt commands while keeping their Python-side assertions.
#
# The subprocess relies on two invariants of dbt's own `project` fixture
# (dbt.tests.fixtures.project): the test chdirs into the generated project
# root, and DBT_PROFILES_DIR points at the generated profiles.yml.
#
# Results are re-hydrated from target/run_results.json into DbtCoreV2RunResult
# objects (.status / .message / .unique_id, plus a lazy .node hydrated from
# target/manifest.json into dbt's real Python node classes). get_manifest()
# is patched to hydrate dbt core v2's manifest.json the same way (v2 never
# writes partial_parse.msgpack). Out of scope: dbt event callbacks and
# Python-format log capture (run_dbt_and_capture returns the binary's
# stdout+stderr instead).
# ---------------------------------------------------------------------------

DBT_CORE_V2_BINARY = os.environ.get('DBT_CH_TEST_CORE_V2_BINARY', '')


class DbtCoreV2Artifact:
    """Duck-typed attribute wrapper over a dict from a dbt core v2 artifact."""

    def __init__(self, data):
        self._data = data
        self.__dict__.update(data)


class DbtCoreV2Manifest:
    """Stand-in for dbt's Manifest, hydrated from dbt core v2's manifest.json."""

    def __init__(self, nodes, sources):
        self.nodes = nodes
        self.sources = sources


class DbtCoreV2RunResult(DbtCoreV2Artifact):
    """Duck-typed stand-in for dbt's RunResult, built from run_results.json."""

    def __init__(self, data, project_root):
        super().__init__(data)
        self._project_root = project_root

    @property
    def node(self):
        manifest = _dbt_core_v2_get_manifest(self._project_root)
        return manifest.nodes[self._data['unique_id']]

    def __repr__(self):
        return f"DbtCoreV2RunResult({self._data.get('unique_id')}: {self._data.get('status')})"


class DbtCoreV2RunResults(list):
    """Python's run_dbt returns a RunExecutionResult that is both iterable and
    exposes the same list under a `.results` attribute; mirror that shape."""

    @property
    def results(self):
        return self


class DbtCoreV2CompileResult(DbtCoreV2RunResult):
    """Compile-invocation result. dbt core v2 writes `results: []` into
    run_results.json for `compile` (Python emits one result per compiled node
    with `compiled_code` populated), so these are synthesized from the manifest
    plus the files under target/compiled/ — see _dbt_core_v2_compile_results."""

    def __init__(self, data, project_root, compiled_code):
        super().__init__(data, project_root)
        self._compiled_code = compiled_code

    @property
    def node(self):
        node = super().node
        try:
            node.compiled_code = self._compiled_code
        except Exception:
            pass
        return node


def _dbt_core_v2_compile_results(project_root, start):
    """Synthesize compile results: one per manifest node whose compiled file was
    (re)written by this invocation (mtime >= start)."""
    manifest_path = Path(project_root) / 'target' / 'manifest.json'
    if not manifest_path.exists():
        return None
    with open(manifest_path) as f:
        manifest = json.load(f)
    results = []
    for uid, data in sorted(manifest.get('nodes', {}).items()):
        package = data.get('package_name') or ''
        original = data.get('original_file_path') or ''
        compiled_path = Path(project_root) / 'target' / 'compiled' / package / original
        if not (original and compiled_path.exists() and compiled_path.stat().st_mtime >= start):
            continue
        with open(compiled_path) as f:
            compiled_code = f.read()
        results.append(
            DbtCoreV2CompileResult(
                {'unique_id': uid, 'status': 'success'}, project_root, compiled_code
            )
        )
    return DbtCoreV2RunResults(results) if results else None


def _dbt_core_v2_hydrate_node(data):
    from dbt.contracts.graph.nodes import (
        AnalysisNode,
        GenericTestNode,
        HookNode,
        ModelNode,
        SeedNode,
        SingularTestNode,
        SnapshotNode,
    )

    resource_type = data.get('resource_type')
    node_class = {
        'model': ModelNode,
        'seed': SeedNode,
        'snapshot': SnapshotNode,
        'analysis': AnalysisNode,
        'operation': HookNode,
        'test': GenericTestNode if data.get('test_metadata') else SingularTestNode,
    }.get(resource_type)
    if node_class is not None:
        try:
            return node_class.from_dict(data)
        except Exception as e:
            sys.stderr.write(
                f"dbt core v2 manifest: could not hydrate {data.get('unique_id')} "
                f"into {node_class.__name__} ({e}); falling back to dict wrapper\n"
            )
    return DbtCoreV2Artifact(data)


def _dbt_core_v2_get_manifest(project_root):
    from dbt.contracts.graph.nodes import SourceDefinition

    path = os.path.join(str(project_root), 'target', 'manifest.json')
    if not os.path.exists(path):
        return None
    with open(path) as f:
        manifest = json.load(f)
    nodes = {
        uid: _dbt_core_v2_hydrate_node(data) for uid, data in manifest.get('nodes', {}).items()
    }
    sources = {}
    for uid, data in manifest.get('sources', {}).items():
        try:
            sources[uid] = SourceDefinition.from_dict(data)
        except Exception:
            sources[uid] = DbtCoreV2Artifact(data)
    return DbtCoreV2Manifest(nodes, sources)


def _dbt_core_v2_load_catalog(project_root, start):
    catalog_path = Path(project_root) / 'target' / 'catalog.json'
    if not (catalog_path.exists() and catalog_path.stat().st_mtime >= start):
        return None
    with open(catalog_path) as f:
        data = json.load(f)
    try:
        from dbt.artifacts.schemas.catalog import CatalogArtifact

        return CatalogArtifact.from_dict(data)
    except Exception as e:
        sys.stderr.write(
            f"dbt core v2 catalog: could not hydrate CatalogArtifact ({e}); "
            f"falling back to dict wrapper\n"
        )
        return DbtCoreV2Artifact(data)


def _dbt_core_v2_mocked_event_time_end():
    # Upstream microbatch/sample tests control "now" via
    # mock.patch.object(MicrobatchBuilder, 'build_end_time', return_value=dt) —
    # an in-process patch the v2 subprocess never sees, so it falls back to
    # Utc::now() and back-fills every daily batch from `begin` to today
    # (~2,400 batches, minutes per run). Detect an active patch and return the
    # mocked end time so it can be forwarded to the CLI.
    from unittest import mock

    from dbt.materializations.incremental.microbatch import MicrobatchBuilder

    build_end_time = MicrobatchBuilder.build_end_time
    if isinstance(build_end_time, mock.Mock):
        return build_end_time()
    return None


def _dbt_core_v2_invoke(args):
    # Python's dbtRunner tolerates non-string args (e.g. --limit 5); subprocess doesn't.
    args = [str(a) for a in args]
    # dbt core v2 dropped `docs generate` (dbt1705); `compile --write-catalog` produces the
    # catalog.json that Python's docs generate returned as a CatalogArtifact.
    is_docs_generate = args[:2] == ['docs', 'generate']
    if is_docs_generate:
        args = ['compile', '--write-catalog'] + args[2:]
    print(f"\n\nInvoking dbt core v2 with {args}")
    start = time.time()
    project_root = os.getcwd()
    # Keep a debug query log per project for post-mortem debugging of failing
    # tests (the pytest tmp project dirs persist). Only appended when the test
    # didn't pass its own log flags (clap rejects duplicates).
    invoke_args = list(args)
    if not any(a.startswith('--log-level') or a == '--debug' or a == '-d' for a in invoke_args):
        invoke_args += ['--log-level', 'debug']
    if not any(a.startswith('--log-path') for a in invoke_args):
        invoke_args += ['--log-path', 'logs_v2']
    if invoke_args and invoke_args[0] in ('run', 'build') and not any(
        a.startswith('--event-time-end') for a in invoke_args
    ):
        mocked_end = _dbt_core_v2_mocked_event_time_end()
        if mocked_end is not None:
            invoke_args += ['--event-time-end', mocked_end.strftime('%Y-%m-%d %H:%M:%S')]
    proc = subprocess.run([DBT_CORE_V2_BINARY] + invoke_args, capture_output=True, text=True)
    output = proc.stdout + proc.stderr
    print(output)
    if is_docs_generate:
        return proc, output, _dbt_core_v2_load_catalog(project_root, start)
    results = None
    run_results_path = Path(project_root) / 'target' / 'run_results.json'
    if run_results_path.exists() and run_results_path.stat().st_mtime >= start:
        with open(run_results_path) as f:
            run_results = json.load(f)
        # dbt core v2's run_results.json includes an entry for every processed
        # node, ephemeral models included; Python's run_results contract omits
        # ephemeral nodes entirely (tests assert on result counts). Filter them
        # here to match the Python artifact shape.
        ephemeral_ids = _dbt_core_v2_ephemeral_node_ids(project_root)
        entries = [
            r
            for r in run_results.get('results', [])
            if r.get('unique_id') not in ephemeral_ids
        ]
        # Python's CloneRunner reports "No-op" for clone targets that already
        # exist (the clone materialization returns without running a `main`
        # statement). v2 reports its generic success message; the artifact
        # still discriminates the case — no adapter response was stored.
        if args and args[0] == 'clone':
            for r in entries:
                if r.get('status') == 'success' and not r.get('adapter_response'):
                    r['message'] = 'No-op'
        results = DbtCoreV2RunResults(DbtCoreV2RunResult(r, project_root) for r in entries)
    # v2 `compile` writes an empty results array; synthesize per-node results
    # (with compiled_code) from target/compiled so tests can assert on them.
    if not results and args and args[0] == 'compile':
        results = _dbt_core_v2_compile_results(project_root, start)
    return proc, output, results


def _dbt_core_v2_ephemeral_node_ids(project_root):
    manifest_path = Path(project_root) / 'target' / 'manifest.json'
    if not manifest_path.exists():
        return set()
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except Exception:
        return set()
    return {
        uid
        for uid, data in manifest.get('nodes', {}).items()
        if (data.get('config') or {}).get('materialized') == 'ephemeral'
    }


def _dbt_core_v2_run_dbt(args=None, expect_pass=True, callbacks=None):
    from dbt_common.exceptions import DbtRuntimeError

    if args is None:
        args = ["run"]
    proc, output, results = _dbt_core_v2_invoke(args)
    success = proc.returncode == 0
    if not success and results is None:
        # No run_results.json means the invocation died before running nodes
        # (parse/config error). Python's run_dbt propagates those as exceptions
        # regardless of expect_pass; only node-level failures return results.
        raise DbtRuntimeError(
            f"dbt core v2 exited with rc={proc.returncode}\n{output}"
        )
    if expect_pass is not None:
        if expect_pass and not success:
            # Python's run_dbt surfaces failures as DbtRuntimeError subclasses,
            # which upstream negative tests catch with pytest.raises(...).
            raise DbtRuntimeError(
                f"dbt core v2 exited with rc={proc.returncode}\n{output}"
            )
        assert success == expect_pass, (
            f"dbt core v2 exit state did not match expected (rc={proc.returncode})\n{output}"
        )
    return results


def _dbt_core_v2_run_dbt_and_capture(args=None, expect_pass=True):
    from dbt_common.exceptions import DbtRuntimeError

    if args is None:
        args = ["run"]
    proc, output, results = _dbt_core_v2_invoke(args)
    success = proc.returncode == 0
    if not success and results is None:
        # Mirror Python: invocation-level failures (parse/config) raise even
        # when expect_pass=False.
        raise DbtRuntimeError(
            f"dbt core v2 exited with rc={proc.returncode}\n{output}"
        )
    if expect_pass is not None:
        assert success == expect_pass, (
            f"dbt core v2 exit state did not match expected (rc={proc.returncode})\n{output}"
        )
    return results, output


if DBT_CORE_V2_BINARY:
    import dbt.tests.util as _dbt_tests_util

    _dbt_tests_util.run_dbt = _dbt_core_v2_run_dbt
    _dbt_tests_util.run_dbt_and_capture = _dbt_core_v2_run_dbt_and_capture
    _dbt_tests_util.get_manifest = _dbt_core_v2_get_manifest


def _connection_settings():
    """Resolve the ClickHouse connection settings shared by the test_config and
    ch_test_users fixtures. Everything derives from the environment, so every
    xdist worker resolves the same values."""
    test_port = int(os.environ.get('DBT_CH_TEST_PORT', 8123))
    client_port = int(os.environ.get('DBT_CH_TEST_CLIENT_PORT', 0))
    test_driver = os.environ.get('DBT_CH_TEST_DRIVER', '').lower()
    if test_driver == '':
        test_driver = 'native' if test_port in (10900, 9000, 9440) else 'http'
    test_secure = test_port in (8443, 9440)
    docker = os.environ.get('DBT_CH_TEST_USE_DOCKER', '').lower() in ('1', 'true', 'yes')
    if docker:
        client_port = client_port or 10723
        test_port = 10900 if test_driver == 'native' else client_port
    elif not client_port:
        if test_driver == 'native':
            client_port = 8443 if test_port == 9440 else 8123
        else:
            client_port = test_port
    return {
        'host': os.environ.get('DBT_CH_TEST_HOST', 'localhost'),
        'port': test_port,
        'client_port': client_port,
        'driver': test_driver,
        'user': os.environ.get('DBT_CH_TEST_USER', 'default'),
        'password': os.environ.get('DBT_CH_TEST_PASSWORD', ''),
        'cluster': os.environ.get('DBT_CH_TEST_CLUSTER', ''),
        'secure': test_secure,
        'docker': docker,
    }


def _get_test_client(conn):
    return get_client(
        host=conn['host'],
        port=conn['client_port'],
        username=conn['user'],
        password=conn['password'],
        secure=conn['secure'],
    )


# Creates the DBT_TEST_USER_1..3 ClickHouse users for the requesting test class
# and exports their names to the environment, where the grants/dbt_clone tests
# read them back via env_var()/os.getenv. Class-scoped rather than session-scoped
# on purpose: creation and consumption happen in the same process, so under
# pytest-xdist no state has to cross worker boundaries. Only the test packages
# that need the users request this fixture (via autouse fixtures in their
# conftest.py); everything else never pays for it.
@pytest.fixture(scope="class")
def ch_test_users(test_config):
    env_keys = [f'DBT_TEST_USER_{x}' for x in range(1, 4)]
    saved_env = {key: os.environ.get(key) for key in env_keys}
    # Unique per class so concurrent xdist workers never collide on CREATE/DROP
    test_users = [f'dbt_test_user_{uuid.uuid4().hex[:10]}' for _ in env_keys]
    cluster = test_config['cluster']
    cluster_clause = f'ON CLUSTER "{cluster}"' if cluster else ''
    test_client = _get_test_client(test_config)
    try:
        for key, dbt_user in zip(env_keys, test_users, strict=True):
            test_client.command(
                f'CREATE USER IF NOT EXISTS %s {cluster_clause} IDENTIFIED WITH sha256_hash BY %s',
                (dbt_user, '5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8'),
            )
            os.environ[key] = dbt_user
        yield test_users
    finally:
        for dbt_user in test_users:
            test_client.command(f'DROP USER IF EXISTS %s {cluster_clause}', (dbt_user,))
        test_client.close()
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="session", autouse=True)
def ch_test_version():
    yield os.environ.get('DBT_CH_TEST_CH_VERSION', 'latest')


# This fixture is for customizing tests that need overrides in adapter
# repos. Example in dbt.tests.adapter.basic.test_base. It also owns the
# docker compose lifecycle when DBT_CH_TEST_USE_DOCKER is set — a sequential-only
# convenience: under pytest-xdist this session fixture runs once per WORKER, and
# concurrent workers would start/stop the cluster underneath each other, so that
# combination is rejected. For parallel runs, start the cluster externally
# (as the GitHub Actions workflows do) and leave DBT_CH_TEST_USE_DOCKER unset.
@pytest.fixture(scope="session")
def test_config(ch_test_version):
    conn = _connection_settings()
    if ch_test_version.startswith('22.3'):
        os.environ['DBT_CH_TEST_SETTINGS'] = '22_3'

    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    docker_started = False
    if conn['docker']:
        if os.environ.get('PYTEST_XDIST_WORKER'):
            raise Exception(
                'DBT_CH_TEST_USE_DOCKER is incompatible with pytest-xdist (-n): each '
                'worker would manage the compose cluster underneath the others. For '
                'parallel runs start it yourself first, e.g. '
                '`docker compose -f tests/integration/docker-compose.yml up -d`, '
                'and unset DBT_CH_TEST_USE_DOCKER.'
            )
        run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
        sys.stderr.write('Starting docker compose')
        os.environ['PROJECT_ROOT'] = '.'
        try:
            up_result = run_cmd(['docker', 'compose', '-f', compose_file, 'up', '-d'])
            if up_result[0]:
                raise Exception(f'Failed to start docker: {up_result[2]}')
            url = f"http://{conn['host']}:{conn['client_port']}"
            wait_until_responsive(timeout=30.0, pause=0.5, check=lambda: is_responsive(url))
            docker_started = True
        except Exception:
            run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
            raise

    # Make sure all system tables are available before starting tests
    cluster_clause = f'''ON CLUSTER "{conn['cluster']}"''' if conn['cluster'] else ''
    test_client = _get_test_client(conn)
    test_client.command(f"SYSTEM FLUSH LOGS {cluster_clause}")
    test_client.close()

    test_cloud = os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes')
    yield {
        'driver': conn['driver'],
        'host': conn['host'],
        'port': conn['port'],
        'client_port': conn['client_port'],
        'user': conn['user'],
        'password': conn['password'],
        'cluster': conn['cluster'],
        'db_engine': os.environ.get('DBT_CH_TEST_DB_ENGINE', 'Shared' if test_cloud else ''),
        'secure': conn['secure'],
        'cluster_mode': os.environ.get('DBT_CH_TEST_CLUSTER_MODE', '').lower()
        in ('1', 'true', 'yes'),
        'database': '',
    }

    if docker_started:
        # `down -v` also removes any leftover test state along with the volumes
        run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target(test_config):
    custom_settings = {
        'distributed_ddl_task_timeout': 300,
        'input_format_skip_unknown_fields': 1,
    }

    # this setting is required for cloud tests until https://github.com/ClickHouse/ClickHouse/issues/63984 would be solved
    if os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'):
        custom_settings.update(
            {
                'enable_parallel_replicas': 0,
                # DEDUPLICATION SETTINGS
                'insert_deduplicate': 1,
                # ADDITIONAL HELPFUL SETTINGS
                'max_replica_delay_for_distributed_queries': 10,
                'fallback_to_stale_replicas_for_distributed_queries': 0,
            }
        )

    return {
        'type': 'clickhouse',
        'threads': 4,
        'driver': test_config['driver'],
        'host': test_config['host'],
        'user': test_config['user'],
        'password': test_config['password'],
        'port': test_config['port'],
        'cluster': test_config['cluster'],
        'database_engine': test_config['db_engine'],
        'cluster_mode': test_config['cluster_mode'],
        'secure': test_config['secure'],
        'check_exchange': False,
        'use_lw_deletes': True,
        'custom_settings': custom_settings,
    }


@pytest.fixture(scope="class")
def prefix():
    # Must be unique across concurrent pytest-xdist workers: dbt derives both the
    # test schema name and the shared logs/<prefix> directory from it.
    worker = os.environ.get('PYTEST_XDIST_WORKER', 'gw0')
    return f"dbt_clickhouse_{worker}_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__.split(".")[-1]
    return f"{prefix}_{test_file}_{int(time.time() * 1000)}"


# Models configured with a custom schema create databases named
# `<unique_schema>_<custom schema>`; dbt's own teardown only drops the schemas it
# created itself, so those derived databases leak. The main `<unique_schema>` can
# also survive: dbt's project teardown swallows drop_test_schema() errors after
# commands like `dbt debug` (dbt-core #5041). This fixture sets up before (and thus
# tears down after) the project fixture, sweeping whatever is left.
@pytest.fixture(scope="class", autouse=True)
def cleanup_derived_databases(test_config, unique_schema):
    yield
    test_client = _get_test_client(test_config)
    cluster = test_config['cluster']
    cluster_clause = f'ON CLUSTER "{cluster}"' if cluster else ''
    try:
        leaked = test_client.query(
            'SELECT name FROM system.databases WHERE name = %s OR startsWith(name, %s)',
            (unique_schema, f'{unique_schema}_'),
        ).result_rows
        for (db_name,) in leaked:
            test_client.command(f'DROP DATABASE IF EXISTS "{db_name}" {cluster_clause} SYNC')
    finally:
        test_client.close()


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except requests.exceptions.ConnectionError:
        return False


def wait_until_responsive(check, timeout, pause, clock=timeit.default_timer):
    ref = clock()
    now = ref
    while (now - ref) < timeout:
        time.sleep(pause)
        if check():
            return
        now = clock()
    raise Exception("Timeout reached while waiting on service!")
