import os
import sys
import time
import timeit
import uuid
from pathlib import Path
from subprocess import PIPE, Popen

import pytest
import requests
from clickhouse_connect import get_client


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
        # Restore the environment first: it cannot fail, unlike the DROPs below
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            for dbt_user in test_users:
                test_client.command(f'DROP USER IF EXISTS %s {cluster_clause}', (dbt_user,))
        finally:
            test_client.close()


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
    test_cloud = os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes')
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
