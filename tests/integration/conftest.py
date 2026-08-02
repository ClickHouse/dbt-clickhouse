import os
import random
import sys
import tempfile
import time
import timeit
import uuid
from pathlib import Path
from subprocess import PIPE, Popen

import pytest
import requests
from clickhouse_connect import get_client

# Import pandas before any test can freeze time: importing it under a frozen
# clock (freezegun, via sample_mode) corrupts its C init and segfaults.
try:
    import pandas  # noqa: F401
except ImportError:
    pass


# Ensure that test users exist in environment
@pytest.fixture(scope="session", autouse=True)
def ch_test_users():

    def generate_random_string():
        return str(uuid.uuid4()).replace('-', '')[:10]

    test_users = [
        os.environ.setdefault(f'DBT_TEST_USER_{x}', f'dbt_test_user_{generate_random_string()}')
        for x in range(1, 4)
    ]
    yield test_users


@pytest.fixture(scope="session", autouse=True)
def ch_test_version():
    yield os.environ.get('DBT_CH_TEST_CH_VERSION', 'latest')


# This fixture is for customizing tests that need overrides in adapter
# repos. Example in dbt.tests.adapter.basic.test_base.
@pytest.fixture(scope="session")
def test_config(ch_test_users, ch_test_version):
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    test_host = os.environ.get('DBT_CH_TEST_HOST', 'localhost')
    test_port = int(os.environ.get('DBT_CH_TEST_PORT', 8123))
    client_port = int(os.environ.get('DBT_CH_TEST_CLIENT_PORT', 0))
    test_driver = os.environ.get('DBT_CH_TEST_DRIVER', '').lower()
    if test_driver == '':
        test_driver = 'native' if test_port in (10900, 9000, 9440) else 'http'
    test_user = os.environ.get('DBT_CH_TEST_USER', 'default')
    test_password = os.environ.get('DBT_CH_TEST_PASSWORD', '')
    test_cluster = os.environ.get('DBT_CH_TEST_CLUSTER', '')
    test_db_engine = os.environ.get('DBT_CH_TEST_DB_ENGINE', '')
    test_secure = test_port in (8443, 9440)
    test_cluster_mode = os.environ.get('DBT_CH_TEST_CLUSTER_MODE', '').lower() in (
        '1',
        'true',
        'yes',
    )
    if ch_test_version.startswith('22.3'):
        os.environ['DBT_CH_TEST_SETTINGS'] = '22_3'

    if test_driver == 'chdb':
        chdb_path = os.environ.get('DBT_CH_TEST_CHDB_PATH') or tempfile.mkdtemp(
            prefix='dbt_chdb_test_'
        )
        yield {
            'driver': 'chdb',
            'chdb_path': chdb_path,
            'host': '',
            'port': 0,
            'user': 'default',
            'password': '',
            'cluster': '',
            'db_engine': test_db_engine,
            'secure': False,
            'cluster_mode': False,
            'database': '',
        }
        return

    docker = os.environ.get('DBT_CH_TEST_USE_DOCKER', '').lower() in ('1', 'true', 'yes')

    if docker:
        client_port = client_port or 10723
        test_port = 10900 if test_driver == 'native' else client_port
        try:
            run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
            sys.stderr.write('Starting docker compose')
            os.environ['PROJECT_ROOT'] = '.'
            up_result = run_cmd(['docker', 'compose', '-f', compose_file, 'up', '-d'])
            if up_result[0]:
                raise Exception(f'Failed to start docker: {up_result[2]}')
            url = f"http://{test_host}:{client_port}"
            wait_until_responsive(timeout=30.0, pause=0.5, check=lambda: is_responsive(url))
        except Exception as e:
            raise Exception('Failed to run docker compose: {}', str(e)) from e
    elif not client_port:
        if test_driver == 'native':
            client_port = 8443 if test_port == 9440 else 8123
        else:
            client_port = test_port

    test_client = get_client(
        host=test_host,
        port=client_port,
        username=test_user,
        password=test_password,
        secure=test_secure,
    )
    cluster_clause = f'ON CLUSTER "{test_cluster}"' if test_cluster else ''
    for dbt_user in ch_test_users:
        cmd = f'CREATE USER IF NOT EXISTS %s {cluster_clause} IDENTIFIED WITH sha256_hash BY %s'
        test_client.command(
            cmd,
            (dbt_user, '5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8'),
        )
    # Make sure all system tables are available before starting tests
    test_client.command(f"SYSTEM FLUSH LOGS {cluster_clause}")
    yield {
        'driver': test_driver,
        'host': test_host,
        'port': test_port,
        'user': test_user,
        'password': test_password,
        'cluster': test_cluster,
        'db_engine': test_db_engine,
        'secure': test_secure,
        'cluster_mode': test_cluster_mode,
        'database': '',
    }

    if docker:
        try:
            run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
        except Exception as e:
            raise Exception('Failed to run docker compose while cleaning up: {}', str(e)) from e
    else:
        for test_user in ch_test_users:
            test_client.command('DROP USER %s', (test_user,))


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
                # CRITICAL SETTINGS FOR CONSISTENCY
                'mutations_sync': 3,
                'replication_alter_partitions_sync': 2,
                'insert_quorum': 'auto',
                # DEDUPLICATION SETTINGS
                'insert_deduplicate': 1,
                # ADDITIONAL HELPFUL SETTINGS
                'max_replica_delay_for_distributed_queries': 10,
                'fallback_to_stale_replicas_for_distributed_queries': 0,
                'distributed_foreground_insert': 1,
            }
        )

    if test_config['driver'] == 'chdb':
        return {
            'type': 'clickhouse',
            'threads': 4,
            'driver': 'chdb',
            'chdb_path': test_config['chdb_path'],
            'user': 'default',
            'check_exchange': False,
            'use_lw_deletes': True,
            'custom_settings': custom_settings,
        }

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
    return f"dbt_clickhouse_{random.randint(1000, 9999)}"


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__.split(".")[-1]
    return f"{prefix}_{test_file}_{int(time.time() * 1000)}"


# Tests that cannot run on the embedded chdb driver, matched against the test
# node id. Each entry documents why.
CHDB_SKIP_PATTERNS = {
    'grants': 'embedded single-user engine has no access control',
    'Distributed': 'distributed tables require a cluster',
    'projections/': 'verification reads system.query_log, not populated embedded',
    'test_query_id_round_trips': 'system.query_log is not populated on the embedded engine',
    'test_clickhouse_sql_header': (
        'asserts the HTTP multi-statement error; chdb executes multi-statements'
    ),
}


def pytest_collection_modifyitems(config, items):
    if os.environ.get('DBT_CH_TEST_DRIVER', '').lower() != 'chdb':
        return
    for item in items:
        for pattern, reason in CHDB_SKIP_PATTERNS.items():
            if pattern in item.nodeid:
                item.add_marker(pytest.mark.skip(reason=f'chdb driver: {reason}'))
                break


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
