import os
import sys
import timeit
from pathlib import Path
from subprocess import PIPE, Popen
from time import sleep

import pytest
import requests
from clickhouse_connect import get_client

# Import the standard integration fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


# Ensure that test users exist in environment
@pytest.fixture(scope="session", autouse=True)
def ch_test_users():
    test_users = [
        os.environ.setdefault(f'DBT_TEST_USER_{x}', f'dbt_test_user_{x}') for x in range(1, 4)
    ]
    yield test_users


# This fixture is for customizing tests that need overrides in adapter
# repos. Example in dbt.tests.adapter.basic.test_base.
@pytest.fixture(scope="session")
def test_config(ch_test_users):
    run_docker = os.environ.get('DBT_CH_TEST_USE_DOCKER', '').lower() in ('1', 'true', 'yes')
    test_port = int(os.environ.get('DBT_CH_TEST_PORT', 8123))
    test_host = os.environ.get('DBT_CH_TEST_HOST', 'localhost')
    client_port = 8123 if test_port in (9000, 9440, 10900) else test_port
    if run_docker:
        client_port = 10723
        # Run docker compose with clickhouse-server image.
        compose_file = f'{Path(__file__).parent}/docker-compose.yml'
        try:
            run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
            sys.stderr.write('Starting docker compose')
            up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
            if up_result[0]:
                raise Exception(f'Failed to start docker: {up_result[2]}')
            url = "http://{}:{}".format(test_host, 10723)
            wait_until_responsive(timeout=30.0, pause=0.5, check=lambda: is_responsive(url))
        except Exception as e:
            raise Exception('Failed to run docker-compose: {}', str(e))
    test_client = get_client(
        host=test_host,
        port=client_port,
        username=os.environ.get('USER_ENV_VAR_NAME', 'default'),
        password=os.environ.get('PASSWORD_ENV_VAR_NAME', ''),
    )
    for user in ch_test_users:
        test_client.command(
            'CREATE USER IF NOT EXISTS %s IDENTIFIED WITH plaintext_password BY %s',
            (user, 'password'),
        )
    yield {}
    if run_docker:
        try:
            # Cleanup after tests complete
            run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        except Exception as e:
            raise Exception('Failed to run docker-compose while cleaning up: {}', str(e))
    else:
        for user in ch_test_users:
            test_client.command('DROP USER %s', (user,))


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    port = int(os.environ.get('DBT_CH_TEST_PORT', 8123))
    if port in (10900, 9000):
        driver = 'native'
    else:
        driver = 'http'
    return {
        'type': 'clickhouse',
        'threads': 1,
        'host': os.environ.get('DBT_CH_TEST_HOST', 'localhost'),
        'user': os.environ.get('DBT_CH_TEST_USER', 'default'),
        'password': os.environ.get('DBT_CH_TEST_PASSWORD', ''),
        'port': int(os.environ.get('DBT_CH_TEST_PORT', 8123)),  # docker client port
        'schema': os.environ.get('DBT_CH_TEST_DATABASE', None),
        'database_engine': os.environ.get('DBT_CH_TEST_DB_ENGINE', ''),
        'secure': False,
        'driver': driver,
        'custom_settings': {'distributed_ddl_task_timeout': 300},
    }


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
    """Wait until a service is responsive."""

    ref = clock()
    now = ref
    while (now - ref) < timeout:
        sleep(pause)
        if check():
            return
        now = clock()

    raise Exception("Timeout reached while waiting on service!")
