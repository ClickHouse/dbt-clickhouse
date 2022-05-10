import sys
from pathlib import Path
from subprocess import Popen, PIPE

import pytest
from time import sleep
import os
import requests
import timeit

# Import the standard integration fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    # Run docker compose with clickhouse-server image.
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
    sys.stderr.write('Starting docker compose')
    up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
    if up_result[0]:
        raise Exception(f'Failed to start docker: {up_result[2]}')
    url = "http://{}:{}".format(os.environ.get('HOST_ENV_VAR_NAME'), 10723)
    wait_until_responsive(timeout=30.0, pause=0.5, check=lambda: is_responsive(url))
    yield {
        'type': 'clickhouse',
        'threads': 1,
        'host': os.environ.get('HOST_ENV_VAR_NAME'),
        'user': os.environ.get('USER_ENV_VAR_NAME'),
        'password': os.environ.get('PASSWORD_ENV_VAR_NAME'),
        'port': 10900,  # docker client port
        'Secure': False
    }
    # Cleanup after tests complete
    run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except ConnectionError:
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