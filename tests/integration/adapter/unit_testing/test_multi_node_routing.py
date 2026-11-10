"""
Regression test for unit tests on a multi-replica service (#737).

A session-scoped temporary table only exists on the node that created it. When
consecutive requests of one dbt run are routed to different nodes, a column lookup
that reads such a table back lands on a node that has never seen it and returns no
columns, and dbt rejects the fixture with "Accepted columns for expected output are:
[]".

The routing is reproduced with a small HTTP proxy that forwards every request to the
next of two cluster nodes in turn, which is the adversarial case of a load balancer
spreading requests across replicas.
"""

import http.client
import http.server
import itertools
import os
import threading

import pytest
import requests
from dbt.tests.util import run_dbt

HOP_BY_HOP = {'connection', 'keep-alive', 'transfer-encoding', 'host', 'content-length'}

people_sql = """
select 1 as id, 'ann' as name
union all
select 2 as id, 'bob' as name
"""

adults_sql = """
select id, name from {{ ref('people') }} where id > 1
"""

unit_tests_yml = """
version: 2

models:
  - name: people
    columns:
      - name: id
        data_type: uint64
      - name: name
        data_type: string
  - name: adults
    columns:
      - name: id
        data_type: uint64
      - name: name
        data_type: string

unit_tests:
  - name: test_adults_keeps_only_bob
    model: adults
    given:
      - input: ref('people')
        rows:
          - {id: 1, name: 'ann'}
          - {id: 2, name: 'bob'}
    expect:
      rows:
        - {id: 2, name: 'bob'}
"""


class _RoundRobinProxy(http.server.ThreadingHTTPServer):
    """Forwards each request to the next node in ``nodes``, in turn."""

    daemon_threads = True

    def __init__(self, nodes):
        super().__init__(('127.0.0.1', 0), _ProxyHandler)
        self._nodes = itertools.cycle(nodes)
        self._lock = threading.Lock()

    def next_node(self):
        with self._lock:
            return next(self._nodes)


class _ProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def _forward(self):
        host, port = self.server.next_node()
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
        upstream = http.client.HTTPConnection(host, port, timeout=60)
        try:
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            payload = response.read()
            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() not in HOP_BY_HOP:
                    self.send_header(key, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            upstream.close()

    do_GET = _forward
    do_POST = _forward


def _responsive(host, port):
    try:
        return requests.get(f'http://{host}:{port}/ping', timeout=5).status_code == 200
    except requests.exceptions.ConnectionError:
        return False


@pytest.fixture(scope="class")
def round_robin_proxy(test_config):
    """A proxy in front of two HTTP nodes of the test cluster.

    The second node is the one docker-compose publishes on ``DBT_CH_TEST_SECOND_HTTP_PORT``
    (10724 by default, see ``tests/integration/docker-compose.yml``).
    """
    if not test_config['cluster']:
        pytest.skip('needs a cluster: the schema has to exist on every node')
    if test_config['driver'] != 'http':
        pytest.skip('the proxy only speaks HTTP')
    nodes = [
        (test_config['host'], test_config['port']),
        (test_config['host'], int(os.environ.get('DBT_CH_TEST_SECOND_HTTP_PORT', 10724))),
    ]
    for host, port in nodes:
        if not _responsive(host, port):
            pytest.skip(f'no ClickHouse node answering on {host}:{port}')
    proxy = _RoundRobinProxy(nodes)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy.server_address
    finally:
        proxy.shutdown()
        proxy.server_close()


class TestUnitTestAcrossNodes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            'people.sql': people_sql,
            'adults.sql': adults_sql,
            'unit_tests.yml': unit_tests_yml,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self, test_config, round_robin_proxy):
        host, port = round_robin_proxy
        return {
            'type': 'clickhouse',
            # one connection, so the requests of a unit test are consecutive and
            # the proxy sends each one to a different node than the one before
            'threads': 1,
            'driver': 'http',
            'host': host,
            'port': port,
            'user': test_config['user'],
            'password': test_config['password'],
            'cluster': test_config['cluster'],
            'database_engine': test_config['db_engine'],
            'cluster_mode': test_config['cluster_mode'],
            'secure': False,
            'check_exchange': False,
            'custom_settings': {'distributed_ddl_task_timeout': 300},
        }

    def test_unit_test_passes_when_requests_alternate_nodes(self, project):
        # the fixture macro reads the input model's columns from its relation, so the
        # models have to exist; ON CLUSTER DDL puts them on every node
        results = run_dbt(['run'])
        assert len(results) == 2

        results = run_dbt(['test', '--select', 'test_type:unit'])
        assert len(results) == 1
        assert results[0].status == 'pass'
