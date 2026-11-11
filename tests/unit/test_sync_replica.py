import pytest

from tests.unit.macro_harness import SandboxSafeMock

REPLICATED = "ReplicatedMergeTree('/x', '{replica}')"
REPLICATED_DB_WITH_ARGS = "Replicated('/clickhouse/db/{uuid}', '{shard}', '{replica}')"


def _context(engine, cluster='"test_shard"', database_engine=None, enabled=True):
    adapter = SandboxSafeMock()
    adapter.get_clickhouse_cluster_name.return_value = cluster
    config = SandboxSafeMock()
    config.get.return_value = engine
    target = SandboxSafeMock(ensure_replica_consistency=enabled, database_engine=database_engine)
    return {'adapter': adapter, 'config': config, 'target': target}


def _relation(name):
    relation = SandboxSafeMock(should_on_cluster=True)
    relation.__str__.return_value = name
    return relation


@pytest.mark.parametrize(
    'engine,database_engine,expected',
    [
        (REPLICATED, None, True),
        ('MergeTree()', 'Replicated', True),
        ('MergeTree()', REPLICATED_DB_WITH_ARGS, True),
        (REPLICATED, 'Shared', True),
        ('Log', 'Replicated', False),
        ('MergeTree()', None, False),
        ('MergeTree()', 'Shared', False),
    ],
)
def test_is_replicated_engine(macros, engine, database_engine, expected):
    ctx = _context(engine, database_engine=database_engine)
    assert macros.call('is_replicated_engine', context=ctx) is expected


@pytest.mark.parametrize(
    'kwargs,expected',
    [
        ({'engine': REPLICATED}, True),
        ({'engine': 'MergeTree()', 'database_engine': 'Replicated'}, True),
        ({'engine': 'MergeTree()'}, False),
        ({'engine': REPLICATED, 'enabled': False}, False),
        # Without ON CLUSTER the sync reaches only the connected node, which wrote the data.
        ({'engine': REPLICATED, 'cluster': None}, False),
    ],
)
def test_should_sync_replica(macros, kwargs, expected):
    assert macros.call('should_sync_replica', context=_context(**kwargs)) is expected


def test_sync_replica_sql(macros):
    rendered = macros.call('sync_replica_sql', '"db"."t"', context=_context(REPLICATED))
    assert ' '.join(rendered.split()) == (
        'SYSTEM SYNC REPLICA ON CLUSTER "test_shard" "db"."t" LIGHTWEIGHT'
    )


def _recording_statement(statements):
    def statement(name, *args, caller=None, **kwargs):
        statements.append((name, ' '.join(caller().split())))
        return ''

    return statement


def test_sync_replica_runs_nothing_when_not_needed(macros):
    statements = []
    ctx = _context('MergeTree()')
    ctx['statement'] = _recording_statement(statements)

    macros.call('sync_replica', _relation('"db"."t"'), context=ctx)

    assert statements == []


def test_exchange_tables_atomic_syncs_both_tables_first(macros):
    statements = []
    ctx = _context(REPLICATED)
    ctx['statement'] = _recording_statement(statements)

    macros.call(
        'exchange_tables_atomic', _relation('"db"."new"'), _relation('"db"."live"'), context=ctx
    )

    assert statements == [
        ('sync_replica', 'SYSTEM SYNC REPLICA ON CLUSTER "test_shard" "db"."new" LIGHTWEIGHT'),
        ('sync_replica', 'SYSTEM SYNC REPLICA ON CLUSTER "test_shard" "db"."live" LIGHTWEIGHT'),
        (
            'exchange_tables_atomic',
            'EXCHANGE TABLES "db"."new" AND "db"."live" ON CLUSTER "test_shard"',
        ),
    ]
