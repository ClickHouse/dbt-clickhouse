import pytest
from dbt.adapters.clickhouse.relation import ClickHouseRelation


@pytest.mark.parametrize(
    'cluster,database_engine,expected',
    [
        ('', None, False),
        ('cluster', None, True),
        ('cluster', '', True),
        ('cluster', 'Atomic', True),
        ('cluster', 'Replicated', False),
        ('cluster', 'ReplicatedDatabase', False),
    ],
)
def test_get_on_cluster(cluster, database_engine, expected):
    assert ClickHouseRelation.get_on_cluster(cluster, database_engine) is expected
