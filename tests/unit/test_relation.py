from datetime import datetime

import pytest
from dbt.adapters.base.relation import EventTimeFilter
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


@pytest.mark.parametrize(
    'start,end,expected',
    [
        (
            datetime(2020, 1, 1),
            datetime(2020, 1, 2),
            "event_time >= toDateTime('2020-01-01 00:00:00')"
            " and event_time < toDateTime('2020-01-02 00:00:00')",
        ),
        (datetime(2020, 1, 1, 10), None, "event_time >= toDateTime('2020-01-01 10:00:00')"),
        (None, datetime(2020, 1, 2), "event_time < toDateTime('2020-01-02 00:00:00')"),
        (None, None, ""),
    ],
)
def test_render_event_time_filtered(start, end, expected):
    relation = ClickHouseRelation.create(schema='db', identifier='events')
    event_time_filter = EventTimeFilter(field_name='event_time', start=start, end=end)
    assert relation._render_event_time_filtered(event_time_filter) == expected
