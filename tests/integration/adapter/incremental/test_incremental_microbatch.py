import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch

_input_model_sql = """
{{
    config(
        materialized='table',
        event_time='event_time'
    )
}}

select 1 as id, toDateTime('2020-01-01 00:00:00') as event_time
union all
select 2 as id, toDateTime('2020-01-02 00:00:00') as event_time
union all
select 3 as id, toDateTime('2020-01-03 00:00:00') as event_time
"""

_microbatch_model_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='id',
        event_time='event_time',
        batch_size='day',
        begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}

select * from {{ ref('input_model') }}
"""


class TestMicrobatchIncremental(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_sql,
            "microbatch_model.sql": _microbatch_model_sql,
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '2020-01-04 00:00:00'), (5, '2020-01-05 00:00:00')"
