import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch
from dbt.tests.util import patch_microbatch_end_time, run_dbt

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


_input_model_date_sql = """
{{
    config(
        materialized='table',
        event_time='event_date'
    )
}}

select 1 as id, toDate('2020-01-01') as event_date
union all
select 2 as id, toDate('2020-01-02') as event_date
union all
select 3 as id, toDate('2020-01-03') as event_date
"""

_microbatch_model_date_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='id',
        event_time='event_date',
        batch_size='day',
        begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}

select * from {{ ref('input_model') }}
"""


class TestMicrobatchIncrementalDateEventTime(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_date_sql,
            "microbatch_model.sql": _microbatch_model_date_sql,
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_date) values (4, '2020-01-04'), (5, '2020-01-05')"


# Date32 and DateTime64 hold dates before 1970, outside the DateTime range. The batch boundaries
# must not be clamped to 1970-01-01, or the first batches select and delete nothing.
_input_model_date32_sql = """
{{
    config(
        materialized='table',
        event_time='event_date'
    )
}}

select 1 as id, toDate32('1960-01-01') as event_date
union all
select 2 as id, toDate32('1960-01-02') as event_date
union all
select 3 as id, toDate32('1960-01-03') as event_date
"""

_input_model_datetime64_sql = """
{{
    config(
        materialized='table',
        event_time='event_time'
    )
}}

select 1 as id, toDateTime64('1960-01-01 00:00:00', 3) as event_time
union all
select 2 as id, toDateTime64('1960-01-02 00:00:00', 3) as event_time
union all
select 3 as id, toDateTime64('1960-01-03 00:00:00', 3) as event_time
"""

_microbatch_model_1960_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='id',
        event_time='%s',
        batch_size='day',
        begin=modules.datetime.datetime(1960, 1, 1, 0, 0, 0)
    )
}}

select * from {{ ref('input_model') }}
"""


class BaseMicrobatchHistorical(BaseMicrobatch):
    """The BaseMicrobatch scenario with the clock set to January 1960."""

    def test_run_with_event_time(self, project, insert_two_rows_sql):
        with patch_microbatch_end_time("1960-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        # same day, no new data: the current batch is reprocessed without duplicating rows
        with patch_microbatch_end_time("1960-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        project.run_sql(insert_two_rows_sql)
        self.assert_row_count(project, "input_model", 5)

        with patch_microbatch_end_time("1960-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        with patch_microbatch_end_time("1960-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 4)

        with patch_microbatch_end_time("1960-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)


class TestMicrobatchIncrementalDate32HistoricalEventTime(BaseMicrobatchHistorical):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_date32_sql,
            "microbatch_model.sql": _microbatch_model_1960_sql % 'event_date',
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_date) values (4, '1960-01-04'), (5, '1960-01-05')"


class TestMicrobatchIncrementalDateTime64HistoricalEventTime(BaseMicrobatchHistorical):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_datetime64_sql,
            "microbatch_model.sql": _microbatch_model_1960_sql % 'event_time',
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '1960-01-04 00:00:00'), (5, '1960-01-05 00:00:00')"
