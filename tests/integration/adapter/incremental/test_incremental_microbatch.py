import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch
from dbt.tests.util import patch_microbatch_end_time, relation_from_name, run_dbt

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


# Rows at 02:00 UTC belong to the previous day in America/New_York, so batch membership and the
# re-run deletes only match BaseMicrobatch's expectations if batch boundaries are UTC.
_input_model_utc_sql = """
{{
    config(
        materialized='table',
        event_time='event_time'
    )
}}

select 1 as id, toDateTime('2020-01-01 02:00:00', 'UTC') as event_time
union all
select 2 as id, toDateTime('2020-01-02 02:00:00', 'UTC') as event_time
union all
select 3 as id, toDateTime('2020-01-03 02:00:00', 'UTC') as event_time
"""


class TestMicrobatchIncrementalNonUtcSession(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        custom_settings = {
            **dbt_profile_target['custom_settings'],
            'session_timezone': 'America/New_York',
        }
        return {**dbt_profile_target, 'custom_settings': custom_settings}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_utc_sql,
            "microbatch_model.sql": _microbatch_model_sql,
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '2020-01-04 02:00:00'), (5, '2020-01-05 02:00:00')"


# Pre-1970 event times: Date32 and DateTime64 can hold them, so batch boundaries before the epoch
# must still select, delete and re-insert exactly the rows of each batch.
_microbatch_model_1960_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key='id',
        event_time='event_time',
        batch_size='day',
        begin=modules.datetime.datetime(1960, 1, 1, 0, 0, 0)
    )
}}

select * from {{ ref('input_model') }}
"""

_input_model_date32_1960_sql = """
{{
    config(
        materialized='table',
        event_time='event_time'
    )
}}

select 1 as id, toDate32('1960-01-01') as event_time
union all
select 2 as id, toDate32('1960-01-02') as event_time
union all
select 3 as id, toDate32('1960-01-03') as event_time
"""

_input_model_datetime64_1960_sql = """
{{
    config(
        materialized='table',
        event_time='event_time'
    )
}}

select 1 as id, toDateTime64('1960-01-01 06:00:00', 3, 'UTC') as event_time
union all
select 2 as id, toDateTime64('1960-01-02 06:00:00', 3, 'UTC') as event_time
union all
select 3 as id, toDateTime64('1960-01-03 06:00:00', 3, 'UTC') as event_time
"""


class HistoricalMicrobatchBase(BaseMicrobatch):
    """BaseMicrobatch's flow, shifted to 1960, plus an explicit reprocess of one earlier batch."""

    def assert_ids(self, project, relation_name: str, expected_ids):
        relation = relation_from_name(project.adapter, relation_name)
        ids = [row[0] for row in project.run_sql(f"select id from {relation}", fetch="all")]
        assert sorted(ids) == sorted(expected_ids), f"{relation_name}: {sorted(ids)}"

    def test_run_with_event_time(self, project, insert_two_rows_sql):
        # initial run -- backfills the three 1960 batches
        with patch_microbatch_end_time("1960-01-03 13:57:00"):
            run_dbt(["run"])
        self.assert_ids(project, "microbatch_model", [1, 2, 3])

        # same day, no new data => same rows
        with patch_microbatch_end_time("1960-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_ids(project, "microbatch_model", [1, 2, 3])

        project.run_sql(insert_two_rows_sql)
        self.assert_ids(project, "input_model", [1, 2, 3, 4, 5])

        with patch_microbatch_end_time("1960-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_ids(project, "microbatch_model", [1, 2, 3])

        with patch_microbatch_end_time("1960-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_ids(project, "microbatch_model", [1, 2, 3, 4])

        with patch_microbatch_end_time("1960-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_ids(project, "microbatch_model", [1, 2, 3, 4, 5])

        # reprocess the 1960-01-02 batch: its row is replaced, nothing else is touched
        run_dbt(
            [
                "run",
                "--select",
                "microbatch_model",
                "--event-time-start",
                "1960-01-02",
                "--event-time-end",
                "1960-01-03",
            ]
        )
        self.assert_ids(project, "microbatch_model", [1, 2, 3, 4, 5])


class TestMicrobatchIncrementalDate32Before1970(HistoricalMicrobatchBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_date32_1960_sql,
            "microbatch_model.sql": _microbatch_model_1960_sql,
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '1960-01-04'), (5, '1960-01-05')"


class TestMicrobatchIncrementalDateTime64Before1970(HistoricalMicrobatchBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_datetime64_1960_sql,
            "microbatch_model.sql": _microbatch_model_1960_sql,
        }

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, '1960-01-04 06:00:00'), (5, '1960-01-05 06:00:00')"
