import pytest
from dbt.tests.util import run_dbt

cine_on = """
{{ config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='id',
        order_by=['id'],
        create_if_not_exists=true,
        settings={'allow_nullable_key': 1}
    )
}}
select toInt32(number) as id, concat('v', toString(number)) as val from numbers(10)
"""

cine_off = """
{{ config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='id',
        order_by=['id'],
        settings={'allow_nullable_key': 1}
    )
}}
select toInt32(number) as id, concat('v', toString(number)) as val from numbers(10)
"""


class TestCreateIfNotExists:
    @pytest.fixture(scope="class")
    def models(self):
        return {"cine_on.sql": cine_on, "cine_off.sql": cine_off}

    def test_builds_with_correct_row_count(self, project):
        run_dbt(["run"])
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10
        assert project.run_sql("select count() from cine_off", fetch="one")[0] == 10

    def test_flag_emits_atomic_create(self, project):
        run_dbt(["run", "--select", "cine_on cine_off"])
        project.run_sql("system flush logs")
        on_atomic = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table if not exists%cine_on%' "
            "and lower(query) not like '%empty%'",
            fetch="one",
        )[0]
        off_empty = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table%cine_off%' "
            "and lower(query) like '%empty%'",
            fetch="one",
        )[0]
        assert on_atomic >= 1, "flagged model should CREATE ... IF NOT EXISTS ... AS SELECT"
        assert off_empty >= 1, "default model should keep the empty-create-then-insert path"

    def test_losing_create_does_not_double(self, project):
        run_dbt(["run", "--select", "cine_on"])
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10
        project.run_sql(
            "create table if not exists cine_on engine = MergeTree order by id as "
            "select toInt32(number) as id, concat('v', toString(number)) as val from numbers(10)"
        )
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10
