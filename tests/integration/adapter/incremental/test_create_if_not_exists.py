import pytest
from dbt.tests.util import run_dbt

# Same model, flag on vs off. The flag makes the table's CREATE use
# `CREATE TABLE IF NOT EXISTS`, so independent concurrent runs building the same
# table into a shared database don't collide on first creation (Code: 57).
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

    def test_flag_emits_if_not_exists_ddl(self, project):
        run_dbt(["run"])

        # Both models build and hold data regardless of the flag.
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10
        assert project.run_sql("select count() from cine_off", fetch="one")[0] == 10

        # The flagged model's CREATE uses IF NOT EXISTS; the default one does not.
        project.run_sql("system flush logs")
        on_ddl = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table if not exists%cine_on%'",
            fetch="one",
        )[0]
        off_ddl = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table%cine_off%' "
            "and lower(query) not like '%if not exists%'",
            fetch="one",
        )[0]
        assert on_ddl >= 1, "flagged model should CREATE TABLE IF NOT EXISTS"
        assert off_ddl >= 1, "default model should CREATE TABLE without IF NOT EXISTS"

    def test_rerun_is_idempotent(self, project):
        run_dbt(["run", "--select", "cine_on"])
        run_dbt(["run", "--select", "cine_on"])
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10
