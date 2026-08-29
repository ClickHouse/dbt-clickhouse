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

    def test_first_build_uses_if_not_exists_and_separate_insert(self, project):
        run_dbt(["run", "--select", "cine_on cine_off"])
        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 10

        project.run_sql("system flush logs")
        on_empty_ine = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table if not exists%cine_on%' "
            "and lower(query) like '%empty%'",
            fetch="one",
        )[0]
        on_separate_insert = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%insert into%cine_on%'",
            fetch="one",
        )[0]
        off_plain = project.run_sql(
            "select count() from system.query_log where type = 'QueryFinish' "
            "and lower(query) like '%create table%cine_off%' "
            "and lower(query) not like '%if not exists%'",
            fetch="one",
        )[0]
        assert on_empty_ine >= 1, "flagged first build should CREATE TABLE IF NOT EXISTS ... empty"
        assert on_separate_insert >= 1, (
            "flagged first build must run a SEPARATE INSERT (not atomic CTAS)"
        )
        assert off_plain >= 1, "default model should CREATE TABLE without IF NOT EXISTS"

    def test_two_concurrent_first_builds_keep_all_rows(self, project):
        project.run_sql("drop table if exists cine_on")

        def first_build(offset):
            project.run_sql(
                "create table if not exists cine_on engine = MergeTree order by id empty as "
                f"(select toInt32(number) + {offset} as id, "
                f"concat('v', toString(number + {offset})) as val from numbers(10))"
            )
            project.run_sql(
                f"insert into cine_on select toInt32(number) + {offset} as id, "
                f"concat('v', toString(number + {offset})) as val from numbers(10)"
            )

        first_build(0)  # invocation A: ids 0-9   (wins the create)
        first_build(100)  # invocation B: ids 100-109 (create no-ops, insert still lands)

        assert project.run_sql("select count() from cine_on", fetch="one")[0] == 20
        assert project.run_sql("select count() from cine_on where id >= 100", fetch="one")[0] == 10
