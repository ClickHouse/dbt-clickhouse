import pytest
from dbt.tests.util import run_dbt

nullable_column_model = """
{{
    config(
        materialized='table',
        query_settings={
            'join_use_nulls': 1
        }
    )
}}
select t2.id as test_id
from (select 1 as id) t1
         left join (select 2 as id) t2
on t1.id=t2.id
"""


class TestNullableColumnJoin:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nullable_column_model.sql": nullable_column_model,
        }

    def test_nullable_column_join(self, project):
        run_dbt(["run", "--select", "nullable_column_model"])
        result = project.run_sql(
            "select isNullable(test_id) as is_nullable_column from nullable_column_model",
            fetch="one",
        )
        assert result[0] == 1


not_nullable_column_model = """
{{
    config(
        materialized='table',
        query_settings={
            'join_use_nulls': 0
        }
    )
}}
select t2.id as test_id
from (select 1 as id) t1
         left join (select 2 as id) t2
on t1.id=t2.id
"""


class TestNotNullableColumnJoin:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "not_nullable_column_model.sql": not_nullable_column_model,
        }

    def test_nullable_column_join(self, project):
        run_dbt(["run", "--select", "not_nullable_column_model"])
        result = project.run_sql(
            "select isNullable(test_id) as is_nullable_column from not_nullable_column_model",
            fetch="one",
        )
        assert result[0] == 0
