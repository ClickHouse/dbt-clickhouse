import os

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


class TestStringSettingsCorrectlyParsed:
    def build_example(self, setting_value: str):
        return f"""
{{{{
config(
    materialized='table',
    query_settings={{
        "join_algorithm": {setting_value}
    }} 
    )
}}}}

select 1
"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "string_setting_unquoted.sql": self.build_example("\"full_sorting_merge\""),
            "string_setting_already_quoted.sql": self.build_example("\"'full_sorting_merge'\""),
        }

    def execute_assert(self, model_name: str, project) -> None:
        run_dbt(["run", "--select", model_name])

        compiled_path = os.path.join(
            project.project_root, "target", "run", "test", "models", f"{model_name}.sql"
        )
        with open(compiled_path, 'r') as content:
            compiled_sql = content.read()

        assert "join_algorithm='full_sorting_merge'" in compiled_sql

    def test_parse_string_query_settings_unquoted(self, project):
        self.execute_assert("string_setting_unquoted", project)

    def test_parse_string_query_settings_already_quoted(self, project):
        self.execute_assert("string_setting_already_quoted", project)
