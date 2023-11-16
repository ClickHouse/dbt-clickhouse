import pytest
from dbt.tests.util import run_dbt

model_sql = """
{{
    config(
        materialized='table'
    )
}}
select 1 as id
"""

another_schema_model_sql = """
{{
    config(
        materialized='table',
        schema='another_schema'
    )
}}
select 1 as id
"""


class BaseCachingTest:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "quoting": {
                "identifier": False,
                "schema": False,
            },
        }

    def run_and_inspect_cache(self, project, run_args=None):
        run_dbt(run_args)

        # the cache was empty at the start of the run.
        # the model materialization returned a relation and added to the cache.
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        relation = list(adapter.cache.relations).pop()
        assert relation.schema == project.test_schema

        # on the second run, dbt will find a relation in the database during cache population.
        run_dbt(run_args)
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        second_relation = list(adapter.cache.relations).pop()

        for key in ["schema", "identifier"]:
            assert getattr(relation, key) == getattr(second_relation, key)

    def test_cache(self, project):
        self.run_and_inspect_cache(project, run_args=["run"])


class TestNoPopulateCache(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }

    def test_cache(self, project):
        # --no-populate-cache still allows the cache to populate all relations
        # under a schema, so the behavior here remains the same as other tests
        run_args = ["--no-populate-cache", "run"]
        self.run_and_inspect_cache(project, run_args)


class TestCachingLowerCaseModel(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }


class TestCachingUppercaseModel(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "MODEL.sql": model_sql,
        }


class TestCachingSelectedSchemaOnly(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
            "another_schema_model.sql": another_schema_model_sql,
        }

    def test_cache(self, project):
        # this should only cache the schema containing the selected model
        run_args = ["--cache-selected-only", "run", "--select", "model"]
        self.run_and_inspect_cache(project, run_args)
