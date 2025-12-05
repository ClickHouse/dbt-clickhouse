import pytest
from dbt.tests.util import relation_from_name, run_dbt

test_models_mergetree = """
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='id'
) }}

select 1 as id, 'test' as name
"""

test_models_memory = """
{{ config(
    materialized='table',
    engine='Memory'
) }}

select 1 as id, 'test' as name
"""


def get_table_engine(project, table_name):
    """Helper function to get the engine of a table."""
    relation = relation_from_name(project.adapter, table_name)

    result = project.run_sql(
        f"SELECT engine_full FROM system.tables "
        f"WHERE database = '{relation.path.schema}' AND name = '{relation.identifier}'",
        fetch="one",
    )
    return result[0]


class TestEngineSettings:
    @pytest.fixture(scope="class")
    def models(self):
        return {"mergetree.sql": test_models_mergetree, "memory.sql": test_models_memory}

    def test_setting_replicated_deduplication_window_only_present_in_mergetree_engine_family(
        self, project
    ):
        # Run dbt to create the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Check MergeTree engine - setting should be present
        mergetree_engine = get_table_engine(project, 'mergetree')
        assert 'replicated_deduplication_window' in mergetree_engine

        # Check Memory engine - setting should be ignored
        memory_engine = get_table_engine(project, 'memory')
        assert 'replicated_deduplication_window' not in memory_engine
