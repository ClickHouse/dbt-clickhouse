import pytest
from dbt.tests.util import run_dbt


class TestAliasesWithEmptyFlag:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": """
                {{ config(materialized='table') }}
                SELECT 1 as id, 'test' as name, true as active
            """,
            "model_with_alias.sql": """
                {{ config(materialized='table') }}
                SELECT 
                    base_alias.id,
                    base_alias.name,
                    base_alias.active
                FROM {{ ref('base_table') }} base_alias
                WHERE base_alias.active = true
            """,
            "incremental_base_table.sql": """
                {{ config(materialized='table') }}
                SELECT 1 as event_id, 'login' as event_type, '2023-01-01'::date as event_date
                UNION ALL
                SELECT 2 as event_id, 'logout' as event_type, '2023-01-02'::date as event_date
            """,
            "incremental_with_alias.sql": """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='append',
                    unique_key='event_id'
                ) }}
                SELECT 
                    src_alias.event_id,
                    src_alias.event_type,
                    src_alias.event_date
                FROM {{ ref('incremental_base_table') }} src_alias
            """,
            "view_with_alias.sql": """
                {{ config(materialized='view') }}
                SELECT 
                    base_alias.id,
                    base_alias.name
                FROM {{ ref('base_table') }} base_alias
                WHERE base_alias.name = 'test'
            """,
        }

    def test_alias_with_empty_flag(self, project):
        def run_and_assert(select_model):
            results = run_dbt(["run", "--empty", "--select", select_model])
            assert len(results) == 1
            assert results[0].status.lower() == "success"

        run_dbt(["run", "--select", "base_table incremental_base_table"])

        run_and_assert("model_with_alias")
        run_and_assert("incremental_with_alias")
        run_and_assert("view_with_alias")
