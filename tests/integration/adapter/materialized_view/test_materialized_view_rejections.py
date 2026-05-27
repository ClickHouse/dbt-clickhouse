"""
Tests for materialized_view syntax that is intentionally unsupported.
"""

import pytest
from dbt.tests.util import run_dbt

from tests.integration.adapter.materialized_view.common import PEOPLE_SEED_CSV, SEED_SCHEMA_YML

TARGET_TABLE_MODEL = """
{{ config(materialized='table') }}

select
    toInt32(0) as id,
    '' as name
where 0
"""

TARGET_TABLE_CONFIG_MODEL = """
{{ config(
       materialized='materialized_view',
       target_table='legacy_target'
) }}

select
    id,
    name
from {{ source('raw', 'people') }}
"""

MATERIALIZATION_TARGET_TABLE_MODEL = """
{{ config(materialized='materialized_view') }}

{{ materialization_target_table(ref('legacy_target')) }}

select
    id,
    name
from {{ source('raw', 'people') }}
"""

NAMED_SECTIONS_MODEL = """
{{ config(materialized='materialized_view') }}

--engineering:begin
select
    id,
    name
from {{ source('raw', 'people') }}
where department = 'engineering'
--engineering:end

union all

--sales:begin
select
    id,
    name
from {{ source('raw', 'people') }}
where department = 'sales'
--sales:end
"""


class BaseMaterializedViewRejection:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }


class TestTargetTableConfigRejected(BaseMaterializedViewRejection):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "uses_target_table_config.sql": TARGET_TABLE_CONFIG_MODEL,
        }

    def test_target_table_config_fails(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"], expect_pass=False)

        assert len(results) == 1
        assert results[0].status == "error"
        assert "The target_table config is no longer supported" in results[0].message


class TestMaterializationTargetTableRejected(BaseMaterializedViewRejection):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "legacy_target.sql": TARGET_TABLE_MODEL,
            "uses_materialization_target_table.sql": MATERIALIZATION_TARGET_TABLE_MODEL,
        }

    def test_materialization_target_table_macro_fails(self, project):
        run_dbt(["seed"])
        results = run_dbt(
            ["run", "--select", "uses_materialization_target_table"], expect_pass=False
        )

        assert len(results) == 1
        assert results[0].status == "error"
        assert "materialization_target_table() is no longer supported" in results[0].message


class TestNamedSectionsRejected(BaseMaterializedViewRejection):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "uses_named_sections.sql": NAMED_SECTIONS_MODEL,
        }

    def test_named_sections_fail(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"], expect_pass=False)

        assert len(results) == 1
        assert results[0].status == "error"
        assert (
            "Named MV sections like --name:begin/--name:end are not supported" in results[0].message
        )
