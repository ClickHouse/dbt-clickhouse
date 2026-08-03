"""
catchup=false must still create configured projections/indexes on the target table.
Regression for #637.
"""

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

MV_WITH_PROJECTION_CATCHUP_FALSE = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(id)',
       catchup=False,
       schema='catchup_projections',
       projections=[
           {
               'name': 'projection_by_name',
               'query': 'SELECT id, name ORDER BY name'
           }
       ]
) }}

select
    id,
    name,
    age,
    department
from {{ source('raw', 'people') }}
where department = 'engineering'
"""


class TestCatchupFalseKeepsProjections:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers.sql": MV_WITH_PROJECTION_CATCHUP_FALSE,
        }

    def test_projections_created_when_catchup_false(self, project):
        schema_unquoted = project.test_schema + "_catchup_projections"
        schema = quote_identifier(schema_unquoted)

        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Target table exists and is empty (no catchup backfill)
        count = project.run_sql(f"select count(*) from {schema}.hackers", fetch="one")
        assert count[0] == 0

        # Projection must still be present
        projections = project.run_sql(
            f"SELECT name FROM system.projections "
            f"WHERE database = '{schema_unquoted}' AND table = 'hackers'",
            fetch="all",
        )
        projection_names = {row[0] for row in projections}
        assert "projection_by_name" in projection_names
