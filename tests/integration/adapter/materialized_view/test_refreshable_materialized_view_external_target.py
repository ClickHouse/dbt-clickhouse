"""
Test refreshable materialized view creation with external target table.
This tests the new implementation where a refreshable MV writes to an existing table
using the `materialization_target_table()` macro.
"""

import json
import os

import pytest
from dbt.tests.util import check_relation_types, run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

# Target table model - this creates the destination table that the MV will write to
TARGET_TABLE_MODEL = """
{{ config(
       materialized='table',
       engine='MergeTree()',
       order_by='(department)'
) }}

SELECT
    '' AS department,
    toFloat64(0) AS average
WHERE 0  -- Creates empty table with correct schema
"""

# Refreshable MV model that writes to the external target table
MV_MODEL = """
{{ config(
       materialized='materialized_view',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE",
               "depends_on": ['depend_on_model'],
               "depends_on_validation": True
           } if var('run_type', '') == 'validate_depends_on' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
) }}

{{ materialization_target_table(ref('hackers_target')) }}

select
    department,
    avg(age) as average
from {{ source('raw', 'people') }}
group by department
"""


class TestBasicExternalTargetRefreshableMV:
    @pytest.fixture(scope="class")
    def seeds(self):
        """
        we need a base table to pull from
        """
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_TABLE_MODEL,
            "hackers.sql": MV_MODEL,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a target table model
        3. create a model as a refreshable materialized view pointing to the target table
        4. check in system.view_refreshes for the MV existence
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the models (target table + refreshable MV)
        results = run_dbt()
        assert len(results) == 2

        # Check the target table structure
        columns = project.run_sql(f"DESCRIBE TABLE hackers_target", fetch="all")
        assert columns[0][1] == "String"

        # Check the MV exists
        columns = project.run_sql(f"DESCRIBE hackers", fetch="all")
        assert columns[0][1] == "String"

        check_relation_types(
            project.adapter,
            {
                "hackers": "materialized_view",
                "hackers_target": "table",
            },
        )

        if os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'):
            result = project.run_sql(
                f"""
                        SELECT 
                            hostName() as replica,
                            status,
                            last_refresh_time
                        FROM clusterAllReplicas('default', 'system', 'view_refreshes')
                        WHERE database = '{project.test_schema}' 
                          AND view = 'hackers'
                    """,
                fetch="all",
            )
            statuses = [row[1] for row in result]
            assert 'Scheduled' in statuses or 'Running' in statuses
        else:
            result = project.run_sql(
                f"select database, view, status from system.view_refreshes where database= '{project.test_schema}' and view='hackers'",
                fetch="all",
            )
            mv_status = result[0][2]
            assert mv_status in ('Scheduled', 'Running')

    def test_validate_dependency(self, project):
        """
        1. create a base table via dbt seed
        2. create a refreshable mv model with non exist dependency and validation config
        3. make sure we get an error
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # re-run dbt but this time with the new MV SQL
        run_vars = {"run_type": "validate_depends_on"}
        result = run_dbt(["run", "--vars", json.dumps(run_vars)], False)
        # Find the result that has an error (might be hackers model)
        error_results = [r for r in result if r.status == 'error']
        assert len(error_results) > 0
        assert 'No existing MV found matching MV' in error_results[0].message
