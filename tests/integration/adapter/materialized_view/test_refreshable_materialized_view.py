"""
test refreshable materialized view creation. This is ClickHouse specific, which has a significantly different implementation
of materialized views from PostgreSQL or Oracle
"""

import json
import os

import pytest
from dbt.tests.util import check_relation_types, run_dbt

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
1000,Alfie,10,sales
2000,Bill,20,sales
3000,Charlie,30,sales
""".lstrip()

# This model is parameterized, in a way, by the "run_type" dbt project variable
# This is to be able to switch between different model definitions within
# the same test run and allow us to test the evolution of a materialized view
MV_MODEL = """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(department)',
       refreshable=(
           {
               "interval": "EVERY 2 MINUTE",
               "depends_on": ['depend_on_model'],
               "depends_on_validation": True
           } if var('run_type', '') == 'validate_depends_on' else {
               "interval": "EVERY 2 MINUTE"
           }
       )
       )
 }}
select
    department,
    avg(age) as average
    from {{ source('raw', 'people') }}
group by department
"""

SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""


class TestBasicRefreshableMV:
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
            "hackers.sql": MV_MODEL,
        }

    def test_create(self, project):
        """
        1. create a base table via dbt seed
        2. create a model as a refreshable materialized view, selecting from the table created in (1)
        3. check in system.view_refreshes for the table existence
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # create the model
        results = run_dbt()
        assert len(results) == 1

        columns = project.run_sql(f"DESCRIBE TABLE hackers", fetch="all")
        assert columns[0][1] == "String"

        columns = project.run_sql(f"DESCRIBE hackers_mv", fetch="all")
        assert columns[0][1] == "String"

        check_relation_types(
            project.adapter,
            {
                "hackers_mv": "view",
                "hackers": "table",
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
                          AND view = 'hackers_mv'
                    """,
                fetch="all",
            )
            statuses = [row[1] for row in result]
            assert 'Scheduled' in statuses or 'Running' in statuses
        else:
            result = project.run_sql(
                f"select database, view, status from system.view_refreshes where database= '{project.test_schema}' and view='hackers_mv'",
                fetch="all",
            )
            mv_status = result[0][2]
            assert mv_status in ('Scheduled', 'Running')

    def test_validate_dependency(self, project):
        """
        1. create a base table via dbt seed
        2. create a refreshable mv model with non exist dependency and validation config, selecting from the table created in (1)
        3. make sure we get an error
        """
        results = run_dbt(["seed"])
        assert len(results) == 1
        columns = project.run_sql(f"DESCRIBE TABLE {project.test_schema}.people", fetch="all")
        assert columns[0][1] == "Int32"

        # re-run dbt but this time with the new MV SQL
        run_vars = {"run_type": "validate_depends_on"}
        result = run_dbt(["run", "--vars", json.dumps(run_vars)], False)
        assert result[0].status == 'error'
        assert 'No existing MV found matching MV' in result[0].message
