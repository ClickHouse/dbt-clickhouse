import os
import time

import pytest
from dbt.tests.util import relation_from_name, run_dbt

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
1232,Eugene,40,malware
9999,Paul,25,sales
""".lstrip()

PEOPLE_MODEL_WITH_PROJECTION = """
{{ config(
       materialized='%s',
       projections=[
           {
               'name': 'projection_avg_age',
               'query': 'SELECT department, avg(age) AS avg_age GROUP BY department'
           }
       ]
) }}

select
    id,
    name,
    age,
    department
from {{ source('raw', 'people') }}
"""

SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""


class TestProjections:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_with_projection.sql": PEOPLE_MODEL_WITH_PROJECTION % "table",
            "distributed_people_with_projection.sql": PEOPLE_MODEL_WITH_PROJECTION
            % "distributed_table",
        }

    def test_create_and_verify_projection(self, project):
        run_dbt(["seed"])
        run_dbt()

        relation = relation_from_name(project.adapter, "people_with_projection")

        query = f"SELECT department, avg(age) AS avg_age FROM {project.test_schema}.{relation.name} GROUP BY department ORDER BY department"

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [('engineering', 43.666666666666664), ('malware', 40.0), ('sales', 25.0)]

        # waiting for system.log table to be created
        time.sleep(10)

        # check that the latest query used the projection
        result = project.run_sql(
            f"SELECT query, projections FROM system.query_log WHERE query like '%{query}%' ORDER BY query_start_time DESC",
            fetch="all",
        )
        assert len(result) > 0
        assert query in result[0][0]

        assert result[0][1] == [f'{project.test_schema}.{relation.name}.projection_avg_age']

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_create_and_verify_distributed_projection(self, project):
        run_dbt(["seed"])
        run_dbt()
        relation = relation_from_name(project.adapter, "distributed_people_with_projection")

        query = f"SELECT department, avg(age) AS avg_age FROM {project.test_schema}.{relation.name} GROUP BY department ORDER BY department"

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [('engineering', 43.666666666666664), ('malware', 40.0), ('sales', 25.0)]

        # waiting for system.log table to be created
        time.sleep(10)

        # check that the latest query used the projection
        result = project.run_sql(
            f"SELECT query, projections FROM system.query_log WHERE query like '%{query}%' ORDER BY query_start_time DESC",
            fetch="all",
        )
        assert len(result) > 0
        assert query in result[0][0]

        assert result[0][1] == [f'{project.test_schema}.{relation.name}_local.projection_avg_age']
