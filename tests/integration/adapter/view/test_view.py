"""
Test ClickHouse view materialization in dbt-clickhouse
"""

import json

import pytest
from dbt.tests.util import run_dbt

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
""".lstrip()

PEOPLE_VIEW_MODEL = """
{{ config(
       materialized='view'
) }}

{% if var('run_type', '') == '' %}
    select id, name, age from {{ source('raw', 'people') }}
{% elif var('run_type', '') == 'update_view' %}
    select id, name, age, department from {{ source('raw', 'people') }}
{% endif %}
"""


SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""


class TestClickHouseView:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"people_view.sql": PEOPLE_VIEW_MODEL}

    def test_create_view(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        run_dbt()

        # Query the view and check if it returns expected data
        result = project.run_sql("SELECT COUNT(*) FROM people_view", fetch="one")
        assert result[0] == 3  # 3 records in the seed data

        # Run dbt again to apply the update
        run_dbt(["run", "--vars", json.dumps({"run_type": "update_view"})])

        # Verify the new column is present
        result = project.run_sql("DESCRIBE TABLE people_view", fetch="all")
        columns = {row[0] for row in result}
        assert "department" in columns  # New column should be present
