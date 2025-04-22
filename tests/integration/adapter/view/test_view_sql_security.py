"""
Test ClickHouse view with sql security settings in dbt-clickhouse
"""

import os

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

PEOPLE_SEED_CSV = """
id,name,age,department
1231,Dade,33,engineering
6666,Ksenia,48,engineering
8888,Kate,50,engineering
""".lstrip()

SEED_SCHEMA_YML = """
version: 2

sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people
"""

PEOPLE_VIEW_CONFIG = """
{{ config(
       materialized='view',
       sql_security='invoker'
) }}
"""

PEOPLE_VIEW_CONFIG_2 = """
{{ config(
       materialized='view',
       sql_security='definer',
       definer='%s'
) }}
"""

PEOPLE_VIEW_CONFIG_3 = """
{{ config(
       materialized='view',
       sql_security='definer'
) }}
"""

PEOPLE_VIEW_CONFIG_4 = """
{{ config(
       materialized='view',
       sql_security='definer',
       definer=''
) }}
"""

PEOPLE_VIEW_CONFIG_5 = """
{{ config(
       materialized='view',
       sql_security='wrong'
) }}
"""

PEOPLE_VIEW_MODEL = """
select
    id,
    name,
    age,
    department
from {{ source('raw', 'people') }}
"""


class TestClickHouseViewSqlSecurity:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_invoker.sql": PEOPLE_VIEW_CONFIG + PEOPLE_VIEW_MODEL,
            "view_definer.sql": PEOPLE_VIEW_CONFIG_2 % os.environ.get('DBT_CH_TEST_USER', 'default')
            + PEOPLE_VIEW_MODEL,
            "view_definer_empty.sql": PEOPLE_VIEW_CONFIG_3 + PEOPLE_VIEW_MODEL,
            "view_definer_wrong.sql": PEOPLE_VIEW_CONFIG_4 + PEOPLE_VIEW_MODEL,
            "view_sql_security.sql": PEOPLE_VIEW_CONFIG_5 + PEOPLE_VIEW_MODEL,
        }

    def test_create_view_invoker(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        run_dbt(["run", "--select", "view_invoker"])

        # Query system table to be sure that view query contains desired statement
        result = project.run_sql(
            """select 1 from system.tables
                where table = 'view_invoker'
                and position(create_table_query, 'SQL SECURITY INVOKER') > 0""",
            fetch="one",
        )
        assert result[0] == 1  # 1 records in the seed data

    def test_create_view_definer(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        run_dbt(["run", "--select", "view_definer"])

        # Query system table to be sure that view query contains desired statement
        result = project.run_sql(
            f"""select 1 from system.tables
                where table = 'view_definer'
                and position(create_table_query, 'DEFINER = {os.environ.get('DBT_CH_TEST_USER', 'default')} SQL SECURITY DEFINER') > 0""",
            fetch="one",
        )
        assert result[0] == 1  # 3 records in the seed data

    def test_fail_view_definer_empty(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        _, stdout = run_dbt_and_capture(
            ["run", "--select", "view_definer_empty"], expect_pass=False
        )

        # Confirm that stdout/console output has error description
        assert (
            "Model 'model.test.view_definer_empty' does not define a required config parameter 'definer'."
            in stdout
        )

    def test_fail_view_definer_wrong(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        _, stdout = run_dbt_and_capture(
            ["run", "--select", "view_definer_wrong"], expect_pass=False
        )

        # Confirm that stdout/console output has error description
        assert "Invalid config parameter `definer`. No value was provided." in stdout

    def test_fail_view_sql_security(self, project):
        # Load seed data
        run_dbt(["seed"])

        # Run dbt to create the view
        _, stdout = run_dbt_and_capture(["run", "--select", "view_sql_security"], expect_pass=False)

        # Confirm that stdout/console output has error description
        assert (
            "Invalid config parameter `sql_security`. Got: `wrong`, but only definer | invoker allowed."
            in stdout
        )
