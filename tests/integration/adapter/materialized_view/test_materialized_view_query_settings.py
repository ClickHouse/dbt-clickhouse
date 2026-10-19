"""
The model's query_settings must be part of the materialized view query, so the rows the
view writes later (on insert, or on refresh for a refreshable view) are computed with the
same settings as the initial fill of the target table.
"""

import json

import pytest
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.tests.util import run_dbt

from tests.integration.adapter.materialized_view.common import (
    PEOPLE_SEED_CSV,
    SEED_SCHEMA_YML,
)

# With join_use_nulls = 1 the unmatched (non-engineering) rows get NULL in
# matched_department. Under the server default they would get ''.
MV_QUERY = """
select
    p.id as id,
    d.department as matched_department
from {{ source('raw', 'people') }} p
left join (select 'engineering' as department) d on p.department = d.department
"""

MV_MODEL = (
    """
{{ config(
       materialized='materialized_view',
       engine='MergeTree()',
       order_by='(id)',
       query_settings={'join_use_nulls': 1},
       refreshable=({'interval': 'EVERY 1 HOUR'} if var('refreshable', False) else none)
) }}

{%- if var('target_table', none) %}
{{ materialization_target_table(ref(var('target_table'))) }}
{%- endif %}
"""
    + MV_QUERY
)

TARGET_MODEL = """
{{ config(materialized='table', engine='MergeTree()', order_by='(id)') }}

select
    toInt32(0) as id,
    cast(null, 'Nullable(String)') as matched_department
where 0
"""


def get_mv_ddl(project, mv_name):
    return project.run_sql(
        f"select create_table_query from system.tables"
        f" where database = '{project.test_schema}' and name = '{mv_name}'",
        fetch="one",
    )[0]


def insert_person(project, person_id, department):
    project.run_sql(
        f"""
        insert into {quote_identifier(project.test_schema)}.people ("id", "name", "age", "department")
            values ({person_id}, 'Person{person_id}', 30, '{department}')
        """
    )


def matched_department(project, table, person_id):
    row = project.run_sql(
        f"select matched_department from {quote_identifier(project.test_schema)}.{table}"
        f" where id = {person_id}",
        fetch="one",
    )
    assert row is not None, f"id {person_id} was not written to {table}"
    return row[0]


class TestMVQuerySettings:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hackers.sql": MV_MODEL}

    def test_query_settings_apply_to_mv_inserts(self, project):
        run_dbt(["seed"])
        run_dbt()

        # the initial fill already used the settings
        assert matched_department(project, 'hackers', 1000) is None
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers_mv')

        # rows written by the view itself must use them too
        insert_person(project, 4000, 'sales')
        assert matched_department(project, 'hackers', 4000) is None

        # a second run updates the view with MODIFY QUERY, which must keep them
        run_dbt()
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers_mv')
        insert_person(project, 5000, 'sales')
        assert matched_department(project, 'hackers', 5000) is None


class TestRefreshableMVQuerySettings:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hackers.sql": MV_MODEL}

    def refresh(self, project):
        schema = quote_identifier(project.test_schema)
        project.run_sql(f"system refresh view {schema}.hackers_mv")
        project.run_sql(f"system wait view {schema}.hackers_mv")

    def test_query_settings_apply_to_refreshes(self, project):
        run_vars = json.dumps({"refreshable": True})
        run_dbt(["seed"])
        run_dbt(["run", "--vars", run_vars])
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers_mv')

        # a refreshable view only picks up new source rows on refresh, so a row inserted
        # after creation proves the refresh ran, and that it ran with the settings
        insert_person(project, 4000, 'sales')
        self.refresh(project)
        assert matched_department(project, 'hackers', 4000) is None

        # a second run updates the view with MODIFY QUERY, which must keep them
        run_dbt(["run", "--vars", run_vars])
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers_mv')

        insert_person(project, 5000, 'sales')
        self.refresh(project)
        assert matched_department(project, 'hackers', 5000) is None


class TestExternalTargetMVQuerySettings:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hackers_target.sql": TARGET_MODEL,
            "hackers.sql": MV_MODEL,
        }

    def test_query_settings_apply_to_mv_inserts(self, project):
        run_vars = json.dumps({"target_table": "hackers_target"})
        run_dbt(["seed"])
        run_dbt(["run", "--vars", run_vars])

        assert matched_department(project, 'hackers_target', 1000) is None
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers')

        insert_person(project, 4000, 'sales')
        assert matched_department(project, 'hackers_target', 4000) is None

        run_dbt(["run", "--select", "hackers", "--vars", run_vars])
        assert 'join_use_nulls = 1' in get_mv_ddl(project, 'hackers')
        insert_person(project, 5000, 'sales')
        assert matched_department(project, 'hackers_target', 5000) is None
