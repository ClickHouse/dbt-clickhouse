import os
import uuid

import pytest
from dbt.tests.util import relation_from_name, run_dbt, write_file

from tests.integration.adapter.helpers import (
    DEFAULT_RETRY_CONFIG,
    retry_until_assertion_passes,
)

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

PEOPLE_MODEL_WITH_MULTIPLE_PROJECTIONS = """
{{ config(
       materialized='%s',
       projections=[
           {
               'name': 'projection_avg_age',
               'query': 'SELECT department, avg(age) AS avg_age GROUP BY department'
           },
            {
               'name': 'projection_sum_age',
               'query': 'SELECT department, sum(age) AS avg_age GROUP BY department'
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

PEOPLE_MODEL_WITH_SINGLE_INDEX_PROJECTION = """
{{ config(
       materialized='table',
       order_by='id',
       projections=[
           {
               'name': 'proj_by_age',
               'index': 'age'
           }
       ]
) }}
select id, name, age, department from {{ source('raw', 'people') }}
"""

PEOPLE_MODEL_WITH_MULTI_COLUMN_INDEX_PROJECTION = """
{{ config(
       materialized='table',
       order_by='id',
       projections=[
           {
               'name': 'proj_by_dept_age',
               'index': ['department', 'age']
           }
       ]
) }}
select id, name, age, department from {{ source('raw', 'people') }}
"""

PEOPLE_MODEL_WITH_QUERY_AND_INDEX = """
{{ config(
       materialized='table',
       order_by='id',
       projections=[
           {
               'name': 'bad_proj',
               'query': 'SELECT department ORDER BY department',
               'index': 'age'
           }
       ]
) }}
select id, name, age, department from {{ source('raw', 'people') }}
"""

PEOPLE_MODEL_WITH_NO_QUERY_OR_INDEX = """
{{ config(
       materialized='table',
       order_by='id',
       projections=[{'name': 'bad_proj'}]
) }}
select id, name, age, department from {{ source('raw', 'people') }}
"""

RETRY_CONFIG = (
    {"max_retries": 30, "delay": 1}
    if os.environ.get("DBT_CH_TEST_CLOUD", "").lower() in ("1", "true", "yes")
    else DEFAULT_RETRY_CONFIG
)


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
            "people_with_multiple_projections.sql": PEOPLE_MODEL_WITH_MULTIPLE_PROJECTIONS
            % "table",
        }

    def _get_cluster(self) -> str:
        # In Cloud we don't need to use `ON CLUSTER` for regular operations, but it's still useful for some
        # edge cases like flushing/querying all the different query_log.
        cluster = os.environ.get("DBT_CH_TEST_CLUSTER", "").strip()
        on_cloud = os.environ.get("DBT_CH_TEST_CLOUD", "").lower() in ("1", "true", "yes")
        return "default" if not cluster and on_cloud else cluster

    def _get_table_reference(self, table: str) -> str:
        cluster = self._get_cluster()
        return table if not cluster else f"clusterAllReplicas('{cluster}', {table})"

    def _flush_system_logs(self, project) -> None:
        cluster = self._get_cluster()
        cluster_clause = f'ON CLUSTER "{cluster}"' if cluster else ""
        project.run_sql(f"SYSTEM FLUSH LOGS {cluster_clause}", fetch="all")

    def test_create_and_verify_projection(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "people_with_projection"])

        relation = relation_from_name(project.adapter, "people_with_projection")
        unique_query_identifier = str(uuid.uuid4())
        query = f""" -- {unique_query_identifier}
        SELECT department, avg(age) AS avg_age FROM {project.test_schema}.{relation.name}
        GROUP BY department ORDER BY department"""

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [
            ("engineering", 43.666666666666664),
            ("malware", 40.0),
            ("sales", 25.0),
        ]

        # check that the latest query used the projection
        def check_that_the_latest_query_used_the_projection():
            self._flush_system_logs(project)
            result = project.run_sql(
                f"SELECT query, projections FROM {self._get_table_reference('system.query_log')} "
                f"WHERE query like '%{unique_query_identifier}%' "
                f"and query not like '%clusterAllReplicas%' and query not like '%system.query_log%' and read_rows > 0 ORDER BY query_start_time DESC",
                fetch="all",
            )
            assert len(result) > 0
            assert query in result[0][0]

            assert result[0][1] == [f"{project.test_schema}.{relation.name}.projection_avg_age"]

        retry_until_assertion_passes(
            check_that_the_latest_query_used_the_projection, **RETRY_CONFIG
        )

    def test_create_and_verify_multiple_projections(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "people_with_multiple_projections"])

        relation = relation_from_name(project.adapter, "people_with_multiple_projections")

        # test  the first projection
        unique_query_identifier = str(uuid.uuid4())
        query = f""" -- {unique_query_identifier}
        SELECT department, avg(age) AS avg_age FROM {project.test_schema}.{relation.name}
        GROUP BY department ORDER BY department"""

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [
            ("engineering", 43.666666666666664),
            ("malware", 40.0),
            ("sales", 25.0),
        ]

        # check that the latest query used the projection
        def check_that_the_latest_query_used_the_projection():
            self._flush_system_logs(project)
            result = project.run_sql(
                f"SELECT query, projections FROM {self._get_table_reference('system.query_log')} "
                f"WHERE query like '%{unique_query_identifier}%' "
                f"and query not like '%clusterAllReplicas%' and query not like '%system.query_log%' and read_rows > 0 ORDER BY query_start_time DESC",
                fetch="all",
            )
            assert len(result) > 0
            assert query in result[0][0]

            assert result[0][1] == [f"{project.test_schema}.{relation.name}.projection_avg_age"]

        retry_until_assertion_passes(
            check_that_the_latest_query_used_the_projection, **RETRY_CONFIG
        )

        # test the second projection
        unique_query_identifier = str(uuid.uuid4())
        query = f""" -- {unique_query_identifier}
                SELECT department, sum(age) AS sum_age FROM {project.test_schema}.{relation.name}
                GROUP BY department ORDER BY department"""

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [("engineering", 131), ("malware", 40), ("sales", 25)]

        def check_that_the_latest_query_used_the_projection():
            self._flush_system_logs(project)
            result = project.run_sql(
                f"SELECT query, projections FROM {self._get_table_reference('system.query_log')} "
                f"WHERE query like '%{unique_query_identifier}%' "
                f"and query not like '%clusterAllReplicas%' and query not like '%system.query_log%' and read_rows > 0 ORDER BY query_start_time DESC",
                fetch="all",
            )
            assert len(result) > 0
            assert query in result[0][0]

            assert result[0][1] == [f"{project.test_schema}.{relation.name}.projection_sum_age"]

        retry_until_assertion_passes(
            check_that_the_latest_query_used_the_projection, **RETRY_CONFIG
        )

    @pytest.mark.xfail
    @pytest.mark.skipif(
        os.environ.get("DBT_CH_TEST_CLUSTER", "").strip() == "",
        reason="Not on a cluster",
    )
    def test_create_and_verify_distributed_projection(self, project):
        run_dbt(["seed"])
        run_dbt()
        relation = relation_from_name(project.adapter, "distributed_people_with_projection")
        unique_query_identifier = str(uuid.uuid4())
        query = f"""-- {unique_query_identifier}
                 SELECT department, avg(age) AS avg_age FROM {project.test_schema}.{relation.name} GROUP BY
                 department ORDER BY department"""

        # Check that the projection works as expected
        result = project.run_sql(query, fetch="all")
        assert len(result) == 3  # We expect 3 departments in the result
        assert result == [
            ("engineering", 43.666666666666664),
            ("malware", 40.0),
            ("sales", 25.0),
        ]

        def check_that_the_latest_query_used_the_projection():
            self._flush_system_logs(project)
            result = project.run_sql(
                f"SELECT query, projections FROM {self._get_table_reference('system.query_log')} "
                f"WHERE query like '%{unique_query_identifier}%' "
                f"and query not like '%system.query_log%' and read_rows > 0 ORDER BY query_start_time DESC",
                fetch="all",
            )
            assert len(result) > 0
            assert query in result[0][0]

            assert result[0][1] == [
                f"{project.test_schema}.{relation.name}_local.projection_avg_age"
            ]

        retry_until_assertion_passes(
            check_that_the_latest_query_used_the_projection, **RETRY_CONFIG
        )


class TestIndexProjections:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "people_with_index_projection.sql": PEOPLE_MODEL_WITH_SINGLE_INDEX_PROJECTION,
            "people_with_multi_index_projection.sql": PEOPLE_MODEL_WITH_MULTI_COLUMN_INDEX_PROJECTION,
        }

    @pytest.mark.parametrize(
        "model_name, proj_name",
        [
            ("people_with_index_projection", "proj_by_age"),
            ("people_with_multi_index_projection", "proj_by_dept_age"),
        ],
    )
    def test_index_projection(self, project, model_name, proj_name):
        run_dbt(["seed"])
        run_dbt(["run", "--select", model_name])
        result = project.run_sql(
            f"SELECT name FROM system.projections "
            f"WHERE database = '{project.test_schema}' "
            f"AND table = '{model_name}' AND name = '{proj_name}'",
            fetch="all",
        )
        assert len(result) == 1


class BaseIndexProjectionValidation:
    materialized = "table"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "both_query_and_index.sql": PEOPLE_MODEL_WITH_QUERY_AND_INDEX.replace(
                "materialized='table'", f"materialized='{self.materialized}'"
            ),
            "no_query_or_index.sql": PEOPLE_MODEL_WITH_NO_QUERY_OR_INDEX.replace(
                "materialized='table'", f"materialized='{self.materialized}'"
            ),
        }

    def test_raises_when_both_query_and_index(self, project):
        run_dbt(["seed"])
        res = run_dbt(["run", "--select", "both_query_and_index"], expect_pass=False)
        assert any(
            "cannot specify both 'query' and 'index'" in (r.message or "") for r in res.results
        )

    def test_raises_when_neither_query_nor_index(self, project):
        run_dbt(["seed"])
        res = run_dbt(["run", "--select", "no_query_or_index"], expect_pass=False)
        assert any(
            "must specify either 'query' or 'index'" in (r.message or "") for r in res.results
        )


class TestIndexProjectionValidation(BaseIndexProjectionValidation):
    pass


@pytest.mark.skipif(
    os.environ.get("DBT_CH_TEST_CLUSTER", "").strip() == "",
    reason="Not on a cluster",
)
class TestDistributedIndexProjectionValidation(BaseIndexProjectionValidation):
    materialized = "distributed_table"


@pytest.mark.skipif(
    os.environ.get("DBT_CH_TEST_CLUSTER", "").strip() == "",
    reason="Not on a cluster",
)
class TestDistributedProjectionValidationPreservesTable:
    """A projection config error must fail before the materialization drops or
    renames anything, leaving the previously built model fully queryable."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": PEOPLE_SEED_CSV,
            "schema.yml": SEED_SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"people_distributed.sql": PEOPLE_MODEL_WITH_PROJECTION % "distributed_table"}

    def test_invalid_config_leaves_existing_model_intact(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "people_distributed"])

        relation = relation_from_name(project.adapter, "people_distributed")
        count_query = f"SELECT count(*) FROM {project.test_schema}.{relation.name}"
        assert project.run_sql(count_query, fetch="one")[0] == 5

        invalid_model = PEOPLE_MODEL_WITH_QUERY_AND_INDEX.replace(
            "materialized='table'", "materialized='distributed_table'"
        )
        write_file(invalid_model, project.project_root, "models", "people_distributed.sql")
        res = run_dbt(["run", "--select", "people_distributed"], expect_pass=False)
        assert any(
            "cannot specify both 'query' and 'index'" in (r.message or "") for r in res.results
        )

        # Both the distributed proxy and the underlying local tables must survive.
        assert project.run_sql(count_query, fetch="one")[0] == 5
        cluster = os.environ.get("DBT_CH_TEST_CLUSTER", "").strip()
        local_count_query = (
            f"SELECT count(*) FROM clusterAllReplicas('{cluster}', "
            f"{project.test_schema}.{relation.name}_local)"
        )
        assert project.run_sql(local_count_query, fetch="one")[0] >= 5
