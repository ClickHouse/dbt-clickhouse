import os
import uuid

import pytest
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture, write_file


class TestModelGrants(BaseModelGrants):
    pass


def _cluster_clause():
    cluster = os.environ.get('DBT_CH_TEST_CLUSTER', '').strip()
    return f'ON CLUSTER "{cluster}"' if cluster else ''


# Incremental, so the second run updates the grants of the existing table: dbt reads the
# current grants with get_show_grant_sql and revokes the ones it did not configure.
grants_incremental_model_sql = """
{{ config(materialized='incremental', order_by='id') }}
select 1 as id, 'a' as name
"""

special_grantee_schema_yml = """
version: 2
models:
  - name: grants_model
    config:
      grants:
        select: ["{grantee}"]
"""


class TestSpecialCharacterGrantee(BaseGrants):
    """Grantee names that need quoting, like `etl-user`, used to produce invalid
    GRANT/REVOKE statements."""

    @pytest.fixture(scope="class")
    def special_user(self, project):
        user = f'dbt-test-user-{uuid.uuid4().hex[:8]}'
        project.run_sql(f'CREATE USER IF NOT EXISTS `{user}` {_cluster_clause()}')
        yield user
        project.run_sql(f'DROP USER IF EXISTS `{user}` {_cluster_clause()}')

    @pytest.fixture(scope="class")
    def models(self):
        return {"grants_model.sql": grants_incremental_model_sql}

    def test_grant_and_revoke_special_character_grantee(
        self, project, special_user, get_test_users
    ):
        test_user = get_test_users[0]

        write_file(
            special_grantee_schema_yml.replace('{grantee}', special_user),
            project.project_root,
            "models",
            "schema.yml",
        )
        run_dbt(["run"])
        self.assert_expected_grants_match_actual(
            project, "grants_model", {"select": [special_user]}
        )

        # A grant to the same user that dbt does not manage (e.g. granted by hand) must be
        # revoked on the next run, together with the SELECT that moved to another user.
        relation = relation_from_name(project.adapter, "grants_model")
        project.run_sql(f'GRANT {_cluster_clause()} INSERT ON {relation} TO `{special_user}`')
        write_file(
            special_grantee_schema_yml.replace('{grantee}', test_user),
            project.project_root,
            "models",
            "schema.yml",
        )
        (_, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert "revoke " in log_output
        self.assert_expected_grants_match_actual(project, "grants_model", {"select": [test_user]})


column_grant_schema_yml = """
version: 2
models:
  - name: grants_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""


class TestColumnAndPartialGrantsAreNotTableGrants(BaseGrants):
    """A column-level grant, or a database-wide grant with this table revoked, must not be
    read as a table-wide grant; otherwise dbt skips the GRANT it was configured to run."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"grants_model.sql": grants_incremental_model_sql}

    def test_column_grant_is_upgraded_to_table_grant(self, project, get_test_users):
        user_1, user_2 = get_test_users[0], get_test_users[1]
        run_dbt(["run"])
        relation = relation_from_name(project.adapter, "grants_model")
        cluster = _cluster_clause()

        project.run_sql(f'GRANT {cluster} SELECT(name) ON {relation} TO {user_1}')
        project.run_sql(f'GRANT {cluster} SELECT ON `{relation.schema}`.* TO {user_2}')
        project.run_sql(f'REVOKE {cluster} SELECT ON {relation} FROM {user_2}')
        self.assert_expected_grants_match_actual(project, "grants_model", {})

        write_file(column_grant_schema_yml, project.project_root, "models", "schema.yml")
        (_, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert "grant " in log_output
        self.assert_expected_grants_match_actual(project, "grants_model", {"select": [user_1]})

        project.run_sql(f'REVOKE {cluster} SELECT ON `{relation.schema}`.* FROM {user_2}')


replicated_grants_schema_yml = """
version: 2
models:
  - name: grants_model
    config:
      materialized: table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
"""


@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
)
@pytest.mark.skipif(
    os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'),
    reason='Replicated is not supported for cloud',
)
class TestReplicatedDatabaseGrantsOnCluster(BaseGrants):
    """The Replicated database engine replicates table DDL but not grants, so GRANT/REVOKE
    must still run ON CLUSTER even though table DDL skips it there."""

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        return {
            **test_config,
            "db_engine": "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "grants_model.sql": "select 1 as id",
            "schema.yml": replicated_grants_schema_yml,
        }

    def test_grants_reach_every_node(self, project, get_test_users):
        run_dbt(["run"])
        relation = relation_from_name(project.adapter, "grants_model")
        cluster = project.test_config['cluster']
        nodes = project.run_sql(
            f"select count() from clusterAllReplicas('{cluster}', system.one)", fetch="one"
        )[0]
        granted = project.run_sql(
            f"select count() from clusterAllReplicas('{cluster}', system.grants) "
            f"where database = '{relation.schema}' and table = '{relation.identifier}' "
            f"and user_name = '{get_test_users[0]}' and access_type = 'SELECT'",
            fetch="one",
        )[0]
        assert nodes > 1
        assert granted == nodes
