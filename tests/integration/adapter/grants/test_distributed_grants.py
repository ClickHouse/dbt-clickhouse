import os

import pytest
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.util import get_manifest, run_dbt_and_capture, write_file

distributed_table_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      materialized: distributed_table
      grants:
        select: ["{{ env_var('DBT_TEST_USER_1') }}"]
        insert: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""


class TestDistributedTableModelGrants(BaseModelGrants):
    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_view_table_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        insert_privilege_name = self.privilege_grantee_name_overrides()["insert"]
        assert len(test_users) == 3
        # Distributed Table materialization, single select grant
        updated_yaml = self.interpolate_name_overrides(distributed_table_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "distributed_table"
        expected = {select_privilege_name: [test_users[0]], insert_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_model", expected)

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        super().assert_expected_grants_match_actual(project, relation_name, expected_grants)

        # also needs grants for local table
        actual_local_grants = self.get_grants_on_relation(project, relation_name + "_local")
        from dbt.context.base import BaseContext

        diff_a_local = BaseContext.diff_of_two_dicts(actual_local_grants, expected_grants)
        diff_b_local = BaseContext.diff_of_two_dicts(expected_grants, actual_local_grants)
        assert diff_a_local == diff_b_local == {}
