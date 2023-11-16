import pytest
from dbt.tests.util import get_manifest, run_dbt, run_dbt_and_capture

from tests.integration.adapter.constraints.contract_fixtures import (
    model_schema_yml,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)


class ClickHouseConstraintsColumnsEqual:
    """
    dbt should catch these mismatches during its "preflight" checks.
    """

    def __test__constraints_wrong_column_order(self, project):
        # This no longer causes an error, since we enforce yaml column order
        run_dbt(["run", "-s", "my_model_wrong_order"], expect_pass=True)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

    def test__constraints_wrong_column_names(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "my_model_wrong_name"], expect_pass=False)
        run_dbt(["run", "-s", "my_model_wrong_name"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected = ["id", "error", "missing in definition", "missing in contract"]
        assert all([(exp in log_output or exp.upper() in log_output) for exp in expected])


class TestTableConstraintsColumnsEqual(ClickHouseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }


# class TestViewConstraintsColumnsEqual(BaseViewConstraintsColumnsEqual):
#     pass
#
#
# class TestIncrementalConstraintsColumnsEqual(BaseIncrementalConstraintsColumnsEqual):
#     pass
#
#
# class TestTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
#     pass
#
#
# class TestTableConstraintsRollback(BaseConstraintsRollback):
#     pass
#
#
# class TestIncrementalConstraintsRuntimeDdlEnforcement(
#     BaseIncrementalConstraintsRuntimeDdlEnforcement
# ):
#     pass
