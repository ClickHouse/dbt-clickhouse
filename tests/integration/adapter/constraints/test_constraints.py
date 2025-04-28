import pytest
from dbt.tests.util import get_manifest, run_dbt, run_dbt_and_capture, write_file

from tests.integration.adapter.constraints.fixtures_constraints import (
    bad_column_constraint_model_sql,
    bad_foreign_key_model_sql,
    check_constraints_model_fail_sql,
    check_constraints_model_sql,
    check_custom_constraints_model_sql,
    constraint_model_schema_yml,
    contract_model_schema_yml,
    custom_constraint_model_schema_yml,
    model_data_type_schema_yml,
    my_model_data_type_sql,
    my_model_incremental_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_view_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)


class ClickHouseContractColumnsEqual:
    """
    dbt should catch these mismatches during its "preflight" checks.
    """

    @pytest.fixture
    def data_types(self):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1::Int32", "Int32", "Int32"],
            ["'1'", "String", "String"],
            ["true", "Bool", "Bool"],
            ["'2013-11-03'::DateTime", "DateTime", "DateTime"],
            ["['a','b','c']", "Array(String)", "Array(String)"],
            ["[1::Int32,2::Int32,3::Int32]", "Array(Int32)", "Array(Int32)"],
            ["'1'::Float64", "Float64", "Float64"],
        ]

    def test__contract_wrong_column_order(self, project):
        # This no longer causes an error, since we enforce yaml column order
        run_dbt(["run", "-s", "my_model_wrong_order"], expect_pass=True)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

    def test__contract_wrong_column_names(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "my_model_wrong_name"], expect_pass=False)
        run_dbt(["run", "-s", "my_model_wrong_name"], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is True

        expected = ["id", "error", "missing in definition", "missing in contract"]
        assert all([(exp in log_output or exp.upper() in log_output) for exp in expected])

    def test__contract_wrong_column_data_types(self, project, data_types):
        for sql_column_value, schema_data_type, error_data_type in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )
            write_file(
                model_data_type_schema_yml.format(data_type='Int128'),
                "models",
                "contract_schema.yml",
            )

            results, log_output = run_dbt_and_capture(
                ["run", "-s", "my_model_data_type"], expect_pass=False
            )
            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config.enforced is True
            expected = [
                "wrong_data_type_column_name",
                error_data_type,
                "Int128",
                "data type mismatch",
            ]
            assert all([(exp in log_output or exp.upper() in log_output) for exp in expected])

    def test__contract_correct_column_data_types(self, project, data_types):
        for sql_column_value, schema_data_type, _ in data_types:
            # Write parametrized data_type to sql file
            write_file(
                my_model_data_type_sql.format(sql_value=sql_column_value),
                "models",
                "my_model_data_type.sql",
            )
            # Write correct data_type to corresponding schema file
            write_file(
                model_data_type_schema_yml.format(data_type=schema_data_type),
                "models",
                "contract_schema.yml",
            )

            run_dbt(["run", "-s", "my_model_data_type"])

            manifest = get_manifest(project.project_root)
            model_id = "model.test.my_model_data_type"
            my_model_config = manifest.nodes[model_id].config
            contract_actual_config = my_model_config.contract

            assert contract_actual_config.enforced is True


class TestTableContractColumnsEqual(ClickHouseContractColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "contract_schema.yml": contract_model_schema_yml,
        }


class TestViewContractColumnsEqual(ClickHouseContractColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "contract_schema.yml": contract_model_schema_yml,
        }


class TestIncrementalContractColumnsEqual(ClickHouseContractColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "contract_schema.yml": contract_model_schema_yml,
        }


class TestBadConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_column_constraint_model.sql": bad_column_constraint_model_sql,
            "bad_foreign_key_model.sql": bad_foreign_key_model_sql,
            "constraints_schema.yml": constraint_model_schema_yml,
        }

    def test_invalid_column_constraint(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "bad_column_constraint_model"])
        assert "not supported" in log_output

    def test_invalid_fk_constraint(self, project):
        _, log_output = run_dbt_and_capture(["run", "-s", "bad_foreign_key_model"])
        assert "not supported" in log_output


class TestModelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_constraints_model.sql": check_constraints_model_sql,
            "constraints_schema.yml": constraint_model_schema_yml,
        }

    def test_model_constraints_ddl(self, project):
        run_dbt(["run", "-s", "check_constraints_model"])


class TestModelConstraintApplied:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_constraints_model.sql": check_constraints_model_fail_sql,
            "constraints_schema.yml": constraint_model_schema_yml,
        }

    def test_model_constraints_fail_ddl(self, project):
        _, log_output = run_dbt_and_capture(
            ["run", "-s", "check_constraints_model"], expect_pass=False
        )
        assert 'violated' in log_output.lower()


class TestModelCustomConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_custom_constraints_model.sql": check_custom_constraints_model_sql,
            "constraints_schema.yml": custom_constraint_model_schema_yml,
        }

    def test_model_constraints_ddl(self, project):
        run_dbt(["run", "-s", "check_custom_constraints_model"])
