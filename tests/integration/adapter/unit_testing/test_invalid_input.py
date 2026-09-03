import os

from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.util import run_dbt, run_dbt_and_capture

IS_CORE_V2 = bool(os.environ.get('DBT_CH_TEST_CORE_V2_BINARY'))


class TestInvalidInput(BaseUnitTestInvalidInput):
    def test_invalid_input(self, project):
        if not IS_CORE_V2:
            super().test_invalid_input(project)
            return

        # dbt core v2 words this error differently (plural, row suffix, raw
        # ref() call as fixture name); assert its pieces, not dbt-core's sentence.
        results = run_dbt(["run"])
        assert len(results) == 2

        # The accepted-columns tail is load-bearing: a broken schema probe yields
        # "Accepted columns ... are: []" and a bare name assert passes vacuously.
        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_input_column_name"], expect_pass=False
        )
        assert "Invalid column name(s): 'invalid_column_name'" in out
        assert (
            'Accepted columns for \'ref(\'my_upstream_model\')\' are: ["tested_column"]' in out
        )

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_invalid_expect_column_name"], expect_pass=False
        )
        assert "Invalid column name(s): 'invalid_column_name'" in out
        assert 'Accepted columns for expected output are: ["tested_column"]' in out
