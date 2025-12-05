"""
Test that unit tests work correctly when column values are omitted from input rows.
The safe_cast macro should provide default values instead of NULL for missing columns.
"""

import pytest
from dbt.tests.util import run_dbt

# First model: a table with non-nullable columns
my_first_dbt_model_sql = """
select 1 as id, 'a' AS foo
union all
select 2 as id, 'b' AS foo
"""

# Second model: filters the first model
my_second_dbt_model_sql = """
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
"""

# Unit test with missing column values (foo is omitted from input rows)
test_my_model_yml = """
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        data_type: uint64
      - name: foo
        data_type: string
  - name: my_second_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        data_type: uint64
      - name: foo
        data_type: string
unit_tests:
  - name: test_not_null
    model: my_second_dbt_model
    given:
      - input: ref('my_first_dbt_model')
        rows:
          - {id: 1}
          - {id: 2}
    expect:
      rows:
        - {id: 1}
"""


class TestMissingColumnValues:
    """
    Test that unit tests handle missing column values correctly.
    The safe_cast macro should provide appropriate default values instead.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_dbt_model.sql": my_first_dbt_model_sql,
            "my_second_dbt_model.sql": my_second_dbt_model_sql,
            "unit_tests.yml": test_my_model_yml,
        }

    def test_missing_column_values(self, project):
        """
        Test that unit tests work when column values are omitted from input rows.

        This test should pass without errors, demonstrating that the safe_cast macro
        correctly handles NULL values by providing appropriate defaults for ClickHouse
        non-nullable types.
        """
        # Run the models
        results = run_dbt(["run"])
        assert len(results) == 2

        # Run the unit test - this should pass without ClickHouse type conversion errors
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
