import pytest
from dbt.exceptions import CompilationError
from dbt.tests.adapter.utils.fixture_listagg import (
    models__test_listagg_yml,
    seeds__data_listagg_csv,
)
from dbt.tests.util import run_dbt

models__test_listagg_sql = """
  select
  group_col,
  {{listagg('string_text', "'_|_'", "order by order_col")}} as actual,
  'bottom_ordered' as version
from {{ ref('data_listagg') }} group by group_col
"""


class TestListagg:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yaml": models__test_listagg_yml,
            "test_listagg.sql": models__test_listagg_sql,
        }

    def test_listagg_exception(self, project):
        try:
            run_dbt(["build"], False)
        except CompilationError as e:
            assert 'does not support' in e.msg
