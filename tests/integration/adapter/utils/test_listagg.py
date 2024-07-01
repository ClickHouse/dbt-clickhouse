import pytest
from dbt.tests.adapter.utils.fixture_listagg import (
    models__test_listagg_yml,
    seeds__data_listagg_csv,
)
from dbt.tests.util import run_dbt

models__test_listagg_sql = """
  select
  group_col,
  {{listagg('string_text', "'_|_'", "order_col")}} as actual,
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

    def test_listagg_run(self, project):
        run_dbt(["seed"])
        run_dbt()
        results = project.run_sql("select * from test_listagg", fetch="all")
        assert len(results) == 3
        assert results[0] == (3, 'g_|_g_|_g', 'bottom_ordered')
        assert results[1] == (2, '1_|_a_|_p', 'bottom_ordered')
        assert results[2] == (1, 'a_|_b_|_c', 'bottom_ordered')
