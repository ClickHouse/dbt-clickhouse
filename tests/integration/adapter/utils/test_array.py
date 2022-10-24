import pytest
from dbt.tests.adapter.utils.base_array_utils import BaseArrayUtils
from dbt.tests.adapter.utils.fixture_array_append import models__array_append_actual_sql
from dbt.tests.adapter.utils.fixture_array_concat import models__array_concat_actual_sql

# Empty arrays are constructed with the DBT default "integer" which is an Int32.  Because ClickHouse will coerce
# the arrays to the smallest possible type, we need to ensure that at least one of the members requires an Int32
models__array_append_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,-77777777]) }} as array_col union all
select 2 as id, {{ array_construct([4]) }} as array_col
"""


class TestArrayAppend(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_append_actual_sql,
            "expected.sql": models__array_append_expected_sql,
        }


models__array_concat_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4,5,-77777777]) }} as array_col union all
select 2 as id, {{ array_construct([2]) }} as array_col union all
select 3 as id, {{ array_construct([3]) }} as array_col
"""


class TestArrayConcat(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }
