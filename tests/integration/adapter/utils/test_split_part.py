import pytest
from dbt.tests.adapter.utils.fixture_split_part import models__test_split_part_yml
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart

models__test_split_part_sql = """
with data as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', "'|'", 1) }} as actual,
    result_1 as expected

from data

union all

select
    {{ split_part('parts', "'|'", 2) }} as actual,
    result_2 as expected

from data

union all

select
    {{ split_part('parts', "'|'", 3) }} as actual,
    result_3 as expected

from data
"""


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": models__test_split_part_sql,
        }
