import pytest
from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff

# Not testing null values (see discussion in test_dateadd.py
seeds__data_datediff_csv = """first_date,second_date,datepart,result
2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
2018-01-01 01:00:00,2018-01-01 02:00:00,hour,1
2018-01-01 01:00:00,2018-01-01 02:01:00,minute,61
2018-01-01 01:00:00,2018-01-01 02:00:01,second,3601
2019-12-31 00:00:00,2019-12-27 00:00:00,week,-1
2019-12-31 00:00:00,2019-12-30 00:00:00,week,0
2019-12-31 00:00:00,2020-01-02 00:00:00,week,0
2019-12-31 00:00:00,2020-01-06 02:00:00,week,1
"""

# Casting strings to dates in ClickHouse requires either a specific format or the use of a function, so skipping
# that for now (also micro and milliseconds are not supported)
models__test_datediff_sql = """
with data as (
    select * from {{ ref('data_datediff') }}
)
select
    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected
from data
"""


class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }
