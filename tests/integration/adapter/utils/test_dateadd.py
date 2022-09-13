import pytest

from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd

# We remove the null row from this test because (1) nullables work fine with Nullable ClickHouse types, but
# (2) dealing with all the type conversions is ugly
seeds__data_dateadd_csv = """from_time,interval_length,datepart,result
2018-01-01 01:00:00,1,day,2018-01-02 01:00:00
2018-01-01 01:00:00,1,month,2018-02-01 01:00:00
2018-01-01 01:00:00,1,year,2019-01-01 01:00:00
2018-01-01 01:00:00,1,hour,2018-01-01 02:00:00
"""


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}
