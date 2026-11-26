import pytest
from dbt.tests.adapter.utils.fixture_date_spine import models__test_date_spine_yml
from dbt.tests.adapter.utils.fixture_get_intervals_between import (
    models__test_get_intervals_between_yml,
)
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween

# Same bounds as the upstream fixture. The spine of a string literal is a DateTime64, which
# ClickHouse does not compare with the fixture's String column, so both sides are cast to
# Date, as the upstream fixture does for BigQuery and Redshift.
models__test_date_spine_sql = """
with generated_dates as (
    select toDate(date_day) as date_day
    from ({{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }})
), expected_dates as (
    select toDate('2023-09-01') + number as expected
    from numbers(9)
), joined as (
    select generated_dates.date_day, expected_dates.expected
    from generated_dates
    left join expected_dates on generated_dates.date_day = expected_dates.expected
)
select * from joined
"""


class TestDateSpine(BaseDateSpine):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }


# The upstream fixture writes its bounds as '09/01/2023' and '09/12/2023'. ClickHouse reads
# a slash date as DD/MM/YYYY, so the same days are written in ISO format here.
models__test_get_intervals_between_sql = """
SELECT
  {{ get_intervals_between("'2023-09-01'", "'2023-09-12'", "day") }} as intervals,
  11 as expected
"""


class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
            ),
        }
