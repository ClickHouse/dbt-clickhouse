import pytest
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.util import run_dbt

models__dates_out_of_date_range_sql = """
select
    {{ dbt.date(1960, 1, 1) }} as before_1970,
    {{ dbt.date(2200, 12, 31) }} as after_2149
"""


class TestDate(BaseDate):
    pass


class TestDateOutsideDateRange:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dates_out_of_date_range.sql": models__dates_out_of_date_range_sql}

    def test_dates_are_not_clamped(self, project):
        run_dbt(["run"])
        result = project.run_sql(
            f"select toString(before_1970), toString(after_2149) "
            f"from {project.test_schema}.dates_out_of_date_range",
            fetch="one",
        )
        assert result == ('1960-01-01', '2200-12-31')
