import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps


class TestCurrentTimestamps(BaseCurrentTimestamps):

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "DateTime",
            "current_timestamp_in_utc_backcompat": "DateTime",
            "current_timestamp_backcompat": "DateTime",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return None

