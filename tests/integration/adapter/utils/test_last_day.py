import pytest
from dbt.tests.adapter.utils.test_last_day import BaseLastDay

from tests.integration.adapter.helpers import MigratedTestArgs


class TestLastDay(MigratedTestArgs, BaseLastDay):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "seeds": {
                "test": {
                    "data_last_day": {
                        "+column_types": {
                            "date_day": "Nullable(Date)",
                            "result": "Nullable(Date)",
                        },
                    },
                },
            },
        }
