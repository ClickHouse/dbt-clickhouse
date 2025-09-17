import pytest
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            [
                "TIMESTAMP '2013-11-03 00:00:00'",
                "2013-11-03 00:00:00",
            ],  # Overridden: no "-0" support
            [
                "'2023-10-29 01:30:00'::DateTime('UTC')",
                "2013-11-03 00:00:00",
            ],  # Overridden: no "-0" support and timezones expressed differently
            ["'1'::numeric", "1"],
            # Not fully supported types:
            # [
            # """'{"bar": "baz", "balance": 7.77, "active": false}'::json""",
            #     """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            # ],
            # TODO: support complex types <-- this TODO came from the DBT test.
            # ["ARRAY['a','b','c']", """'{"a", "b", "c"}'"""],
            # ["ARRAY[1,2,3]", """'{1, 2, 3}'"""],
        ]
