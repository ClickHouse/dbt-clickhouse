"""
test UDF creation support for dbt-clickhouse
"""

import pytest

import dbt.tests.adapter.functions.files as files
from dbt.tests.adapter.functions.test_udfs import UDFsBasic

# we'll import helper used by the core test project to write directories

MY_UDF_SQL = """
price * 2
""".strip()


class TestUDFBasics(UDFsBasic):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }
