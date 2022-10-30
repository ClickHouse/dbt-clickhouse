import pytest
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp

models__bigint_expected_sql = """
select -9223372036854775800 as bigint_col
"""

models__bigint_actual_sql = """
select cast('-9223372036854775800' as {{ type_bigint() }}) as bigint_col
"""


class TestTypeBigInt(BaseDataTypeMacro):
    # Using negative numbers instead since BIGINT on ClickHouse is signed, but the SELECT without a sign
    # will be automatically cast to a UInt64
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "expected.sql": models__bigint_expected_sql,
            "actual.sql": self.interpolate_macro_namespace(
                models__bigint_actual_sql, "type_bigint"
            ),
        }


class TestTypeBoolean(BaseTypeBoolean):
    pass


class TestTypeFloat(BaseTypeFloat):
    pass


class TestTypeInt(BaseTypeInt):
    pass


class TestTypeNumeric(BaseTypeNumeric):
    pass


class TestTypeString(BaseTypeString):
    pass


class TestTypeTimestamp(BaseTypeTimestamp):
    pass
