import pytest
from datetime import datetime, date, timezone
from uuid import UUID
from dbt.tests.util import run_dbt


# Model that tests safe_cast with various ClickHouse types

safe_cast_model_sql = """
select
  -- String types
  {{ safe_cast("null", "String") }} as string_default,
  {{ safe_cast("null", "FixedString(10)") }} as fixedstring_default,
  
  -- Integer types
  {{ safe_cast("null", "Int32") }} as int_default,
  {{ safe_cast("null", "UInt32") }} as uint_default,
  
  -- Floating point types
  {{ safe_cast("null", "Float32") }} as float_default,
  {{ safe_cast("null", "Decimal(10, 2)") }} as decimal_default,
  
  -- Date/Time types
  {{ safe_cast("null", "Date") }} as date_default,
  {{ safe_cast("null", "DateTime") }} as datetime_default,
  {{ safe_cast("null", "DateTime64(3)") }} as datetime64_default,
  {{ safe_cast("null", "DateTime('Europe/Paris')") }} as datetime_tz_default,
  
  -- Other types
  {{ safe_cast("null", "UUID") }} as uuid_default,
  {{ safe_cast("null", "Bool") }} as bool_default,
  
  -- Complex types
  {{ safe_cast("null", "Array(String)") }} as array_default,
  {{ safe_cast("null", "Map(String, Int32)") }} as map_default,
  {{ safe_cast("null", "Tuple(String, Int32)") }} as tuple_default,
  
  -- Nullable
  {{ safe_cast("null", "Nullable(String)") }} as nullable_default,
  
  -- Provided values (non-null)
  {{ safe_cast("'Alice'", "String") }} as provided_string,
  {{ safe_cast("42", "Int32") }} as provided_int,
  {{ safe_cast("toUUID('00000000-0000-0000-0000-000000000001')", "UUID") }} as provided_uuid
"""


class TestSafeCast:
    """Test ClickHouse-specific safe_cast functionality"""
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "safe_cast_test.sql": safe_cast_model_sql,
        }
    
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        """Run the model once for all tests in this class"""
        results = run_dbt(["run", "--select", "safe_cast_test"])
        assert len(results) == 1
        yield

    def test_safe_cast_defaults(self, project):
        """Test that safe_cast generates correct default values for ClickHouse types"""
        
        # Query the results
        result = project.run_sql(
            "select * from safe_cast_test",
            fetch="one"
        )
        
        # String types
        assert result[0] == ''  # String default
        # FixedString(10) default: some drivers return bytes of nulls, others empty string
        if isinstance(result[1], (bytes, bytearray)):
            assert result[1] == b'\x00' * 10
        else:
            # In some environments, trailing nulls are stripped and returned as empty string
            assert result[1] in ('', '\x00' * 10)
        
        # Integer types
        assert result[2] == 0   # Int32 default
        assert result[3] == 0   # UInt32 default
        
        # Floating point types
        assert result[4] == 0.0  # Float32 default
        assert result[5] == 0.0  # Decimal default
        
        # Date/Time types
        assert result[6] == date(1970, 1, 1)  # Date default
        assert result[7] == datetime(1970, 1, 1, 0, 0, 0)  # DateTime default
        assert result[8] == datetime(1970, 1, 1, 0, 0, 0)  # DateTime64 default
        # For timezone-aware DateTime, compare in UTC to avoid local TZ shifts
        assert result[9].astimezone(timezone.utc) == datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)  # DateTime with timezone default
        
        # Other types
        assert result[10] == UUID('00000000-0000-0000-0000-000000000000')  # UUID default
        assert result[11] is False  # Bool default
        
        # Complex types
        assert result[12] == []  # Array default
        assert result[13] == {}  # Map default
        assert result[14] == ('', 0)  # Tuple default
        
        # Nullable
        assert result[15] is None  # Nullable default
        
        # Provided values (should be kept as-is)
        assert result[16] == 'Alice'  # Provided string
        assert result[17] == 42       # Provided int
        assert result[18] == UUID('00000000-0000-0000-0000-000000000001')  # Provided UUID
    
    def test_safe_cast_types(self, project):
        """Test that safe_cast preserves the expected data types"""
        # Get column types from ClickHouse
        columns = project.run_sql(
            "SELECT name, type FROM system.columns WHERE table = 'safe_cast_test' AND database = currentDatabase() ORDER BY name",
            fetch="all"
        )
        
        # Create a dict for easier lookup
        column_types = {col[0]: col[1] for col in columns}
        
        # Verify each column has the expected type
        # String types
        assert column_types['string_default'] == 'String'
        assert column_types['fixedstring_default'] == 'FixedString(10)'
        
        # Integer types
        assert column_types['int_default'] == 'Int32'
        assert column_types['uint_default'] == 'UInt32'
        
        # Floating point types
        assert column_types['float_default'] == 'Float32'
        assert column_types['decimal_default'] == 'Decimal(10, 2)'
        
        # Date/Time types
        assert column_types['date_default'] == 'Date'
        assert column_types['datetime_default'] == 'DateTime'
        assert column_types['datetime64_default'] == 'DateTime64(3)'
        assert column_types['datetime_tz_default'] == "DateTime('Europe/Paris')"
        
        # Other types
        assert column_types['uuid_default'] == 'UUID'
        assert column_types['bool_default'] == 'Bool'
        
        # Complex types
        assert column_types['array_default'] == 'Array(String)'
        assert column_types['map_default'] == 'Map(String, Int32)'
        assert column_types['tuple_default'] == 'Tuple(String, Int32)'
        
        # Nullable
        assert column_types['nullable_default'] == 'Nullable(String)'
        
        # Provided values
        assert column_types['provided_string'] == 'String'
        assert column_types['provided_int'] == 'Int32'
        assert column_types['provided_uuid'] == 'UUID'
        