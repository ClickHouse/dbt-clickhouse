import csv
import io
import subprocess
import tempfile
import os
import json
import sys
import pandas as pd
from dataclasses import dataclass
from dbt.adapters.clickhouse.python_executor import ClickHousePythonExecutor
from dbt. adapters.capability import Capability
from multiprocessing.context import SpawnContext
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from dbt.adapters.base import AdapterConfig, available
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.base.impl import BaseAdapter, ConstraintSupport
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.clickhouse.cache import ClickHouseRelationsCache
from dbt.adapters.clickhouse.column import ClickHouseColumn, ClickHouseColumnChanges
from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager
from dbt.adapters.clickhouse.errors import (
    schema_change_datatype_error,
    schema_change_fail_error,
    schema_change_missing_source_error,
)
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.query import quote_identifier
from dbt.adapters.clickhouse.relation import ClickHouseRelation, ClickHouseRelationType
from dbt.adapters.clickhouse.util import compare_versions, engine_can_atomic_exchange
from dbt.adapters.contracts.relation import Path, RelationConfig
from dbt.adapters.events.types import ConstraintNotSupported
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType, ModelLevelConstraint
from dbt_common.events.functions import warn_or_error
from dbt_common.exceptions import DbtInternalError, DbtRuntimeError, NotImplementedError
from dbt_common.utils import filter_null_values

if TYPE_CHECKING:
    import agate

GET_CATALOG_MACRO_NAME = 'get_catalog'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'

MERGETREE_EXCLUSIVE_SETTINGS = {'replicated_deduplication_window'}


@dataclass
class ClickHouseConfig(AdapterConfig):
    engine: str = 'MergeTree()'
    force_on_cluster: Optional[bool] = False
    order_by: Optional[Union[List[str], str]] = 'tuple()'
    partition_by: Optional[Union[List[str], str]] = None
    sharding_key: Optional[Union[List[str], str]] = 'rand()'
    ttl: Optional[Union[List[str], str]] = None


class ClickHouseAdapter(SQLAdapter):
    Relation = ClickHouseRelation
    Column = ClickHouseColumn
    ConnectionManager = ClickHouseConnectionManager
    AdapterSpecificConfigs = ClickHouseConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Unsupported),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Unsupported),
        }
    )

    def __init__(self, config, mp_context: SpawnContext):
        BaseAdapter.__init__(self, config, mp_context)
        self.cache = ClickHouseRelationsCache()
        self.python_executor = ClickHousePythonExecutor(self.connections)

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'String'

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        # We match these type to the Column.TYPE_LABELS for consistency
        return 'Float32' if decimals else 'Int32'

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Bool'

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'DateTime'

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return 'Date'

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        raise NotImplementedError('`convert_time_type` is not implemented for this adapter!')
    
    from dbt.adapters.contracts.connection import AdapterResponse

    # In the ClickHouseAdapter class, add these methods: 
    @available
    def submit_python_job(self, model: dict, compiled_code: str, is_incremental: bool = False, target_relation = None) -> AdapterResponse:
        """Execute Python model and materialize to ClickHouse"""
        
        import pandas as pd
        
        # Extract model info
        schema = model.get('schema')
        identifier = model.get('alias')
        unique_key = model.get('config', {}).get('unique_key')
        incremental_strategy = model.get('config', {}).get('incremental_strategy', 'append')
        
        
        # Execute the compiled Python code in a namespace
        namespace = {}
        exec(compiled_code, namespace)
        
        # The model function should be in the namespace
        if 'model' in namespace:
            # Create dbt object with incremental support
            class DbtRef:
                def __init__(self, adapter, return_type='pandas', is_incremental=False, target_relation=None):
                    self.adapter = adapter
                    self.return_type = return_type
                    self._is_incremental = is_incremental
                    self.target_relation = target_relation
                
                def is_incremental(self):
                    """Check if this is an incremental run"""
                    return self._is_incremental
                
                def this(self):
                    """Get reference to the current target table (for incremental)"""
                    if not self._is_incremental or not self.target_relation:
                        if self.return_type == 'polars':
                            import polars as pl
                            return pl.DataFrame()
                        return pd.DataFrame()
                    
                    # Query the existing table
                    connection = self.adapter.connections.get_thread_connection()
                    sql = f"SELECT * FROM {self.target_relation}"
                    
                    try:
                        result = connection.handle.query(sql).result_set
                        
                        if result:
                            columns = [col[0] for col in connection.handle.query(f"DESCRIBE TABLE {self.target_relation}").result_set]
                            df = pd.DataFrame(result, columns=columns)
                            
                            if self.return_type == 'polars':
                                import polars as pl
                                return pl.from_pandas(df)
                            return df
                    except:
                        pass
                    
                    if self.return_type == 'polars':
                        import polars as pl
                        return pl.DataFrame()
                    return pd.DataFrame()
                
                def ref(self, model_name, limit=None, where=None, start_date=None, end_date=None, date_column=None, order_by=None):
                    """Reference another dbt model and return its data as DataFrame"""
                    connection = self.adapter.connections.get_thread_connection()
                    
                    # ✅ Helper function to format dates for SQL
                    def format_date_for_sql(date_value):
                        if date_value is None:
                            return None
                        
                        # If it's already a string, return it
                        if isinstance(date_value, str):
                            return date_value
                        
                        # If it has strftime method (Python datetime)
                        if hasattr(date_value, 'strftime'):
                            return date_value.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # If it has isoformat (Pandas/Polars datetime)
                        if hasattr(date_value, 'isoformat'):
                            return date_value.isoformat().replace('T', ' ').split('+')[0].split('.')[0]
                        
                        # Fallback: convert to string and clean
                        date_str = str(date_value)
                        # Remove timezone info and microseconds
                        date_str = date_str.split('+')[0].split('.')[0]
                        # Replace T with space if present
                        date_str = date_str.replace('T', ' ')
                        return date_str
                    
                    # Build SQL query
                    sql_parts = [f"SELECT * FROM {model_name}"]
                    
                    # Build WHERE clause
                    where_conditions = []
                    
                    if where:
                        where_conditions.append(f"({where})")
                    
                    if start_date and date_column:
                        # ✅ Format the date properly
                        formatted_start = format_date_for_sql(start_date)
                        where_conditions.append(f"{date_column} >= '{formatted_start}'")
                    
                    if end_date and date_column:
                        # ✅ Format the date properly
                        formatted_end = format_date_for_sql(end_date)
                        where_conditions.append(f"{date_column} <= '{formatted_end}'")
                    
                    if where_conditions:
                        sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
                    
                    # Add ORDER BY
                    if order_by:
                        sql_parts.append(f"ORDER BY {order_by}")
                    
                    # Add LIMIT
                    if limit:
                        sql_parts.append(f"LIMIT {limit}")
                    
                    sql = " ".join(sql_parts)
                    
                    print(f"🔍 Executing SQL: {sql}")
                    
                    result = connection.handle.query(sql).result_set
                    
                    print(f"📊 Query returned {len(result) if result else 0} rows")
                    
                    if result:
                        columns = [col[0] for col in connection.handle.query(f"DESCRIBE TABLE {model_name}").result_set]
                        df = pd.DataFrame(result, columns=columns)
                        
                        if self.return_type == 'polars':
                            import polars as pl
                            return pl.from_pandas(df)
                        return df
                    
                    # Return empty DataFrame WITH columns when no results
                    try:
                        columns = [col[0] for col in connection.handle.query(f"DESCRIBE TABLE {model_name}").result_set]
                        if self.return_type == 'polars':
                            import polars as pl
                            return pl.DataFrame({col: [] for col in columns})
                        return pd.DataFrame(columns=columns)
                    except:
                        if self.return_type == 'polars':
                            import polars as pl
                            return pl.DataFrame()
                        return pd.DataFrame()
                
                def source(self, source_name, table_name, limit=None, where=None, start_date=None, end_date=None, date_column=None, order_by=None):
                    """Reference a source table with optional filters"""
                    connection = self.adapter.connections.get_thread_connection()
                    
                    # Build SQL query
                    sql_parts = [f"SELECT * FROM {source_name}.{table_name}"]
                    
                    # Build WHERE clause
                    where_conditions = []
                    
                    if where:
                        where_conditions.append(f"({where})")
                    
                    if start_date and date_column:
                        where_conditions.append(f"{date_column} >= '{start_date}'")
                    
                    if end_date and date_column:
                        where_conditions.append(f"{date_column} <= '{end_date}'")
                    
                    if where_conditions:
                        sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
                    
                    # Add ORDER BY
                    if order_by:
                        sql_parts.append(f"ORDER BY {order_by}")
                    
                    # Add LIMIT
                    if limit:
                        sql_parts.append(f"LIMIT {limit}")
                    
                    sql = " ".join(sql_parts)
                    
                    result = connection.handle.query(sql).result_set
                    
                    if result:
                        columns = [col[0] for col in connection.handle.query(f"DESCRIBE TABLE {source_name}.{table_name}").result_set]
                        df = pd.DataFrame(result, columns=columns)
                        
                        if self.return_type == 'polars':
                            import polars as pl
                            return pl.from_pandas(df)
                        return df
                    
                    if self.return_type == 'polars':
                        import polars as pl
                        return pl.DataFrame()
                    return pd.DataFrame()
                
                def config(self, **kwargs):
                    pass
            
            class MockSession: 
                pass
            
            # Detect if model uses polars
            return_type = 'polars' if 'import polars' in compiled_code or 'from polars' in compiled_code else 'pandas'
            
            # Call the model function with incremental support
            dbt = DbtRef(self, return_type=return_type, is_incremental=is_incremental, target_relation=target_relation)
            session = MockSession()
            df = namespace['model'](dbt, session)
            
            # Convert to pandas if needed
            if hasattr(df, '__class__') and 'polars' in str(type(df).__module__):
                df_pandas = df.to_pandas()
            else:
                df_pandas = df
            
            # Handle incremental vs full refresh
            if is_incremental and target_relation:
                # Incremental run - append or merge
                if incremental_strategy == 'append':
                    self._append_to_table(df_pandas, schema, identifier)
                    rows_affected = len(df_pandas)
                    message = f"Appended {rows_affected} rows"
                    
                elif incremental_strategy == 'delete+insert' and unique_key:
                    rows_affected = self._merge_table(df_pandas, schema, identifier, unique_key)
                    message = f"Merged {rows_affected} rows (delete+insert)"
                    
                else:
                    self._append_to_table(df_pandas, schema, identifier)
                    rows_affected = len(df_pandas)
                    message = f"Appended {rows_affected} rows (default)"
            else:
                # Full refresh - drop and recreate table
                self._materialize_python_result(df, schema, identifier, drop_if_exists=True)
                rows_affected = len(df_pandas) if df_pandas is not None else 0
                message = f"Created table with {rows_affected} rows"
            
            # Return response
            return AdapterResponse(
                _message=message,
                code="success",
                rows_affected=rows_affected
            )
        else:
            raise Exception("No 'model' function found in Python code")
    
    def _materialize_python_result(self, df, schema: str, table_name: str, drop_if_exists: bool = True) -> None:
        """Materialize Python result to ClickHouse table - supports both pandas and polars"""
        
        import pandas as pd
        from datetime import datetime, date
        
        # Check if it's a Polars DataFrame and convert to Pandas
        if hasattr(df, '__class__') and 'polars' in str(type(df).__module__):
            df = df.to_pandas()
        
        if df is None or df.empty:
            create_sql = f'''
            CREATE TABLE IF NOT EXISTS `{schema}`.`{table_name}` (
                dummy String
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            '''
            self.execute(create_sql, auto_begin=False, fetch=False)
            return
        
        # ✅ Drop table if requested
        if drop_if_exists:
            drop_sql = f"DROP TABLE IF EXISTS `{schema}`.`{table_name}`"
            self.execute(drop_sql, auto_begin=False, fetch=False)
        
        # Create table with proper columns
        columns = []
        for col, dtype in zip(df.columns, df.dtypes):
            ch_type = self._pandas_to_ch_type(dtype)
            columns.append(f"`{col}` {ch_type}")
        
        # ✅ Use appropriate CREATE statement
        create_keyword = "CREATE TABLE IF NOT EXISTS" if not drop_if_exists else "CREATE TABLE"
        create_sql = f'''
        {create_keyword} `{schema}`.`{table_name}` (
            {", ".join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        '''
        
        self.execute(create_sql, auto_begin=False, fetch=False)
        
        # Insert data
        connection = self.connections.get_thread_connection()
        client = connection.handle
        
        # Batch insert
        batch_size = 500
        total_rows = len(df)
        
        print(f"📊 Inserting {total_rows} rows in batches of {batch_size}")
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            values_list = []
            for _, row in batch_df.iterrows():
                values = []
                for v in row:
                    if pd.isna(v):
                        values.append('NULL')
                    elif isinstance(v, bool):
                        values.append('1' if v else '0')
                    elif isinstance(v, (datetime, pd.Timestamp)):
                        dt_str = v.strftime('%Y-%m-%d %H:%M:%S')
                        values.append(f"'{dt_str}'")
                    elif isinstance(v, date):
                        date_str = v.strftime('%Y-%m-%d')
                        values.append(f"'{date_str}'")
                    elif isinstance(v, str):
                        escaped = v.replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped}'")
                    elif isinstance(v, (int, float)):
                        values.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped}'")
                
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"INSERT INTO `{schema}`.`{table_name}` VALUES {', '.join(values_list)}"
            client.command(insert_sql)
            
            if (i + batch_size) % 5000 == 0:
                print(f"   Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
        
        print(f"✅ Successfully inserted all {total_rows} rows")

    def _batch_insert(self, client, schema: str, table_name: str, df):
        """Helper method for batch insert"""
        import pandas as pd
        
        # Insert in batches of 10000 rows
        batch_size = 10000
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            values_list = []
            for _, row in batch_df.iterrows():
                values = []
                for v in row:
                    if pd.isna(v):
                        values.append('NULL')
                    elif isinstance(v, str):
                        escaped = v.replace("'", "''")
                        values.append(f"'{escaped}'")
                    elif isinstance(v, bool):
                        values.append('1' if v else '0')
                    else:
                        values.append(str(v))
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"INSERT INTO `{schema}`.`{table_name}` VALUES {', '.join(values_list)}"
            client.command(insert_sql)
                
    @staticmethod
    def _pandas_to_ch_type(dtype):
        """Convert pandas dtype to ClickHouse type"""
        dtype_str = str(dtype).lower()
        
        if 'int64' in dtype_str:
            return 'Int64'
        elif 'int32' in dtype_str: 
            return 'Int32'
        elif 'int16' in dtype_str: 
            return 'Int16'
        elif 'int8' in dtype_str:
            return 'Int8'
        elif 'float64' in dtype_str:
            return 'Float64'
        elif 'float32' in dtype_str:
            return 'Float32'
        elif 'bool' in dtype_str:
            return 'UInt8'
        elif 'datetime' in dtype_str:
            return 'DateTime'
        elif 'date' in dtype_str: 
            return 'Date'
        else:
            return 'String'
    def _append_to_table(self, df, schema: str, table_name: str) -> None:
        """Append data to existing table"""
        import pandas as pd
        from datetime import datetime, date
        
        connection = self.connections.get_thread_connection()
        client = connection.handle
        
        # Small batch inserts
        batch_size = 500
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            values_list = []
            for _, row in batch_df.iterrows():
                values = []
                for v in row:
                    if pd.isna(v):
                        values.append('NULL')
                    elif isinstance(v, bool):
                        values.append('1' if v else '0')
                    elif isinstance(v, (datetime, pd.Timestamp)):
                        dt_str = v.strftime('%Y-%m-%d %H:%M:%S')
                        values.append(f"'{dt_str}'")
                    elif isinstance(v, date):
                        date_str = v.strftime('%Y-%m-%d')
                        values.append(f"'{date_str}'")
                    elif isinstance(v, str):
                        escaped = v.replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped}'")
                    elif isinstance(v, (int, float)):
                        values.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped}'")
                
                values_list.append(f"({', '.join(values)})")
            
            insert_sql = f"INSERT INTO `{schema}`.`{table_name}` VALUES {', '.join(values_list)}"
            client.command(insert_sql)

    def _merge_table(self, df, schema: str, table_name: str, unique_key: Union[str, List[str]]) -> int:
        """
        Delete+Insert merge strategy matching dbt-clickhouse SQL incremental logic
        
        Returns the NET row change (not just inserted rows)
        """
        import time
        
        connection = self.connections.get_thread_connection()
        client = connection.handle
        
        # ✅ Handle composite unique keys
        unique_key_list = [unique_key] if isinstance(unique_key, str) else unique_key
        
        # ✅ Verify all unique key columns exist
        missing_cols = [col for col in unique_key_list if col not in df.columns]
        if missing_cols:
            raise Exception(f"Unique key columns {missing_cols} not found in DataFrame. Available columns: {df.columns.tolist()}")
        
        print(f"\n{'='*60}")
        print(f"🔄 Delete+Insert Strategy (Python Model)")
        print(f"{'='*60}")
        print(f"Target table: {schema}.{table_name}")
        print(f"Unique key: {unique_key_list}")
        print(f"Rows to merge: {len(df)}")
        print(f"{'='*60}\n")
        
        # ============================================================================
        # STEP 0: Get row counts BEFORE merge
        # ============================================================================
        target_relation = f"`{schema}`.`{table_name}`"
        
        count_before_sql = f"SELECT COUNT(*) as cnt FROM {target_relation}"
        result = client.query(count_before_sql).result_set
        rows_before = result[0][0] if result else 0
        
        print(f"📊 Current state:")
        print(f"   Rows in target table: {rows_before:,}")
        print(f"   Rows to merge: {len(df):,}\n")
        
        # ============================================================================
        # STEP 1: Create temporary table with new data
        # ============================================================================
        temp_table = f"{table_name}__dbt_tmp_{int(time.time())}"
        temp_relation = f"`{schema}`.`{temp_table}`"
        
        print(f"📋 STEP 1: Creating temporary table")
        print(f"   Temp table: {temp_relation}")
        
        try:
            # Create temp table with same structure as target
            self._materialize_python_result(df, schema, temp_table, drop_if_exists=True)
            print(f"   ✅ Temp table created with {len(df):,} rows\n")
            
            # ============================================================================
            # STEP 1.5: Count how many rows will be deleted
            # ============================================================================
            unique_key_cols = ', '.join(f'`{k}`' for k in unique_key_list)
            
            if len(unique_key_list) == 1:
                count_to_delete_sql = f"""
                SELECT COUNT(*) as cnt
                FROM {target_relation}
                WHERE `{unique_key_list[0]}` IN (
                    SELECT `{unique_key_list[0]}` FROM {temp_relation}
                )
                """
            else:
                count_to_delete_sql = f"""
                SELECT COUNT(*) as cnt
                FROM {target_relation}
                WHERE ({unique_key_cols}) IN (
                    SELECT {unique_key_cols} FROM {temp_relation}
                )
                """
            
            result = client.query(count_to_delete_sql).result_set
            rows_to_delete = result[0][0] if result else 0
            
            print(f"📊 Rows that will be deleted: {rows_to_delete:,}")
            print(f"   (These are existing rows matching the unique key)\n")
            
            # ============================================================================
            # STEP 2: Delete rows from target that exist in temp table
            # ============================================================================
            print(f"🗑️  STEP 2: Deleting matching rows from target")
            
            if len(unique_key_list) == 1:
                delete_sql = f"""
                ALTER TABLE {target_relation} DELETE
                WHERE `{unique_key_list[0]}` IN (
                    SELECT `{unique_key_list[0]}` FROM {temp_relation}
                )
                """
            else:
                delete_sql = f"""
                ALTER TABLE {target_relation} DELETE
                WHERE ({unique_key_cols}) IN (
                    SELECT {unique_key_cols} FROM {temp_relation}
                )
                """
            
            print(f"   Executing DELETE mutation...")
            client.command(delete_sql)
            print(f"   ✅ DELETE mutation submitted\n")
            
            # ============================================================================
            # STEP 3: Wait for mutations to complete (CRITICAL!)
            # ============================================================================
            print(f"⏳ STEP 3: Waiting for mutations to complete")
            
            self._wait_for_mutations(client, schema, table_name, max_wait_seconds=60)
            
            print(f"   ✅ All mutations completed\n")
            
            # ============================================================================
            # STEP 4: Insert data from temp table to target
            # ============================================================================
            print(f"📥 STEP 4: Inserting rows from temp to target")
            
            insert_sql = f"""
            INSERT INTO {target_relation}
            SELECT * FROM {temp_relation}
            """
            
            print(f"   Executing INSERT...")
            client.command(insert_sql)
            print(f"   ✅ Inserted {len(df):,} rows\n")
            
            # ============================================================================
            # STEP 5: Drop temporary table
            # ============================================================================
            print(f"🧹 STEP 5: Cleaning up")
            
            drop_sql = f"DROP TABLE IF EXISTS {temp_relation}"
            client.command(drop_sql)
            print(f"   ✅ Temp table dropped\n")
            
            # ============================================================================
            # STEP 6: Get final row counts and calculate changes
            # ============================================================================
            result = client.query(count_before_sql).result_set
            rows_after = result[0][0] if result else 0
            
            net_change = rows_after - rows_before
            rows_updated = min(rows_to_delete, len(df))
            rows_inserted_new = len(df) - rows_updated
            
            print(f"{'='*60}")
            print(f"✅ Delete+Insert Strategy Complete!")
            print(f"{'='*60}")
            print(f"📊 Summary:")
            print(f"   Rows before:        {rows_before:,}")
            print(f"   Rows deleted:       {rows_to_delete:,}")
            print(f"   Rows inserted:      {len(df):,}")
            print(f"   ├─ Updated:         {rows_updated:,}")
            print(f"   └─ New:             {rows_inserted_new:,}")
            print(f"   Rows after:         {rows_after:,}")
            print(f"   Net change:         {net_change:+,}")
            print(f"{'='*60}\n")
            
            # ✅ Return net change instead of just inserted rows
            return net_change
            
        except Exception as e:
            # Cleanup temp table on error
            print(f"\n❌ Error during delete+insert: {e}")
            try:
                client.command(f"DROP TABLE IF EXISTS {temp_relation}")
                print(f"   🧹 Cleaned up temp table")
            except:
                pass
            raise


    def _wait_for_mutations(self, client, schema: str, table_name: str, max_wait_seconds: int = 60, check_interval: int = 2):
        """
        Wait for ClickHouse mutations to complete - matches dbt-clickhouse SQL logic
        
        This is CRITICAL for delete+insert to work correctly because ALTER TABLE DELETE
        is asynchronous in ClickHouse.
        """
        import time
        
        waited = 0
        
        while waited < max_wait_seconds:
            # Check system.mutations table for pending mutations
            check_sql = f"""
            SELECT COUNT(*) as pending
            FROM system.mutations
            WHERE database = '{schema}'
            AND table = '{table_name}'
            AND is_done = 0
            """
            
            result = client.query(check_sql).result_set
            pending = result[0][0] if result else 0
            
            if pending == 0:
                if waited > 0:
                    print(f"   ✅ All mutations completed (waited {waited}s)")
                return
            
            print(f"   ⏱️  {pending} mutation(s) still pending... ({waited}s elapsed)")
            time.sleep(check_interval)
            waited += check_interval
        
        # Timeout - mutations still running
        print(f"   ⚠️  Timeout after {max_wait_seconds}s - some mutations may still be running")
        print(f"   ⚠️  This may cause duplicates if INSERT happens before DELETE completes!")


    def _verify_no_duplicates(self, client, schema: str, table_name: str, unique_key: Union[str, List[str]]) -> bool:
        """
        Verify no duplicates exist after merge - for debugging
        """
        unique_key_list = [unique_key] if isinstance(unique_key, str) else unique_key
        unique_key_cols = ', '.join(f'`{k}`' for k in unique_key_list)
        
        check_sql = f"""
        SELECT 
            {unique_key_cols},
            COUNT(*) as cnt
        FROM `{schema}`.`{table_name}`
        GROUP BY {unique_key_cols}
        HAVING cnt > 1
        LIMIT 10
        """
        
        result = client.query(check_sql).result_set
        
        if result:
            print(f"\n⚠️  WARNING: Found {len(result)} duplicate key(s):")
            for row in result[:5]:
                print(f"   {row}")
            if len(result) > 5:
                print(f"   ... and {len(result) - 5} more")
            return False
        else:
            print(f"\n✅ No duplicates found - merge successful!")
            return True
        
    def _insert_overwrite(self, df, schema: str, table_name: str, partition_by: str) -> int:
        """
        Insert Overwrite strategy - deletes entire partitions and inserts new data
        
        This is more efficient than delete+insert when you're replacing entire partitions.
        Used for time-based incremental models where you reload entire days/months.
        
        Example: If partition_by='toYYYYMM(date)' and new data has months [2025-01, 2025-02],
                it will delete ALL rows in those months and insert the new data.
        """
        import pandas as pd
        from datetime import datetime, date
        
        connection = self.connections.get_thread_connection()
        client = connection.handle
        
        target_relation = f"`{schema}`.`{table_name}`"
        
        print(f"\n{'='*60}")
        print(f"🔄 Insert Overwrite Strategy (Python Model)")
        print(f"{'='*60}")
        print(f"Target table: {schema}.{table_name}")
        print(f"Partition by: {partition_by}")
        print(f"Rows to insert: {len(df)}")
        print(f"{'='*60}\n")
        
        # ============================================================================
        # STEP 1: Get unique partition values from new data
        # ============================================================================
        print(f"📊 STEP 1: Identifying partitions to overwrite")
        
        # Extract partition column from partition_by expression
        # Examples:
        #   "toYYYYMM(date)" -> "date"
        #   "toStartOfMonth(created_at)" -> "created_at"
        #   "city" -> "city"
        
        import re
        match = re.search(r'\(([^)]+)\)', partition_by)
        if match:
            partition_column = match.group(1)
        else:
            partition_column = partition_by
        
        if partition_column not in df.columns:
            raise Exception(f"Partition column '{partition_column}' not found in DataFrame. Available: {df.columns.tolist()}")
        
        # Get unique partition values
        unique_partition_values = df[partition_column].unique()
        print(f"   Partition column: {partition_column}")
        print(f"   Unique partitions in new data: {len(unique_partition_values)}")
        
        # ============================================================================
        # STEP 2: Delete partitions
        # ============================================================================
        print(f"\n🗑️  STEP 2: Deleting partitions")
        
        # Build WHERE clause to match partitions
        where_conditions = []
        for val in unique_partition_values:
            if pd.isna(val):
                continue
            elif isinstance(val, str):
                escaped = val.replace("'", "''").replace("\\", "\\\\")
                where_conditions.append(f"{partition_by} = '{escaped}'")
            elif isinstance(val, (datetime, pd.Timestamp)):
                dt_str = val.strftime('%Y-%m-%d %H:%M:%S')
                where_conditions.append(f"{partition_by} = '{dt_str}'")
            elif isinstance(val, date):
                date_str = val.strftime('%Y-%m-%d')
                where_conditions.append(f"{partition_by} = '{date_str}'")
            else:
                where_conditions.append(f"{partition_by} = {val}")
        
        if where_conditions:
            delete_sql = f"""
            ALTER TABLE {target_relation} DELETE
            WHERE {' OR '.join(where_conditions)}
            """
            
            print(f"   Deleting {len(where_conditions)} partition(s)...")
            client.command(delete_sql)
            print(f"   ✅ DELETE mutation submitted\n")
            
            # ============================================================================
            # STEP 3: Wait for mutations
            # ============================================================================
            print(f"⏳ STEP 3: Waiting for mutations to complete")
            self._wait_for_mutations(client, schema, table_name, max_wait_seconds=60)
            print(f"   ✅ All mutations completed\n")
        else:
            print(f"   ⚠️  No valid partition values found\n")
        
        # ============================================================================
        # STEP 4: Insert new data
        # ============================================================================
        print(f"📥 STEP 4: Inserting new data")
        self._append_to_table(df, schema, table_name)
        print(f"   ✅ Inserted {len(df)} rows\n")
        
        print(f"{'='*60}")
        print(f"✅ Insert Overwrite Complete!")
        print(f"   Overwrote {len(where_conditions)} partition(s)")
        print(f"   Inserted {len(df)} rows")
        print(f"{'='*60}\n")
        
        return len(df)

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_cluster_name(self):
        conn = self.connections.get_if_exists()
        if conn.credentials.cluster:
            return f'"{conn.credentials.cluster}"'

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_local_suffix(self):
        conn = self.connections.get_if_exists()
        suffix = conn.credentials.local_suffix
        if suffix:
            if suffix.startswith('_'):
                return f'{suffix}'
            return f'_{suffix}'
        return ''

    @available.parse(lambda *a, **k: {})
    def get_clickhouse_local_db_prefix(self):
        conn = self.connections.get_if_exists()
        prefix = conn.credentials.local_db_prefix
        if prefix:
            if prefix.endswith('_'):
                return f'{prefix}'
            return f'{prefix}_'
        return ''

    @available
    def clickhouse_db_engine_clause(self):
        conn = self.connections.get_if_exists()
        if conn and conn.credentials.database_engine:
            return f'ENGINE {conn.credentials.database_engine}'
        return ''

    @available
    def is_before_version(self, version: str) -> bool:
        conn = self.connections.get_if_exists()
        if conn:
            server_version = conn.handle.server_version
            return compare_versions(version, server_version) > 0
        return False

    @available.parse_none
    def supports_atomic_exchange(self) -> bool:
        conn = self.connections.get_if_exists()
        return conn and conn.handle.atomic_exchange

    @available.parse_none
    def can_exchange(self, schema: str, rel_type: str) -> bool:
        if rel_type != 'table' or not schema or not self.supports_atomic_exchange():
            return False
        ch_db = self.get_ch_database(schema)
        return ch_db and engine_can_atomic_exchange(ch_db.engine)

    @available.parse_none
    def should_on_cluster(self, materialized: str = '', engine: str = '') -> bool:
        conn = self.connections.get_if_exists()
        if conn and conn.credentials.cluster:
            return ClickHouseRelation.get_on_cluster(
                cluster=conn.credentials.cluster, database_engine=conn.credentials.database_engine
            )
        return ClickHouseRelation.get_on_cluster()

    @available.parse_none
    def calculate_incremental_strategy(self, strategy: str) -> str:
        conn = self.connections.get_if_exists()
        if not strategy or strategy == 'default':
            strategy = 'delete_insert' if conn.handle.use_lw_deletes else 'legacy'
        strategy = strategy.replace('+', '_')
        return strategy

    @available.parse_none
    def validate_incremental_strategy(
        self,
        strategy: str,
        predicates: list,
        unique_key: str,
        partition_by: str,
    ) -> None:
        conn = self.connections.get_if_exists()
        if strategy not in ('legacy', 'append', 'delete_insert', 'insert_overwrite', 'microbatch'):
            raise DbtRuntimeError(
                f"The incremental strategy '{strategy}' is not valid for ClickHouse."
            )
        if strategy in ('delete_insert', 'microbatch') and not conn.handle.has_lw_deletes:
            raise DbtRuntimeError(
                f"'{strategy}' strategy requires setting the profile config 'use_lw_deletes' to true."
            )
        if strategy in ('delete_insert', 'microbatch') and not unique_key:
            raise DbtRuntimeError(f"'{strategy}' strategy requires a non-empty 'unique_key'.")
        if strategy not in ('delete_insert', 'microbatch') and predicates:
            raise DbtRuntimeError(
                f"Cannot apply incremental predicates with '{strategy}' strategy."
            )
        if strategy == 'insert_overwrite' and not partition_by:
            raise DbtRuntimeError(
                f"'{strategy}' strategy requires non-empty 'partition_by'. Current partition_by is {partition_by}."
            )
        if strategy == 'insert_overwrite' and unique_key:
            raise DbtRuntimeError(f"'{strategy}' strategy does not support unique_key.")

    @available.parse_none
    def check_incremental_schema_changes(
        self,
        on_schema_change,
        existing,
        target_sql,
        materialization: str = 'incremental',
    ) -> ClickHouseColumnChanges:
        if on_schema_change not in ('fail', 'ignore', 'append_new_columns', 'sync_all_columns'):
            raise DbtRuntimeError(
                "Only `fail`, `ignore`, `append_new_columns`, and `sync_all_columns` supported for `on_schema_change`."
            )

        source = self.get_columns_in_relation(existing)
        source_map = {column.name: column for column in source}
        target = self.get_column_schema_from_query(target_sql)
        target_map = {column.name: column for column in target}

        source_not_in_target = [column for column in source if column.name not in target_map.keys()]
        target_not_in_source = [column for column in target if column.name not in source_map.keys()]
        target_in_source = [column for column in target if column.name in source_map.keys()]
        changed_data_types = []
        for column in target_in_source:
            source_column = source_map.get(column.name)
            if source_column is not None and column.dtype != source_column.dtype:
                changed_data_types.append(column)

        clickhouse_column_changes = ClickHouseColumnChanges(
            columns_to_add=target_not_in_source,
            columns_to_drop=source_not_in_target,
            columns_to_modify=changed_data_types,
            on_schema_change=on_schema_change,
        )

        if clickhouse_column_changes.has_conflicting_changes:

            def format_column_names(columns) -> List[str]:
                return [str(col) for col in columns]

            raise DbtRuntimeError(
                schema_change_fail_error.format(
                    materialization,
                    format_column_names(source_not_in_target),
                    format_column_names(target_not_in_source),
                    format_column_names(changed_data_types),
                )
            )

        return clickhouse_column_changes

    @available.parse_none
    def s3source_clause(
        self,
        config_name: str,
        s3_model_config: dict,
        bucket: str,
        path: str,
        fmt: str,
        structure: Union[str, list, dict],
        aws_access_key_id: str,
        aws_secret_access_key: str,
        role_arn: str,
        compression: str = '',
    ) -> str:
        s3config = self.config.vars.vars.get(config_name, {})
        s3config.update(s3_model_config)
        structure = structure or s3config.get('structure', '')
        struct = ''
        if structure:
            if isinstance(structure, dict):
                cols = [f'{name} {col_type}' for name, col_type in structure.items()]
                struct = f", '{','.join(cols)}'"
            elif isinstance(structure, list):
                struct = f", '{','.join(structure)}'"
            else:
                struct = f",'{structure}'"
        fmt = fmt or s3config.get('fmt')
        bucket = bucket or s3config.get('bucket', '')
        path = path or s3config.get('path', '')
        url = bucket.replace('https://', '')
        if path:
            if bucket and path and not bucket.endswith('/') and not bucket.startswith('/'):
                path = f'/{path}'
            url = f'{url}{path}'.replace('//', '/')
        url = f'https://{url}'
        access = ''
        if aws_access_key_id and not aws_secret_access_key:
            raise DbtRuntimeError('S3 aws_access_key_id specified without aws_secret_access_key')
        if aws_secret_access_key and not aws_access_key_id:
            raise DbtRuntimeError('S3 aws_secret_access_key specified without aws_access_key_id')
        if aws_access_key_id:
            access = f", '{aws_access_key_id}', '{aws_secret_access_key}'"
        comp = compression or s3config.get('compression', '')
        if comp:
            comp = f"', {comp}'"
        extra_credentials = ''
        if role_arn:
            extra_credentials = f", extra_credentials(role_arn='{role_arn}')"
        return f"s3('{url}'{access}, '{fmt}'{struct}{comp}{extra_credentials})"

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={'database': database})
        return schema in (row[0] for row in results)

    def drop_schema(self, relation: BaseRelation) -> None:
        super().drop_schema(relation)
        conn = self.connections.get_if_exists()
        if conn:
            conn.handle.database_dropped(relation.schema)

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
            }
        )

    def list_relations_without_caching(
        self, schema_relation: ClickHouseRelation
    ) -> List[ClickHouseRelation]:
        kwargs = {'schema_relation': schema_relation}
        results = self.execute_macro('list_relations_without_caching', kwargs=kwargs)
        conn_supports_exchange = self.supports_atomic_exchange()

        relations = []
        for row in results:
            name, schema, type_info, db_engine, on_cluster = row
            if 'view' in type_info:
                rel_type = ClickHouseRelationType.View
            elif type_info == 'dictionary':
                rel_type = ClickHouseRelationType.Dictionary
            else:
                rel_type = ClickHouseRelationType.Table
            can_exchange = (
                conn_supports_exchange
                and rel_type == ClickHouseRelationType.Table
                and engine_can_atomic_exchange(db_engine)
            )
            can_on_cluster = (on_cluster >= 1) and db_engine != 'Replicated'

            relation = self.Relation.create(
                database='',
                schema=schema,
                identifier=name,
                type=rel_type,
                can_exchange=can_exchange,
                can_on_cluster=can_on_cluster,
            )
            relations.append(relation)

        return relations

    def get_relation(self, database: Optional[str], schema: str, identifier: str):
        return super().get_relation('', schema, identifier)

    @available.parse_none
    def get_ch_database(self, schema: str):
        try:
            results = self.execute_macro('clickhouse__get_database', kwargs={'database': schema})
            if len(results.rows):
                return ClickHouseDatabase(**results.rows[0])
            return None
        except DbtRuntimeError:
            return None

    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> Tuple["agate.Table", List[Exception]]:
        from dbt_common.clients.agate_helper import empty_table

        relations = self._get_catalog_relations(relation_configs)
        schemas = set(relation.schema for relation in relations)
        if schemas:
            catalog = self._get_one_catalog(InformationSchema(Path()), schemas, used_schemas)
        else:
            catalog = empty_table()
        return catalog, []

    def get_filtered_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
        relations: Optional[Set[BaseRelation]] = None,
    ):
        catalog, exceptions = self.get_catalog(relation_configs, used_schemas)
        if relations and catalog:
            relation_map = {(r.schema, r.identifier) for r in relations}

            def in_map(row: "agate.Row"):
                s = _expect_row_value("table_schema", row)
                i = _expect_row_value("table_name", row)
                return (s, i) in relation_map

            catalog = catalog.where(in_map)
        return catalog, exceptions

    def get_rows_different_sql(
        self,
        relation_a: ClickHouseRelation,
        relation_b: ClickHouseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: Optional[str] = None,
    ) -> str:
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = 'ta'
        alias_b = 'tb'
        columns_csv_a = ', '.join([f'{alias_a}.{name}' for name in names])
        columns_csv_b = ', '.join([f'{alias_b}.{name}' for name in names])
        join_condition = ' AND '.join([f'{alias_a}.{name} = {alias_b}.{name}' for name in names])
        first_column = names[0]

        # Clickhouse doesn't have an EXCEPT operator
        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f'alter table {dst_name} update {dst_column} = {clause}'
        if where_clause is not None:
            clause += f' where {where_clause}'
        return clause

    @available
    def get_csv_data(self, table):
        csv_funcs = [c.csvify for c in table._column_types]

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        for row in table.rows:
            writer.writerow(tuple(csv_funcs[i](d) for i, d in enumerate(row)))

        return buf.getvalue()

    def run_sql_for_tests(self, sql, fetch, conn):
        client = conn.handle
        try:
            if fetch:
                result = client.query(sql).result_set
            else:
                result = client.command(sql)
            if fetch == "one" and len(result) > 0:
                return result[0]
            if fetch == "all":
                return result
        except BaseException as e:
            logger.error(sql)
            logger.error(e)
            raise
        finally:
            conn.state = 'close'

    def _build_settings_str(self, settings: Dict[str, Any]) -> str:
        res = []
        for key in settings:
            if isinstance(settings[key], str) and not settings[key].startswith("'"):
                res.append(f"{key}='{settings[key]}'")
            else:
                # Support old workaround https://github.com/ClickHouse/dbt-clickhouse/issues/240#issuecomment-1894692117
                res.append(f"{key}={settings[key]}")
        return '' if len(res) == 0 else 'SETTINGS ' + ', '.join(res) + '\n'

    @available
    def get_model_settings(self, model, engine='MergeTree'):
        settings = model['config'].get('settings', {})
        materialization_type = model['config'].get('materialized')
        conn = self.connections.get_if_exists()
        conn.handle.update_model_settings(settings, materialization_type)
        settings = self.filter_settings_by_engine(settings, engine)
        settings_str = self._build_settings_str(settings)
        return f"""
                    -- end_of_sql
                    {settings_str}
                    """

    @available
    def filter_settings_by_engine(self, settings, engine):
        filtered_settings = {}

        for key, value in settings.items():
            if 'MergeTree' not in engine and key in MERGETREE_EXCLUSIVE_SETTINGS:
                logger.warning(f"Setting {key} not available for engine {engine}, ignoring.")
            else:
                filtered_settings[key] = value

        return filtered_settings

    @available
    def get_model_query_settings(self, model):
        settings = model['config'].get('query_settings', {})
        settings_str = self._build_settings_str(settings)
        return (
            ''
            if settings_str == ''
            else f"""
            -- settings_section
            {settings_str}
            """
        )

    @available.parse_none
    def get_column_schema_from_query(self, sql: str, *_) -> List[ClickHouseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        conn = self.connections.get_if_exists()
        return conn.handle.columns_in_query(sql)

    @available.parse_none
    def format_columns(self, columns) -> List[Dict]:
        return [{'name': column.name, 'data_type': column.data_type} for column in columns]

    @available
    def get_credentials(self, connection_overrides) -> Dict:
        conn = self.connections.get_if_exists()
        if conn is None or conn.credentials is None:
            return dict()
        credentials = {
            'user': conn.credentials.user,
            'password': conn.credentials.password,
            'database': conn.credentials.database,
            'host': conn.credentials.host,
            'port': conn.credentials.port,
        }
        credentials.update(connection_overrides)

        for key in connection_overrides.keys():
            if not credentials[key]:
                credentials.pop(key)

        return credentials

    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_columns = []
        for v in raw_columns.values():
            codec = f"CODEC({_codec})" if (_codec := v.get('codec')) else ""
            ttl = f"TTL {ttl}" if (ttl := v.get('ttl')) else ""
            # Codec and TTL are optional clauses. The adapter should support scenarios where one
            # or both are omitted. If specified together, the codec clause should appear first.
            clauses = " ".join(filter(None, [codec, ttl]))
            rendered_columns.append(
                f"{quote_identifier(v['name'])} {v['data_type']} {clauses}".rstrip()
            )
            if v.get("constraints"):
                warn_or_error(ConstraintNotSupported(constraint='column', adapter='clickhouse'))
        return rendered_columns

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        if constraint.type == ConstraintType.check and constraint.expression:
            if not constraint.name:
                raise DbtRuntimeError("CHECK Constraint 'name' is required")
            return f"CONSTRAINT {constraint.name} CHECK ({constraint.expression})"
        return None


@dataclass
class ClickHouseDatabase:
    name: str
    engine: str
    comment: str


def _expect_row_value(key: str, row: "agate.Row"):
    if key not in row.keys():
        raise DbtInternalError(f'Got a row without \'{key}\' column, columns: {row.keys()}')

    return row[key]


def _catalog_filter_schemas(
    used_schemas: FrozenSet[Tuple[str, str]],
) -> Callable[["agate.Row"], bool]:
    """Return a function that takes a row and decides if the row should be
    included in the catalog output.
    """
    schemas = frozenset((d.lower(), s.lower()) for d, s in used_schemas)

    def test(row: "agate.Row") -> bool:
        table_database = _expect_row_value('table_database', row)
        table_schema = _expect_row_value('table_schema', row)
        if table_schema is None:
            return False
        return (table_database, table_schema) in schemas

    return test


COLUMNS_EQUAL_SQL = '''
SELECT
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
FROM (
    SELECT
        1 as id,
        (SELECT COUNT(*) as num_rows FROM {relation_a}) -
        (SELECT COUNT(*) as num_rows FROM {relation_b}) as difference
    ) as row_count_diff
INNER JOIN (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            SELECT
                {columns_a}
            FROM {relation_a} as {alias_a}
            LEFT OUTER JOIN {relation_b} as {alias_b}
                ON {join_condition}
            WHERE {alias_b}.{first_column} IS NULL
            UNION ALL
            SELECT
                {columns_b}
            FROM {relation_b} as {alias_b}
            LEFT OUTER JOIN {relation_a} as {alias_a}
                ON {join_condition}
            WHERE {alias_a}.{first_column} IS NULL
        ) as missing
    ) as diff_count ON row_count_diff.id = diff_count.id
'''.strip()
