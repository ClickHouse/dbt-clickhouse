import subprocess
import json
import tempfile
import os
from typing import Any, Dict, Optional
import pandas as pd
from dbt.adapters.clickhouse.logger import logger


class ClickHousePythonExecutor:
    """
    Executes Python models for ClickHouse by running them in a subprocess
    and materializing results back to ClickHouse tables.
    """
    
    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
    
    def execute_python_model(
        self, 
        compiled_code: str, 
        model_config: Dict[str, Any],
        adapter: Any
    ) -> pd.DataFrame:
        """
        Execute Python model code and return results as DataFrame. 
        
        Args:
            compiled_code: The compiled Python model code
            model_config: Model configuration from dbt
            adapter: The ClickHouse adapter instance
            
        Returns:
            pd.DataFrame: The result of the model execution
        """
        logger.info(f"Starting Python model execution for {model_config.get('alias')}")
        
        try:
            # Create a temporary file to write the model code
            with tempfile.NamedTemporaryFile(
                mode='w', 
                suffix='.py', 
                delete=False
            ) as tmp_file:
                tmp_file.write(compiled_code)
                tmp_file_path = tmp_file.name
            
            # Execute the Python script in a subprocess
            result = subprocess.run(
                ['python', tmp_file_path],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            # Clean up temp file
            os.unlink(tmp_file_path)
            
            if result.returncode != 0:
                logger.error(f"Python model execution failed: {result.stderr}")
                raise Exception(f"Python execution error: {result.stderr}")
            
            # Parse the output as JSON and convert to DataFrame
            output = json.loads(result.stdout)
            df = pd.DataFrame(output)
            
            logger. info(f"Python model execution completed:  {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error executing Python model:  {str(e)}")
            raise
    
    def materialize_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name:  str, 
        schema_name: str,
        materialization: str,
        connection:  Any
    ) -> None:
        """
        Write DataFrame to ClickHouse table. 
        
        Args:
            df: The DataFrame to materialize
            table_name: Name of the target table
            schema_name: Schema/database name
            materialization: Type of materialization ('table' or 'view')
            connection: ClickHouse connection object
        """
        logger.info(f"Materializing DataFrame to {schema_name}.{table_name}")
        
        try:
            # Generate CREATE TABLE statement from DataFrame
            create_table_sql = self._generate_create_table_sql(
                df, 
                table_name, 
                schema_name
            )
            
            # Execute the CREATE TABLE statement
            connection.execute(create_table_sql)
            
            # Insert data into the table
            self._insert_dataframe(df, table_name, schema_name, connection)
            
            logger.info(f"Successfully materialized {len(df)} rows to {schema_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Error materializing DataFrame: {str(e)}")
            raise
    
    def _generate_create_table_sql(
        self, 
        df:  pd.DataFrame, 
        table_name: str, 
        schema_name: str
    ) -> str:
        """
        Generate CREATE TABLE SQL based on DataFrame schema.
        """
        column_defs = []
        
        for col_name, col_type in zip(df.columns, df.dtypes):
            ch_type = self._pandas_to_clickhouse_type(col_type)
            column_defs.append(f"`{col_name}` {ch_type}")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{schema_name}`.`{table_name}` (
            {', '.join(column_defs)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        return create_sql
    
    def _pandas_to_clickhouse_type(self, pandas_dtype) -> str:
        """
        Map pandas dtype to ClickHouse type. 
        """
        dtype_str = str(pandas_dtype)
        
        if 'int64' in dtype_str:
            return 'Int64'
        elif 'int32' in dtype_str: 
            return 'Int32'
        elif 'float64' in dtype_str:
            return 'Float64'
        elif 'float32' in dtype_str:
            return 'Float32'
        elif 'bool' in dtype_str:
            return 'UInt8'
        elif 'object' in dtype_str or 'string' in dtype_str:
            return 'String'
        elif 'datetime' in dtype_str:
            return 'DateTime'
        else:
            return 'String'  # Default fallback
    
    def _insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema_name: str,
        connection: Any
    ) -> None:
        """
        Insert DataFrame rows into ClickHouse table. 
        """
        # Convert DataFrame to list of tuples for insertion
        for _, row in df.iterrows():
            values = [self._format_value(v) for v in row.values]
            insert_sql = f"""
            INSERT INTO `{schema_name}`.`{table_name}` VALUES ({', '.join(values)})
            """
            connection.execute(insert_sql)
    
    def _format_value(self, value:  Any) -> str:
        """
        Format a Python value for ClickHouse SQL.
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            return f"'{value. replace(chr(39), chr(39) + chr(39))}'"  # Escape quotes
        elif isinstance(value, bool):
            return '1' if value else '0'
        else:
            return str(value)