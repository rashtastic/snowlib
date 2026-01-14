"""Utilities for inferring and resolving Snowflake schema from pandas DataFrames"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

from snowlib.utils.json_columns import is_json_eligible

if TYPE_CHECKING:
    from snowlib.models.table import Table


@dataclass
class ColumnSchema:
    """Schema definition for a single column"""
    name: str
    snowflake_type: str
    
    def to_ddl(self) -> str:
        """Return DDL fragment for this column (e.g., 'NAME VARCHAR')"""
        return f"{self.name} {self.snowflake_type}"


def _pandas_dtype_to_snowflake(dtype: Any, series: pd.Series) -> str:
    """Map pandas dtype to Snowflake type.
    
    Args:
        dtype: The pandas dtype (numpy dtype or ExtensionDtype)
        series: The actual series (for object dtype inspection)
    
    Returns:
        Snowflake type string
    """
    dtype_str = str(dtype)
    
    # Integer types
    if dtype_str in ('int8', 'int16', 'int32', 'int64', 'Int8', 'Int16', 'Int32', 'Int64'):
        return 'INT'
    if dtype_str in ('uint8', 'uint16', 'uint32', 'uint64', 'UInt8', 'UInt16', 'UInt32', 'UInt64'):
        return 'INT'
    
    # Float types
    if dtype_str in ('float16', 'float32', 'float64', 'Float32', 'Float64'):
        return 'FLOAT'
    
    # Boolean
    if dtype_str in ('bool', 'boolean'):
        return 'BOOLEAN'
    
    # Datetime types
    if 'datetime64' in dtype_str:
        # Check if timezone-aware
        if hasattr(dtype, 'tz') and dtype.tz is not None:  # type: ignore
            return 'TIMESTAMP_TZ'
        return 'TIMESTAMP_NTZ'
    
    # Date type (from pandas date accessor or object with date values)
    if dtype_str == 'object':
        # Check first non-null value
        non_null = series.dropna()
        if len(non_null) > 0:
            first_val = non_null.iloc[0]
            from datetime import date, datetime
            if isinstance(first_val, date) and not isinstance(first_val, datetime):
                return 'DATE'
    
    # Timedelta
    if 'timedelta' in dtype_str:
        return 'TIME'
    
    # String types
    if dtype_str in ('string', 'str'):
        return 'VARCHAR'
    
    # Object type - default to VARCHAR
    if dtype_str == 'object':
        return 'VARCHAR'
    
    # Catch-all for other types
    return 'VARCHAR'


def infer_snowflake_schema(
    df: pd.DataFrame,
    variant_columns: list[str] | None = None,
) -> list[ColumnSchema]:
    """Infer Snowflake column schema from a pandas DataFrame.
    
    Args:
        df: Source DataFrame
        variant_columns: Explicit list of columns to treat as VARIANT.
            If None, JSON-eligible columns are auto-detected.
    
    Returns:
        List of ColumnSchema objects defining the Snowflake table schema
    """
    variant_cols_set: set[str] = set()
    
    if variant_columns is not None:
        variant_cols_set = {col.upper() for col in variant_columns}
    else:
        # Auto-detect JSON-eligible columns
        for col in df.columns:
            if is_json_eligible(df[col]):
                variant_cols_set.add(str(col).upper())
    
    schema: list[ColumnSchema] = []
    
    for col in df.columns:
        col_name = str(col).upper()
        
        if col_name in variant_cols_set:
            sf_type = 'VARIANT'
        else:
            sf_type = _pandas_dtype_to_snowflake(df[col].dtype, df[col])
        
        schema.append(ColumnSchema(name=col_name, snowflake_type=sf_type))
    
    return schema


def get_table_schema(table: 'Table') -> list[ColumnSchema]:
    """Get column schema from an existing Snowflake table.
    
    Args:
        table: Table object to inspect
    
    Returns:
        List of ColumnSchema objects from the existing table
    """
    desc_df = table.describe()
    
    schema: list[ColumnSchema] = []
    for _, row in desc_df.iterrows():
        col_name = row.get('name', '')
        col_type = row.get('type', 'VARCHAR')
        
        # Normalize type - DESCRIBE returns types like "VARCHAR(16777216)"
        # Extract base type for comparison
        base_type = col_type.split('(')[0].upper()
        
        schema.append(ColumnSchema(name=col_name, snowflake_type=base_type))
    
    return schema


def resolve_target_schema(
    table: 'Table',
    df: pd.DataFrame,
    variant_columns: list[str] | None = None,
) -> list[ColumnSchema]:
    """Resolve the target schema for writing data.
    
    For existing tables: Uses the table's current schema.
    For new tables: Infers schema from the DataFrame.
    
    Args:
        table: Target Table object
        df: DataFrame to write
        variant_columns: Explicit VARIANT column overrides (for new tables only)
    
    Returns:
        List of ColumnSchema objects for the target table
    
    Raises:
        ValueError: If DataFrame columns don't match existing table columns
    """
    if table.exists():
        existing_schema = get_table_schema(table)
        existing_cols = {col.name.upper() for col in existing_schema}
        df_cols = {str(col).upper() for col in df.columns}
        
        # Check for missing columns in target table
        missing_in_table = df_cols - existing_cols
        if missing_in_table:
            raise ValueError(
                f"DataFrame has columns not in target table: {missing_in_table}. "
                + f"Table columns: {existing_cols}"
            )
        
        # Note: Extra columns in table (not in df) are okay - they'll be NULL
        return existing_schema
    else:
        return infer_snowflake_schema(df, variant_columns)


def schema_to_ddl(schema: list[ColumnSchema]) -> str:
    """Convert schema to CREATE TABLE column DDL.
    
    Args:
        schema: List of ColumnSchema objects
    
    Returns:
        DDL string like "COL1 INT, COL2 VARCHAR, COL3 VARIANT"
    """
    return ", ".join(col.to_ddl() for col in schema)


def detect_json_columns(df: pd.DataFrame) -> list[str]:
    """Detect columns containing JSON-eligible data (dicts/lists).
    
    Args:
        df: DataFrame to inspect
    
    Returns:
        List of column names (uppercased) that contain JSON data
    """
    json_cols: list[str] = []
    for col in df.columns:
        if is_json_eligible(df[col]):
            json_cols.append(str(col).upper())
    return json_cols
