"""Data I/O primitives.

Functions for reading and writing pandas DataFrames to/from Snowflake tables.
"""

from typing import Any, Union
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from snowlib.primitives.context import SnowflakeContext


def read_table(
    table: str,
    schema: str,
    database: str,
    context: Union[str, SnowflakeContext],
    lowercase_columns: bool = True,
    **overrides: Any
) -> pd.DataFrame:
    """Read entire table into pandas DataFrame.
    
    This is a strict primitive that requires explicit database and schema.
    It's a thin wrapper that builds a SELECT * query.
    
    Args:
        table: Table name (REQUIRED - just the name, not qualified)
        schema: Schema name (REQUIRED - explicit, no inference)
        database: Database name (REQUIRED - explicit, no inference)
        context: SnowflakeContext object or profile name
        lowercase_columns: Convert column names to lowercase (default: True)
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        pandas DataFrame with entire table contents
        
    Example:
        >>> df = read_table("SALES_DATA", "MY_SCHEMA", "MY_DATABASE", context="main")
        >>> df = read_table("MY_TABLE", "ANALYTICS", "PROD_DB", context="main")
        
    Warning:
        Loads entire table into memory. Use fetch_df() with WHERE clause
        for large tables.
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_df
    
    # Build fully qualified table name
    qualified_table = f"{database}.{schema}.{table}"
    
    # Use fetch_df to read the table
    return fetch_df(
        f"SELECT * FROM {qualified_table}",
        context=context,
        lowercase_columns=lowercase_columns,
        **overrides
    )


def write_table(
    df: pd.DataFrame,
    table: str,
    schema: str,
    database: str,
    context: Union[str, SnowflakeContext],
    mode: str = "replace",
    uppercase_columns: bool = True,
    uppercase_table: bool = True,
    **overrides: Any
) -> bool:
    """Write pandas DataFrame to Snowflake table.
    
    This is a strict primitive that requires explicit database and schema.
    It's a thin wrapper around write_pandas() with predictable behavior.
    
    For convenience features like parsing qualified names or inferring from
    connection context, use higher-level helpers (coming in Phase 3+).
    
    Args:
        df: pandas DataFrame to write
        table: Table name (REQUIRED - just the name, not qualified)
        schema: Schema name (REQUIRED - explicit, no inference)
        database: Database name (REQUIRED - explicit, no inference)
        context: SnowflakeContext object or profile name
        mode: Write mode - "replace", "append", or "fail" (default: "replace")
            - "replace": Drop table if exists, create new (SAFE DEFAULT)
            - "append": Add to existing table (create if not exists)
            - "fail": Raise error if table exists
        uppercase_columns: Convert column names to uppercase (default: True)
        uppercase_table: Convert table/schema/database to uppercase (default: True)
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        True if successful
        
    Example:
        >>> df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        >>> write_table(df, "TEST_DATA", "MY_SCHEMA", "MY_DATABASE", context="main", mode="replace")
        True
        
        >>> # Append to existing table
        >>> write_table(new_df, "TEST_DATA", "MY_SCHEMA", "MY_DATABASE", context="main", mode="append")
        
        >>> # Reuse context in tests (only authenticates once)
        >>> ctx = SnowflakeContext(profile="main")
        >>> write_table(df1, "TABLE1", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        >>> write_table(df2, "TABLE2", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        
    Note:
        This primitive REQUIRES explicit database and schema for predictability.
        Names are uppercased by default to match Snowflake conventions.
        
    Raises:
        ValueError: If mode is not valid or table exists (mode="fail")
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    if mode not in ("append", "replace", "fail"):
        raise ValueError(
            f"Invalid mode '{mode}'. Must be 'append', 'replace', or 'fail'"
        )
    
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import execute_sql
    from snowlib.primitives.metadata import table_exists
    
    # Uppercase names if requested (Snowflake convention)
    if uppercase_table:
        table_name = table.upper()
        schema_name = schema.upper()
        database_name = database.upper()
    else:
        table_name = table
        schema_name = schema
        database_name = database
    
    # Build fully qualified table name for SQL operations
    qualified_table = f"{database_name}.{schema_name}.{table_name}"
    
    # Convert string profile to context object
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Handle mode
    if mode == "replace":
        # Drop table if it exists
        execute_sql(f"DROP TABLE IF EXISTS {qualified_table}", context=context)
    elif mode == "fail":
        # Check if table exists first
        if table_exists(table_name, schema_name, database_name, context=context):
            # Raise ProgrammingError to match what write_pandas would raise
            from snowflake.connector.errors import ProgrammingError
            raise ProgrammingError(f"Table {qualified_table} already exists")
    
    # Prepare DataFrame
    df_to_write = df.copy()
    if uppercase_columns:
        df_to_write.columns = df_to_write.columns.str.upper()
    
    # Write to Snowflake using write_pandas
    # Pass database and schema separately (not in table_name)
    # This allows write_pandas to properly create temp stages
    write_pandas(
        context.connection,
        df_to_write,
        table_name,  # Just the table name, already uppercased if requested
        schema=schema_name,
        database=database_name,
        auto_create_table=True,
        overwrite=False,  # We handle overwrite with DROP TABLE above
        use_logical_type=True,
    )
    
    return True
