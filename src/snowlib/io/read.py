"""Read functions with name resolution.

Provides high-level read() and query() functions that:
- Parse qualified table names (database.schema.table)
- Infer missing components from connection context
- Call primitives layer with explicit parameters
"""

from typing import Union
import pandas as pd

from snowlib.primitives import SnowflakeContext, fetch_df, read_table as primitive_read_table
from snowlib.io.names import resolve_table_name


def read(
    table: str,
    context: Union[str, SnowflakeContext],
    lowercase_columns: bool = True,
    **overrides
) -> pd.DataFrame:
    """Read a Snowflake table into a pandas DataFrame with name resolution.
    
    This is the high-level convenience function. It:
    - Accepts qualified names: "database.schema.table", "schema.table", or "table"
    - Infers missing database/schema from connection context
    - Validates identifier format
    - Calls primitives.read_table with explicit parameters
    
    Args:
        table: Table name (optionally qualified)
            - "table" -> infer database and schema from connection
            - "schema.table" -> infer database from connection
            - "database.schema.table" -> fully explicit
        context: SnowflakeContext object or profile name
        lowercase_columns: Convert column names to lowercase (default: True)
        **overrides: Connection parameter overrides
        
    Returns:
        pandas DataFrame with table contents
        
    Raises:
        ValueError: If name is invalid or context missing required info
        snowflake.connector.errors.*: Any Snowflake errors
        
    Examples:
        >>> # Fully qualified
        >>> df = read("MY_DATABASE.MY_SCHEMA.sales_data", context="main")
        
        >>> # Infer database from connection
        >>> df = read("MY_SCHEMA.sales_data", context="main")
        
        >>> # Infer both from connection (must have USE DATABASE/SCHEMA)
        >>> df = read("sales_data", context="main")
        
        >>> # Reuse connection context
        >>> ctx = SnowflakeContext(profile="main")
        >>> df1 = read("table1", context=ctx)
        >>> df2 = read("table2", context=ctx)
        
        >>> # Use different profile
        >>> df = read("MY_DATABASE.MY_SCHEMA.sales_data", context="prod")
    """
    # Resolve the table name
    database, schema, table_name = resolve_table_name(table, context, **overrides)
    
    # Convert string profile to context if needed (for reuse)
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Call primitive with explicit parameters
    return primitive_read_table(
        table=table_name,
        schema=schema,
        database=database,
        context=context,
        lowercase_columns=lowercase_columns
    )


def query(
    sql: str,
    context: Union[str, SnowflakeContext],
    lowercase_columns: bool = True,
    **overrides
) -> pd.DataFrame:
    """Execute a SQL query and return results as a pandas DataFrame.
    
    This is a convenience wrapper around primitives.fetch_df that provides
    a shorter name and consistent interface with read().
    
    Args:
        sql: SQL query to execute
        context: SnowflakeContext object or profile name
        lowercase_columns: Convert column names to lowercase (default: True)
        **overrides: Connection parameter overrides
        
    Returns:
        pandas DataFrame with query results
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake errors
        
    Examples:
        >>> # Simple query
        >>> df = query("SELECT * FROM MY_DATABASE.MY_SCHEMA.sales_data WHERE amount > 1000", context="main")
        
        >>> # Reuse connection
        >>> ctx = SnowflakeContext(profile="main")
        >>> df1 = query("SELECT * FROM table1", context=ctx)
        >>> df2 = query("SELECT * FROM table2", context=ctx)
        
        >>> # Use different profile
        >>> df = query("SELECT COUNT(*) FROM large_table", context="prod")
    """
    # Convert string profile to context if needed
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Call primitive
    return fetch_df(
        sql=sql,
        context=context,
        lowercase_columns=lowercase_columns
    )
