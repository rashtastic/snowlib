"""Metadata query primitives.

Functions for querying Snowflake metadata (tables, columns, context).
"""

from typing import Any, Union

from snowlib.primitives.context import SnowflakeContext


def get_columns(
    table: str,
    schema: str,
    database: str,
    context: Union[str, SnowflakeContext],
    uppercase: bool = True,
    **overrides: Any
) -> list[str]:
    """Get column names from a table or view.
    
    Uses SHOW COLUMNS pattern with result_scan().
    
    Args:
        table: Table name
        schema: Schema name (REQUIRED)
        database: Database name (REQUIRED)
        context: SnowflakeContext object or profile name
        uppercase: Return column names in uppercase (default: True)
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        List of column names
        
    Example:
        >>> cols = get_columns("sales_data", "MY_SCHEMA", "MY_DATABASE", context="main")
        >>> print(cols)
        ['ID', 'DATE', 'AMOUNT', 'PRODUCT']
        
        >>> cols = get_columns("sales_data", "MY_SCHEMA", "MY_DATABASE", context="main", uppercase=False)
        >>> print(cols)
        ['id', 'date', 'amount', 'product']
        
        >>> # Reuse context in tests
        >>> ctx = SnowflakeContext(profile="main")
        >>> cols1 = get_columns("TABLE1", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        >>> cols2 = get_columns("TABLE2", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        
    Note:
        Based on snow6/core.py get_columns() pattern.
        Requires explicit database and schema (strict primitive).
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_df
    
    # Convert string profile to context object
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Build fully qualified table name
    qualified_table = f"{database}.{schema}.{table}"
    
    # Execute SHOW COLUMNS
    result = context.cursor.execute(f"SHOW COLUMNS IN TABLE {qualified_table}")
    sfqid = result.sfqid
    
    # Fetch column data using result_scan
    cols_df = fetch_df(
        f"SELECT * FROM TABLE(RESULT_SCAN('{sfqid}'))",
        context=context,
        lowercase_columns=True,
    )
    
    # Extract column names
    column_names = cols_df["column_name"].tolist()
    
    if uppercase:
        return [str(name).upper() for name in column_names]
    else:
        return [str(name).lower() for name in column_names]


def list_tables(
    schema: str,
    database: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> list[str]:
    """List tables in a schema.
    
    Args:
        schema: Schema name (REQUIRED)
        database: Database name (REQUIRED)
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        List of table names
        
    Example:
        >>> tables = list_tables("MY_SCHEMA", "MY_DATABASE", context="main")
        >>> print(tables)
        ['sales_data', 'customer_data', 'product_data']
        
        >>> # Reuse context in tests
        >>> ctx = SnowflakeContext(profile="main")
        >>> tables1 = list_tables("SCHEMA1", "MY_DATABASE", context=ctx)
        >>> tables2 = list_tables("SCHEMA2", "MY_DATABASE", context=ctx)
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_df
    
    # Convert string profile to context object
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Build SHOW TABLES query
    show_query = f"SHOW TABLES IN {database}.{schema}"
    
    # Execute SHOW TABLES
    result = context.cursor.execute(show_query)
    sfqid = result.sfqid
    
    # Fetch table data using result_scan
    tables_df = fetch_df(
        f"SELECT * FROM TABLE(RESULT_SCAN('{sfqid}'))",
        context=context,
        lowercase_columns=True,
    )
    
    # Extract table names
    if not tables_df.empty and "name" in tables_df.columns:
        return tables_df["name"].tolist()
    else:
        return []


def table_exists(
    table: str,
    schema: str,
    database: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> bool:
    """Check if table exists in Snowflake.
    
    Uses list_tables() to get all tables in schema and checks if table is present.
    
    Args:
        table: Table name
        schema: Schema name (REQUIRED)
        database: Database name (REQUIRED)
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        True if table exists, False otherwise
        
    Example:
        >>> if table_exists("temp_data", "MY_SCHEMA", "MY_DATABASE", context="main"):
        ...     print("Table exists")
        
        >>> # Reuse context in tests
        >>> ctx = SnowflakeContext(profile="main")
        >>> exists1 = table_exists("TABLE1", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        >>> exists2 = table_exists("TABLE2", "MY_SCHEMA", "MY_DATABASE", context=ctx)
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Get list of all tables in schema
    tables = list_tables(schema, database, context, **overrides)
    
    # Check if table is in list (case-insensitive comparison)
    table_upper = table.upper()
    return table_upper in [t.upper() for t in tables]


def get_current_database(context: Union[str, SnowflakeContext], **overrides: Any) -> str:
    """Get current database name.
    
    Args:
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides
        
    Returns:
        Current database name
        
    Example:
        >>> db = get_current_database(context="main")
        >>> print(f"Connected to database: {db}")
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_one
    
    result = fetch_one("SELECT CURRENT_DATABASE()", context=context, **overrides)
    return str(result[0]) if result else ""


def get_current_schema(context: Union[str, SnowflakeContext], **overrides: Any) -> str:
    """Get current schema name.
    
    Args:
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides
        
    Returns:
        Current schema name
        
    Example:
        >>> schema = get_current_schema(context="main")
        >>> print(f"Using schema: {schema}")
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_one
    
    result = fetch_one("SELECT CURRENT_SCHEMA()", context=context, **overrides)
    return str(result[0]) if result else ""


def get_current_warehouse(context: Union[str, SnowflakeContext], **overrides: Any) -> str:
    """Get current warehouse name.
    
    Args:
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides
        
    Returns:
        Current warehouse name
        
    Example:
        >>> wh = get_current_warehouse(context="main")
        >>> print(f"Using warehouse: {wh}")
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_one
    
    result = fetch_one("SELECT CURRENT_WAREHOUSE()", context=context, **overrides)
    return str(result[0]) if result else ""


def get_current_role(context: Union[str, SnowflakeContext], **overrides: Any) -> str:
    """Get current role name.
    
    Args:
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides
        
    Returns:
        Current role name
        
    Example:
        >>> role = get_current_role(context="main")
        >>> print(f"Using role: {role}")
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Import here to avoid circular dependency
    from snowlib.primitives.execution import fetch_one
    
    result = fetch_one("SELECT CURRENT_ROLE()", context=context, **overrides)
    return str(result[0]) if result else ""
