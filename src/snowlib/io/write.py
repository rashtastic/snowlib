"""Write function with name resolution.

Provides high-level write() function that:
- Parses qualified table names (database.schema.table)
- Infers missing components from connection context
- Calls primitives layer with explicit parameters
"""

from typing import Union, Literal
import pandas as pd

from snowlib.primitives import SnowflakeContext, write_table as primitive_write_table
from snowlib.io.names import resolve_table_name


def write(
    df: pd.DataFrame,
    table: str,
    context: Union[str, SnowflakeContext],
    mode: Literal["replace", "append", "fail"] = "replace",
    uppercase_columns: bool = True,
    uppercase_table: bool = True,
    **overrides
) -> bool:
    """Write a pandas DataFrame to a Snowflake table with name resolution.
    
    This is the high-level convenience function. It:
    - Accepts qualified names: "database.schema.table", "schema.table", or "table"
    - Infers missing database/schema from connection context
    - Validates identifier format
    - Calls primitives.write_table with explicit parameters
    
    Args:
        df: pandas DataFrame to write
        table: Table name (optionally qualified)
            - "table" -> infer database and schema from connection
            - "schema.table" -> infer database from connection
            - "database.schema.table" -> fully explicit
        context: SnowflakeContext object or profile name
        mode: Write mode (default: "replace")
            - "replace": DROP IF EXISTS then create new table
            - "append": Add to existing table (create if not exists)
            - "fail": Raise error if table exists
        uppercase_columns: Convert column names to uppercase (default: True)
        uppercase_table: Convert table name to uppercase (default: True)
        **overrides: Connection parameter overrides
        
    Returns:
        True if successful
        
    Raises:
        ValueError: If name is invalid or context missing required info
        snowflake.connector.errors.*: Any Snowflake errors
        
    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        
        >>> # Fully qualified
        >>> write(df, "MY_DATABASE.MY_SCHEMA.my_table", context="main")
        
        >>> # Infer database from connection
        >>> write(df, "MY_SCHEMA.my_table", context="main")
        
        >>> # Infer both from connection (must have USE DATABASE/SCHEMA)
        >>> write(df, "my_table", context="main")
        
        >>> # Append mode
        >>> write(df, "MY_DATABASE.MY_SCHEMA.my_table", context="main", mode="append")
        
        >>> # Fail if exists
        >>> write(df, "MY_DATABASE.MY_SCHEMA.my_table", context="main", mode="fail")
        
        >>> # Reuse connection context
        >>> ctx = SnowflakeContext(profile="main")
        >>> write(df1, "table1", context=ctx)
        >>> write(df2, "table2", context=ctx)
        
        >>> # Use different profile
        >>> write(df, "MY_DATABASE.MY_SCHEMA.my_table", context="prod")
        
        >>> # Keep original casing
        >>> write(df, "MY_DATABASE.MY_SCHEMA.MyTable", context="main",
        ...       uppercase_columns=False, uppercase_table=False)
    """
    # Resolve the table name
    database, schema, table_name = resolve_table_name(table, context, **overrides)
    
    # Convert string profile to context if needed (for reuse)
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Call primitive with explicit parameters
    return primitive_write_table(
        df=df,
        table=table_name,
        schema=schema,
        database=database,
        mode=mode,
        context=context,
        uppercase_columns=uppercase_columns,
        uppercase_table=uppercase_table
    )
