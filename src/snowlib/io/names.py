"""Name resolution utilities for Snowflake identifiers.

This module provides functions for:
- Parsing qualified table names (database.schema.table)
- Validating Snowflake identifiers
- Resolving missing components using connection context
- Uppercasing and normalization
"""

import re
from typing import Optional, Union
from snowlib.primitives import SnowflakeContext, get_current_database, get_current_schema


def validate_identifier(name: str) -> bool:
    """Validate a Snowflake identifier.
    
    Args:
        name: Identifier to validate (table, schema, or database name)
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If identifier is invalid
        
    Rules:
        - Must start with letter or underscore
        - Can contain letters, numbers, underscores
        - Max 255 characters
        
    Example:
        >>> validate_identifier("my_table")
        True
        >>> validate_identifier("123bad")
        ValueError: Invalid identifier '123bad'
    """
    if not name:
        raise ValueError("Identifier cannot be empty")
    
    if len(name) > 255:
        raise ValueError(f"Identifier too long (max 255 chars): '{name}'")
    
    # Snowflake identifiers: start with letter/underscore, then letter/digit/underscore
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(
            f"Invalid identifier '{name}': must start with letter or underscore, "
            + "contain only letters, numbers, and underscores"
        )
    
    return True


def parse_table_name(
    table_name: str,
    default_schema: Optional[str] = None,
    default_database: Optional[str] = None
) -> tuple[str, str, str]:
    """Parse a potentially qualified table name.
    
    Supports formats:
        - "table" -> uses default_schema, default_database
        - "schema.table" -> uses default_database
        - "database.schema.table" -> fully qualified
        
    Args:
        table_name: Table name (optionally qualified)
        default_schema: Schema to use if not in table_name
        default_database: Database to use if not in table_name
        
    Returns:
        Tuple of (database, schema, table)
        
    Raises:
        ValueError: If name is invalid or missing required components
        
    Examples:
        >>> parse_table_name("MY_TABLE", "MY_SCHEMA", "MY_DATABASE")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
        
        >>> parse_table_name("MY_SCHEMA.MY_TABLE", default_database="MY_DATABASE")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
        
        >>> parse_table_name("MY_DATABASE.MY_SCHEMA.MY_TABLE")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
    """
    if not table_name:
        raise ValueError("Table name cannot be empty")
    
    parts = table_name.split(".")
    
    # Initialize to satisfy type checker
    database: str
    schema: str
    table: str
    
    if len(parts) == 1:
        # Just table name
        validate_identifier(parts[0])
        table = parts[0]
        
        if not default_schema:
            raise ValueError(
                f"Schema required for table '{table_name}'. "
                + "Provide as 'schema.table' or pass default_schema parameter"
            )
        if not default_database:
            raise ValueError(
                f"Database required for table '{table_name}'. "
                + "Provide as 'database.schema.table' or pass default_database parameter"
            )
        
        validate_identifier(default_schema)
        validate_identifier(default_database)
        schema = default_schema
        database = default_database
        
    elif len(parts) == 2:
        # schema.table
        validate_identifier(parts[0])
        validate_identifier(parts[1])
        schema = parts[0]
        table = parts[1]
        
        if not default_database:
            raise ValueError(
                f"Database required for table '{table_name}'. "
                + "Provide as 'database.schema.table' or pass default_database parameter"
            )
        
        validate_identifier(default_database)
        database = default_database
        
    elif len(parts) == 3:
        # database.schema.table
        validate_identifier(parts[0])
        validate_identifier(parts[1])
        validate_identifier(parts[2])
        database = parts[0]
        schema = parts[1]
        table = parts[2]
        
    else:
        raise ValueError(
            f"Invalid table name '{table_name}'. "
            + "Expected format: 'table', 'schema.table', or 'database.schema.table'"
        )
    
    return database, schema, table


def resolve_table_name(
    table_name: str,
    context: Union[str, SnowflakeContext],
    **overrides
) -> tuple[str, str, str]:
    """Resolve table name using connection context for missing components.
    
    Queries the connection to get current database/schema when not provided.
    
    Args:
        table_name: Table name (optionally qualified)
        context: SnowflakeContext or profile name
        **overrides: Connection overrides
        
    Returns:
        Tuple of (database, schema, table)
        
    Raises:
        ValueError: If name is invalid or context doesn't have required info
        
    Examples:
        >>> # Connection has USE DATABASE MY_DATABASE; USE SCHEMA MY_SCHEMA;
        >>> resolve_table_name("my_table", context="main")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
        
        >>> # Partial qualification
        >>> resolve_table_name("MY_SCHEMA.my_table", context="main")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
        
        >>> # Full qualification (no context needed)
        >>> resolve_table_name("MY_DATABASE.MY_SCHEMA.my_table", context="main")
        ('MY_DATABASE', 'MY_SCHEMA', 'MY_TABLE')
    """
    if not table_name:
        raise ValueError("Table name cannot be empty")
    
    parts = table_name.split(".")
    
    if len(parts) == 3:
        # Fully qualified - no need to query context
        return parse_table_name(table_name)
    
    # Need to get defaults from connection context
    # Convert string profile to context if needed
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Determine what we need
    need_database = len(parts) <= 2
    need_schema = len(parts) == 1
    
    default_database = None
    default_schema = None
    
    if need_database:
        default_database = get_current_database(context=context)
        if not default_database:
            raise ValueError(
                f"Cannot resolve database for '{table_name}'. "
                + "Connection has no current database. "
                + "Provide fully qualified name or set database in connection."
            )
    
    if need_schema:
        default_schema = get_current_schema(context=context)
        if not default_schema:
            raise ValueError(
                f"Cannot resolve schema for '{table_name}'. "
                + "Connection has no current schema. "
                + "Provide qualified name or set schema in connection."
            )
    
    return parse_table_name(table_name, default_schema, default_database)


def format_qualified_name(database: str, schema: str, table: str) -> str:
    """Format a fully qualified table name.
    
    Args:
        database: Database name
        schema: Schema name
        table: Table name
        
    Returns:
        Formatted name: "DATABASE.SCHEMA.TABLE"
        
    Example:
        >>> format_qualified_name("MY_DATABASE", "MY_SCHEMA", "my_table")
        'MY_DATABASE.MY_SCHEMA.MY_TABLE'
    """
    return f"{database.upper()}.{schema.upper()}.{table.upper()}"
