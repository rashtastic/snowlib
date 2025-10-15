"""
snowlib - Python-Snowflake utilities

Code is organized in layers
- config/ and connection/ as the interface for Snowflake packages
- primitives/ wraps these in low-level functions
- io/ is the beginning of higher-level convenience functions

Likely to expand to cortex and leaning more into snowpark
"""

# Layer 1: Core connectivity
from snowlib.config import load_profile, list_profiles
from snowlib.connection import SnowflakeConnector, SnowparkConnector

# Layer 2: Primitives
from snowlib.primitives import (
    # Context
    SnowflakeContext,
    QueryResult,
    QueryJob,
    # Execution
    execute_sql,
    execute_sql_async,
    fetch_one,
    fetch_all,
    fetch_df,
    execute_block,
    # Data I/O
    read_table,
    write_table,
    # Metadata
    get_columns,
    table_exists,
    list_tables,
    get_current_database,
    get_current_schema,
    get_current_warehouse,
    get_current_role,
    # Streaming
    fetch_batches,
)

# Layer 3: I/O with name resolution
from snowlib.io import read, write, query

__version__ = "0.1.1"
__all__ = [
    # Layer 1
    "load_profile", 
    "list_profiles",
    "SnowflakeConnector",
    "SnowparkConnector",
    # Layer 2: Context
    "SnowflakeContext",
    "QueryResult",
    "QueryJob",
    # Layer 2: Execution
    "execute_sql",
    "execute_sql_async",
    "fetch_one",
    "fetch_all",
    "fetch_df",
    "execute_block",
    # Layer 2: Data I/O
    "read_table",
    "write_table",
    # Layer 2: Metadata
    "get_columns",
    "table_exists",
    "list_tables",
    "get_current_database",
    "get_current_schema",
    "get_current_warehouse",
    "get_current_role",
    # Layer 2: Streaming
    "fetch_batches",
    # Layer 3: I/O
    "read",
    "write",
    "query",
]
