"""
snowlib - Python-Snowflake utilities

Code is organized in layers
- config/ and connection/ as the interface for Snowflake packages
- primitives/ wraps these in low-level functions
- client/ is the beginning of higher-level convenience functions

Likely to expand to cortex and leaning more into snowpark
"""

# Layer 1: Core connectivity
from snowlib.connection import load_profile, list_profiles
from snowlib.connection import SnowflakeConnector, SnowparkConnector
from snowlib.context import SnowflakeContext

# Layer 2: Primitives
from snowlib.primitives import (
    # Query results
    QueryResult,
    AsyncQuery,
    # Execution
    execute_sql,
    execute_sql_async,
    execute_block,
    query,
    Executor,
)

# Layer 3: Models (OOP interface for Snowflake objects)
from snowlib.models import (
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    DynamicTable,
    Column,
    Show,
)

__version__ = "0.3.0"
__all__ = [
    # Layer 1: Configuration & Connection
    "load_profile", 
    "list_profiles",
    "SnowflakeConnector",
    "SnowparkConnector",
    # Layer 2: Context
    "SnowflakeContext",
    "QueryResult",
    "AsyncQuery",
    # Layer 2: Execution
    "execute_sql",
    "execute_sql_async",
    "execute_block",
    "query",
    "Executor",
    # Layer 3: Models (OOP interface)
    "Database",
    "Schema",
    "Table",
    "View",
    "MaterializedView",
    "DynamicTable",
    "Column",
    "Show",
]