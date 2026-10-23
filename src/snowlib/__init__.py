"""
snowlib - Python-Snowflake utilities

Code is organized in layers
- config/ and connection/ as the interface for Snowflake packages
- primitives/ wraps these in low-level functions
- client/ is the beginning of higher-level convenience functions

Likely to expand to cortex and leaning more into snowpark
"""

# Layer 1: Core connectivity
from snowlib.connection import (
    SnowflakeConnector,
    SnowparkConnector,
    list_profiles,
    load_profile,
)
from snowlib.context import SnowflakeContext

# Layer 3: Models (OOP interface for Snowflake objects)
from snowlib.models import (
    Column,
    Database,
    DynamicTable,
    MaterializedView,
    Schema,
    Show,
    Stage,
    StageObject,
    Table,
    View,
)

# Layer 2: Primitives
from snowlib.primitives import (
    AsyncQuery,
    # Query results
    QueryResult,
    execute_block,
    # Execution
    execute_sql,
    execute_sql_async,
    query,
)

# Session: Context-bound convenience layer
from snowlib.session import Session, create_session
from snowlib.utils.ddl import DdlTarget

__version__ = "0.5.0"
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
    # Layer 3: Models (OOP interface)
    "Database",
    "Schema",
    "Table",
    "View",
    "MaterializedView",
    "DynamicTable",
    "Column",
    "Stage",
    "StageObject",
    "Show",
    "DdlTarget",
    # Session
    "Session",
    "create_session",
]
