"""Primitive operations to wrap direct Snowflake connector calls"""

from snowlib.primitives.context import SnowflakeContext
from snowlib.primitives.result import QueryResult
from snowlib.primitives.job import QueryJob

from snowlib.primitives.execution import (
    execute_sql,
    execute_sql_async,
    fetch_one,
    fetch_all,
    fetch_df,
    execute_block,
)

from snowlib.primitives.data import (
    read_table,
    write_table,
)

from snowlib.primitives.metadata import (
    get_columns,
    table_exists,
    list_tables,
    get_current_database,
    get_current_schema,
    get_current_warehouse,
    get_current_role,
)

from snowlib.primitives.streaming import (
    fetch_batches,
)

__all__ = [
    # Context
    "SnowflakeContext",
    "QueryResult",
    "QueryJob",
    # Execution
    "execute_sql",
    "execute_sql_async",
    "fetch_one",
    "fetch_all",
    "fetch_df",
    "execute_block",
    # Data I/O
    "read_table",
    "write_table",
    # Metadata
    "get_columns",
    "table_exists",
    "list_tables",
    "get_current_database",
    "get_current_schema",
    "get_current_warehouse",
    "get_current_role",
    # Streaming
    "fetch_batches",
]
