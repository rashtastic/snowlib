"""Primitive operations to wrap direct Snowflake connector calls"""

from snowlib.context import SnowflakeContext

from snowlib.primitives.execute import (
    execute_sql,
    execute_sql_async,
    execute_block,
    query,
    Executor,  # Internal use, not in __all__
)

from snowlib.primitives.result import QueryResult

from snowlib.primitives.async_query import AsyncQuery

__all__ = [
    # Context
    "SnowflakeContext",
    # Query execution
    "execute_sql",
    "execute_sql_async",
    "execute_block",
    "query",
    # Query results
    "QueryResult",
    "AsyncQuery",
]
