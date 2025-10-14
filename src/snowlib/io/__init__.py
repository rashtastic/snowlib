"""Convenience read/write functions

Public API:
    read(table, ...) - Read table into DataFrame
    write(df, table, ...) - Write DataFrame to table
    query(sql, ...) - Execute SQL query, return DataFrame
"""

from snowlib.io.read import read, query
from snowlib.io.write import write

__all__ = ["read", "write", "query"]
