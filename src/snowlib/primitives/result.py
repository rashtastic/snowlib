"""Query execution result wrapper with metadata access.

Provides a clean interface for accessing query results and metadata
(query ID, row count, column info) without exposing raw cursor objects.
"""

from typing import Any, Optional
from dataclasses import dataclass
import pandas as pd


@dataclass
class QueryResult:
    """Wrapper for query execution results with metadata access.
    
    Provides convenient access to both query results and metadata like
    query ID, row count, and column information. Enables result caching,
    query monitoring, and async execution patterns.
    
    Attributes:
        _cursor: The underlying Snowflake cursor (private)
        
    Properties:
        query_id: Snowflake query ID (sfqid) for monitoring and caching
        rowcount: Number of rows affected/returned
        sql: The SQL statement that was executed
        description: Column metadata (name, type, size, etc.)
        
    Example:
        >>> result = execute_sql("DELETE FROM table WHERE x > 100")
        >>> print(f"Deleted {result.rowcount} rows")
        >>> print(f"Query ID: {result.query_id}")
        
        >>> result = execute_sql("SELECT * FROM table")
        >>> df = result.to_df()
        >>> print(f"Query {result.query_id}: loaded {len(df)} rows")
    """
    
    _cursor: Any  # SnowflakeCursor type (avoid import)
    
    @property
    def query_id(self) -> str:
        """Snowflake query ID (sfqid).
        
        Can be used for:
        - Monitoring queries in Snowflake UI
        - Retrieving cached results (24-hour cache)
        - Canceling running queries
        """
        return self._cursor.sfqid
    
    @property
    def rowcount(self) -> int:
        """Number of rows affected by DML or returned by SELECT.
        
        Returns -1 for DDL statements (CREATE, DROP, ALTER).
        """
        return self._cursor.rowcount if self._cursor.rowcount is not None else -1
    
    @property
    def sql(self) -> str:
        """The SQL statement that was executed."""
        return self._cursor.query
    
    @property
    def description(self) -> Optional[list[tuple]]:
        """Column metadata for query results.
        
        Returns list of tuples with:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        
        Returns None for non-SELECT statements.
        """
        return self._cursor.description
    
    def fetchone(self) -> Optional[tuple[Any, ...]]:
        """Fetch the next row of results.
        
        Returns:
            Tuple of column values, or None if no more rows
        """
        return self._cursor.fetchone()
    
    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows.
        
        Warning:
            Loads all results into memory. Use to_df() with batching for large results.
            
        Returns:
            List of tuples (one per row)
        """
        result = self._cursor.fetchall()
        return result if result else []
    
    def to_df(self, lowercase_columns: bool = True) -> pd.DataFrame:
        """Fetch results as pandas DataFrame.
        
        Uses fetch_pandas_all() which works for both sync and async cursors
        in modern Snowflake connector versions (3.0+).
        
        Args:
            lowercase_columns: Convert column names to lowercase (default: True)
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            # Modern snowflake-connector-python (3.0+) supports fetch_pandas_all()
            # for both sync and async cursors
            df = self._cursor.fetch_pandas_all()
        except AttributeError:
            # Fallback for mock cursors in unit tests
            if hasattr(self._cursor, 'fetchall') and self._cursor.description:
                columns = [desc[0] for desc in self._cursor.description]
                rows = self._cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
            else:
                df = pd.DataFrame()

        if lowercase_columns and not df.empty:
            df.columns = df.columns.str.lower()
        elif lowercase_columns and df.empty and len(df.columns) > 0:
            # Also lowercase columns for empty DataFrames
            df.columns = df.columns.str.lower()
            
        return df
    
    def __repr__(self) -> str:
        """String representation showing key metadata."""
        return (
            f"QueryResult(query_id='{self.query_id}', "
            f"rowcount={self.rowcount})"
        )
