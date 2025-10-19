"""A unified, simplified interface for Snowflake query results"""
from typing import Any, Generator, Optional
from dataclasses import dataclass
import pandas as pd

try:
    import pyarrow  # noqa: F401
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@dataclass
class QueryResult:
    """A unified, simplified interface for Snowflake query results"""
    _cursor: Any
    
    @property
    def query_id(self) -> str:
        """The Snowflake query ID (sfqid)"""
        return self._cursor.sfqid
    
    @property
    def rowcount(self) -> int:
        """The number of rows affected or returned"""
        return self._cursor.rowcount if self._cursor.rowcount is not None else -1
    
    @property
    def sql(self) -> str:
        """The SQL statement that was executed"""
        return self._cursor.query
    
    @property
    def description(self) -> Optional[list[tuple]]:
        """A description of the result columns"""
        return self._cursor.description
    
    def fetch_one(self) -> Optional[tuple[Any, ...]]:
        """Fetch the next row of a query result set"""
        return self._cursor.fetchone()

    def fetch_all(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result set"""
        result = self._cursor.fetchall()
        return result if result else []

    def fetch_batches(self, lowercase_columns: bool = True) -> Generator[pd.DataFrame, None, None]:
        """Fetch results in batches of DataFrames with optional column casing"""
        if HAS_PYARROW:
            for batch_df in self._cursor.fetch_pandas_batches():
                if lowercase_columns:
                    batch_df.columns = batch_df.columns.str.lower()
                yield batch_df
        else:
            # Fallback: fetch all at once (not ideal but works)
            df = self.to_df(lowercase_columns=lowercase_columns)
            if not df.empty:
                yield df

    def to_df(self, lowercase_columns: bool = True) -> pd.DataFrame:
        """Fetch all results as a single DataFrame with optional column casing"""
        if HAS_PYARROW:
            df = self._cursor.fetch_pandas_all()
        else:
            # Fallback for Python 3.14 or when pandas extras not installed
            if self._cursor.description:
                columns = [desc[0] for desc in self._cursor.description]
                rows = self._cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
            else:
                df = pd.DataFrame()

        if lowercase_columns and (not df.empty or len(df.columns) > 0):
            df.columns = df.columns.str.lower()
            
        return df
    
    def __repr__(self) -> str:
        """String representation"""
        return (
            f"QueryResult(query_id='{self.query_id}', "
            f"rowcount={self.rowcount})"
        )
