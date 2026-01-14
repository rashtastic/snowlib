"""A unified, simplified interface for Snowflake query results"""
from typing import Any, Generator, Optional
from dataclasses import dataclass, field
import logging
import warnings

import pandas as pd

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    pa = None  # type: ignore
    HAS_PYARROW = False


logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """A unified, simplified interface for Snowflake query results"""
    _cursor: Any
    _use_arrow: bool = field(default=True, repr=False)
    
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

    def _fetch_native_df(self, lowercase_columns: bool = True) -> pd.DataFrame:
        """Fetch results using native Python types (slower but handles edge cases)"""
        if self._cursor.description:
            columns = [desc[0] for desc in self._cursor.description]
            rows = self._cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
        else:
            df = pd.DataFrame()
        
        if lowercase_columns and (not df.empty or len(df.columns) > 0):
            df.columns = df.columns.str.lower()
        
        return df

    def fetch_batches(self, lowercase_columns: bool = True) -> Generator[pd.DataFrame, None, None]:
        """Fetch results in batches of DataFrames with optional column casing"""
        if HAS_PYARROW and self._use_arrow and pa is not None:
            try:
                for batch_df in self._cursor.fetch_pandas_batches():
                    if lowercase_columns:
                        batch_df.columns = batch_df.columns.str.lower()
                    yield batch_df
                return
            except pa.ArrowInvalid as e:
                warnings.warn(
                    f"Arrow batch fetch failed due to schema mismatch (often caused by extreme dates like 9999-01-01). Falling back to native fetch. Original error: {e}",
                    UserWarning,
                    stacklevel=2,
                )
                # Fall through to native fetch below
            except Exception as e:
                # DDL/DML statements don't return Arrow format - silently fall back
                if "NotSupportedError" in type(e).__name__:
                    pass  # Expected for DDL/DML, fall through to native fetch
                else:
                    warnings.warn(
                        f"Arrow batch fetch failed unexpectedly. Falling back to native fetch. Error: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        
        # Fallback: fetch all at once using native types
        df = self._fetch_native_df(lowercase_columns=lowercase_columns)
        if not df.empty:
            yield df

    def to_df(self, lowercase_columns: bool = True) -> pd.DataFrame:
        """Fetch all results as a single DataFrame with optional column casing"""
        if HAS_PYARROW and self._use_arrow and pa is not None:
            try:
                df = self._cursor.fetch_pandas_all()
                if lowercase_columns and (not df.empty or len(df.columns) > 0):
                    df.columns = df.columns.str.lower()
                return df
            except pa.ArrowInvalid as e:
                warnings.warn(
                    f"Arrow fetch failed due to schema mismatch (often caused by extreme dates like 9999-01-01). Falling back to native fetch. Original error: {e}",
                    UserWarning,
                    stacklevel=2,
                )
                # Fall through to native fetch below
            except Exception as e:
                # DDL/DML statements don't return Arrow format - silently fall back
                # Common case: NotSupportedError when _query_result_format != "arrow"
                if "NotSupportedError" in type(e).__name__:
                    pass  # Expected for DDL/DML, fall through to native fetch
                else:
                    warnings.warn(
                        f"Arrow fetch failed unexpectedly. Falling back to native fetch. Error: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
        
        return self._fetch_native_df(lowercase_columns=lowercase_columns)
    
    def __repr__(self) -> str:
        """String representation"""
        return (
            f"QueryResult(query_id='{self.query_id}', "
            f"rowcount={self.rowcount})"
        )
