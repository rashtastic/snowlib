"""A unified, simplified interface for Snowflake query results"""

import logging
import warnings
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from snowlib.utils.connector import connector_version

try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    pa = None  # type: ignore
    HAS_PYARROW = False


logger = logging.getLogger(__name__)

# Connector 4.3 added force_microsecond_precision. Without it, each Arrow batch
# picks its own timestamp unit: nanoseconds, unless the batch holds a value
# outside pandas' nanosecond range (years 1677 to 2262, so a 9999-12-31
# sentinel), in which case microseconds. A result with both kinds of batch then
# fails to combine with ArrowInvalid. Always asking for microseconds keeps every
# batch alike, and a column's dtype the same whatever the result's size.
MICROSECOND_PRECISION_SINCE = (4, 3)


def _arrow_fetch_kwargs() -> dict[str, Any]:
    """Keyword arguments for fetch_pandas_all and fetch_pandas_batches"""
    if connector_version() >= MICROSECOND_PRECISION_SINCE:
        return {"force_microsecond_precision": True}
    return {}


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

    def fetch_batches(
        self, lowercase_columns: bool = True
    ) -> Generator[pd.DataFrame, None, None]:
        """Fetch results in batches of DataFrames with optional column casing

        Falls back to a native fetch only if Arrow fails before any batch is
        yielded. After that the cursor is partly read, so a fallback would
        repeat or skip rows; the error is raised instead.
        """
        if HAS_PYARROW and self._use_arrow and pa is not None:
            yielded = 0
            try:
                for batch_df in self._cursor.fetch_pandas_batches(
                    **_arrow_fetch_kwargs()
                ):
                    if lowercase_columns:
                        batch_df.columns = batch_df.columns.str.lower()
                    yielded += 1
                    yield batch_df
                return
            except Exception as e:
                if yielded:
                    raise RuntimeError(
                        f"Arrow batch fetch failed after {yielded} batch(es); "
                        + "not falling back, because a native fetch of a partly "
                        + "read cursor would repeat or skip rows"
                    ) from e
                if pa is not None and isinstance(e, pa.ArrowInvalid):
                    warnings.warn(
                        "Arrow batch fetch failed due to schema mismatch "
                        + "(often caused by extreme dates like 9999-01-01). "
                        + f"Falling back to native fetch. Original error: {e}",
                        UserWarning,
                        stacklevel=2,
                    )
                elif "NotSupportedError" not in type(e).__name__:
                    # DDL/DML statements don't return Arrow format - silently fall back
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
                df = self._cursor.fetch_pandas_all(**_arrow_fetch_kwargs())
                if lowercase_columns and (not df.empty or len(df.columns) > 0):
                    df.columns = df.columns.str.lower()
                return df
            except pa.ArrowInvalid as e:
                warnings.warn(
                    "Arrow fetch failed due to schema mismatch "
                    + "(often caused by extreme dates like 9999-01-01). "
                    + f"Falling back to native fetch. Original error: {e}",
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
        return f"QueryResult(query_id='{self.query_id}', rowcount={self.rowcount})"
