"""Streaming primitives.

Functions for handling large result sets via batching/streaming.
"""

from typing import Any, Generator, Union
import pandas as pd

from snowlib.primitives.context import SnowflakeContext


def fetch_batches(
    sql: str,
    context: Union[str, SnowflakeContext],
    batch_size: int = 10000,
    lowercase_columns: bool = True,
    **overrides: Any
) -> Generator[pd.DataFrame, None, None]:
    """Execute query and yield results in DataFrame batches.
    
    Memory-efficient for large result sets.
    
    Args:
        sql: SQL SELECT query
        context: SnowflakeContext object or profile name
        batch_size: Rows per batch (default: 10000)
        lowercase_columns: Convert column names to lowercase (default: True)
        **overrides: Runtime overrides (only used if context is a string)
        
    Yields:
        pandas DataFrame batches
        
    Example:
        >>> for batch_df in fetch_batches("SELECT * FROM huge_table", context="main"):
        ...     process_batch(batch_df)
        ...     print(f"Processed {len(batch_df)} rows")
        
        >>> # Custom batch size
        >>> for batch_df in fetch_batches("SELECT * FROM data", context="main", batch_size=50000):
        ...     upload_to_s3(batch_df)
        
        >>> # Reuse context in tests
        >>> ctx = SnowflakeContext(profile="main")
        >>> for batch in fetch_batches("SELECT * FROM table1", context=ctx):
        ...     process(batch)
        >>> for batch in fetch_batches("SELECT * FROM table2", context=ctx):
        ...     process(batch)
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context object
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    # Execute query
    context.cursor.execute(sql)
    
    # Yield batches from fetch_pandas_batches
    for batch_df in context.cursor.fetch_pandas_batches():
        if lowercase_columns:
            batch_df.columns = batch_df.columns.str.lower()
        yield batch_df
