"""SQL execution primitives.

Plain functions for executing SQL statements and fetching results.
These are thin wrappers around snowflake.connector cursor operations.
"""

from typing import Any, Union, Optional
from io import StringIO
import pandas as pd

from snowlib.primitives.context import SnowflakeContext
from snowlib.primitives.result import QueryResult
from snowlib.primitives.job import QueryJob


def execute_sql(
    sql: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> QueryResult:
    """Execute SQL statement and return result with metadata.
    
    Use for: DDL (CREATE/DROP/ALTER), DML (INSERT/UPDATE/DELETE)
    
    Args:
        sql: SQL statement to execute
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides for connection creation (only used if context is a string)
        
    Returns:
        QueryResult object with access to rowcount, query_id, and metadata
        
    Example:
        >>> result = execute_sql("DELETE FROM temp_table WHERE processed = TRUE", context="main")
        >>> print(f"Deleted {result.rowcount} rows")
        >>> print(f"Query ID: {result.query_id}")
        
        >>> # Reuse context in tests (only authenticates once)
        >>> ctx = SnowflakeContext(profile="main")
        >>> execute_sql("CREATE TABLE test1 (id INT)", context=ctx)
        >>> execute_sql("CREATE TABLE test2 (id INT)", context=ctx)
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    cursor = context.cursor.execute(sql)
    return QueryResult(_cursor=cursor)


def fetch_one(
    sql: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> Optional[tuple[Any, ...]]:
    """Execute query and return first row as tuple.
    
    Args:
        sql: SQL SELECT query
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        First row as tuple, or None if no results
        
    Example:
        >>> row = fetch_one("SELECT CURRENT_TIMESTAMP()", context="main")
        >>> print(row[0])
        
        >>> row = fetch_one("SELECT COUNT(*) FROM large_table", context="main")
        >>> count = row[0]
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    context.cursor.execute(sql)
    return context.cursor.fetchone()


def fetch_all(
    sql: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> list[tuple[Any, ...]]:
    """Execute query and return all rows as list of tuples.
    
    Args:
        sql: SQL SELECT query
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        List of tuples (one per row), empty list if no results
        
    Example:
        >>> rows = fetch_all("SELECT id, name FROM users WHERE active = TRUE", context="main")
        >>> for user_id, name in rows:
        ...     print(f"{user_id}: {name}")
        
    Warning:
        Loads all results into memory. Use fetch_batches() for large result sets.
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    context.cursor.execute(sql)
    result = context.cursor.fetchall()
    return result if result else []


def fetch_df(
    sql: str,
    context: Union[str, SnowflakeContext],
    lowercase_columns: bool = True,
    **overrides: Any
) -> pd.DataFrame:
    """Execute query and return pandas DataFrame.
    
    Uses fetch_pandas_batches() for efficient memory usage.
    Automatically handles empty results with proper column structure.
    
    Args:
        sql: SQL SELECT query
        context: SnowflakeContext object or profile name
        lowercase_columns: Convert column names to lowercase (default: True)
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        pandas DataFrame with query results
        
    Example:
        >>> df = fetch_df("SELECT * FROM sales WHERE date > '2025-01-01'", context="main")
        >>> print(df.shape)
        (1500, 8)
        
        >>> # Keep uppercase columns
        >>> df = fetch_df("SELECT * FROM MY_TABLE", context="main", lowercase_columns=False)
        
    Note:
        Uses fetch_pandas_batches for efficient memory usage
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    cursor = context.cursor.execute(sql)
    result = QueryResult(_cursor=cursor)
    return result.to_df(lowercase_columns=lowercase_columns)


def execute_block(
    sql: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> list[list[Any]]:
    """Execute a block of SQL statements and return all results.
    
    Uses execute_stream() to handle multiple statements separated by semicolons.
    Useful for running SQL scripts or multi-statement operations.
    
    Args:
        sql: Block of SQL statements (semicolon-separated)
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides (only used if context is a string)
        
    Returns:
        List of results, one per statement. Each result is a list of rows.
        
    Example:
        >>> sql_block = '''
        ... CREATE TEMP TABLE temp_data (id INT);
        ... INSERT INTO temp_data VALUES (1), (2), (3);
        ... SELECT * FROM temp_data;
        ... '''
        >>> results = execute_block(sql_block, context="main")
        >>> print(results[-1])  # Results from SELECT
        [[1], [2], [3]]
        
    Note:
        Based on snow6/core.py ExecuteBlock() pattern using execute_stream().
        Note: execute_stream requires connection object.
        
    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)
    
    results: list[list[Any]] = []
    
    # execute_stream needs the connection object
    for cursor_result in context.connection.execute_stream(StringIO(sql)):
        results.append([row for row in cursor_result])
    
    return results


def execute_sql_async(
    sql: str,
    context: Union[str, SnowflakeContext],
    **overrides: Any
) -> QueryJob:
    """Execute SQL statement asynchronously and return a job handle.

    Submits the query to Snowflake and returns immediately without waiting
    for the query to complete. Use the returned QueryJob to check status
    and retrieve results when ready.

    Args:
        sql: SQL statement to execute
        context: SnowflakeContext object or profile name
        **overrides: Runtime overrides for connection creation (only used if context is a string)

    Returns:
        QueryJob object that can be used to monitor and retrieve results

    Example:
        >>> # Submit a long-running query
        >>> job = execute_sql_async("SELECT * FROM large_table", context="main")
        >>> print(f"Query submitted: {job.query_id}")
        
        >>> # Do other work...
        >>> import time
        >>> while not job.is_done():
        ...     print("Still running...")
        ...     time.sleep(5)
        
        >>> # Get the results when ready
        >>> result = job.get_result()
        >>> print(f"Query completed. Rows: {result.rowcount}")
        
        >>> # Or just block until complete
        >>> job = execute_sql_async("SELECT COUNT(*) FROM huge_table", context="main")
        >>> result = job.get_result()  # Waits for completion

    Note:
        Ensure ABORT_DETACHED_QUERY is FALSE (default) to prevent queries
        from being aborted if the connection is lost. See Snowflake
        documentation for details on async query behavior.

    Raises:
        snowflake.connector.errors.*: Any Snowflake-specific errors
        RuntimeError: If the query submission fails to return a queryId
    """
    # Convert string profile to context
    if isinstance(context, str):
        context = SnowflakeContext(profile=context, **overrides)

    # Execute asynchronously - returns dict with queryId
    cursor = context.cursor
    response_data = cursor.execute_async(sql)
    
    query_id = response_data.get("queryId")
    if not query_id:
        raise RuntimeError(
            f"Failed to get queryId from async execution response. Response: {response_data}"
        )

    return QueryJob(query_id=query_id, sql=sql, _conn=context.connection)
