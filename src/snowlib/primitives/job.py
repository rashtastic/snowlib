"""Query job module for asynchronous query execution.

This module provides the QueryJob class which represents a query
that is executing asynchronously on Snowflake.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

    from .result import QueryResult


@dataclass(frozen=True)
class QueryJob:
    """Represents a query executing asynchronously on Snowflake.

    This object is returned by async execution functions and allows for
    checking the query's status and retrieving its result when complete.

    Attributes:
        query_id: The Snowflake query ID (sfqid) for this async query.
        sql: The SQL statement that was submitted.
        _conn: The SnowflakeConnection used to submit the query.

    Example:
        >>> job = execute_sql_async("SELECT COUNT(*) FROM large_table", conn)
        >>> print(f"Query submitted: {job.query_id}")
        >>> # Do other work...
        >>> result = job.get_result()  # Blocks until complete
        >>> print(result.rowcount)
    """

    query_id: str
    sql: str
    _conn: SnowflakeConnection

    def get_result(self) -> QueryResult:
        """Block until the query completes and return the results.

        This method creates a new cursor and uses the `get_results_from_sfqid`
        method, which polls Snowflake until the query finishes and then
        retrieves the result set.

        Returns:
            QueryResult: The final result of the query, with the same interface
                as a synchronous query result.

        Raises:
            ProgrammingError: If the query failed during execution.
            DatabaseError: If there was a problem retrieving the results.

        Example:
            >>> job = execute_sql_async("SELECT * FROM table", conn)
            >>> result = job.get_result()  # Waits for completion
            >>> df = result.to_df()
        """
        from .result import QueryResult

        cursor = self._conn.cursor()
        try:
            cursor.get_results_from_sfqid(self.query_id)
            # QueryResult takes ownership of the cursor and will close it when done
            return QueryResult(_cursor=cursor)
        except Exception:
            # Clean up cursor on any error and re-raise
            cursor.close()
            raise

    @property
    def status(self) -> str:
        """Return the current status of the query as a string.

        Possible values include: 'RUNNING', 'SUCCESS', 'FAILED_WITH_ERROR', 'ABORTED', etc.
        This is a direct value from the Snowflake Connector's QueryStatus enum.

        Returns:
            str: The name of the current query status.
        """
        status_enum = self._conn.get_query_status(self.query_id)
        return status_enum.name

    def is_running(self) -> bool:
        """Check if the query is still running.

        Returns:
            bool: True if the query is still running (in states like RUNNING, QUEUED, etc.),
                False otherwise.

        Example:
            >>> job = execute_sql_async("SELECT * FROM table", conn)
            >>> while job.is_running():
            ...     print("Still running...")
            ...     time.sleep(1)
            >>> result = job.get_result()
        """
        status_enum = self._conn.get_query_status(self.query_id)
        return self._conn.is_still_running(status_enum)

    def abort(self) -> bool:
        """Abort the running query.

        Returns:
            bool: True if the abort request was successful, False otherwise.

        Example:
            >>> job = execute_sql_async("SELECT * FROM huge_table", conn)
            >>> # Changed our mind...
            >>> job.abort()
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(f"SELECT SYSTEM$CANCEL_QUERY('{self.query_id}')")
            result = cursor.fetchone()
            return result is not None and "cancelled" in result[0]
        finally:
            cursor.close()
