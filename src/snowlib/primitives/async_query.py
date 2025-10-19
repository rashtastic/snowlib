"""Asynchronous query execution handler"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .result import QueryResult

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection


@dataclass(frozen=True)
class AsyncQuery:
    """Represents a query executing asynchronously on Snowflake"""

    query_id: str
    sql: str
    _conn: SnowflakeConnection

    def get_result(self) -> QueryResult:
        """Block until the query completes and return the results"""
        cursor = self._conn.cursor()
        try:
            cursor.get_results_from_sfqid(self.query_id)
            return QueryResult(_cursor=cursor)
        except Exception:
            cursor.close()
            raise

    @property
    def status(self) -> str:
        """Return the current status of the query as a string"""
        status_enum = self._conn.get_query_status(self.query_id)
        return status_enum.name

    def is_running(self) -> bool:
        """Check if the query is still running"""
        status_enum = self._conn.get_query_status(self.query_id)
        return self._conn.is_still_running(status_enum)
    
    def is_done(self) -> bool:
        """Check if the query has completed (successfully or with error).

        Returns:
            bool: True if the query is no longer running, False otherwise.

        Example:
            >>> job = execute_sql_async("SELECT * FROM table", conn)
            >>> while not job.is_done():
            ...     print("Still running...")
            ...     time.sleep(1)
            >>> result = job.get_result()
        """
        return not self.is_running()

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
