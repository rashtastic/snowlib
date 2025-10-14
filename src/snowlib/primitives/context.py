"""Snowflake connection context for primitives.

Provides a clean abstraction for managing connection lifecycle in primitives.
"""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from snowlib.connection import SnowflakeConnector


class SnowflakeContext:
    """Manages Snowflake connection and cursor lifecycle for primitives.

    This class provides lazy initialization of connections and cursors,
    allowing primitives to reuse connections efficiently while maintaining
    a simple API.

    You can:
    - Pass a profile name (creates connection on demand)
    - Pass an existing connection (creates cursor on demand)
    - Pass both connection and cursor (reuses both)

    The connection and cursor are created lazily - only when accessed.
    This allows tests to reuse a single context across multiple primitive calls,
    avoiding repeated SSO authentication.

    Example:
        >>> # Simple usage - profile creates connection on demand
        >>> ctx = SnowflakeContext(profile="dev")
        >>> execute_sql("CREATE TABLE test (id INT)", context=ctx)
        >>> df = fetch_df("SELECT * FROM test", context=ctx)

        >>> # Reuse existing connection
        >>> connector = SnowflakeConnector()
        >>> with connector as (conn, cur):
        ...     ctx = SnowflakeContext(connection=conn, cursor=cur)
        ...     execute_sql("...", context=ctx)
        ...     fetch_df("...", context=ctx)

        >>> # Tests can create once, reuse many times
        >>> ctx = SnowflakeContext()
        >>> execute_sql("CREATE TABLE t1 (id INT)", context=ctx)
        >>> execute_sql("CREATE TABLE t2 (id INT)", context=ctx)
        >>> # Only authenticates once

    Note:
        When using profile-based initialization, the context manages its own
        connector lifecycle. When using explicit connection/cursor, the caller
        is responsible for closing them.
    """

    def __init__(
        self,
        profile: Optional[str] = None,
        connection: Optional[Any] = None,
        cursor: Optional[Any] = None,
        **overrides: Any,
    ):
        """Initialize Snowflake context.

        Args:
            profile: Profile name for lazy connection creation
            connection: Existing Snowflake connection object
            cursor: Existing Snowflake cursor object
            **overrides: Runtime overrides for connection creation
            
        Note:
            Must provide either `profile` or `connection` (not both, not neither).
        """
        if profile is None and connection is None:
            raise ValueError(
                "SnowflakeContext requires either 'profile' or 'connection'. " +
                "Cannot create context without a connection source."
            )
        if profile is not None and connection is not None:
            raise ValueError(
                "SnowflakeContext: provide either 'profile' or 'connection', not both. " +
                "Use profile for lazy connection creation, or connection for reuse."
            )
        
        self._profile = profile
        self._connection = connection
        self._cursor = cursor
        self._overrides = overrides
        self._connector: Optional["SnowflakeConnector"] = None
        self._owns_connector = False

    @property
    def connection(self) -> Any:
        """Get Snowflake connection, creating if needed.

        Returns:
            Snowflake connection object
        """
        if self._connection is None:
            # Need to create connection from profile
            from snowlib.connection import SnowflakeConnector

            # We know profile is not None because of validation in __init__
            assert self._profile is not None
            self._connector = SnowflakeConnector(
                profile=self._profile, **self._overrides
            )
            conn, cur = self._connector.connect()
            self._connection = conn
            self._cursor = cur  # Cache the cursor too
            self._owns_connector = True

        return self._connection

    @property
    def cursor(self) -> Any:
        """Get Snowflake cursor, creating if needed.

        Returns:
            Snowflake cursor object
        """
        if self._cursor is None:
            # Create cursor from connection
            self._cursor = self.connection.cursor()

        return self._cursor

    def close(self) -> None:
        """Close connection if owned by this context.

        Only closes connections created by this context (via profile).
        Does not close externally-provided connections.
        """
        if self._owns_connector and self._connector is not None:
            self._connector.close()
            self._connector = None
            self._connection = None
            self._cursor = None

    def __enter__(self) -> "SnowflakeContext":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes owned connections."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        if self._connection is not None:
            return f"SnowflakeContext(connection=<active>)"
        else:
            return f"SnowflakeContext(profile='{self._profile}')"
