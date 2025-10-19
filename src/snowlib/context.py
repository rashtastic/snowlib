"""Snowflake connection context management"""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from snowlib.connection import SnowflakeConnector


class SnowflakeContext:
    """Manages Snowflake connection and cursor lifecycle with lazy initialization"""

    def __init__(
        self,
        profile: Optional[str] = None,
        connection: Optional[Any] = None,
        cursor: Optional[Any] = None,
        **overrides: Any,
    ):
        """Initialize Snowflake context with profile or connection"""
        if profile is None and connection is None:
            raise ValueError(
                "SnowflakeContext requires either 'profile' or 'connection'"
            )
        if profile is not None and connection is not None:
            raise ValueError(
                "SnowflakeContext: provide either 'profile' or 'connection', not both"
            )
        
        self._profile = profile
        self._connection = connection
        self._cursor = cursor
        self._overrides = overrides
        self._connector: Optional["SnowflakeConnector"] = None
        self._owns_connector = False

    @property
    def connection(self) -> Any:
        """Get Snowflake connection, creating if needed"""
        if self._connection is None:
            from snowlib.connection import SnowflakeConnector

            assert self._profile is not None
            self._connector = SnowflakeConnector(
                profile=self._profile, **self._overrides
            )
            conn, cur = self._connector.connect()
            self._connection = conn
            self._cursor = cur
            self._owns_connector = True

        return self._connection

    @property
    def cursor(self) -> Any:
        """Get Snowflake cursor, creating if needed"""
        if self._cursor is None:
            self._cursor = self.connection.cursor()

        return self._cursor

    def close(self) -> None:
        """Close connection if owned by this context"""
        if self._owns_connector and self._connector is not None:
            self._connector.close()
            self._connector = None
            self._connection = None
            self._cursor = None

    def __enter__(self) -> "SnowflakeContext":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()

    @property
    def current_database(self) -> str:
        """Get current database from session context"""
        result = self.cursor.execute("SELECT CURRENT_DATABASE()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_schema(self) -> str:
        """Get current schema from session context"""
        result = self.cursor.execute("SELECT CURRENT_SCHEMA()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_warehouse(self) -> str:
        """Get current warehouse from session context"""
        result = self.cursor.execute("SELECT CURRENT_WAREHOUSE()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_role(self) -> str:
        """Get current role from session context"""
        result = self.cursor.execute("SELECT CURRENT_ROLE()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_user(self) -> str:
        """Get current user from session context"""
        result = self.cursor.execute("SELECT CURRENT_USER()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_account(self) -> str:
        """Get current account identifier from session context"""
        result = self.cursor.execute("SELECT CURRENT_ACCOUNT()").fetchone()
        return str(result[0]) if result and result[0] else ""
    
    @property
    def current_region(self) -> str:
        """Get current region from session context"""
        result = self.cursor.execute("SELECT CURRENT_REGION()").fetchone()
        return str(result[0]) if result and result[0] else ""

    def __repr__(self) -> str:
        """String representation"""
        if self._connection is not None:
            return f"SnowflakeContext(connection=<active>)"
        else:
            return f"SnowflakeContext(profile='{self._profile}')"
