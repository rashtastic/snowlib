"""Snowflake connection context management"""

import warnings
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
            self._validate_session_context()

        return self._connection

    @property
    def cursor(self) -> Any:
        """Get Snowflake cursor, creating if needed
        
        Note: This cached cursor is used for connection validation and internal operations.
        For query execution, use new_cursor() to avoid thread-safety issues.
        """
        if self._cursor is None:
            self._cursor = self.connection.cursor()

        return self._cursor

    def new_cursor(self) -> Any:
        """Create a new cursor for query execution.
        
        Each query should use its own cursor to ensure thread-safety.
        The cursor holds result state internally, so sharing a cursor between
        concurrent queries causes race conditions where one query's results
        can be overwritten by another before being fetched.
        
        Creating multiple cursors from the same connection does not trigger
        re-authentication - the connection holds auth state, not the cursor.
        """
        return self.connection.cursor()

    def _validate_session_context(self) -> None:
        """Validate that declared connection values match actual session values"""
        if self._connector is None:
            return
        
        cfg = self._connector._cfg
        
        # Map config keys to (CURRENT_*() query, friendly name)
        validations = [
            ("warehouse", "SELECT CURRENT_WAREHOUSE()", "warehouse"),
            ("role", "SELECT CURRENT_ROLE()", "role"),
            ("database", "SELECT CURRENT_DATABASE()", "database"),
            ("schema", "SELECT CURRENT_SCHEMA()", "schema"),
        ]
        
        for config_key, query, friendly_name in validations:
            declared = cfg.get(config_key)
            if declared:
                assert self._cursor is not None
                self._cursor.execute(query)
                result = self._cursor.fetchone()
                actual = result[0] if result and result[0] else None
                
                if actual is None:
                    warnings.warn(
                        f"Declared {friendly_name} '{declared}' is not active in session. The {friendly_name} may be suspended or inaccessible.",
                        UserWarning,
                        stacklevel=4,
                    )
                elif declared.upper() != actual.upper():
                    warnings.warn(
                        f"Declared {friendly_name} '{declared}' does not match session {friendly_name} '{actual}'.",
                        UserWarning,
                        stacklevel=4,
                    )

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
