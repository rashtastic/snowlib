"""Snowflake connection management with profile support."""

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
import snowflake.connector
from typing import Optional, Tuple, Any, Literal

from .base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """
    Snowflake connection manager with TOML profile support and context manager protocol.
    
    This class loads connection parameters from a TOML configuration file and manages
    the connection lifecycle. It implements the context manager protocol for automatic
    resource cleanup.
    
    Args:
        profile: Name of the profile to load from connections.toml
        **kwargs: Additional connection parameters to override profile settings
    
    Example:
        >>> with SnowflakeConnector(profile="dev") as (conn, cur):
        ...     cur.execute("SELECT CURRENT_VERSION()")
        ...     print(cur.fetchone())
        
        >>> # Override warehouse from profile
        >>> with SnowflakeConnector(profile="dev", warehouse="YOUR_WAREHOUSE") as (conn, cur):
        ...     cur.execute("SELECT * FROM huge_table")
    """
    
    def __init__(self, profile: str, **kwargs: Any) -> None:
        """
        Initialize the connector with a configuration profile.
        
        Args:
            profile: Name of the connection profile to load
            **kwargs: Override any connection parameters from the profile
        """
        # Initialize base class (loads profile, processes keypair auth)
        super().__init__(profile, **kwargs)
        
        # Connection and cursor initialized lazily
        self._connection: Optional[SnowflakeConnection] = None
        self._cursor: Optional[SnowflakeCursor] = None

    def connect(self) -> Tuple[SnowflakeConnection, SnowflakeCursor]:
        """
        Establish connection to Snowflake if not already connected.
        
        Returns:
            Tuple of (connection, cursor) objects
        """
        if self._connection is None:
            self._connection = snowflake.connector.connect(**self._cfg)  # type: ignore[misc]
            self._cursor = self._connection.cursor()
        
        assert self._connection is not None
        assert self._cursor is not None
        return self._connection, self._cursor

    def close(self) -> None:
        """Close the cursor and connection, releasing resources."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> Tuple[SnowflakeConnection, SnowflakeCursor]:
        """
        Context manager entry: establish connection.
        
        Returns:
            Tuple of (connection, cursor)
        """
        return self.connect()

    def __exit__(
        self, 
        exc_type: Any, 
        exc_val: Any, 
        exc_tb: Any
    ) -> Literal[False]:
        """
        Context manager exit: close connection.
        
        Always returns False to propagate any exceptions.
        """
        self.close()
        return False
    
    def __repr__(self) -> str:
        """String representation of the connector."""
        status = "connected" if self._connection else "not connected"
        return f"SnowflakeConnector(profile='{self._profile}', {status})"


class SnowparkConnector(BaseConnector):
    """
    Snowpark session manager with TOML profile support.
    
    This class creates a Snowpark Session using the same TOML configuration
    profiles as SnowflakeConnector.
    
    Args:
        profile: Name of the profile to load from connections.toml
        **kwargs: Additional session parameters to override profile settings
    
    Example:
        >>> connector = SnowparkConnector(profile="dev")
        >>> session = connector.session()
        >>> df = session.sql("SELECT CURRENT_VERSION()").collect()
    """
    
    def __init__(self, profile: str, **kwargs: Any) -> None:
        """
        Initialize the Snowpark connector with a configuration profile.
        
        Args:
            profile: Name of the connection profile to load
            **kwargs: Override any session parameters from the profile
        """
        try:
            from snowflake.snowpark import Session
        except ImportError:
            raise ImportError(
                "Snowpark is not installed. " +
                "Install it with: pip install snowflake-snowpark-python"
            )
        
        # Initialize base class (loads profile, processes keypair auth)
        super().__init__(profile, **kwargs)
        
        # Session initialized lazily
        self._session: Optional[Any] = None
        self._Session = Session

    def session(self) -> Any:
        """
        Get or create a Snowpark Session.
        
        Returns:
            Snowpark Session object
        """
        if self._session is None:
            self._session = self._Session.builder.configs(self._cfg).create()
        return self._session
    
    def close(self) -> None:
        """Close the Snowpark session."""
        if self._session:
            self._session.close()
            self._session = None
    
    def __repr__(self) -> str:
        """String representation of the connector."""
        status = "active" if self._session else "inactive"
        return f"SnowparkConnector(profile='{self._profile}', {status})"
