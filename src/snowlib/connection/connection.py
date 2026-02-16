"""Snowflake connection management with profile support."""

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
import snowflake.connector
from typing import Optional, Tuple, Any, Literal, Union

from .base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """Snowflake connection manager with TOML profile support and context manager protocol"""
    
    def __init__(self, profile: str, config_path: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize the connector with a configuration profile and optional parameter overrides"""
        super().__init__(profile, config_path=config_path, **kwargs)
        
        self._connection: Optional[SnowflakeConnection] = None
        self._cursor: Optional[SnowflakeCursor] = None

    def connect(self) -> Tuple[SnowflakeConnection, SnowflakeCursor]:
        """Establish connection to Snowflake if not already connected and return connection and cursor"""
        if self._connection is None:
            self._connection = snowflake.connector.connect(**self._cfg)  # type: ignore[misc]
            self._cursor = self._connection.cursor()
        
        assert self._connection is not None
        assert self._cursor is not None
        return self._connection, self._cursor

    def close(self) -> None:
        """Close the cursor and connection to release resources"""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> Tuple[SnowflakeConnection, SnowflakeCursor]:
        """Establish connection and return connection and cursor for context manager"""
        return self.connect()

    def __exit__(
        self, 
        exc_type: Any, 
        exc_val: Any, 
        exc_tb: Any
    ) -> Literal[False]:
        """Close connection and propagate any exceptions"""
        self.close()
        return False
    
    def __repr__(self) -> str:
        """Return string representation showing profile name and connection status"""
        status = "connected" if self._connection else "not connected"
        return f"SnowflakeConnector(profile='{self._profile}', {status})"


class SnowparkConnector(BaseConnector):
    """Snowpark session manager with TOML profile support"""
    
    def __init__(self, profile: str, config_path: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize the Snowpark connector with a configuration profile and optional parameter overrides"""
        try:
            from snowflake.snowpark import Session
        except ImportError:
            raise ImportError(
                "Snowpark is not installed. " +
                "Install it with: pip install snowflake-snowpark-python"
            )
        
        super().__init__(profile, config_path=config_path, **kwargs)
        
        self._session: Optional[Any] = None
        self._Session = Session

    def session(self) -> Any:
        """Get or create a Snowpark Session"""
        if self._session is None:
            self._session = self._Session.builder.configs(self._cfg).create()
        return self._session
    
    def close(self) -> None:
        """Close the Snowpark session"""
        if self._session:
            self._session.close()
            self._session = None
    
    def __repr__(self) -> str:
        """Return string representation showing profile name and session status"""
        status = "active" if self._session else "inactive"
        return f"SnowparkConnector(profile='{self._profile}', {status})"
