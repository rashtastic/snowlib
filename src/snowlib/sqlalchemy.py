# snowlib/sqlalchemy.py
"""Optional SQLAlchemy integration for snowlib profiles.

This module provides a bridge between snowlib's TOML profile system
and SQLAlchemy's engine interface. Install with: pip install snowlib[sqlalchemy]
"""

from typing import Any, Optional
from sqlalchemy import create_engine, Engine
from snowlib.connection import SnowflakeConnector
from snowlib.connection.base import BaseConnector


def create_engine_from_profile(
    profile: str = "default",
    pool_size: int = 5,
    max_overflow: int = 10,
    **engine_kwargs: Any
) -> Engine:
    """Create SQLAlchemy engine from snowlib profile.
    
    Uses snowlib's TOML profile system to configure a Snowflake
    SQLAlchemy engine. Supports externalbrowser and keypair authentication.
    
    Args:
        profile: Profile name from ~/.snowlib/connections.toml
        pool_size: SQLAlchemy connection pool size
        max_overflow: Max connections beyond pool_size
        **engine_kwargs: Additional arguments passed to create_engine()
        
    Returns:
        Configured SQLAlchemy Engine for Snowflake
        
    Example:
        >>> from snowlib.sqlalchemy import create_engine_from_profile
        >>> engine = create_engine_from_profile("production")
        >>> 
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT CURRENT_VERSION()"))
        ...     print(result.scalar())
        
    Note:
        Requires: pip install snowlib[sqlalchemy]
    """
    # Use BaseConnector to load profile and process auth
    connector = BaseConnector(profile)
    cfg = connector._cfg
    
    # Build Snowflake SQLAlchemy URL
    account = cfg["account"]
    user = cfg["user"]
    database = cfg.get("database", "")
    schema = cfg.get("schema", "")
    
    # Base URL
    url_parts = [f"snowflake://{user}@{account}"]
    if database:
        url_parts.append(f"/{database}")
        if schema:
            url_parts.append(f"/{schema}")
    
    url = "".join(url_parts)
    
    # Extract connect_args from config
    connect_args = {}
    
    # Authentication: Use processed values from the connector
    if "authenticator" in cfg:
        connect_args["authenticator"] = cfg["authenticator"]
    
    if connector.private_key:
        # Keypair auth (already processed by BaseConnector)
        connect_args["private_key"] = connector.private_key
    
    # Optional parameters
    for key in ["warehouse", "role"]:
        if key in cfg:
            connect_args[key] = cfg[key]

    if connector.password:
        connect_args["password"] = connector.password
    
    # Session parameters
    if "session_parameters" in cfg:
        connect_args.update(cfg["session_parameters"])
    
    # Create engine
    return create_engine(
        url,
        connect_args=connect_args,
        pool_size=pool_size,
        max_overflow=max_overflow,
        **engine_kwargs
    )