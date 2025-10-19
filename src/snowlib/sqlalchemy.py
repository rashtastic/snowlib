"""Optional SQLAlchemy integration for snowlib profiles"""

from typing import Any
from sqlalchemy import create_engine, Engine
from snowlib.connection.base import BaseConnector


def create_engine_from_profile(
    profile: str = "default",
    pool_size: int = 5,
    max_overflow: int = 10,
    **engine_kwargs: Any
) -> Engine:
    """Create SQLAlchemy engine from snowlib profile"""
    connector = BaseConnector(profile)
    cfg = connector._cfg
    account = cfg["account"]
    user = cfg["user"]
    database = cfg.get("database", "")
    schema = cfg.get("schema", "")
    
    url_parts = [f"snowflake://{user}@{account}"]
    if database:
        url_parts.append(f"/{database}")
        if schema:
            url_parts.append(f"/{schema}")
    
    url = "".join(url_parts)
    
    connect_args = {}
    if "authenticator" in cfg:
        connect_args["authenticator"] = cfg["authenticator"]
    
    if connector.private_key:
        connect_args["private_key"] = connector.private_key
    
    for key in ["warehouse", "role"]:
        if key in cfg:
            connect_args[key] = cfg[key]

    if connector.password:
        connect_args["password"] = connector.password
    
    if "session_parameters" in cfg:
        connect_args.update(cfg["session_parameters"])
    
    return create_engine(
        url,
        connect_args=connect_args,
        pool_size=pool_size,
        max_overflow=max_overflow,
        **engine_kwargs
    )