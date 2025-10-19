"""Profile loading for Snowflake connections."""

import sys
from pathlib import Path
from typing import Dict, Union, Optional, Any

# Use tomllib for Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        raise ImportError(
            "Python < 3.11 requires 'tomli' package. " +
            "Install it with: pip install tomli"
        )

from .paths import resolve_config_path


def load_profile(
    profile: str, 
    path: Optional[Union[str, Path]] = None
) -> Dict[str, Any]:
    """Load a Snowflake connection profile from connections.toml file"""
    config_file = resolve_config_path(path)

    if not config_file.exists():
        raise FileNotFoundError(
            f"Snowflake configuration file not found at {config_file}. " +
            "Create a connections.toml file or see connections.toml.example for template."
        )

    with open(config_file, "rb") as f:
        all_profiles = tomllib.load(f)

    if profile not in all_profiles:
        available = ", ".join(all_profiles.keys())
        raise KeyError(
            f"Profile '{profile}' not found in {config_file}. " +
            f"Available profiles: {available}"
        )

    return all_profiles[profile]


def list_profiles(path: Optional[Union[str, Path]] = None) -> list[str]:
    """List all available profile names in connections.toml file"""
    config_file = resolve_config_path(path)
    
    if not config_file.exists():
        return []
    
    with open(config_file, "rb") as f:
        all_profiles = tomllib.load(f)
    
    return list(all_profiles.keys())
