"""Path resolution for snowlib configuration files."""

import os
from pathlib import Path
from typing import Optional, Union

try:
    from importlib.resources import files as importlib_files
except ImportError:
    # Python < 3.9 fallback
    from importlib_resources import files as importlib_files  # type: ignore


def _get_config_directory() -> Path:
    """
    Get the configuration directory for snowlib.
    
    Uses the dotfile approach (~/.snowlib) to align with other Snowflake ecosystem
    tools like snowsql (~/.snowsql) and snowflake CLI (~/.snowflake).
    
    Priority order:
    1. SNOWLIB_CONFIG_DIR environment variable (override)
    2. ~/.snowlib/ (dotfile directory in user home)
    
    The ~/.snowlib path resolves to:
    - Windows: C:\\Users\\username\\.snowlib
    - Linux: /home/username/.snowlib
    - macOS: /Users/username/.snowlib
    
    Returns:
        Path: Configuration directory path
    """
    # 1. Environment variable override (highest priority)
    env_config_dir = os.getenv("SNOWLIB_CONFIG_DIR")
    if env_config_dir:
        return Path(env_config_dir)
    
    # 2. Use dotfile directory in user home (like .snowsql, .snowflake)
    dotfile_config = Path.home() / ".snowlib"
    dotfile_config.mkdir(parents=True, exist_ok=True)
    return dotfile_config


def _get_example_files_dir() -> Path:
    """
    Get the directory containing example configuration files from the installed package.
    
    Returns:
        Path: Directory containing .example files
        
    Raises:
        ModuleNotFoundError: If the snowlib package is not installed
        FileNotFoundError: If the _data directory is not found in the package
    """
    package_data = importlib_files("snowlib") / "_data"
    return Path(str(package_data))


# Configuration directory (dynamically resolved)
CONF_DIR = _get_config_directory()


def get_default_config_path() -> Path:
    """
    Get the path to connections.toml configuration file.
    
    Searches in priority order:
    1. SNOWLIB_CONFIG_DIR/connections.toml (environment override)
    2. ./SNOWLIB_HOME_DIR/connections.toml (development mode)
    3. Platform user config/snowlib/connections.toml (production mode)
    
    Returns:
        Path: The path to connections.toml
    
    Raises:
        FileNotFoundError: If connections.toml doesn't exist in any location
    """
    config_path = CONF_DIR / "connections.toml"
    
    if not config_path.exists():
        # Try to help user by showing where example files are
        example_dir = _get_example_files_dir()
        example_file = example_dir / "connections.toml.example"
        
        error_msg = (
            f"Configuration file 'connections.toml' not found at: {config_path}\n\n"
            f"To create it:\n"
            f"1. Copy example: {example_file}\n"
            f"2. To: {config_path}\n"
            f"3. Edit with your connection details\n\n"
            f"Configuration directory priority:\n"
            f"  1. SNOWLIB_CONFIG_DIR environment variable (if set)\n"
            f"  2. ~/.snowlib/ (dotfile directory)\n"
        )
        
        raise FileNotFoundError(error_msg)
    
    return config_path


def resolve_config_path(path: Optional[Union[str, Path]] = None) -> Path:
    """
    Resolve the configuration file path.
    
    Args:
        path: Optional explicit path to connections.toml file.
              If None, uses default resolution logic.
    
    Returns:
        Path: Resolved path object
    """
    if path:
        return Path(path)
    return get_default_config_path()


def get_example_config_path(filename: str = "connections.toml.example") -> Path:
    """
    Get path to an example configuration file from the package.
    
    Args:
        filename: Name of the example file (default: connections.toml.example)
    
    Returns:
        Path: Path to the example file
        
    Raises:
        FileNotFoundError: If the example file doesn't exist
    """
    example_path = _get_example_files_dir() / filename
    if not example_path.exists():
        raise FileNotFoundError(f"Example file not found: {example_path}")
    return example_path
