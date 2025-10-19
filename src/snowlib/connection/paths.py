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
    """Get the configuration directory for snowlib using SNOWLIB_CONFIG_DIR environment variable or ~/.snowlib dotfile directory"""
    env_config_dir = os.getenv("SNOWLIB_CONFIG_DIR")
    if env_config_dir:
        return Path(env_config_dir)
    
    dotfile_config = Path.home() / ".snowlib"
    dotfile_config.mkdir(parents=True, exist_ok=True)
    return dotfile_config


def _get_example_files_dir() -> Path:
    """Get the directory containing example configuration files from the installed package"""
    package_data = importlib_files("snowlib") / "_data"
    return Path(str(package_data))


# Configuration directory (dynamically resolved)
CONF_DIR = _get_config_directory()


def get_default_config_path() -> Path:
    """Get the path to connections.toml configuration file"""
    config_path = CONF_DIR / "connections.toml"
    
    if not config_path.exists():
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
    """Resolve the path to connections.toml file using explicit path or default config path"""
    if path is None:
        return get_default_config_path()
    
    return Path(path)
