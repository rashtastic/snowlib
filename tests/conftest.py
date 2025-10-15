"""Pytest configuration and shared fixtures for integration tests."""

import sys
from typing import Dict, Any

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

import pytest
from snowlib.config import CONF_DIR


def _load_test_config() -> Dict[str, Any]:
    """
    Load test configuration from conf/test_config.toml.
    
    Returns:
        Dictionary with test configuration settings
    
    Raises:
        FileNotFoundError: If test_config.toml is not found
    """
    test_config_path = CONF_DIR / "test_config.toml"
    
    if not test_config_path.exists():
        raise FileNotFoundError(
            f"Test configuration not found at {test_config_path}. " +
            f"Create it from {CONF_DIR / 'test_config.toml.example'}"
        )
    
    with open(test_config_path, "rb") as f:
        config = tomllib.load(f)
    
    return config.get("test", {})


# Load test config once at module level
_TEST_CONFIG = _load_test_config()


@pytest.fixture(scope="session")
def test_profile() -> str:
    """Snowflake profile to use for integration tests."""
    profile = _TEST_CONFIG.get("profile")
    if not profile:
        raise ValueError(
            "Test profile not specified in conf/test_config.toml. " +
            "Add 'profile = \"your_profile_name\"' to the [test] section."
        )
    return profile


@pytest.fixture(scope="session")
def test_profile2() -> str:
    """Optional second Snowflake profile for testing different auth methods."""
    profile = _TEST_CONFIG.get("profile2")
    if not profile:
        pytest.skip("Second test profile 'profile2' not configured in test_config.toml")
    return profile


@pytest.fixture(scope="session")
def test_database() -> str:
    """Database to use for integration tests."""
    return _TEST_CONFIG.get("database", "O_CRI")


@pytest.fixture(scope="session")
def test_schema() -> str:
    """Schema to use for integration tests."""
    return _TEST_CONFIG.get("schema", "PUBLIC")


@pytest.fixture(scope="session")
def test_write_table() -> str:
    """Table name for write tests (will be created/dropped)."""
    return _TEST_CONFIG.get("write_table", "TEST_WRITE_TABLE")


@pytest.fixture(scope="session")
def test_temp_table() -> str:
    """Table name for temporary test data (will be created/dropped)."""
    return _TEST_CONFIG.get("temp_table", "TEST_TEMP_TABLE")


@pytest.fixture(scope="session")
def test_read_table() -> str:
    """Existing table name for read tests."""
    table = _TEST_CONFIG.get("read_table")
    if not table:
        raise ValueError(
            "Test read table not specified in conf/test_config.toml. " +
            "Add 'read_table = \"DATABASE.SCHEMA.TABLE\"' to the [test] section."
        )
    return table


@pytest.fixture(scope="session")
def test_qualified_table(test_database: str, test_schema: str, test_write_table: str) -> str:
    """Fully qualified test table name."""
    return f"{test_database}.{test_schema}.{test_write_table}"


@pytest.fixture(scope="session")
def check_pandas_integration():
    """
    Check if pandas integration is available in snowflake-connector-python.
    
    Pandas integration requires the [pandas] extra which includes pyarrow.
    This may not be available on newer Python versions (e.g., 3.14) until
    pyarrow releases compatible wheels.
    
    Skips tests if pandas integration is not available.
    """
    try:
        # Try to check if pandas integration is available
        # This will fail if the [pandas] extra is not installed
        import pyarrow  # noqa: F401  # pyright: ignore[reportUnusedImport]
        return True
    except ImportError:
        pytest.skip(
            "Pandas integration not available. " +
            "Requires pyarrow from snowflake-connector-python[pandas] extra."
        )
