"""Unit tests for snowlib configuration module."""

import pytest
from pathlib import Path

from snowlib.config import load_profile, list_profiles, resolve_config_path, get_default_config_path


class TestLoadProfile:
    """Tests for load_profile function."""
    
    @pytest.fixture
    def temp_config_file(self, tmp_path):
        """Create a temporary TOML config file for testing."""
        config_content = """
[default]
account = "test-account.region"
user = "test-user@example.com"
warehouse = "TEST_WH"
database = "TEST_DB"

[dev]
account = "dev-account.region"
user = "dev-user@example.com"
warehouse = "DEV_WH"
database = "DEV_DB"
schema = "DEV_SCHEMA"

[prod]
account = "prod-account.region"
user = "prod-user@example.com"
warehouse = "PROD_WH"
database = "PROD_DB"
role = "PROD_ROLE"
"""
        config_path = tmp_path / "connections.toml"
        config_path.write_text(config_content)
        return config_path
    
    def test_load_default_profile(self, temp_config_file):
        """Test loading the default profile."""
        config = load_profile("default", path=temp_config_file)
        
        assert config["account"] == "test-account.region"
        assert config["user"] == "test-user@example.com"
        assert config["warehouse"] == "TEST_WH"
        assert config["database"] == "TEST_DB"
    
    def test_load_dev_profile(self, temp_config_file):
        """Test loading a non-default profile."""
        config = load_profile("dev", path=temp_config_file)
        
        assert config["account"] == "dev-account.region"
        assert config["warehouse"] == "DEV_WH"
        assert config["schema"] == "DEV_SCHEMA"
    
    def test_load_prod_profile(self, temp_config_file):
        """Test loading prod profile with role."""
        config = load_profile("prod", path=temp_config_file)
        
        assert config["role"] == "PROD_ROLE"
        assert config["database"] == "PROD_DB"
    
    def test_missing_profile_raises_error(self, temp_config_file):
        """Test that requesting a non-existent profile raises KeyError."""
        with pytest.raises(KeyError) as exc_info:
            load_profile("nonexistent", path=temp_config_file)
        
        assert "Profile 'nonexistent' not found" in str(exc_info.value)
        assert "Available profiles: default, dev, prod" in str(exc_info.value)
    
    def test_missing_file_raises_error(self, tmp_path):
        """Test that a missing config file raises FileNotFoundError."""
        nonexistent_path = tmp_path / "does_not_exist.toml"
        
        with pytest.raises(FileNotFoundError) as exc_info:
            load_profile("default", path=nonexistent_path)
        
        assert "not found" in str(exc_info.value).lower()


class TestListProfiles:
    """Tests for list_profiles function."""
    
    @pytest.fixture
    def temp_config_file(self, tmp_path):
        """Create a temporary TOML config file."""
        config_content = """
[default]
account = "test"

[dev]
account = "test"

[staging]
account = "test"

[prod]
account = "test"
"""
        config_path = tmp_path / "connections.toml"
        config_path.write_text(config_content)
        return config_path
    
    def test_list_all_profiles(self, temp_config_file):
        """Test listing all available profiles."""
        profiles = list_profiles(path=temp_config_file)
        
        assert len(profiles) == 4
        assert "default" in profiles
        assert "dev" in profiles
        assert "staging" in profiles
        assert "prod" in profiles
    
    def test_list_profiles_missing_file(self, tmp_path):
        """Test listing profiles when file doesn't exist returns empty list."""
        nonexistent_path = tmp_path / "does_not_exist.toml"
        profiles = list_profiles(path=nonexistent_path)
        
        assert profiles == []


class TestResolveConfigPath:
    """Tests for path resolution functions."""
    
    def test_explicit_path_takes_precedence(self, tmp_path):
        """Test that an explicit path is used when provided."""
        explicit_path = tmp_path / "my_config.toml"
        resolved = resolve_config_path(path=explicit_path)
        
        assert resolved == explicit_path
    
    def test_string_path_converted_to_path_object(self):
        """Test that string paths are converted to Path objects."""
        string_path = "some/path/config.toml"
        resolved = resolve_config_path(path=string_path)
        
        assert isinstance(resolved, Path)
        assert resolved.name == "config.toml"
    
    def test_get_default_config_path_returns_path(self):
        """Test that get_default_config_path returns a Path object."""
        path = get_default_config_path()
        
        assert isinstance(path, Path)
        assert path.name == "connections.toml"
