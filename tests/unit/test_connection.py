"""Unit tests for snowlib connection module."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from snowlib.connection import SnowflakeConnector, SnowparkConnector

# Check if Snowpark is available for unit tests
try:
    import snowflake.snowpark  # noqa: F401
    SNOWPARK_AVAILABLE = True
except ImportError:
    SNOWPARK_AVAILABLE = False


class TestSnowflakeConnector:
    """Tests for SnowflakeConnector class."""
    
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
"""
        config_path = tmp_path / "connections.toml"
        config_path.write_text(config_content)
        return config_path
    
    def test_init_loads_default_profile(self, temp_config_file):
        """Test that connector initializes with default profile."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            
            assert connector._cfg["account"] == "test-account.region"
            assert connector._cfg["user"] == "test-user@example.com"
            assert connector._cfg["warehouse"] == "TEST_WH"
    
    def test_init_loads_dev_profile(self, temp_config_file):
        """Test that connector can load non-default profiles."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="dev")
            
            assert connector._cfg["account"] == "dev-account.region"
            assert connector._cfg["warehouse"] == "DEV_WH"
            assert connector._cfg["schema"] == "DEV_SCHEMA"
    
    def test_runtime_overrides(self, temp_config_file):
        """Test that kwargs override config values."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(
                profile="default",
                warehouse="OVERRIDE_WH",
                role="OVERRIDE_ROLE"
            )
            
            # Original values
            assert connector._cfg["account"] == "test-account.region"
            # Overridden values
            assert connector._cfg["warehouse"] == "OVERRIDE_WH"
            assert connector._cfg["role"] == "OVERRIDE_ROLE"
    
    def test_connection_lazy_initialization(self, temp_config_file):
        """Test that connection is not created until connect() is called."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            
            assert connector._connection is None
            assert connector._cursor is None
    
    @patch('snowflake.connector.connect')
    def test_connect_creates_connection(self, mock_connect, temp_config_file):
        """Test that connect() creates connection and cursor."""
        # Setup mocks
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            conn, cur = connector.connect()
            
            # Verify connection was created
            mock_connect.assert_called_once()
            assert conn == mock_connection
            assert cur == mock_cursor
            assert connector._connection is not None
            assert connector._cursor is not None
    
    @patch('snowflake.connector.connect')
    def test_connect_reuses_existing_connection(self, mock_connect, temp_config_file):
        """Test that calling connect() twice doesn't create a new connection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            
            # First connect
            conn1, cur1 = connector.connect()
            # Second connect
            conn2, cur2 = connector.connect()
            
            # Should only call connect once
            assert mock_connect.call_count == 1
            assert conn1 == conn2
            assert cur1 == cur2
    
    @patch('snowflake.connector.connect')
    def test_close_releases_resources(self, mock_connect, temp_config_file):
        """Test that close() properly releases connection and cursor."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            connector.connect()
            connector.close()
            
            # Verify resources were released
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
            assert connector._connection is None
            assert connector._cursor is None
    
    @patch('snowflake.connector.connect')
    def test_context_manager_protocol(self, mock_connect, temp_config_file):
        """Test that connector works as a context manager."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            with SnowflakeConnector(profile="default") as (conn, cur):
                assert conn == mock_connection
                assert cur == mock_cursor
            
            # After context exit, resources should be closed
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
    
    @patch('snowflake.connector.connect')
    def test_context_manager_handles_exceptions(self, mock_connect, temp_config_file):
        """Test that context manager closes resources even on exception."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            with pytest.raises(RuntimeError):
                with SnowflakeConnector(profile="default") as (_conn, _cur):
                    raise RuntimeError("Test exception")
            
            # Resources should still be closed
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
    
    def test_repr_before_connection(self, temp_config_file):
        """Test string representation before connecting."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="dev")
            repr_str = repr(connector)
            
            assert "SnowflakeConnector" in repr_str
            assert "dev" in repr_str
            assert "not connected" in repr_str
    
    @patch('snowflake.connector.connect')
    def test_repr_after_connection(self, mock_connect, temp_config_file):
        """Test string representation after connecting."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowflakeConnector(profile="default")
            connector.connect()
            repr_str = repr(connector)
            
            assert "SnowflakeConnector" in repr_str
            assert "default" in repr_str
            assert "connected" in repr_str


@pytest.mark.skipif(not SNOWPARK_AVAILABLE, reason="Snowpark is not installed")
class TestSnowparkConnector:
    """Tests for SnowparkConnector class."""
    
    @pytest.fixture
    def temp_config_file(self, tmp_path):
        """Create a temporary TOML config file for testing."""
        config_content = """
[default]
account = "test-account.region"
user = "test-user@example.com"
warehouse = "TEST_WH"
database = "TEST_DB"
"""
        config_path = tmp_path / "connections.toml"
        config_path.write_text(config_content)
        return config_path
    
    @patch('snowflake.snowpark.Session')
    def test_init_loads_profile(self, mock_session_class, temp_config_file):
        """Test that SnowparkConnector loads profile on init."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            
            assert connector._cfg["account"] == "test-account.region"
            assert connector._cfg["warehouse"] == "TEST_WH"
    
    @patch('snowflake.snowpark.Session')
    def test_runtime_overrides(self, mock_session_class, temp_config_file):
        """Test that kwargs override config values for Snowpark."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(
                profile="default",
                warehouse="LARGE_WH",
                role="CUSTOM_ROLE"
            )
            
            assert connector._cfg["warehouse"] == "LARGE_WH"
            assert connector._cfg["role"] == "CUSTOM_ROLE"
    
    @patch('snowflake.snowpark.Session')
    def test_session_lazy_initialization(self, mock_session_class, temp_config_file):
        """Test that session is not created until session() is called."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            
            assert connector._session is None
    
    @patch('snowflake.snowpark.Session')
    def test_session_creation(self, mock_session_class, temp_config_file):
        """Test that calling session() creates a Snowpark session."""
        # Setup mock
        mock_builder = MagicMock()
        mock_session_instance = Mock()
        mock_builder.configs.return_value.create.return_value = mock_session_instance
        mock_session_class.builder = mock_builder
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            session = connector.session()
            
            # Verify session was created
            mock_builder.configs.assert_called_once_with(connector._cfg)
            assert session == mock_session_instance
    
    @patch('snowflake.snowpark.Session')
    def test_session_reuse(self, mock_session_class, temp_config_file):
        """Test that calling session() twice returns the same session."""
        mock_builder = MagicMock()
        mock_session_instance = Mock()
        mock_builder.configs.return_value.create.return_value = mock_session_instance
        mock_session_class.builder = mock_builder
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            
            session1 = connector.session()
            session2 = connector.session()
            
            # Should only create once
            assert mock_builder.configs.return_value.create.call_count == 1
            assert session1 == session2
    
    @patch('snowflake.snowpark.Session')
    def test_close_closes_session(self, mock_session_class, temp_config_file):
        """Test that close() closes the Snowpark session."""
        mock_builder = MagicMock()
        mock_session_instance = Mock()
        mock_builder.configs.return_value.create.return_value = mock_session_instance
        mock_session_class.builder = mock_builder
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            connector.session()
            connector.close()
            
            # Verify session was closed
            mock_session_instance.close.assert_called_once()
            assert connector._session is None
    
    @patch('snowflake.snowpark.Session')
    def test_repr_before_session(self, mock_session_class, temp_config_file):
        """Test string representation before creating session."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            repr_str = repr(connector)
            
            assert "SnowparkConnector" in repr_str
            assert "default" in repr_str
            assert "inactive" in repr_str
    
    @patch('snowflake.snowpark.Session')
    def test_repr_after_session(self, mock_session_class, temp_config_file):
        """Test string representation after creating session."""
        mock_builder = MagicMock()
        mock_session_instance = Mock()
        mock_builder.configs.return_value.create.return_value = mock_session_instance
        mock_session_class.builder = mock_builder
        
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            connector = SnowparkConnector(profile="default")
            connector.session()
            repr_str = repr(connector)
            
            assert "SnowparkConnector" in repr_str
            assert "default" in repr_str
            assert "active" in repr_str
    
    def test_missing_snowpark_raises_import_error(self, temp_config_file):
        """Test that missing snowpark package raises helpful error."""
        with patch('snowlib.config.config.resolve_config_path', return_value=temp_config_file):
            with patch.dict('sys.modules', {'snowflake.snowpark': None}):
                with pytest.raises(ImportError) as exc_info:
                    # Force reimport to trigger the ImportError
                    import importlib
                    import snowlib.connection
                    importlib.reload(snowlib.connection)
                    from snowlib.connection import SnowparkConnector as TestConnector
                    TestConnector(profile="default")
                
                assert "snowpark" in str(exc_info.value).lower()
