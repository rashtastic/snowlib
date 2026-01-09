"""Unit tests for SnowflakeContext class."""

import pytest
from unittest.mock import Mock, patch

from snowlib.context import SnowflakeContext


class TestSnowflakeContextInitialization:
    """Tests for SnowflakeContext initialization."""

    def test_init_with_profile(self):
        """Test initialization with profile name."""
        ctx = SnowflakeContext(profile="test")
        
        assert ctx._profile == "test"
        assert ctx._connection is None
        assert ctx._cursor is None
        assert ctx._connector is None
        assert ctx._owns_connector is False

    def test_init_with_connection(self):
        """Test initialization with existing connection."""
        mock_conn = Mock()
        ctx = SnowflakeContext(connection=mock_conn)
        
        assert ctx._profile is None
        assert ctx._connection is mock_conn
        assert ctx._cursor is None
        assert ctx._connector is None
        assert ctx._owns_connector is False

    def test_init_with_connection_and_cursor(self):
        """Test initialization with both connection and cursor."""
        mock_conn = Mock()
        mock_cur = Mock()
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        assert ctx._profile is None
        assert ctx._connection is mock_conn
        assert ctx._cursor is mock_cur
        assert ctx._connector is None
        assert ctx._owns_connector is False

    def test_init_with_profile_and_overrides(self):
        """Test initialization with profile and runtime overrides."""
        ctx = SnowflakeContext(
            profile="test",
            warehouse="COMPUTE_WH",
            role="ANALYST"
        )
        
        assert ctx._profile == "test"
        assert ctx._overrides == {"warehouse": "COMPUTE_WH", "role": "ANALYST"}

    def test_init_requires_profile_or_connection(self):
        """Test that initialization fails without profile or connection."""
        with pytest.raises(ValueError, match="requires either 'profile' or 'connection'"):
            SnowflakeContext()

    def test_init_rejects_both_profile_and_connection(self):
        """Test that initialization fails with both profile and connection."""
        mock_conn = Mock()
        with pytest.raises(ValueError, match="provide either 'profile' or 'connection', not both"):
            SnowflakeContext(profile="test", connection=mock_conn)


class TestSnowflakeContextLazyLoading:
    """Tests for lazy connection and cursor creation."""

    @patch('snowlib.connection.SnowflakeConnector')
    def test_connection_property_creates_connector(self, mock_connector_class):
        """Test that accessing connection property creates connector from profile."""
        # Setup mocks
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        # Create context with profile
        ctx = SnowflakeContext(profile="test")
        
        # Verify no connection yet
        assert ctx._connection is None
        
        # Access connection property (triggers lazy creation)
        conn = ctx.connection
        
        # Verify connector was created
        mock_connector_class.assert_called_once_with(profile="test")
        mock_connector_instance.connect.assert_called_once()
        
        # Verify connection is cached
        assert ctx._connection is mock_conn
        assert ctx._cursor is mock_cur
        assert ctx._owns_connector is True
        assert conn is mock_conn

    @patch('snowlib.connection.SnowflakeConnector')
    def test_connection_property_with_overrides(self, mock_connector_class):
        """Test that connection creation passes overrides to connector."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(
            profile="test",
            warehouse="LARGE_WH",
            role="ADMIN"
        )
        
        # Trigger connection creation
        _ = ctx.connection
        
        # Verify overrides were passed
        mock_connector_class.assert_called_once_with(
            profile="test",
            warehouse="LARGE_WH",
            role="ADMIN"
        )

    @patch('snowlib.connection.SnowflakeConnector')
    def test_connection_property_caches_result(self, mock_connector_class):
        """Test that connection property returns cached connection on subsequent calls."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(profile="test")
        
        # Access connection multiple times
        conn1 = ctx.connection
        conn2 = ctx.connection
        conn3 = ctx.connection
        
        # Should only call connect once
        assert mock_connector_instance.connect.call_count == 1
        assert conn1 is conn2 is conn3 is mock_conn

    def test_connection_property_returns_existing_connection(self):
        """Test that connection property returns pre-existing connection."""
        mock_conn = Mock()
        ctx = SnowflakeContext(connection=mock_conn)
        
        # Access connection property
        conn = ctx.connection
        
        # Should return the provided connection without creating new one
        assert conn is mock_conn

    def test_cursor_property_creates_from_connection(self):
        """Test that cursor property creates cursor from connection."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        ctx = SnowflakeContext(connection=mock_conn)
        
        # Access cursor property
        cur = ctx.cursor
        
        # Verify cursor was created from connection
        mock_conn.cursor.assert_called_once()
        assert cur is mock_cur

    def test_cursor_property_returns_existing_cursor(self):
        """Test that cursor property returns pre-existing cursor."""
        mock_conn = Mock()
        mock_cur = Mock()
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        # Access cursor property
        cur = ctx.cursor
        
        # Should return the provided cursor without creating new one
        assert cur is mock_cur
        mock_conn.cursor.assert_not_called()

    @patch('snowlib.connection.SnowflakeConnector')
    def test_cursor_property_uses_cached_cursor(self, mock_connector_class):
        """Test that cursor property uses cursor cached during connection creation."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(profile="test")
        
        # Access connection first (caches cursor)
        _ = ctx.connection
        
        # Access cursor
        cur = ctx.cursor
        
        # Should use cached cursor, not create new one
        assert cur is mock_cur
        mock_conn.cursor.assert_not_called()


class TestSnowflakeContextClose:
    """Tests for context cleanup."""

    @patch('snowlib.connection.SnowflakeConnector')
    def test_close_releases_owned_connector(self, mock_connector_class):
        """Test that close() releases connector created by context."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(profile="test")
        _ = ctx.connection  # Create connection
        
        # Close the context
        ctx.close()
        
        # Verify connector was closed
        mock_connector_instance.close.assert_called_once()
        assert ctx._connector is None
        assert ctx._connection is None
        assert ctx._cursor is None

    def test_close_does_not_close_external_connection(self):
        """Test that close() does not close externally-provided connection."""
        mock_conn = Mock()
        mock_cur = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        ctx.close()
        
        # Should not call close on external connection
        mock_conn.close.assert_not_called()
        mock_cur.close.assert_not_called()

    @patch('snowlib.connection.SnowflakeConnector')
    def test_close_is_idempotent(self, mock_connector_class):
        """Test that calling close() multiple times is safe."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(profile="test")
        _ = ctx.connection
        
        # Call close multiple times
        ctx.close()
        ctx.close()
        ctx.close()
        
        # Should only close once
        assert mock_connector_instance.close.call_count == 1


class TestSnowflakeContextManager:
    """Tests for context manager protocol."""

    @patch('snowlib.connection.SnowflakeConnector')
    def test_context_manager_closes_on_exit(self, mock_connector_class):
        """Test that context manager closes owned connections on exit."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        with SnowflakeContext(profile="test") as ctx:
            _ = ctx.connection
            # Verify connection is active
            assert ctx._connection is not None
        
        # After exiting, connection should be closed
        mock_connector_instance.close.assert_called_once()

    @patch('snowlib.connection.SnowflakeConnector')
    def test_context_manager_closes_on_exception(self, mock_connector_class):
        """Test that context manager closes connections even on exception."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        with pytest.raises(RuntimeError):
            with SnowflakeContext(profile="test") as ctx:
                _ = ctx.connection
                raise RuntimeError("Test error")
        
        # Connection should still be closed
        mock_connector_instance.close.assert_called_once()

    def test_context_manager_returns_self(self):
        """Test that __enter__ returns the context itself."""
        mock_conn = Mock()
        ctx = SnowflakeContext(connection=mock_conn)
        
        with ctx as returned_ctx:
            assert returned_ctx is ctx


class TestSnowflakeContextCurrentProperties:
    """Tests for current_* properties."""

    def test_current_database(self):
        """Test current_database property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("MY_DB",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_database
        
        assert result == "MY_DB"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_DATABASE()")

    def test_current_schema(self):
        """Test current_schema property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("PUBLIC",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_schema
        
        assert result == "PUBLIC"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_SCHEMA()")

    def test_current_warehouse(self):
        """Test current_warehouse property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("COMPUTE_WH",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_warehouse
        
        assert result == "COMPUTE_WH"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_WAREHOUSE()")

    def test_current_role(self):
        """Test current_role property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("ANALYST",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_role
        
        assert result == "ANALYST"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_ROLE()")

    def test_current_user(self):
        """Test current_user property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("JOHN_DOE",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_user
        
        assert result == "JOHN_DOE"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_USER()")

    def test_current_account(self):
        """Test current_account property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("ABC12345",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_account
        
        assert result == "ABC12345"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_ACCOUNT()")

    def test_current_region(self):
        """Test current_region property."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = ("US-EAST-1",)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        result = ctx.current_region
        
        assert result == "US-EAST-1"
        mock_cur.execute.assert_called_once_with("SELECT CURRENT_REGION()")

    def test_current_property_handles_none(self):
        """Test that current_* properties handle None values gracefully."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = (None,)
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        assert ctx.current_database == ""
        assert ctx.current_schema == ""

    def test_current_property_handles_empty_result(self):
        """Test that current_* properties handle empty results."""
        mock_cur = Mock()
        mock_cur.execute.return_value.fetchone.return_value = None
        mock_conn = Mock()
        
        ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
        
        assert ctx.current_database == ""


class TestSnowflakeContextRepr:
    """Tests for string representation."""

    def test_repr_with_profile(self):
        """Test __repr__ shows profile when using profile-based context."""
        ctx = SnowflakeContext(profile="test")
        
        repr_str = repr(ctx)
        
        assert "SnowflakeContext" in repr_str
        assert "profile='test'" in repr_str

    def test_repr_with_active_connection(self):
        """Test __repr__ shows active connection."""
        mock_conn = Mock()
        ctx = SnowflakeContext(connection=mock_conn)
        
        repr_str = repr(ctx)
        
        assert "SnowflakeContext" in repr_str
        assert "connection=<active>" in repr_str

    @patch('snowlib.connection.SnowflakeConnector')
    def test_repr_after_lazy_connection_creation(self, mock_connector_class):
        """Test __repr__ after lazy connection is created."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = {}  # Empty config for validation
        mock_connector_class.return_value = mock_connector_instance
        
        ctx = SnowflakeContext(profile="test")
        _ = ctx.connection  # Trigger connection creation
        
        repr_str = repr(ctx)
        
        assert "SnowflakeContext" in repr_str
        assert "connection=<active>" in repr_str


class TestSessionContextValidation:
    """Tests for session context validation warnings"""

    def _setup_mock_connector(self, mock_connector_class, cfg, cursor_results):
        """Helper to set up mocked connector with config and cursor results"""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connector_instance = Mock()
        mock_connector_instance.connect.return_value = (mock_conn, mock_cur)
        mock_connector_instance._cfg = cfg
        mock_connector_class.return_value = mock_connector_instance
        
        # Setup cursor to return different results for different queries
        def execute_side_effect(query):
            result_mock = Mock()
            for key, value in cursor_results.items():
                if key in query:
                    result_mock.fetchone.return_value = (value,)
                    return result_mock
            result_mock.fetchone.return_value = (None,)
            return result_mock
        
        mock_cur.execute.side_effect = execute_side_effect
        return mock_conn, mock_cur

    @patch('snowlib.connection.SnowflakeConnector')
    def test_no_warning_when_values_match(self, mock_connector_class):
        """No warning when declared values match session values"""
        cfg = {"warehouse": "MY_WH", "role": "MY_ROLE"}
        cursor_results = {
            "CURRENT_WAREHOUSE": "MY_WH",
            "CURRENT_ROLE": "MY_ROLE",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 0

    @patch('snowlib.connection.SnowflakeConnector')
    def test_no_warning_when_values_match_case_insensitive(self, mock_connector_class):
        """No warning when values match case-insensitively"""
        cfg = {"warehouse": "my_wh", "role": "my_role"}
        cursor_results = {
            "CURRENT_WAREHOUSE": "MY_WH",
            "CURRENT_ROLE": "MY_ROLE",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 0

    @patch('snowlib.connection.SnowflakeConnector')
    def test_warning_when_warehouse_not_active(self, mock_connector_class):
        """Warning when declared warehouse is not active (suspended)"""
        cfg = {"warehouse": "SUSPENDED_WH"}
        cursor_results = {
            "CURRENT_WAREHOUSE": None,
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 1
            assert "SUSPENDED_WH" in str(w[0].message)
            assert "not active" in str(w[0].message)

    @patch('snowlib.connection.SnowflakeConnector')
    def test_warning_when_role_mismatched(self, mock_connector_class):
        """Warning when declared role does not match session role"""
        cfg = {"role": "REQUESTED_ROLE"}
        cursor_results = {
            "CURRENT_ROLE": "DIFFERENT_ROLE",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 1
            assert "REQUESTED_ROLE" in str(w[0].message)
            assert "DIFFERENT_ROLE" in str(w[0].message)
            assert "does not match" in str(w[0].message)

    @patch('snowlib.connection.SnowflakeConnector')
    def test_warning_when_database_mismatched(self, mock_connector_class):
        """Warning when declared database does not match session database"""
        cfg = {"database": "MY_DB"}
        cursor_results = {
            "CURRENT_DATABASE": "OTHER_DB",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 1
            assert "MY_DB" in str(w[0].message)
            assert "OTHER_DB" in str(w[0].message)

    @patch('snowlib.connection.SnowflakeConnector')
    def test_no_warning_for_undeclared_values(self, mock_connector_class):
        """No warning when values are not declared in config"""
        cfg = {}  # No warehouse, role, database, or schema declared
        cursor_results = {
            "CURRENT_WAREHOUSE": None,
            "CURRENT_ROLE": "SOME_ROLE",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 0

    @patch('snowlib.connection.SnowflakeConnector')
    def test_multiple_warnings_for_multiple_mismatches(self, mock_connector_class):
        """Multiple warnings when multiple values are mismatched"""
        cfg = {"warehouse": "WH1", "role": "ROLE1", "database": "DB1"}
        cursor_results = {
            "CURRENT_WAREHOUSE": None,
            "CURRENT_ROLE": "ROLE2",
            "CURRENT_DATABASE": "DB2",
        }
        self._setup_mock_connector(mock_connector_class, cfg, cursor_results)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(profile="test")
            _ = ctx.connection
            
            assert len(w) == 3

    def test_no_validation_when_connection_passed_directly(self):
        """No validation when connection is passed directly (no connector)"""
        mock_conn = Mock()
        mock_cur = Mock()
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = SnowflakeContext(connection=mock_conn, cursor=mock_cur)
            # Access connection to ensure no errors
            _ = ctx.connection
            
            # No warnings because we can't validate without a connector
            assert len(w) == 0
