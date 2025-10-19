"""Integration tests for snowlib - requires actual Snowflake connection.

These tests use the profile configured in conf/test_config.toml and connect to real Snowflake.
Run with: pytest tests/integration/test_integration.py -v

Skip these tests if connections.toml doesn't exist:
pytest tests/integration/test_integration.py -v -m "not integration"
"""

import pytest

from snowlib import SnowflakeConnector, SnowparkConnector, load_profile, list_profiles
from snowlib.connection import get_default_config_path


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def check_config_exists():
    """Check if connections.toml exists, skip tests if not."""
    config_path = get_default_config_path()
    if not config_path.exists():
        pytest.skip(
            f"Skipping integration tests: connections.toml not found at {config_path}. " +
            "Create this file from connections.toml.example"
        )


@pytest.fixture(scope="class")
def shared_connection(check_config_exists, test_profile):
    """Shared SnowflakeConnector connection for tests in a class.
    
    This creates ONE connection that all tests in the class can reuse,
    dramatically reducing browser authentication popups.
    """
    connector = SnowflakeConnector(profile=test_profile)
    conn, cur = connector.connect()
    yield (conn, cur)
    connector.close()


class TestProfileLoading:
    """Integration tests for profile loading."""
    
    def test_list_available_profiles(self, check_config_exists):
        """Test that we can list all profiles."""
        profiles = list_profiles()
        
        assert isinstance(profiles, list)
        assert len(profiles) > 0


class TestSnowflakeConnection:
    """Integration tests for SnowflakeConnector with real connection."""
    
    def test_connect_with_default_profile(self, shared_connection):
        """Test connecting to Snowflake with default profile."""
        conn, cur = shared_connection
        
        # Verify we have a connection and cursor
        assert conn is not None
        assert cur is not None
        
        # Execute simple query
        cur.execute("SELECT 1 as test_col")
        result = cur.fetchone()
        
        assert result is not None
        assert result[0] == 1
    
    def test_query_current_version(self, shared_connection):
        """Test querying Snowflake version."""
        _conn, cur = shared_connection
        cur.execute("SELECT CURRENT_VERSION()")
        result = cur.fetchone()
        
        assert result is not None
        assert len(result[0]) > 0  # Version string
    
    def test_query_current_user(self, shared_connection, test_profile):
        """Test querying current user."""
        config = load_profile(test_profile)
        expected_user = config["user"]
        
        _conn, cur = shared_connection
        cur.execute("SELECT CURRENT_USER()")
        result = cur.fetchone()
        
        assert result is not None
        # Snowflake may or may not uppercase
        assert result[0].upper() == expected_user.upper()
    
    def test_query_current_database(self, shared_connection, test_profile):
        """Test querying current database."""
        config = load_profile(test_profile)
        expected_db = config["database"].upper()
        
        _conn, cur = shared_connection
        cur.execute("SELECT CURRENT_DATABASE()")
        result = cur.fetchone()
        
        assert result is not None
        assert result[0] == expected_db
    
    def test_query_current_warehouse(self, shared_connection, test_profile):
        """Test querying current warehouse."""
        config = load_profile(test_profile)
        expected_wh = config["warehouse"].upper()
        
        _conn, cur = shared_connection
        cur.execute("SELECT CURRENT_WAREHOUSE()")
        result = cur.fetchone()
        
        assert result is not None
        assert result[0] == expected_wh
    
    def test_runtime_warehouse_override(self, check_config_exists, test_profile):
        """Test that runtime warehouse override works.
        
        Note: This test creates its own connection to test the override parameter.
        """
        config = load_profile(test_profile)
        override_wh = config["warehouse"]  # Use same warehouse for test
        
        with SnowflakeConnector(profile=test_profile, warehouse=override_wh) as (_conn, cur):
            cur.execute("SELECT CURRENT_WAREHOUSE()")
            result = cur.fetchone()
            
            assert result is not None
            assert result[0] == override_wh.upper()
    
    def test_multiple_queries_same_connection(self, shared_connection):
        """Test executing multiple queries on the same connection."""
        _conn, cur = shared_connection
        
        # First query
        cur.execute("SELECT 1 as col1")
        result1 = cur.fetchone()
        
        # Second query
        cur.execute("SELECT 2 as col2")
        result2 = cur.fetchone()
        
        # Third query
        cur.execute("SELECT CURRENT_VERSION()")
        result3 = cur.fetchone()
        
        assert result1 is not None
        assert result2 is not None
        assert result3 is not None
        assert result1[0] == 1
        assert result2[0] == 2
        assert len(result3[0]) > 0
    
    def test_connection_closes_properly(self, check_config_exists, test_profile):
        """Test that connection closes without errors.
        
        Note: This test creates its own connection to test cleanup.
        """
        connector = SnowflakeConnector(profile=test_profile)
        
        # Connect and close manually
        conn, cur = connector.connect()  # noqa: F841
        assert conn is not None
        
        connector.close()
        assert connector._connection is None
        assert connector._cursor is None
    
    def test_sequential_connections(self, check_config_exists, test_profile):
        """Test creating multiple sequential connections.
        
        Note: This test intentionally creates separate connections to test that pattern.
        """
        # First connection
        with SnowflakeConnector(profile=test_profile) as (conn1, cur1):
            cur1.execute("SELECT 1")
            result1 = cur1.fetchone()
        
        # Second connection (after first is closed)
        with SnowflakeConnector(profile=test_profile) as (conn2, cur2):
            cur2.execute("SELECT 2")
            result2 = cur2.fetchone()
        
        assert result1 is not None
        assert result2 is not None
        assert result1[0] == 1
        assert result2[0] == 2


@pytest.fixture(scope="class")
def check_snowpark_installed():
    """Check if Snowpark is installed, skip if not."""
    try:
        import snowflake.snowpark  # noqa: F401  # pyright: ignore[reportUnusedImport]
        return True
    except ImportError:
        pytest.skip(
            "Snowpark is not installed. " +
            "Install with: pip install snowflake-snowpark-python"
        )


@pytest.fixture(scope="class")
def shared_snowpark_session(check_config_exists, check_snowpark_installed, test_profile):
    """Shared Snowpark session for tests in a class.
    
    This creates ONE session that all tests in the class can reuse,
    reducing authentication popups.
    """
    connector = SnowparkConnector(profile=test_profile)
    session = connector.session()
    yield session
    connector.close()


class TestSnowparkConnection:
    """Integration tests for SnowparkConnector with real connection."""
    
    def test_create_snowpark_session(self, shared_snowpark_session):
        """Test creating a Snowpark session."""
        session = shared_snowpark_session
        
        assert session is not None
    
    def test_snowpark_simple_query(self, shared_snowpark_session):
        """Test executing a simple query with Snowpark."""
        session = shared_snowpark_session
        
        # Execute query
        df = session.sql("SELECT 1 as test_col").collect()
        
        assert len(df) == 1
        assert df[0]["TEST_COL"] == 1
    
    def test_snowpark_current_database(self, shared_snowpark_session, test_profile):
        """Test querying current database with Snowpark."""
        config = load_profile(test_profile)
        expected_db = config["database"].upper()
        
        session = shared_snowpark_session
        df = session.sql("SELECT CURRENT_DATABASE()").collect()
        
        assert df[0][0] == expected_db
    
    def test_snowpark_session_reuse(self, check_config_exists, check_snowpark_installed, test_profile):
        """Test that calling session() twice returns the same session.
        
        Note: This test creates its own connector to test the reuse pattern.
        """
        connector = SnowparkConnector(profile=test_profile)
        
        session1 = connector.session()
        session2 = connector.session()
        
        assert session1 is session2
        
        connector.close()
    
    def test_snowpark_close(self, check_config_exists, check_snowpark_installed, test_profile):
        """Test that Snowpark session closes properly."""
        connector = SnowparkConnector(profile=test_profile)
        session = connector.session()
        
        assert connector._session is not None
        
        connector.close()
        
        assert connector._session is None


class TestMultipleProfiles:
    """Integration tests for multiple profiles if they exist."""
    
    def test_switch_between_profiles(self, check_config_exists):
        """Test switching between different profiles."""
        profiles = list_profiles()
        
        # Only test if we have multiple profiles
        if len(profiles) < 2:
            pytest.skip("Only one profile available, cannot test switching")
        
        # Test first profile
        with SnowflakeConnector(profile=profiles[0]) as (conn1, cur1):
            cur1.execute("SELECT CURRENT_USER()")
            user1_result = cur1.fetchone()
            assert user1_result is not None
            user1 = user1_result[0]
        
        # Test second profile
        with SnowflakeConnector(profile=profiles[1]) as (conn2, cur2):
            cur2.execute("SELECT CURRENT_USER()")
            user2_result = cur2.fetchone()
            assert user2_result is not None
            user2 = user2_result[0]
        
        # Both should work (users might be same or different)
        assert user1 is not None
        assert user2 is not None


class TestSnowflakeContextProperties:
    """Tests for SnowflakeContext session properties."""
    
    @pytest.fixture(scope="class")
    def ctx(self, test_profile):
        """Shared SnowflakeContext for all tests in class."""
        from snowlib.context import SnowflakeContext
        context = SnowflakeContext(profile=test_profile)
        yield context
        context.close()
    
    def test_current_database(self, ctx, test_database):
        """Test current_database property."""
        db = ctx.current_database
        
        assert isinstance(db, str)
        assert len(db) > 0
        assert db == test_database.upper()
    
    def test_current_schema(self, ctx, test_schema):
        """Test current_schema property."""
        schema = ctx.current_schema
        
        assert isinstance(schema, str)
        assert len(schema) > 0
        assert schema == test_schema.upper()
    
    def test_current_warehouse(self, ctx):
        """Test current_warehouse property."""
        wh = ctx.current_warehouse
        
        assert isinstance(wh, str)
        assert len(wh) > 0
        # Warehouse should be uppercase per Snowflake conventions
        assert wh.isupper()
    
    def test_current_role(self, ctx):
        """Test current_role property."""
        role = ctx.current_role
        
        assert isinstance(role, str)
        assert len(role) > 0
        # Role should be uppercase per Snowflake conventions
        assert role.isupper()
    
    def test_current_user(self, ctx, test_profile):
        """Test current_user property."""
        from snowlib import load_profile
        config = load_profile(test_profile)
        expected_user = config["user"].upper()
        
        user = ctx.current_user
        
        assert isinstance(user, str)
        assert len(user) > 0
        assert user.upper() == expected_user
    
    def test_current_account(self, ctx):
        """Test current_account property."""
        account = ctx.current_account
        
        assert isinstance(account, str)
        assert len(account) > 0
    
    def test_current_region(self, ctx):
        """Test current_region property."""
        region = ctx.current_region
        
        assert isinstance(region, str)
        assert len(region) > 0
    
    def test_all_properties_are_uppercase(self, ctx):
        """Test that session identifiers follow Snowflake uppercase convention."""
        # Database, schema, warehouse, role should all be uppercase
        assert ctx.current_database.isupper()
        assert ctx.current_schema.isupper()
        assert ctx.current_warehouse.isupper()
        assert ctx.current_role.isupper()
