"""Comprehensive integration tests for Snowpark connectivity.

These tests verify that SnowparkConnector works correctly with real Snowflake.
Tests cover basic connectivity, DataFrame operations, and Snowpark pandas API.

Run with: pytest tests/test_snowpark_integration.py -v
"""

import pytest

from snowlib import SnowparkConnector, load_profile
from snowlib.connection import get_default_config_path


# Disable Snowpark pandas hybrid execution warnings
try:
    from modin.config import AutoSwitchBackend
    AutoSwitchBackend.disable()
except ImportError:
    pass  # Snowpark pandas not installed, no need to suppress


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def check_config_exists():
    """Check if connections.toml exists, skip tests if not."""
    config_path = get_default_config_path()
    if not config_path.exists():
        pytest.skip(
            f"Skipping Snowpark integration tests: connections.toml not found at {config_path}"
        )


@pytest.fixture(scope="module")
def check_snowpark_installed():
    """Check if Snowpark is installed, skip if not."""
    try:
        import snowflake.snowpark  # noqa: F401  # pyright: ignore[reportUnusedImport]
        return True
    except ImportError:
        pytest.skip(
            "Snowpark is not installed. " +
            "Install with: pip install snowflake-snowpark-python[modin]"
        )


@pytest.fixture(scope="class")
def shared_session(check_config_exists, check_snowpark_installed, test_profile):
    """Shared Snowpark session for all tests in a class.
    
    This creates ONE session that all tests in the class can reuse,
    dramatically reducing browser authentication popups.
    """
    connector = SnowparkConnector(profile=test_profile)
    session = connector.session()
    yield session
    connector.close()


class TestSnowparkBasicConnectivity:
    """Test basic Snowpark session creation and connectivity."""
    
    def test_create_session(self, shared_session):
        """Test that we can create a Snowpark session."""
        session = shared_session
        
        assert session is not None
    
    def test_session_sql_query(self, shared_session):
        """Test executing a simple SQL query through Snowpark."""
        session = shared_session
        
        # Execute simple query
        result = session.sql("SELECT 1 as test_col").collect()
        
        assert len(result) == 1
        assert result[0]["TEST_COL"] == 1
    
    def test_session_current_version(self, shared_session):
        """Test querying Snowflake version through Snowpark."""
        session = shared_session
        
        result = session.sql("SELECT CURRENT_VERSION()").collect()
        
        assert len(result) == 1
        assert len(result[0][0]) > 0  # Version string exists
    
    def test_session_current_database(self, shared_session, test_profile):
        """Test querying current database through Snowpark."""
        config = load_profile(test_profile)
        expected_db = config["database"].upper()
        
        session = shared_session
        
        result = session.sql("SELECT CURRENT_DATABASE()").collect()
        
        assert len(result) == 1
        assert result[0][0] == expected_db
    
    def test_session_current_user(self, shared_session, test_profile):
        """Test querying current user through Snowpark."""
        config = load_profile(test_profile)
        expected_user = config["user"]
        
        session = shared_session
        
        result = session.sql("SELECT CURRENT_USER()").collect()
        
        assert len(result) == 1
        # Snowflake may or may not uppercase email addresses
        assert result[0][0].upper() == expected_user.upper()
    
    def test_session_current_warehouse(self, shared_session, test_profile):
        """Test querying current warehouse through Snowpark."""
        config = load_profile(test_profile)
        expected_wh = config["warehouse"].upper()
        
        session = shared_session
        
        result = session.sql("SELECT CURRENT_WAREHOUSE()").collect()
        
        assert len(result) == 1
        assert result[0][0] == expected_wh


class TestSnowparkDataFrameOperations:
    """Test Snowpark DataFrame creation and operations."""
    
    def test_create_dataframe(self, shared_session):
        """Test creating a Snowpark DataFrame from data."""
        session = shared_session
        
        # Create DataFrame
        df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        result = df.collect()
        
        assert len(result) == 2
        assert result[0]["A"] == 1
        assert result[0]["B"] == 2
        assert result[1]["A"] == 3
        assert result[1]["B"] == 4
    
    def test_dataframe_filter(self, shared_session):
        """Test filtering a Snowpark DataFrame."""
        session = shared_session
        
        # Create and filter DataFrame
        df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
        filtered = df.filter(df.a > 1)
        result = filtered.collect()
        
        assert len(result) == 2
        assert result[0]["A"] == 3
        assert result[1]["A"] == 5
    
    def test_dataframe_select(self, shared_session):
        """Test selecting columns from Snowpark DataFrame."""
        session = shared_session
        
        # Create DataFrame and select column
        df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        selected = df.select("a")
        result = selected.collect()
        
        assert len(result) == 2
        assert "A" in result[0]
        assert "B" not in result[0]
    
    def test_dataframe_count(self, shared_session):
        """Test counting rows in Snowpark DataFrame."""
        session = shared_session
        
        # Create DataFrame and count
        df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
        count = df.count()
        
        assert count == 3
    
    def test_dataframe_to_pandas(self, shared_session):
        """Test converting Snowpark DataFrame to pandas."""
        session = shared_session
        
        # Create Snowpark DataFrame
        df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        
        # Convert to pandas
        pandas_df = df.to_pandas()
        
        assert pandas_df is not None
        assert len(pandas_df) == 2
        assert list(pandas_df.columns) == ["A", "B"]
        assert pandas_df.iloc[0]["A"] == 1
        assert pandas_df.iloc[1]["A"] == 3


class TestSnowparkPandasAPI:
    """Test Snowpark pandas API (modin-based)."""
    
    @pytest.fixture(scope="class")
    def check_modin_installed(self):  # pyright: ignore[reportUnusedFunction]
        """Check if modin is installed (comes with snowpark[modin])."""
        pytest.importorskip("modin.pandas")
        pytest.importorskip("snowflake.snowpark.modin.plugin")
    
    @pytest.fixture(scope="class")
    def shared_pandas_session(self, check_config_exists, check_snowpark_installed, check_modin_installed, test_profile):
        """Create a shared Snowpark session for pandas tests."""
        import snowflake.snowpark.modin.plugin  # noqa: F401
        from snowlib.connection import SnowparkConnector
        
        connector = SnowparkConnector(profile=test_profile)
        session = connector.session()
        yield session
        connector.close()
    
    def test_create_pandas_dataframe(self, shared_pandas_session):
        """Test creating a Snowpark pandas DataFrame."""
        import modin.pandas as pd  # type: ignore
        
        # Create Snowpark pandas DataFrame
        df = pd.DataFrame(
            [['a', 2.0, 1], ['b', 4.0, 2], ['c', 6.0, None]], 
            columns=["COL_STR", "COL_FLOAT", "COL_INT"]
        )
        
        assert df.shape == (3, 3)
    
    def test_pandas_operations(self, shared_pandas_session):
        """Test basic pandas operations on Snowpark pandas DataFrame."""
        import modin.pandas as pd  # type: ignore
        
        # Create DataFrame
        df = pd.DataFrame(
            [['a', 2.0, 1], ['b', 4.0, 2], ['c', 6.0, None]], 
            columns=["COL_STR", "COL_FLOAT", "COL_INT"]
        )
        
        # Test head
        head_df = df.head(2)
        assert len(head_df) == 2
        
        # Test dropna
        df_clean = df.dropna(subset=["COL_INT"])
        assert df_clean.shape == (2, 3)


class TestSnowparkSessionManagement:
    """Test Snowpark session lifecycle and resource management."""
    
    def test_session_reuse(self, shared_session, test_profile):
        """Test that calling session() multiple times returns same session.
        
        Note: This test uses shared_session but actually tests the connector's
        session reuse behavior by creating its own connector.
        """
        connector = SnowparkConnector(profile=test_profile)
        
        session1 = connector.session()
        session2 = connector.session()
        
        assert session1 is session2
        
        connector.close()
    
    def test_session_close(self, check_config_exists, check_snowpark_installed, test_profile):
        """Test that session closes properly.
        
        This test creates its own connection to test the close() behavior.
        """
        connector = SnowparkConnector(profile=test_profile)
        session = connector.session()  # noqa: F841
        
        assert connector._session is not None
        
        connector.close()
        
        assert connector._session is None
    
    def test_sequential_sessions(self, check_config_exists, check_snowpark_installed, test_profile):
        """Test creating multiple sequential sessions.
        
        This test creates separate connections to test sequential pattern.
        """
        # First session
        connector1 = SnowparkConnector(profile=test_profile)
        session1 = connector1.session()
        result1 = session1.sql("SELECT 1").collect()
        connector1.close()
        
        # Second session
        connector2 = SnowparkConnector(profile=test_profile)
        session2 = connector2.session()
        result2 = session2.sql("SELECT 2").collect()
        connector2.close()
        
        assert result1[0][0] == 1
        assert result2[0][0] == 2
    
    def test_runtime_warehouse_override(self, check_config_exists, check_snowpark_installed, test_profile):
        """Test overriding warehouse at runtime.
        
        This test creates its own connection to test the override parameter.
        """
        config = load_profile(test_profile)
        override_wh = config["warehouse"]  # Use same for test
        
        connector = SnowparkConnector(profile=test_profile, warehouse=override_wh)
        session = connector.session()
        
        result = session.sql("SELECT CURRENT_WAREHOUSE()").collect()
        
        assert result[0][0] == override_wh.upper()
        
        connector.close()


class TestSnowparkAdvancedFeatures:
    """Test advanced Snowpark features."""
    
    def test_sql_with_params(self, shared_session):
        """Test parameterized SQL queries."""
        session = shared_session
        
        # Execute parameterized query
        result = session.sql("SELECT ? as test_col", params=[42]).collect()
        
        assert len(result) == 1
        assert result[0]["TEST_COL"] == 42
    
    def test_multiple_queries_same_session(self, shared_session):
        """Test executing multiple queries on same session."""
        session = shared_session
        
        # Execute multiple queries
        result1 = session.sql("SELECT 1 as col1").collect()
        result2 = session.sql("SELECT 2 as col2").collect()
        result3 = session.sql("SELECT CURRENT_VERSION()").collect()
        
        assert result1[0]["COL1"] == 1
        assert result2[0]["COL2"] == 2
        assert len(result3[0][0]) > 0
    
    def test_dataframe_chain_operations(self, shared_session):
        """Test chaining multiple DataFrame operations."""
        session = shared_session
        
        # Chain operations
        result = (
            session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
            .filter("a > 1")
            .select("a")
            .collect()
        )
        
        assert len(result) == 2
        assert result[0]["A"] == 3
        assert result[1]["A"] == 5
