"""Unit tests for primitives execution module.

Tests use mocked SnowflakeContext to avoid real Snowflake connections.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from io import StringIO


@pytest.fixture
def mock_context():
    """Create a mock SnowflakeContext."""
    # Patch in the execution module where it's USED, not where it's DEFINED
    with patch("snowlib.primitives.execution.SnowflakeContext") as mock:
        yield mock


class TestExecuteSQL:
    """Tests for execute_sql function."""
    
    def test_execute_sql_returns_rowcount(self, mock_context):
        """Test that execute_sql returns rowcount for DML."""
        from snowlib.primitives import execute_sql
        
        # Setup mock context
        mock_cur = Mock()
        mock_result = Mock()
        mock_result.rowcount = 5
        mock_result.sfqid = "test-query-id-123"
        mock_result.query = "DELETE FROM test_table WHERE id < 100"
        mock_result.description = None
        mock_cur.execute.return_value = mock_result
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Verify the mock is being used
        assert mock_context.called is False  # Not yet called
        
        # Test - pass profile as string which triggers SnowflakeContext creation
        result = execute_sql("DELETE FROM test_table WHERE id < 100", context="test")
        
        # Verify SnowflakeContext was instantiated
        mock_context.assert_called_once_with(profile="test")
        
        # Check QueryResult attributes
        assert result.rowcount == 5
        assert result.query_id == "test-query-id-123"
        
        mock_cur.execute.assert_called_once_with(
            "DELETE FROM test_table WHERE id < 100"
        )
    
    def test_execute_sql_returns_minus_one_for_ddl(self, mock_context):
        """Test that execute_sql returns -1 for DDL."""
        from snowlib.primitives import execute_sql
        
        # Setup mock - DDL returns rowcount of -1
        mock_cur = Mock()
        mock_result = Mock()
        mock_result.rowcount = -1
        mock_result.sfqid = "test-ddl-query-id"
        mock_result.query = "CREATE TABLE test (id INT)"
        mock_result.description = None
        mock_cur.execute.return_value = mock_result
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = execute_sql("CREATE TABLE test (id INT)", context="test")
        
        assert result.rowcount == -1
    
    def test_execute_sql_with_profile_and_overrides(self, mock_context):
        """Test execute_sql passes profile and overrides to context."""
        from snowlib.primitives import execute_sql
        
        # Setup mock
        mock_cur = Mock()
        mock_cur.execute.return_value = Mock(rowcount=0)
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test with custom profile and overrides
        execute_sql(
            "SELECT 1",
            context="dev",
            warehouse="TEST_WH",
            database="TEST_DB"
        )
        
        # Verify context was called with correct args
        mock_context.assert_called_once_with(
            profile="dev",
            warehouse="TEST_WH",
            database="TEST_DB"
        )


class TestFetchOne:
    """Tests for fetch_one function."""
    
    def test_fetch_one_returns_tuple(self, mock_context):
        """Test that fetch_one returns a single tuple."""
        from snowlib.primitives import fetch_one
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute.return_value = None
        mock_cur.fetchone.return_value = (42, "test", 3.14)
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_one("SELECT id, name, value FROM test_table LIMIT 1", context="test")
        
        assert result == (42, "test", 3.14)
        mock_cur.execute.assert_called_once()
    
    def test_fetch_one_returns_none_when_no_results(self, mock_context):
        """Test that fetch_one returns None when no results."""
        from snowlib.primitives import fetch_one
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute.return_value = None
        mock_cur.fetchone.return_value = None
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_one("SELECT * FROM empty_table", context="test")
        
        assert result is None


class TestFetchAll:
    """Tests for fetch_all function."""
    
    def test_fetch_all_returns_list_of_tuples(self, mock_context):
        """Test that fetch_all returns list of tuples."""
        from snowlib.primitives import fetch_all
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute.return_value = None
        mock_cur.fetchall.return_value = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie")
        ]
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_all("SELECT id, name FROM users", context="test")
        
        assert len(result) == 3
        assert result[0] == (1, "Alice")
        assert result[2] == (3, "Charlie")
    
    def test_fetch_all_returns_empty_list_when_no_results(self, mock_context):
        """Test that fetch_all returns empty list when no results."""
        from snowlib.primitives import fetch_all
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute.return_value = None
        mock_cur.fetchall.return_value = []
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_all("SELECT * FROM empty_table", context="test")
        
        assert result == []


class TestFetchDF:
    """Tests for fetch_df function."""
    
    def test_fetch_df_returns_dataframe(self, mock_context):
        """Test that fetch_df returns DataFrame."""
        from snowlib.primitives import fetch_df
        
        # Setup mock context
        mock_cur = Mock()
        test_df = pd.DataFrame({"ID": [1, 2, 3], "NAME": ["A", "B", "C"]})
        mock_cur.fetch_pandas_all.return_value = test_df
        mock_cur.sfqid = "test-query-id"
        mock_cur.query = "SELECT * FROM test_table"
        mock_cur.rowcount = 3
        mock_cur.description = [("ID",), ("NAME",)]
        # cursor.execute() should return the cursor itself
        mock_cur.execute.return_value = mock_cur
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_df("SELECT * FROM test_table", context="test")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        # Columns should be lowercased by default
        assert "id" in result.columns
        assert "name" in result.columns
    
    def test_fetch_df_concatenates_batches(self, mock_context):
        """Test that fetch_df handles complete result sets."""
        from snowlib.primitives import fetch_df
        
        # Setup mock context
        mock_cur = Mock()
        complete_df = pd.DataFrame({"ID": [1, 2, 3, 4], "VALUE": [10, 20, 30, 40]})
        mock_cur.fetch_pandas_all.return_value = complete_df
        mock_cur.sfqid = "test-query-id"
        mock_cur.query = "SELECT * FROM test_table"
        mock_cur.rowcount = 4
        mock_cur.description = [("ID",), ("VALUE",)]
        mock_cur.execute.return_value = mock_cur
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_df("SELECT * FROM test_table", context="test")
        
        assert len(result) == 4
        assert result["id"].tolist() == [1, 2, 3, 4]
    
    def test_fetch_df_handles_empty_results_with_description(self, mock_context):
        """Test fetch_df handles empty results with column description."""
        from snowlib.primitives import fetch_df
        
        # Setup mock context
        mock_cur = Mock()
        empty_df = pd.DataFrame(columns=["ID", "NAME", "VALUE"])
        mock_cur.fetch_pandas_all.return_value = empty_df
        mock_cur.description = [("ID",), ("NAME",), ("VALUE",)]
        mock_cur.sfqid = "test-query-id"
        mock_cur.query = "SELECT * FROM empty_table"
        mock_cur.rowcount = 0
        mock_cur.execute.return_value = mock_cur
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_df("SELECT * FROM empty_table", context="test")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["id", "name", "value"]
    
    def test_fetch_df_uppercase_columns(self, mock_context):
        """Test fetch_df with uppercase_columns=False."""
        from snowlib.primitives import fetch_df
        
        # Setup mock context
        mock_cur = Mock()
        test_df = pd.DataFrame({"ID": [1, 2], "NAME": ["A", "B"]})
        mock_cur.fetch_pandas_all.return_value = test_df
        mock_cur.sfqid = "test-query-id"
        mock_cur.query = "SELECT * FROM test"
        mock_cur.rowcount = 2
        mock_cur.description = [("ID",), ("NAME",)]
        mock_cur.execute.return_value = mock_cur
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_context.return_value = mock_ctx
        
        # Test
        result = fetch_df("SELECT * FROM test", context="test", lowercase_columns=False)
        
        assert "ID" in result.columns
        assert "NAME" in result.columns


class TestExecuteBlock:
    """Tests for execute_block function."""
    
    def test_execute_block_returns_all_results(self, mock_context):
        """Test that execute_block returns results from all statements."""
        from snowlib.primitives import execute_block
        
        # Setup mock context
        mock_conn = Mock()
        
        # Mock execute_stream to return multiple cursor results
        mock_cursor1 = Mock()
        mock_cursor1.__iter__ = Mock(return_value=iter([]))
        
        mock_cursor2 = Mock()
        mock_cursor2.__iter__ = Mock(return_value=iter([[1], [2], [3]]))
        
        mock_conn.execute_stream.return_value = [mock_cursor1, mock_cursor2]
        
        mock_ctx = Mock()
        mock_ctx.connection = mock_conn
        mock_context.return_value = mock_ctx
        
        # Test
        sql_block = """
        CREATE TEMP TABLE temp (id INT);
        INSERT INTO temp VALUES (1), (2), (3);
        SELECT * FROM temp;
        """
        
        results = execute_block(sql_block, context="test")
        
        assert len(results) == 2
        assert results[0] == []  # CREATE TABLE returns no rows
        assert results[1] == [[1], [2], [3]]  # SELECT returns rows
    
    def test_execute_block_uses_string_io(self, mock_context):
        """Test that execute_block uses StringIO for SQL."""
        from snowlib.primitives import execute_block
        
        # Setup mock context
        mock_conn = Mock()
        mock_conn.execute_stream.return_value = []
        
        mock_ctx = Mock()
        mock_ctx.connection = mock_conn
        mock_context.return_value = mock_ctx
        
        # Test
        sql_block = "SELECT 1; SELECT 2;"
        execute_block(sql_block, context="test")
        
        # Verify execute_stream was called with StringIO
        call_args = mock_conn.execute_stream.call_args[0]
        assert len(call_args) == 1
        assert isinstance(call_args[0], StringIO)


class TestExecuteSQLAsync:
    """Tests for execute_sql_async function."""
    
    def test_execute_sql_async_returns_query_job(self, mock_context):
        """Test that execute_sql_async returns a QueryJob."""
        from snowlib.primitives import execute_sql_async
        from snowlib.primitives.job import QueryJob
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute_async.return_value = {
            "queryId": "test-async-query-id-456",
            "success": True
        }
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_ctx.connection = Mock()
        mock_context.return_value = mock_ctx
        
        # Test - submit async query
        job = execute_sql_async("SELECT COUNT(*) FROM huge_table", context="test")
        
        # Verify SnowflakeContext was instantiated
        mock_context.assert_called_once_with(profile="test")
        
        # Verify execute_async was called
        mock_cur.execute_async.assert_called_once_with("SELECT COUNT(*) FROM huge_table")
        
        # Check QueryJob attributes
        assert isinstance(job, QueryJob)
        assert job.query_id == "test-async-query-id-456"
        assert job.sql == "SELECT COUNT(*) FROM huge_table"
        assert job._conn == mock_ctx.connection
    
    def test_execute_sql_async_with_context_object(self, mock_context):
        """Test execute_sql_async with SnowflakeContext object."""
        from snowlib.primitives import execute_sql_async
        from snowlib.primitives.context import SnowflakeContext
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute_async.return_value = {
            "queryId": "test-query-id",
            "success": True
        }
        
        mock_ctx = Mock(spec=SnowflakeContext)
        mock_ctx.cursor = mock_cur
        mock_ctx.connection = Mock()
        
        # Test - pass context object directly
        job = execute_sql_async("SELECT * FROM table", context=mock_ctx)
        
        # Verify SnowflakeContext constructor was NOT called
        mock_context.assert_not_called()
        
        # Verify execute_async was called on the provided context
        mock_cur.execute_async.assert_called_once_with("SELECT * FROM table")
        
        # Check QueryJob
        assert job.query_id == "test-query-id"
        assert job.sql == "SELECT * FROM table"
    
    def test_execute_sql_async_raises_on_missing_query_id(self, mock_context):
        """Test that execute_sql_async raises RuntimeError if queryId is missing."""
        from snowlib.primitives import execute_sql_async
        
        # Setup mock context with response missing queryId
        mock_cur = Mock()
        mock_cur.execute_async.return_value = {
            "success": True
            # Missing "queryId"
        }
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_ctx.connection = Mock()
        mock_context.return_value = mock_ctx
        
        # Test - should raise RuntimeError
        with pytest.raises(RuntimeError, match="Failed to get queryId"):
            execute_sql_async("SELECT 1", context="test")
    
    def test_execute_sql_async_with_overrides(self, mock_context):
        """Test execute_sql_async passes overrides to SnowflakeContext."""
        from snowlib.primitives import execute_sql_async
        
        # Setup mock context
        mock_cur = Mock()
        mock_cur.execute_async.return_value = {
            "queryId": "test-query-id",
            "success": True
        }
        
        mock_ctx = Mock()
        mock_ctx.cursor = mock_cur
        mock_ctx.connection = Mock()
        mock_context.return_value = mock_ctx
        
        # Test - pass profile string with overrides
        job = execute_sql_async(
            "SELECT 1",
            context="prod",
            warehouse="COMPUTE_WH"
        )
        
        # Verify SnowflakeContext was called with overrides
        mock_context.assert_called_once_with(
            profile="prod",
            warehouse="COMPUTE_WH"
        )
        
        assert job.query_id == "test-query-id"
