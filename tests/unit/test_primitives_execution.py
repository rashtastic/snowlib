"""Unit tests for primitives execution module.

Tests use mocked SnowflakeContext to avoid real Snowflake connections.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from io import StringIO


@pytest.fixture
def mock_context():
    """Create a mock SnowflakeContext."""
    # Patch in the execute module where it's USED, not where it's DEFINED
    with patch("snowlib.primitives.execute.SnowflakeContext") as mock:
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


class TestExecuteBlock:
    """Tests for execute_block function."""
    
    def test_execute_block_returns_all_results(self, mock_context):
        """Test that execute_block returns results from all statements."""
        from snowlib.primitives import execute_block, QueryResult
        
        # Setup mock context
        mock_conn = Mock()
        
        # Mock execute_stream to return multiple cursor results
        mock_cursor1 = Mock()
        mock_cursor1.sfqid = "query-id-1"
        mock_cursor1.rowcount = -1
        mock_cursor1.query = "CREATE TEMP TABLE temp (id INT)"
        
        mock_cursor2 = Mock()
        mock_cursor2.sfqid = "query-id-2"
        mock_cursor2.rowcount = 3
        mock_cursor2.query = "SELECT * FROM temp"
        
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
        assert isinstance(results[0], QueryResult)
        assert results[0].rowcount == -1  # DDL returns -1
        assert isinstance(results[1], QueryResult)
        assert results[1].rowcount == 3
    
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
    
    def test_execute_sql_async_returns_async_query(self, mock_context):
        """Test that execute_sql_async returns an AsyncQuery."""
        from snowlib.primitives import execute_sql_async, AsyncQuery
        
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
        
        # Check AsyncQuery attributes
        assert isinstance(job, AsyncQuery)
        assert job.query_id == "test-async-query-id-456"
        assert job.sql == "SELECT COUNT(*) FROM huge_table"
        assert job._conn == mock_ctx.connection
    
    def test_execute_sql_async_with_context_object(self, mock_context):
        """Test execute_sql_async with SnowflakeContext object."""
        from snowlib.primitives import execute_sql_async
        from snowlib.context import SnowflakeContext
        
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
        
        # Check AsyncQuery
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
