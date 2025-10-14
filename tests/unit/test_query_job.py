"""Unit tests for QueryJob class."""

from unittest.mock import MagicMock

import pytest
from snowflake.connector.constants import QueryStatus

from snowlib.primitives.job import QueryJob
from snowlib.primitives.result import QueryResult


class TestQueryJob:
    """Tests for the QueryJob class."""

    def test_init(self):
        """Test QueryJob initialization."""
        mock_conn = MagicMock()
        job = QueryJob(
            query_id="test-query-id-123",
            sql="SELECT 1",
            _conn=mock_conn
        )
        
        assert job.query_id == "test-query-id-123"
        assert job.sql == "SELECT 1"
        assert job._conn is mock_conn

    def test_get_result_success(self):
        """Test get_result() retrieves results successfully."""
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock successful query completion
        mock_cursor.get_results_from_sfqid.return_value = None
        mock_cursor.sfqid = "test-query-id"
        mock_cursor.rowcount = 5
        mock_cursor.query = "SELECT * FROM table"
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM table",
            _conn=mock_conn
        )
        
        # Get the result
        result = job.get_result()
        
        # Verify cursor methods were called
        mock_conn.cursor.assert_called_once()
        mock_cursor.get_results_from_sfqid.assert_called_once_with("test-query-id")
        
        # Verify result is a QueryResult
        assert isinstance(result, QueryResult)
        assert result.query_id == "test-query-id"
        assert result.rowcount == 5
        assert result.sql == "SELECT * FROM table"

    def test_get_result_closes_cursor_on_error(self):
        """Test that get_result() closes cursor when an error occurs."""
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query failure - make get_results_from_sfqid raise an exception
        mock_cursor.get_results_from_sfqid.side_effect = Exception("Query failed")
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM table",
            _conn=mock_conn
        )
        
        # Verify exception is raised and cursor is closed
        with pytest.raises(Exception, match="Query failed"):
            job.get_result()
        
        # Cursor should have been closed in the except block
        mock_cursor.close.assert_called_once()

    def test_is_running_still_running(self):
        """Test is_running() returns True when query is still running."""
        mock_conn = MagicMock()
        mock_conn.get_query_status.return_value = QueryStatus.RUNNING
        mock_conn.is_still_running.return_value = True
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM table",
            _conn=mock_conn
        )
        
        assert job.is_running() is True
        mock_conn.get_query_status.assert_called_once_with("test-query-id")
        mock_conn.is_still_running.assert_called_once_with(QueryStatus.RUNNING)

    def test_is_running_completed(self):
        """Test is_running() returns False when query is complete."""
        mock_conn = MagicMock()
        mock_conn.get_query_status.return_value = QueryStatus.SUCCESS
        mock_conn.is_still_running.return_value = False
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM table",
            _conn=mock_conn
        )
        
        assert job.is_running() is False
        mock_conn.get_query_status.assert_called_once_with("test-query-id")
        mock_conn.is_still_running.assert_called_once_with(QueryStatus.SUCCESS)

    def test_abort_success(self):
        """Test abort() successfully aborts a query."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("Statement executed successfully: query ... cancelled.",)
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM huge_table",
            _conn=mock_conn
        )
        
        result = job.abort()
        
        assert result is True
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT SYSTEM$CANCEL_QUERY('test-query-id')")
        mock_cursor.close.assert_called_once()

    def test_abort_failure(self):
        """Test abort() returns False when abort fails."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("Statement executed successfully: query ... not found.",)
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM huge_table",
            _conn=mock_conn
        )
        
        result = job.abort()
        
        assert result is False
        mock_cursor.execute.assert_called_once_with("SELECT SYSTEM$CANCEL_QUERY('test-query-id')")
        mock_cursor.close.assert_called_once()

    def test_abort_closes_cursor_on_exception(self):
        """Test that abort() closes cursor even when an exception occurs."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Abort failed")
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT * FROM huge_table",
            _conn=mock_conn
        )
        
        with pytest.raises(Exception, match="Abort failed"):
            job.abort()
        
        mock_cursor.close.assert_called_once()

    def test_frozen_dataclass(self):
        """Test that QueryJob is frozen (immutable)."""
        mock_conn = MagicMock()
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT 1",
            _conn=mock_conn
        )
        
        # Verify we can't modify attributes (frozen dataclass)
        with pytest.raises(AttributeError):
            job.query_id = "new-id"  # type: ignore

    def test_status_property(self):
        """Test the status property returns the correct status string."""
        mock_conn = MagicMock()
        mock_conn.get_query_status.return_value = QueryStatus.SUCCESS
        
        job = QueryJob(
            query_id="test-query-id",
            sql="SELECT 1",
            _conn=mock_conn
        )
        
        assert job.status == "SUCCESS"
        mock_conn.get_query_status.assert_called_once_with("test-query-id")
