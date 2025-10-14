"""Tests for QueryResult wrapper class."""

from unittest.mock import Mock
import pandas as pd


class TestQueryResult:
    """Tests for QueryResult class."""
    
    def test_query_result_properties(self):
        """Test that QueryResult exposes cursor properties."""
        from snowlib.primitives.result import QueryResult
        
        # Create mock cursor
        mock_cursor = Mock()
        mock_cursor.sfqid = "01bf9ad6-0515-eff2-0000-89c52392cd3e"
        mock_cursor.rowcount = 42
        mock_cursor.query = "SELECT * FROM test_table"
        mock_cursor.description = [
            ("ID", "NUMBER", None, None, None, None, None),
            ("NAME", "VARCHAR", None, None, None, None, None),
        ]
        
        # Create QueryResult
        result = QueryResult(_cursor=mock_cursor)
        
        # Test properties
        assert result.query_id == "01bf9ad6-0515-eff2-0000-89c52392cd3e"
        assert result.rowcount == 42
        assert result.sql == "SELECT * FROM test_table"
        assert result.description is not None
        assert len(result.description) == 2
    
    def test_query_result_fetchone(self):
        """Test fetchone() delegation."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1, "Alice")
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 1
        mock_cursor.query = "SELECT * FROM users LIMIT 1"
        
        result = QueryResult(_cursor=mock_cursor)
        row = result.fetchone()
        
        assert row == (1, "Alice")
        mock_cursor.fetchone.assert_called_once()
    
    def test_query_result_fetchall(self):
        """Test fetchall() delegation."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 2
        mock_cursor.query = "SELECT * FROM users"
        
        result = QueryResult(_cursor=mock_cursor)
        rows = result.fetchall()
        
        assert rows == [(1, "Alice"), (2, "Bob")]
        mock_cursor.fetchall.assert_called_once()
    
    def test_query_result_to_df(self):
        """Test to_df() with fetch_pandas_all."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        test_df = pd.DataFrame({"ID": [1, 2, 3], "NAME": ["A", "B", "C"]})
        mock_cursor.fetch_pandas_all.return_value = test_df
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 3
        mock_cursor.query = "SELECT * FROM test"
        mock_cursor.description = [("ID",), ("NAME",)]
        
        result = QueryResult(_cursor=mock_cursor)
        df = result.to_df()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        # Default is lowercase columns
        assert "id" in df.columns
        assert "name" in df.columns
    
    def test_query_result_to_df_uppercase(self):
        """Test to_df() preserving uppercase columns."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        test_df = pd.DataFrame({"ID": [1, 2], "NAME": ["A", "B"]})
        mock_cursor.fetch_pandas_all.return_value = test_df
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 2
        mock_cursor.query = "SELECT * FROM test"
        mock_cursor.description = [("ID",), ("NAME",)]
        
        result = QueryResult(_cursor=mock_cursor)
        df = result.to_df(lowercase_columns=False)
        
        assert "ID" in df.columns
        assert "NAME" in df.columns
    
    def test_query_result_repr(self):
        """Test string representation."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.sfqid = "test-query-123"
        mock_cursor.rowcount = 5
        mock_cursor.query = "SELECT * FROM test"
        
        result = QueryResult(_cursor=mock_cursor)
        repr_str = repr(result)
        
        assert "QueryResult" in repr_str
        assert "test-query-123" in repr_str
        assert "5" in repr_str
    
    def test_query_result_handles_none_rowcount(self):
        """Test that None rowcount is converted to -1."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.rowcount = None
        mock_cursor.sfqid = "test-id"
        mock_cursor.query = "CREATE TABLE test (id INT)"
        
        result = QueryResult(_cursor=mock_cursor)
        
        assert result.rowcount == -1
    
    def test_query_result_empty_dataframe_with_columns(self):
        """Test empty result with column structure."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        empty_df = pd.DataFrame(columns=["ID", "NAME", "VALUE"])
        mock_cursor.fetch_pandas_all.return_value = empty_df
        mock_cursor.description = [("ID",), ("NAME",), ("VALUE",)]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 0
        mock_cursor.query = "SELECT * FROM empty_table"
        
        result = QueryResult(_cursor=mock_cursor)
        df = result.to_df()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "name", "value"]
