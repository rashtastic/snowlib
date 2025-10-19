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
    
    def test_query_result_to_df(self):
        """Test to_df() with fetch_pandas_all."""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        test_df = pd.DataFrame({"ID": [1, 2, 3], "NAME": ["A", "B", "C"]})
        mock_cursor.fetch_pandas_all.return_value = test_df
        mock_cursor.fetchall.return_value = [(1, "A"), (2, "B"), (3, "C")]
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
    
    def test_query_result_to_df_lowercases_columns(self):
        """Test to_df() automatically lowercases columns"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        test_df = pd.DataFrame({"ID": [1, 2], "NAME": ["A", "B"]})
        mock_cursor.fetch_pandas_all.return_value = test_df
        mock_cursor.fetchall.return_value = [(1, "A"), (2, "B")]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 2
        mock_cursor.query = "SELECT * FROM test"
        mock_cursor.description = [("ID",), ("NAME",)]
        
        result = QueryResult(_cursor=mock_cursor)
        df = result.to_df()
        
        assert "id" in df.columns
        assert "name" in df.columns
    
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
        """Test empty result with column structure"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        empty_df = pd.DataFrame(columns=["ID", "NAME", "VALUE"])
        mock_cursor.fetch_pandas_all.return_value = empty_df
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [("ID",), ("NAME",), ("VALUE",)]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 0
        mock_cursor.query = "SELECT * FROM empty_table"
        
        result = QueryResult(_cursor=mock_cursor)
        df = result.to_df()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "name", "value"]


class TestQueryResultMethods:
    """Tests for QueryResult data retrieval methods"""
    
    def test_fetch_one_returns_tuple(self):
        """Test that fetch_one returns a single tuple"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (42, "test", 3.14)
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 1
        mock_cursor.query = "SELECT * FROM test"
        
        result = QueryResult(_cursor=mock_cursor)
        row = result.fetch_one()
        
        assert row == (42, "test", 3.14)
        mock_cursor.fetchone.assert_called_once()
    
    def test_fetch_one_returns_none_when_empty(self):
        """Test that fetch_one returns None for empty results"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 0
        mock_cursor.query = "SELECT * FROM empty_table"
        
        result = QueryResult(_cursor=mock_cursor)
        row = result.fetch_one()
        
        assert row is None
    
    def test_fetch_all_returns_list(self):
        """Test that fetch_all returns list of tuples"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 3
        mock_cursor.query = "SELECT * FROM test"
        
        result = QueryResult(_cursor=mock_cursor)
        rows = result.fetch_all()
        
        assert rows == [(1,), (2,), (3,)]
        assert len(rows) == 3
    
    def test_fetch_all_returns_empty_list(self):
        """Test that fetch_all returns empty list for empty results"""
        from snowlib.primitives.result import QueryResult
        
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = None
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 0
        mock_cursor.query = "SELECT * FROM empty_table"
        
        result = QueryResult(_cursor=mock_cursor)
        rows = result.fetch_all()
        
        assert rows == []
    
    def test_fetch_batches_yields_dataframes(self):
        """Test that fetch_batches yields DataFrame batches with lowercased columns"""
        from snowlib.primitives.result import QueryResult
        import pandas as pd
        
        mock_cursor = Mock()
        batch1 = pd.DataFrame({"ID": [1, 2], "NAME": ["A", "B"]})
        batch2 = pd.DataFrame({"ID": [3, 4], "NAME": ["C", "D"]})
        mock_cursor.fetch_pandas_batches.return_value = iter([batch1, batch2])
        mock_cursor.fetchall.return_value = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        mock_cursor.description = [("ID",), ("NAME",)]
        mock_cursor.sfqid = "test-id"
        mock_cursor.rowcount = 4
        mock_cursor.query = "SELECT * FROM test"
        
        result = QueryResult(_cursor=mock_cursor)
        batches = list(result.fetch_batches())
        
        # When HAS_PYARROW is True, we get 2 batches; when False, we get 1 batch (fallback to fetch all)
        assert len(batches) >= 1
        assert "id" in batches[0].columns
        assert "name" in batches[0].columns
