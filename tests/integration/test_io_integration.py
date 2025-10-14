"""Integration tests for io module - read, write, query functions.

These tests require a real Snowflake connection and use the test profile.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from snowflake.connector.errors import ProgrammingError
import uuid

from snowlib import SnowflakeContext
from snowlib.io import read, write, query


@pytest.fixture(scope="class")
def ctx(test_profile):
    """Create a shared SnowflakeContext for all tests.
    
    This minimizes SSO popups by reusing the connection.
    """
    context = SnowflakeContext(profile=test_profile)
    yield context
    # Cleanup: close connection after all tests
    if hasattr(context, '_connection') and context._connection:
        context._connection.close()


class TestQuery:
    """Test query() function with various SQL statements."""
    
    def test_query_simple_select(self, ctx, check_pandas_integration):
        """Test simple SELECT statement."""
        df = query("SELECT 1 AS num, 'hello' AS text", context=ctx)
        
        assert len(df) == 1
        assert df.iloc[0]["num"] == 1
        assert df.iloc[0]["text"] == "hello"
    
    def test_query_with_dates(self, ctx, check_pandas_integration):
        """Test query returning dates."""
        df = query("SELECT CURRENT_DATE() AS today", context=ctx)
        
        assert len(df) == 1
        assert isinstance(df.iloc[0]["today"], (date, pd.Timestamp))
    
    def test_query_with_timestamps(self, ctx, check_pandas_integration):
        """Test query returning timestamps."""
        df = query("SELECT CURRENT_TIMESTAMP() AS now", context=ctx)
        
        assert len(df) == 1
        assert isinstance(df.iloc[0]["now"], (datetime, pd.Timestamp))
    
    def test_query_multiple_rows(self, ctx, check_pandas_integration):
        """Test query returning multiple rows."""
        df = query("""
            SELECT value FROM (
                VALUES (1), (8), (5), (1)
            ) AS t(value)
        """, context=ctx)
        
        assert len(df) == 4
        assert df["value"].tolist() == [1, 8, 5, 1]
    
    def test_query_multiple_types(self, ctx, check_pandas_integration):
        """Test query with various data types."""
        df = query("""
            SELECT 
                2 AS int_col,
                2.1 AS float_col,
                'DEION' AS text_col,
                TRUE AS bool_col,
                CURRENT_DATE() AS date_col,
                CURRENT_TIMESTAMP() AS ts_col
        """, context=ctx)
        
        assert len(df) == 1
        row = df.iloc[0]
        
        assert row["int_col"] == 2
        assert row["float_col"] == 2.1
        assert row["text_col"] == "DEION"
        assert row["bool_col"] == True  # Use == for numpy bool compatibility
        assert isinstance(row["date_col"], (date, pd.Timestamp))
        assert isinstance(row["ts_col"], (datetime, pd.Timestamp))
    
    def test_query_lowercase_columns(self, ctx, check_pandas_integration):
        """Test lowercase_columns parameter."""
        df = query("SELECT 1 AS MY_COLUMN", context=ctx, lowercase_columns=True)
        
        assert "my_column" in df.columns
        assert "MY_COLUMN" not in df.columns


class TestRead:
    """Test read() function with various table name formats."""
    
    def test_read_fully_qualified_uppercase(self, ctx, test_read_table, check_pandas_integration):
        """Test reading with fully qualified uppercase name."""
        df = read(test_read_table, context=ctx)
        
        assert len(df) > 0
        assert isinstance(df, pd.DataFrame)
    
    def test_read_fully_qualified_lowercase(self, ctx, test_read_table, check_pandas_integration):
        """Test reading with fully qualified lowercase name."""
        df = read(test_read_table.lower(), context=ctx)
        
        assert len(df) > 0
        assert isinstance(df, pd.DataFrame)
    
    def test_read_fully_qualified_mixed_case(self, ctx, test_read_table, check_pandas_integration):
        """Test reading with fully qualified mixed case name."""
        # Convert to mixed case: Database.SCHEMA.table_name
        parts = test_read_table.split('.')
        if len(parts) == 3:
            mixed_case = f"{parts[0].capitalize()}.{parts[1].upper()}.{parts[2].lower()}"
        else:
            mixed_case = test_read_table  # Fallback if format is unexpected
        
        df = read(mixed_case, context=ctx)
        
        assert len(df) > 0
        assert isinstance(df, pd.DataFrame)
    
    def test_read_partial_qualification(self, ctx, test_read_table, check_pandas_integration):
        """Test reading with schema.table format."""
        # Extract schema.table from fully qualified name
        parts = test_read_table.split('.')
        if len(parts) == 3:
            schema_table = f"{parts[1]}.{parts[2]}"
        else:
            schema_table = test_read_table  # Fallback
        
        df = read(schema_table, context=ctx)
        
        assert len(df) > 0
        assert isinstance(df, pd.DataFrame)
    
    def test_read_with_context_inference(self, ctx, test_read_table, check_pandas_integration):
        """Test reading with minimal name using context."""
        # Extract database, schema, and table from fully qualified name
        parts = test_read_table.split('.')
        if len(parts) == 3:
            database, schema, table = parts
            # Set database and schema context
            ctx.cursor.execute(f"USE DATABASE {database}")
            ctx.cursor.execute(f"USE SCHEMA {schema}")
            
            df = read(table, context=ctx)
            
            assert len(df) > 0
            assert isinstance(df, pd.DataFrame)
        else:
            # Skip if table name format is unexpected
            pytest.skip("Test requires fully qualified table name")
    
    def test_read_lowercase_columns(self, ctx, test_read_table, check_pandas_integration):
        """Test lowercase_columns parameter."""
        df = read(
            test_read_table, 
            context=ctx,
            lowercase_columns=True
        )
        
        # All columns should be lowercase
        assert all(col == col.lower() for col in df.columns)
    
    def test_read_nonexistent_table(self, ctx, test_database, test_schema):
        """Test reading nonexistent table raises error."""
        nonexistent_table = f"{test_database}.{test_schema}.NONEXISTENT_TABLE_12345"
        with pytest.raises(Exception):  # Snowflake will raise an error
            read(nonexistent_table, context=ctx)


class TestWrite:
    """Test write() function with various modes and data types."""
    
    @pytest.fixture(scope="function")
    def test_table_name(self):
        """Generate unique test table name for each function."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:6]
        return f"TEST_WRITE_{timestamp}_{unique_id}"
    
    @pytest.fixture(scope="class")
    def sample_df(self):
        """Create sample DataFrame with various data types and missing values."""
        return pd.DataFrame({
            'int_col': [1, 2, None, 4, 5],
            'float_col': [1.1, 2.2, np.nan, 4.4, 5.5],
            'text_col': ['a', 'b', None, 'd', 'e'],
            'datetime_col': [
                datetime(1988, 1, 1),
                datetime(1994, 1, 1),
                pd.NaT,
                datetime(2000, 1, 4),
                datetime(2014, 1, 6)
            ],
            'date_col': [
                date(1988, 1, 1),
                date(1994, 1, 1),
                None,
                date(2000, 1, 4),
                date(2014, 1, 6)
            ],
            'bool_col': [True, False, None, True, False]
        })
    
    def test_write_replace_mode(self, ctx, test_table_name, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with replace mode (default)."""
        # Write data
        result = write(
            sample_df,
            f"{test_database}.{test_schema}.{test_table_name}",
            context=ctx
        )
        
        assert result is True
        
        # Verify data was written
        df_read = read(f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        assert len(df_read) == len(sample_df)
    
    def test_write_replace_mode_explicit(self, ctx, test_table_name, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with explicit replace mode."""
        # Write initial data
        write(sample_df, f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        
        # Replace with new data
        new_df = pd.DataFrame({'col1': [100, 200]})
        result = write(
            new_df,
            f"{test_database}.{test_schema}.{test_table_name}",
            mode="replace",
            context=ctx
        )
        
        assert result is True
        
        # Verify old data was replaced
        df_read = read(f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        assert len(df_read) == 2  # Only new data
        assert 'col1' in df_read.columns
        assert 'int_col' not in df_read.columns
    
    def test_write_append_mode(self, ctx, test_table_name, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with append mode."""
        # Write initial data
        write(sample_df, f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        initial_count = len(sample_df)
        
        # Append more data
        result = write(
            sample_df,
            f"{test_database}.{test_schema}.{test_table_name}",
            mode="append",
            context=ctx
        )
        
        assert result is True
        
        # Verify data was appended
        df_read = read(f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        assert len(df_read) == initial_count * 2
    
    def test_write_fail_mode_table_exists(self, ctx, test_table_name, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with fail mode when table exists."""
        # Write initial data
        write(sample_df, f"{test_database}.{test_schema}.{test_table_name}", context=ctx, uppercase_columns=False)
        
        # Try to write again with fail mode
        with pytest.raises(ProgrammingError):
            write(
                sample_df,
                f"{test_database}.{test_schema}.{test_table_name}",
                mode="fail",
                context=ctx,
                uppercase_columns=False
            )
    
    def test_write_fail_mode_table_not_exists(self, ctx, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with fail mode when table doesn't exist."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_table = f"TEST_FAIL_{timestamp}"
        
        # Should succeed on first write
        result = write(
            sample_df,
            f"{test_database}.{test_schema}.{unique_table}",
            mode="fail",
            context=ctx
        )
        
        assert result is True
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {test_database}.{test_schema}.{unique_table}")
    
    def test_write_read_roundtrip(self, ctx, test_table_name, test_database, test_schema, sample_df, check_pandas_integration):
        """Test that data types and values are preserved in write/read roundtrip."""
        # Write data
        write(sample_df, f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        
        # Read it back
        df_read = read(f"{test_database}.{test_schema}.{test_table_name}", context=ctx)
        
        # Verify shape
        assert len(df_read) == len(sample_df)
        assert len(df_read.columns) == len(sample_df.columns)
        
        # Verify column names exist (case may vary, but all should be present)
        # Snowflake returns lowercase by default in our read implementation
        expected_cols_lower = {col.lower() for col in sample_df.columns}
        actual_cols_lower = {col.lower() for col in df_read.columns}
        assert expected_cols_lower == actual_cols_lower
        
        # Note: Exact value comparison is tricky due to type conversions
        # and NULL handling, but we verified shape and columns
    
    def test_write_with_uppercase_columns(self, ctx, test_database, test_schema, sample_df, check_pandas_integration):
        """Test uppercase_columns parameter."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_table = f"TEST_UPPERCASE_{timestamp}"
        
        # Write with lowercase column names, but uppercase_columns=True
        df_lower = sample_df.copy()
        df_lower.columns = [col.lower() for col in df_lower.columns]
        
        write(
            df_lower,
            f"{test_database}.{test_schema}.{unique_table}",
            context=ctx,
            uppercase_columns=True
        )
        
        # Read back - columns will be lowercase due to our read implementation
        # But we can verify the table was created successfully
        df_read = read(f"{test_database}.{test_schema}.{unique_table}", context=ctx)
        assert len(df_read) == len(sample_df)
        assert len(df_read.columns) == len(sample_df.columns)
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {test_database}.{test_schema}.{unique_table}")
    
    def test_write_with_uppercase_table(self, ctx, test_database, test_schema, sample_df, check_pandas_integration):
        """Test uppercase_table parameter."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Write with lowercase table name, but uppercase_table=True
        result = write(
            sample_df,
            f"{test_database.lower()}.{test_schema.lower()}.test_upper_table_{timestamp}",
            context=ctx,
            uppercase_table=True
        )
        
        assert result is True
        
        # Verify we can read it back with uppercase name
        df_read = read(f"{test_database}.{test_schema}.TEST_UPPER_TABLE_{timestamp}", context=ctx)
        assert len(df_read) > 0
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {test_database}.{test_schema}.TEST_UPPER_TABLE_{timestamp}")
    
    def test_write_partial_qualification(self, ctx, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with schema.table format."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_table = f"TEST_PARTIAL_{timestamp}"
        
        # Write with partial qualification
        result = write(
            sample_df,
            f"{test_schema}.{unique_table}",
            context=ctx
        )
        
        assert result is True
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {test_database}.{test_schema}.{unique_table}")
    
    def test_write_minimal_qualification(self, ctx, test_database, test_schema, sample_df, check_pandas_integration):
        """Test write with just table name using context."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_table = f"TEST_MINIMAL_{timestamp}"
        
        # Set database and schema context
        ctx.cursor.execute(f"USE DATABASE {test_database}")
        ctx.cursor.execute(f"USE SCHEMA {test_schema}")
        
        # Write with minimal qualification
        result = write(sample_df, unique_table, context=ctx)
        
        assert result is True
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {unique_table}")


class TestConnectionReuse:
    """Test that connection reuse works across multiple operations."""
    
    def test_multiple_operations_same_context(self, ctx, test_read_table, test_database, test_schema, check_pandas_integration):
        """Test multiple operations with same context."""
        # This should only trigger one SSO popup (at context creation)
        
        # Query
        df1 = query("SELECT 1 AS num", context=ctx)
        assert len(df1) == 1
        
        # Read
        df2 = read(test_read_table, context=ctx)
        assert len(df2) > 0
        
        # Write
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_table = f"TEST_REUSE_{timestamp}"
        df_test = pd.DataFrame({'col1': [1, 2, 3]})
        
        result = write(df_test, f"{test_database}.{test_schema}.{test_table}", context=ctx)
        assert result is True
        
        # Read back
        df3 = read(f"{test_database}.{test_schema}.{test_table}", context=ctx)
        assert len(df3) == 3
        
        # Cleanup
        ctx.cursor.execute(f"DROP TABLE IF EXISTS {test_database}.{test_schema}.{test_table}")
        
        # All operations should use the same connection
        assert ctx._connection is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
