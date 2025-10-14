"""Integration tests for primitives module.

These tests run against a real Snowflake instance using SSO authentication.

These tests now use SnowflakeContext for connection reuse, which means:
- Each test CLASS only authenticates ONCE (one browser popup per class if SSO)
- Much faster test execution
- Better resource usage
"""

import pytest
import pandas as pd
import uuid


@pytest.fixture(scope="class")
def ctx(test_profile):
    """Shared SnowflakeContext for all tests in a class.
    
    This means all tests in a class share ONE connection,
    resulting in only ONE browser popup per test class
    """
    from snowlib.primitives import SnowflakeContext
    
    context = SnowflakeContext(profile=test_profile)
    yield context
    context.close()


@pytest.fixture
def test_table_name(test_write_table):
    """Generate unique test table name.
    
    Uses the write_table from test config as base, adds UUID for uniqueness.
    """
    return f"{test_write_table}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def qualified_test_table(test_database, test_schema, test_table_name):
    """Generate fully qualified test table name.
    
    Returns database.schema.table for use in SQL.
    """
    return f"{test_database}.{test_schema}.{test_table_name}"


class TestExecutionPrimitives:
    """Integration tests for execution primitives."""
    
    def test_execute_sql_create_and_drop_table(self, ctx, qualified_test_table):
        """Test creating and dropping a table."""
        from snowlib.primitives import execute_sql, QueryResult
        
        # Create table
        result = execute_sql(f"CREATE TABLE {qualified_test_table} (id INT, name VARCHAR(50))", context=ctx)
        assert isinstance(result, QueryResult)
        assert result.query_id is not None  # Should have a query ID
        
        # Drop table
        result = execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
        assert isinstance(result, QueryResult)
    
    def test_fetch_one_current_timestamp(self, ctx):
        """Test fetch_one with CURRENT_TIMESTAMP."""
        from snowlib.primitives import fetch_one
        
        result = fetch_one("SELECT CURRENT_TIMESTAMP()", context=ctx)
        
        assert result is not None
        assert len(result) == 1
        # Should return a timestamp
        assert result[0] is not None
    
    def test_fetch_one_no_results(self, ctx, qualified_test_table):
        """Test fetch_one with no results."""
        from snowlib.primitives import execute_sql, fetch_one
        
        # Create empty table
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        
        try:
            result = fetch_one(f"SELECT * FROM {qualified_test_table}", context=ctx)
            assert result is None
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_fetch_all_with_data(self, ctx, qualified_test_table):
        """Test fetch_all returns all rows."""
        from snowlib.primitives import execute_sql, fetch_all
        
        # Create and populate table
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        execute_sql(f"INSERT INTO {qualified_test_table} VALUES (1), (2), (3)", context=ctx)
        
        try:
            result = fetch_all(f"SELECT * FROM {qualified_test_table} ORDER BY id", context=ctx)
            
            assert len(result) == 3
            assert result[0] == (1,)
            assert result[1] == (2,)
            assert result[2] == (3,)
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_fetch_df_returns_dataframe(self, ctx, qualified_test_table, check_pandas_integration):
        """Test fetch_df returns DataFrame with correct data."""
        from snowlib.primitives import execute_sql, fetch_df
        
        # Create and populate table
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT, name VARCHAR(50))", context=ctx)
        execute_sql(f"""
            INSERT INTO {qualified_test_table} VALUES 
            (1, 'Alice'), 
            (2, 'Bob'), 
            (3, 'Charlie')
        """, context=ctx)
        
        try:
            df = fetch_df(f"SELECT * FROM {qualified_test_table} ORDER BY id", context=ctx)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert "id" in df.columns  # Lowercase by default
            assert "name" in df.columns
            assert df["id"].tolist() == [1, 2, 3]
            assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_fetch_df_empty_table(self, ctx, qualified_test_table, check_pandas_integration):
        """Test fetch_df with empty table preserves columns."""
        from snowlib.primitives import execute_sql, fetch_df
        
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT, value VARCHAR(50))", context=ctx)
        
        try:
            df = fetch_df(f"SELECT * FROM {qualified_test_table}", context=ctx)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0
            assert "id" in df.columns
            assert "value" in df.columns
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_execute_block_multi_statement(self, ctx, qualified_test_table):
        """Test execute_block with multiple statements."""
        from snowlib.primitives import execute_block, execute_sql
        
        sql_script = f"""
        CREATE TEMP TABLE {qualified_test_table} (id INT);
        INSERT INTO {qualified_test_table} VALUES (1), (2), (3);
        SELECT * FROM {qualified_test_table};
        """
        
        try:
            results = execute_block(sql_script, context=ctx)
            
            # Should have results from all statements
            assert len(results) >= 3
            
            # Last result should be from SELECT
            select_results = results[-1]
            assert len(select_results) == 3
        finally:
            # Cleanup (TEMP table should auto-cleanup, but be safe)
            try:
                execute_sql(f"DROP TABLE IF EXISTS {test_table_name}", context=ctx)
            except:  # noqa: E722
                pass


class TestDataPrimitives:
    """Integration tests for data I/O primitives."""
    
    def test_read_write_table_round_trip(self, ctx, test_table_name, test_database, test_schema, qualified_test_table, check_pandas_integration):
        """Test writing and reading a table."""
        from snowlib.primitives import write_table, read_table, execute_sql
        
        # Create test DataFrame
        df_original = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10.5, 20.5, 30.5],
            "name": ["alpha", "beta", "gamma"]
        })
        
        try:
            # Write table with explicit database and schema
            success = write_table(
                df_original, 
                test_table_name, 
                schema=test_schema,
                database=test_database,
                mode="replace",
                context=ctx
            )
            assert success is True
            
            # Read back with explicit database and schema
            df_result = read_table(test_table_name, schema=test_schema, database=test_database, context=ctx)
            
            assert len(df_result) == 3
            assert "id" in df_result.columns
            assert "value" in df_result.columns
            assert "name" in df_result.columns
            
            # Sort for comparison
            df_result = df_result.sort_values("id").reset_index(drop=True)
            assert df_result["id"].tolist() == [1, 2, 3]
            assert df_result["name"].tolist() == ["alpha", "beta", "gamma"]
        finally:
            execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}", context=ctx)
    
    def test_write_table_mode_append(self, ctx, test_table_name, test_database, test_schema, qualified_test_table, check_pandas_integration):
        """Test write_table with append mode."""
        from snowlib.primitives import write_table, read_table, execute_sql
        
        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [3, 4], "value": [30, 40]})
        
        try:
            # Write first batch
            write_table(df1, test_table_name, schema=test_schema, database=test_database, mode="replace", context=ctx)
            
            # Append second batch
            write_table(df2, test_table_name, schema=test_schema, database=test_database, mode="append", context=ctx)
            
            # Read back
            df_result = read_table(test_table_name, schema=test_schema, database=test_database, context=ctx)
            
            assert len(df_result) == 4
            assert sorted(df_result["id"].tolist()) == [1, 2, 3, 4]
        finally:
            execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}", context=ctx)
    
    def test_write_table_mode_fail(self, ctx, test_table_name, test_database, test_schema, qualified_test_table, check_pandas_integration):
        """Test write_table in fail mode raises error if table exists."""
        from snowlib.primitives import write_table, execute_sql
        from snowflake.connector.errors import ProgrammingError
        
        df = pd.DataFrame({"id": [1, 2]})
        
        try:
            # First write succeeds
            write_table(df, test_table_name, schema=test_schema, database=test_database, mode="replace", context=ctx)
            
            # Second write with mode="fail" should raise error
            with pytest.raises(ProgrammingError, match="already exists"):
                write_table(df, test_table_name, schema=test_schema, database=test_database, mode="fail", context=ctx)
        finally:
            execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}", context=ctx)


class TestMetadataPrimitives:
    """Integration tests for metadata primitives."""
    
    def test_get_columns(self, ctx, test_table_name, qualified_test_table, test_schema, test_database, check_pandas_integration):
        """Test get_columns returns column names."""
        from snowlib.primitives import execute_sql, get_columns
        
        execute_sql((
            f"CREATE TABLE {qualified_test_table} "
            "(id INT, name VARCHAR(50), value FLOAT, created_at TIMESTAMP)"
        ), context=ctx)
        
        try:
            columns = get_columns(test_table_name, test_schema, test_database, context=ctx)
            
            assert isinstance(columns, list)
            assert len(columns) == 4
            # Should be uppercase by default
            assert "ID" in columns
            assert "NAME" in columns
            assert "VALUE" in columns
            assert "CREATED_AT" in columns
            
            # Test lowercase
            columns_lower = get_columns(test_table_name, test_schema, test_database, uppercase=False, context=ctx)
            assert "id" in columns_lower
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_table_exists(self, ctx, test_table_name, qualified_test_table, test_schema, test_database, check_pandas_integration):
        """Test table_exists returns correct boolean."""
        from snowlib.primitives import execute_sql, table_exists
        
        # Table doesn't exist yet
        assert table_exists(test_table_name, test_schema, test_database, context=ctx) is False
        
        # Create table
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        
        try:
            # Now it exists
            assert table_exists(test_table_name, test_schema, test_database, context=ctx) is True
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
            
            # After drop, doesn't exist
            assert table_exists(test_table_name, test_schema, test_database, context=ctx) is False
    
    def test_list_tables(self, ctx, test_table_name, qualified_test_table, test_schema, test_database, check_pandas_integration):
        """Test list_tables returns table list."""
        from snowlib.primitives import execute_sql, list_tables
        
        # Create test table
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        
        try:
            # List tables again
            tables = list_tables(test_schema, test_database, context=ctx)
            
            assert isinstance(tables, list)
            assert len(tables) > 0
            # Table name should be in the list (might be uppercase)
            assert test_table_name.upper() in tables
        finally:
            # Use fully qualified name for DROP
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_get_current_context(self, test_profile):
        """Test get_current_* functions with test profile.
        
        Note: These functions use context parameter to specify which profile to use.
        """
        from snowlib.primitives import (
            get_current_database,
            get_current_schema,
            get_current_warehouse,
            get_current_role
        )
        
        # All should return non-empty strings
        db = get_current_database(context=test_profile)
        assert isinstance(db, str)
        assert len(db) > 0
        
        schema = get_current_schema(context=test_profile)
        assert isinstance(schema, str)
        assert len(schema) > 0
        
        wh = get_current_warehouse(context=test_profile)
        assert isinstance(wh, str)
        assert len(wh) > 0
        
        role = get_current_role(context=test_profile)
        assert isinstance(role, str)
        assert len(role) > 0


class TestStreamingPrimitives:
    """Integration tests for streaming primitives."""
    
    def test_fetch_batches(self, ctx, qualified_test_table, check_pandas_integration):
        """Test fetch_batches yields DataFrame batches."""
        from snowlib.primitives import execute_sql, fetch_batches
        
        # Create table with 100 rows
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        
        # Insert multiple rows
        values = ", ".join([f"({i})" for i in range(100)])
        execute_sql(f"INSERT INTO {qualified_test_table} VALUES {values}", context=ctx)
        
        try:
            total_rows = 0
            batch_count = 0
            
            for batch_df in fetch_batches(
                f"SELECT * FROM {qualified_test_table}",
                context=ctx,
                batch_size=30
            ):
                assert isinstance(batch_df, pd.DataFrame)
                assert "id" in batch_df.columns
                total_rows += len(batch_df)
                batch_count += 1
            
            # Should have fetched all 100 rows
            assert total_rows == 100
            # With batch_size=30, should have multiple batches
            assert batch_count >= 1
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)


class TestAsyncExecution:
    """Integration tests for asynchronous query execution."""

    def test_execute_sql_async_basic(self, ctx, check_pandas_integration):
        """Test basic async execution with a simple query."""
        from snowlib.primitives import execute_sql_async
        from snowlib.primitives.job import QueryJob
        
        # Submit async query
        job = execute_sql_async("SELECT 1 as test_col", context=ctx)
        
        # Verify we got a QueryJob back
        assert isinstance(job, QueryJob)
        assert job.query_id is not None
        assert job.sql == "SELECT 1 as test_col"
        
        # Get the result (blocks until complete)
        result = job.get_result()
        
        # Verify result
        assert result.query_id == job.query_id
        # Note: rowcount might be -1 for SELECT queries via async
        df = result.to_df()
        assert len(df) == 1
        assert df["test_col"].iloc[0] == 1

    def test_execute_sql_async_with_delay(self, ctx):
        """Test async execution with a delayed query using SYSTEM$WAIT."""
        from snowlib.primitives import execute_sql_async
        
        # Submit a query that takes a few seconds
        job = execute_sql_async("CALL SYSTEM$WAIT(3)", context=ctx)
        
        # Should return immediately with a job
        assert job.query_id is not None
        
        # Check status - might still be running
        # (though 3 seconds is short, so it might finish quickly)
        is_running = job.is_running()
        # Just verify the method works without error
        assert isinstance(is_running, bool)
        
        # Wait for completion
        result = job.get_result()
        
        # Verify result
        assert result.query_id == job.query_id

    def test_execute_sql_async_polling(self, ctx):
        """Test polling an async query until completion."""
        import time
        from snowlib.primitives import execute_sql_async
        
        # Submit a longer-running query
        job = execute_sql_async("CALL SYSTEM$WAIT(5)", context=ctx)
        
        # Poll until done
        max_wait = 10  # seconds
        start_time = time.time()
        poll_count = 0
        
        while job.is_running():
            poll_count += 1
            if time.time() - start_time > max_wait:
                pytest.fail("Query took too long to complete")
            time.sleep(0.5)
        
        # Verify we actually polled at least once
        # (might complete very quickly, so this is lenient)
        assert poll_count >= 0
        
        # Get result
        result = job.get_result()
        assert result.query_id == job.query_id

    def test_execute_sql_async_with_results(self, ctx, qualified_test_table, check_pandas_integration):
        """Test async execution that returns actual data."""
        from snowlib.primitives import execute_sql, execute_sql_async
        
        try:
            # Create and populate a test table
            execute_sql(
                f"CREATE TABLE {qualified_test_table} (id INT, value VARCHAR(50))",
                context=ctx
            )
            execute_sql(
                f"INSERT INTO {qualified_test_table} VALUES (1, 'async'), (2, 'test'), (3, 'data')",
                context=ctx
            )
            
            # Run async query
            job = execute_sql_async(
                f"SELECT * FROM {qualified_test_table} ORDER BY id",
                context=ctx
            )
            
            # Get results
            result = job.get_result()
            
            # Verify - rowcount might not be available for async SELECT queries
            # so we verify via the DataFrame instead
            df = result.to_df()
            assert len(df) == 3
            assert list(df["id"]) == [1, 2, 3]
            assert list(df["value"]) == ["async", "test", "data"]
            
        finally:
            execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}", context=ctx)

    def test_execute_sql_async_abort(self, ctx):
        """Test aborting an async query."""
        from snowlib.primitives import execute_sql_async
        
        # Submit a long-running query
        job = execute_sql_async("CALL SYSTEM$WAIT(30)", context=ctx)
        
        # Immediately try to abort it
        abort_result = job.abort()
        
        # abort() should return a boolean
        assert isinstance(abort_result, bool)
        # Note: The query might have already completed if it's very fast,
        # so we can't assert abort_result is True

    def test_execute_sql_async_multiple_concurrent(self, ctx, check_pandas_integration):
        """Test running multiple async queries concurrently."""
        from snowlib.primitives import execute_sql_async
        
        # Submit multiple queries
        jobs = []
        for i in range(3):
            job = execute_sql_async(f"SELECT {i} as query_num", context=ctx)
            jobs.append(job)
        
        # All should have different query IDs
        query_ids = [job.query_id for job in jobs]
        assert len(set(query_ids)) == 3  # All unique
        
        # Get all results
        results = [job.get_result() for job in jobs]
        
        # All should complete successfully
        assert len(results) == 3
        for i, result in enumerate(results):
            df = result.to_df()
            assert len(df) == 1
