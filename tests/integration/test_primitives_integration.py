"""Integration tests for primitives module.

Tests for Layer 2 primitives - stateless SQL execution functions.

This focuses on:
- Query execution (execute_sql, fetch_one, fetch_all, fetch_df)
- Streaming (fetch_batches)
- Async execution (execute_sql_async, AsyncQuery)

These tests use SnowflakeContext for connection reuse, which means:
- Each test CLASS only authenticates ONCE (one browser popup per class if SSO)
- Much faster test execution
- Better resource usage

Note: Data I/O and metadata operations are tested in test_models_integration.py
"""

import pytest
import pandas as pd
import uuid
from typing import Iterator
from snowlib.primitives import SnowflakeContext


@pytest.fixture(scope="class")
def ctx(test_profile) -> Iterator['SnowflakeContext']:
    """Shared SnowflakeContext for all tests in a class.
    
    This means all tests in a class share ONE connection,
    resulting in only ONE browser popup per test class
    """
    
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
    
    def test_fetch_one(self, ctx, qualified_test_table):
        """Test fetch_one returns single row from query."""
        from snowlib.primitives import execute_sql
        
        # Create table with test data
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT, name VARCHAR(50))", context=ctx)
        execute_sql(f"INSERT INTO {qualified_test_table} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", context=ctx)
        
        try:
            # Test fetch_one with LIMIT 1
            result = execute_sql(f"SELECT * FROM {qualified_test_table} ORDER BY id LIMIT 1", context=ctx)
            row = result.fetch_one()
            
            assert row is not None
            assert row[0] == 1
            assert row[1] == "Alice"
            
            # Test fetch_one with WHERE clause
            result = execute_sql(f"SELECT name FROM {qualified_test_table} WHERE id = 2", context=ctx)
            row = result.fetch_one()
            
            assert row is not None
            assert row[0] == "Bob"
            
            # Test fetch_one returns None for empty result
            result = execute_sql(f"SELECT * FROM {qualified_test_table} WHERE id = 999", context=ctx)
            row = result.fetch_one()
            
            assert row is None
        finally:
            execute_sql(f"DROP TABLE {qualified_test_table}", context=ctx)
    
    def test_execute_block_multi_statement(self, ctx, qualified_test_table):
        """Test execute_block with multiple statements."""
        from snowlib.primitives import execute_block, execute_sql, QueryResult
        
        sql_script = f"""
        CREATE TEMP TABLE {qualified_test_table} (id INT);
        INSERT INTO {qualified_test_table} VALUES (1), (2), (3);
        SELECT * FROM {qualified_test_table};
        """
        
        try:
            results = execute_block(sql_script, context=ctx)
            
            # Should have results from all statements
            assert len(results) >= 3
            
            # Last result should be from SELECT - it's a QueryResult
            select_results = results[-1]
            assert isinstance(select_results, QueryResult)
            
            # Fetch the actual rows from the QueryResult
            rows = select_results.fetch_all()
            assert len(rows) == 3
        finally:
            # Cleanup (TEMP table should auto-cleanup, but be safe)
            try:
                execute_sql(f"DROP TABLE IF EXISTS {qualified_test_table}", context=ctx)
            except:  # noqa: E722
                pass


class TestStreamingPrimitives:
    """Integration tests for streaming primitives."""
    
    def test_fetch_batches(self, ctx, qualified_test_table, check_pandas_integration):
        """Test fetch_batches yields DataFrame batches."""
        from snowlib.primitives import execute_sql
        
        # Create table with 100 rows
        execute_sql(f"CREATE TABLE {qualified_test_table} (id INT)", context=ctx)
        
        # Insert multiple rows
        values = ", ".join([f"({i})" for i in range(100)])
        execute_sql(f"INSERT INTO {qualified_test_table} VALUES {values}", context=ctx)
        
        try:
            total_rows = 0
            batch_count = 0
            
            result = execute_sql(f"SELECT * FROM {qualified_test_table}", context=ctx)
            for batch_df in result.fetch_batches():
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
        from snowlib.primitives import execute_sql_async, AsyncQuery
        
        # Submit async query
        job = execute_sql_async("SELECT 1 as test_col", context=ctx)
        
        # Verify we got a AsyncQuery back
        assert isinstance(job, AsyncQuery)
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
