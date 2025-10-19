"""Execute SQL queries with safe parameter binding"""

from io import StringIO
from typing import Any, Optional, Union, Sequence

import pandas as pd

from snowlib.context import SnowflakeContext

from .async_query import AsyncQuery
from .result import QueryResult


class Executor:
    """Execute SQL queries with various strategies"""
    
    def __init__(self, context: Union[str, SnowflakeContext], **overrides: Any):
        """Initialize with a context profile name or SnowflakeContext instance"""
        if isinstance(context, str):
            self.context = SnowflakeContext(profile=context, **overrides)
        else:
            self.context = context

    def run(
        self, 
        sql: str, 
        bindings: Optional[Sequence[Any]] = None
    ) -> QueryResult:
        """Execute SQL and return a QueryResult"""
        if bindings is None:
            cursor = self.context.cursor.execute(sql)
        else:
            cursor = self.context.cursor.execute(sql, bindings)
        return QueryResult(_cursor=cursor)

    def run_async(
        self, 
        sql: str, 
        bindings: Optional[Sequence[Any]] = None
    ) -> AsyncQuery:
        """Execute SQL asynchronously and return an AsyncQuery"""
        cursor = self.context.cursor
        if bindings is None:
            response_data = cursor.execute_async(sql)
        else:
            response_data = cursor.execute_async(sql, bindings)
        query_id = response_data.get("queryId")
        if not query_id:
            raise RuntimeError(
                f"Failed to get queryId from async execution response. Response: {response_data}"
            )
        return AsyncQuery(query_id=query_id, sql=sql, _conn=self.context.connection)

    def run_block(self, sql: str) -> list[QueryResult]:
        """Execute a block of SQL statements and return a list of QueryResults"""
        results: list[QueryResult] = []
        for cursor_result in self.context.connection.execute_stream(StringIO(sql)):
            results.append(QueryResult(_cursor=cursor_result))
        return results
    
    def run_with_result_scan(
        self,
        sql: str,
        bindings: Optional[Sequence[Any]] = None
    ) -> QueryResult:
        """Execute SQL and fetch results via RESULT_SCAN"""
        result = self.run(sql, bindings=bindings)
        sfqid = result.query_id
        
        return self.run("SELECT * FROM TABLE(RESULT_SCAN(%s))", bindings=[sfqid])


def execute_sql(
    sql: str, context: Union[str, SnowflakeContext], **overrides: Any
) -> QueryResult:
    """Execute SQL and return a QueryResult"""
    return Executor(context, **overrides).run(sql)


def execute_sql_async(
    sql: str, context: Union[str, SnowflakeContext], **overrides: Any
) -> AsyncQuery:
    """Execute SQL asynchronously and return an AsyncQuery"""
    return Executor(context, **overrides).run_async(sql)


def execute_block(
    sql: str, context: Union[str, SnowflakeContext], **overrides: Any
) -> list[QueryResult]:
    """Execute a block of SQL statements and return a list of QueryResults"""
    return Executor(context, **overrides).run_block(sql)


def query(
    sql: str, context: Union[str, SnowflakeContext], **overrides: Any
) -> pd.DataFrame:
    """Execute SQL and return results as a DataFrame"""
    return Executor(context, **overrides).run(sql).to_df()
