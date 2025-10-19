"""MaterializedView class - cached view with refresh capability"""

from typing import ClassVar

from snowlib.models.table.view import View


class MaterializedView(View):
    """Represents a Snowflake materialized view with cached query results"""
    
    SHOW_PLURAL: ClassVar[str] = "MATERIALIZED VIEWS"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    def refresh(self) -> None:
        """Refresh the materialized view's cached results by re-executing the view's query"""
        from snowlib.primitives import execute_sql
        execute_sql(
            f"ALTER MATERIALIZED VIEW {self.fqn} REFRESH",
            context=self._context
        )
