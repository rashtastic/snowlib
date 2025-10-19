"""DynamicTable class - automated query-based table"""

from typing import Optional, ClassVar

from snowlib.models.table.base import TableLike


class DynamicTable(TableLike):
    """Represents a Snowflake dynamic table that automatically refreshes based on source table changes"""
    
    SHOW_PLURAL: ClassVar[str] = "DYNAMIC TABLES"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    @property
    def definition(self) -> Optional[str]:
        """Get the dynamic table's query definition from metadata"""
        metadata = self.metadata
        if metadata:
            return metadata.get('text')
        return None
    
    def suspend(self) -> None:
        """Suspend automatic refresh of the dynamic table"""
        from snowlib.primitives import execute_sql
        execute_sql(
            f"ALTER DYNAMIC TABLE {self.fqn} SUSPEND",
            context=self._context
        )
    
    def resume(self) -> None:
        """Resume automatic refresh of the dynamic table based on target lag setting"""
        from snowlib.primitives import execute_sql
        execute_sql(
            f"ALTER DYNAMIC TABLE {self.fqn} RESUME",
            context=self._context
        )
    
    def refresh(self) -> None:
        """Manually refresh the dynamic table immediately regardless of target lag setting"""
        from snowlib.primitives import execute_sql
        execute_sql(
            f"ALTER DYNAMIC TABLE {self.fqn} REFRESH",
            context=self._context
        )
