"""Column object class"""

from typing import TYPE_CHECKING, ClassVar, Any

from snowlib.context import SnowflakeContext
from .base import SnowflakeObject, FQN

if TYPE_CHECKING:
    from .table import Table
    from .schema import Schema
    from .database import Database


class Column(SnowflakeObject):
    """Represents a table column"""
    
    SHOW_PLURAL: ClassVar[str] = "COLUMNS"
    SHOW_NAME_COLUMN: ClassVar[str] = "column_name"
    
    def __init__(
        self,
        database: str,
        schema: str,
        table: str,
        name: str,
        context: SnowflakeContext
    ):
        """Initialize column object"""
        super().__init__(context)
        self._fqn = FQN.from_parts(database, schema, table, name)
    
    @property
    def table(self) -> 'Table':
        """Get parent table object"""
        from .table import Table
        return Table(
            self._database_name,
            self._schema_name,
            self._table_name,
            self._context
        )
    
    @property
    def schema(self) -> 'Schema':
        """Get parent schema object"""
        from .schema import Schema
        return Schema(self._database_name, self._schema_name, self._context)
    
    @property
    def database(self) -> 'Database':
        """Get parent database object"""
        from .database import Database
        return Database(self._database_name, self._context)
    
    @property
    def container(self) -> 'Table':
        """Parent container is the table"""
        return self.table
    
    def describe(self) -> 'Any':
        """Get column description by describing the parent table and filtering for this column"""
        table_desc_df = self.table.describe()
        
        col_df = table_desc_df[
            table_desc_df['name'].str.upper() == self._name
        ]
        
        return col_df

