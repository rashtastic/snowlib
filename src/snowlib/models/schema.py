"""Schema object class"""

import warnings
from typing import TYPE_CHECKING, ClassVar

from snowlib.context import SnowflakeContext
from .base import Container, FQN

if TYPE_CHECKING:
    from .database import Database
    from .table import Table, View


class Schema(Container):
    """Represents a Snowflake schema"""
    
    SHOW_PLURAL: ClassVar[str] = "SCHEMAS"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    def __init__(self, database: str, name: str, context: SnowflakeContext):
        """Initialize schema object"""
        super().__init__(context)
        self._fqn = FQN.from_parts(database, name)
    
    @property
    def database(self) -> 'Database':
        """Get parent database object"""
        from .database import Database
        return Database(self._database_name, self._context)
    
    @property
    def container(self) -> 'Database':
        """Parent container is the database"""
        return self.database
    
    @property
    def tables(self) -> list['Table']:
        """Get all tables in schema by querying Snowflake for current state"""
        from .table import Table
        from snowlib.utils.identifiers import is_valid_identifier
        
        results = self._show_children(Table)
        
        tables = []
        for row in results:
            table_name = row['name']
            if is_valid_identifier(table_name):
                tables.append(Table(self._database_name, self._schema_name, table_name, self._context))
            else:
                msg = (
                    f"Skipping table with quoted identifier: {table_name!r}. "
                    "Models layer only supports unquoted identifiers. "
                    "Use primitives layer for quoted identifiers."
                )
                warnings.warn(msg, UserWarning, stacklevel=2)
        
        return tables
    
    @property
    def views(self) -> list['View']:
        """Get all views in schema"""
        from .table import View
        from snowlib.utils.identifiers import is_valid_identifier
        
        results = self._show_children(View)
        
        views = []
        for row in results:
            view_name = row['name']
            if is_valid_identifier(view_name):
                views.append(View(self._database_name, self._schema_name, view_name, self._context))
            else:
                msg = (
                    f"Skipping view with quoted identifier: {view_name!r}. "
                    "Models layer only supports unquoted identifiers. "
                    "Use primitives layer for quoted identifiers."
                )
                warnings.warn(msg, UserWarning, stacklevel=2)
        
        return views
    
    def table(self, name: str) -> 'Table':
        """Get specific table by name without validating existence"""
        from .table import Table
        return Table(self._database_name, self._schema_name, name, self._context)
    
    def view(self, name: str) -> 'View':
        """Get specific view by name"""
        from .table import View
        return View(self._database_name, self._schema_name, name, self._context)
    
    def has_table(self, name: str) -> bool:
        """Check if table exists in schema"""
        from .table import Table
        return self._show_instance.exists(Table, name, container=self)
    
    def has_view(self, name: str) -> bool:
        """Check if view exists in schema"""
        from .table import View
        return self._show_instance.exists(View, name, container=self)