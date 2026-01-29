"""Schema object class"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, ClassVar

from snowlib.context import SnowflakeContext
from .base import Container, FQN

if TYPE_CHECKING:
    from .database import Database
    from .table import Table, View
    from .stage import Stage


class Schema(Container):
    """Represents a Snowflake schema"""
    
    SHOW_PLURAL: ClassVar[str] = "SCHEMAS"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    def __init__(self, database: str, name: str, context: SnowflakeContext):
        """Initialize schema object"""
        super().__init__(context)
        self._fqn = FQN.from_parts(database, name)
    
    @classmethod
    def from_name(
        cls,
        name: str,
        context: SnowflakeContext,
        default_database: str | None = None,
        default_schema: str | None = None,
        **kwargs: object
    ) -> 'Schema':
        """Parse qualified name and resolve missing parts from context.
        
        Args:
            name: Schema name in format 'DATABASE.SCHEMA' or 'SCHEMA'
            context: SnowflakeContext to use for resolution
            default_database: Ignored (Schema doesn't use default_database)
            default_schema: Ignored (Schema doesn't use default_schema)
            **kwargs: Additional arguments (ignored)
        
        Returns:
            Schema instance
        
        Raises:
            ValueError: If name cannot be resolved
        
        Examples:
            >>> Schema.from_name("MY_DB.MY_SCHEMA", ctx)
            >>> Schema.from_name("MY_SCHEMA", ctx)  # Uses current database
        """
        parts = name.split(".")
        
        if len(parts) == 2:
            # Fully qualified: DATABASE.SCHEMA
            return cls(parts[0], parts[1], context)
        
        elif len(parts) == 1:
            # Need database from context: SCHEMA
            db = context.current_database
            if not db:
                msg = (
                    f"Cannot resolve '{name}': no database in context. "
                    "Use DATABASE.SCHEMA format or USE DATABASE first."
                )
                raise ValueError(msg)
            return cls(db, parts[0], context)
        
        else:
            msg = (
                f"Invalid schema name '{name}'. "
                "Expected format: 'DATABASE.SCHEMA' or 'SCHEMA'."
            )
            raise ValueError(msg)
    
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
    
    def stage(self, name: str) -> 'Stage':
        """Get specific stage by name"""
        from .stage import Stage
        return Stage(self._database_name, self._schema_name, name, self._context)
    
    def has_table(self, name: str) -> bool:
        """Check if table exists in schema"""
        from .table import Table
        return self._show_instance.exists(Table, name, container=self)
    
    def has_view(self, name: str) -> bool:
        """Check if view exists in schema"""
        from .table import View
        return self._show_instance.exists(View, name, container=self)
    
    def has_stage(self, name: str) -> bool:
        """Check if stage exists in schema"""
        from .stage import Stage
        return self._show_instance.exists(Stage, name, container=self)