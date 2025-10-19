"""Database object class"""

import warnings
from typing import TYPE_CHECKING, ClassVar

from snowlib.context import SnowflakeContext
from .base import Container, FQN

if TYPE_CHECKING:
    from .schema import Schema


class Database(Container):
    """Represents a Snowflake database"""
    
    SHOW_PLURAL: ClassVar[str] = "DATABASES"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    def __init__(self, name: str, context: SnowflakeContext):
        """Initialize database object"""
        super().__init__(context)
        self._fqn = FQN.from_parts(name)
    
    @property
    def schemas(self) -> list['Schema']:
        """Get all schemas in this database by querying Snowflake for current state"""
        from .schema import Schema
        from snowlib.utils.identifiers import is_valid_identifier
        
        results = self._show_children(Schema)
        name_col = Schema.SHOW_NAME_COLUMN
        
        schemas = []
        for row in results:
            schema_name = row[name_col]
            if is_valid_identifier(schema_name):
                schemas.append(Schema(self._database_name, schema_name, self._context))
            else:
                msg = (
                    f"Skipping schema with quoted identifier: {schema_name!r}. "
                    "Models layer only supports unquoted identifiers. "
                    "Use primitives layer for quoted identifiers."
                )
                warnings.warn(msg, UserWarning, stacklevel=2)
        
        return schemas
    
    def schema(self, name: str) -> 'Schema':
        """Get specific schema by name without validating existence"""
        from .schema import Schema
        return Schema(self._database_name, name, self._context)
    
    def has_schema(self, name: str) -> bool:
        """Check if schema exists in this database"""
        from .schema import Schema
        return self._show_instance.exists(Schema, name, container=self)