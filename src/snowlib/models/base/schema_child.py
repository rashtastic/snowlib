"""Base class for objects that live inside a schema"""

from typing import TYPE_CHECKING, Any, Optional

from snowlib.context import SnowflakeContext
from .core import SnowflakeObject
from .fqn import FQN

if TYPE_CHECKING:
    from ..database import Database
    from ..schema import Schema


class SchemaChild(SnowflakeObject):
    """Base for objects that live inside a schema with shared name resolution logic"""
    
    def __init__(
        self,
        database: str,
        schema: str,
        name: str,
        context: SnowflakeContext
    ):
        """Initialize with database, schema, name, and context"""
        super().__init__(context)
        self._fqn = FQN.from_parts(database, schema, name)
    
    @property
    def database(self) -> 'Database':
        """Get parent database object (lazy construction)."""
        from ..database import Database
        return Database(self._database_name, self._context)
    
    @property
    def schema(self) -> 'Schema':
        """Get parent schema object (lazy construction)."""
        from ..schema import Schema
        return Schema(self._database_name, self._schema_name, self._context)
    
    @property
    def container(self) -> 'Schema':
        """Parent container is the schema."""
        return self.schema
    
    @classmethod
    def from_name(
        cls,
        name: str,
        context: SnowflakeContext,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        **kwargs: Any
    ) -> 'SchemaChild':
        """Parse qualified name and resolve missing parts from context or defaults."""
        parts = name.split(".")
        
        if len(parts) == 3:
            # Fully qualified: DB.SCHEMA.NAME
            return cls(parts[0], parts[1], parts[2], context)
        
        elif len(parts) == 2:
            # Need database: SCHEMA.NAME
            db = default_database or context.current_database
            if not db:
                msg = (
                    f"Cannot resolve '{name}': no database in context. "
                    "Provide default_database or USE DATABASE first."
                )
                raise ValueError(msg)
            return cls(db, parts[0], parts[1], context)
        
        elif len(parts) == 1:
            # Need both: NAME
            db = default_database or context.current_database
            schema = default_schema or context.current_schema
            if not db or not schema:
                msg = (
                    f"Cannot resolve '{name}': no database/schema in context. "
                    "Provide defaults or USE DATABASE/SCHEMA first."
                )
                raise ValueError(msg)
            return cls(db, schema, parts[0], context)
        
        else:
            raise ValueError(f"Invalid name: '{name}' (too many periods)")
