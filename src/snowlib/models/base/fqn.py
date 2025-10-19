"""Fully Qualified Name class for Snowflake objects"""

from dataclasses import dataclass
from typing import Optional

from snowlib.utils.identifiers import is_valid_identifier


@dataclass(frozen=True)
class FQN:
    """Fully Qualified Name for Snowflake objects with validated unquoted identifiers
    
    All parts are validated to ensure they are valid Snowflake unquoted identifiers
    and automatically uppercased to match Snowflake's default behavior
    """
    
    parts: tuple[str, ...]
    
    def __post_init__(self):
        """Validate and uppercase all parts"""
        if not self.parts:
            raise ValueError("FQN must have at least one part")
        
        validated_parts = []
        for i, part in enumerate(self.parts):
            if not is_valid_identifier(part):
                msg = (
                    f"Invalid identifier at position {i}: {part!r}. "
                    "Only unquoted Snowflake identifiers are supported "
                    "(letters, digits, underscores; must start with letter or underscore). "
                    "For quoted identifiers, use the primitives layer."
                )
                raise ValueError(msg)
            
            validated_parts.append(part.upper())
        
        object.__setattr__(self, 'parts', tuple(validated_parts))
    
    @property
    def database(self) -> Optional[str]:
        """Database name (first part) if available"""
        return self.parts[0] if len(self.parts) >= 1 else None
    
    @property
    def schema(self) -> Optional[str]:
        """Schema name (second part) if available"""
        return self.parts[1] if len(self.parts) >= 2 else None
    
    @property
    def table(self) -> Optional[str]:
        """Table name (third part) if available"""
        return self.parts[2] if len(self.parts) >= 3 else None
    
    @property
    def column(self) -> Optional[str]:
        """Column name (fourth part) if available"""
        return self.parts[3] if len(self.parts) >= 4 else None
    
    @property
    def name(self) -> str:
        """Object name (last part)"""
        return self.parts[-1]
    
    def __str__(self) -> str:
        """String representation for use in SQL (dot-separated parts)"""
        return ".".join(self.parts)
    
    def __len__(self) -> int:
        """Number of parts in the FQN"""
        return len(self.parts)
    
    @classmethod
    def from_parts(cls, *parts: str) -> 'FQN':
        """Create FQN from individual parts
        
        Example:
            >>> fqn = FQN.from_parts("MY_DB", "PUBLIC", "SALES")
            >>> str(fqn)
            'MY_DB.PUBLIC.SALES'
        """
        return cls(parts=parts)
    
    @classmethod
    def parse(cls, qualified_name: str) -> 'FQN':
        """Parse a dot-separated string into FQN
        
        Example:
            >>> fqn = FQN.parse("MY_DB.PUBLIC.SALES")
            >>> fqn.database
            'MY_DB'
            >>> fqn.schema
            'PUBLIC'
            >>> fqn.name
            'SALES'
        """
        if not qualified_name:
            raise ValueError("Cannot parse empty string")
        return cls(parts=tuple(qualified_name.split(".")))
