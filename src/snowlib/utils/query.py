"""Utilities for building SQL queries with safe parameter binding"""

from typing import Any


class SafeQuery:
    """Build SQL queries with safe parameter binding"""
    
    def __init__(self, base: str):
        """Initialize with a base SQL statement"""
        self._parts: list[str] = [base]
        self._bindings: list[Any] = []
    
    def when(self, condition: Any, template: str, *values: Any) -> 'SafeQuery':
        """Add a clause and maybe bind values when condition is truthy"""
        if condition:
            self._parts.append(template)
            if values:
                self._bindings.extend(values)
        return self
    
    def sql(self) -> str:
        """Get the SQL string with placeholders"""
        return " ".join(self._parts)
    
    def bindings(self) -> tuple[Any, ...]:
        """Get the bindings tuple"""
        return tuple(self._bindings)
    
    def as_tuple(self) -> tuple[str, tuple[Any, ...]]:
        """Get both SQL and bindings as a tuple"""
        return self.sql(), self.bindings()
