"""Base class for table-like Snowflake objects"""

import warnings
from typing import Optional, TYPE_CHECKING, ClassVar
import pandas as pd

from snowlib.models.base import SchemaChild, Container
from snowlib.utils.query import SafeQuery

if TYPE_CHECKING:
    from ..column import Column


class TableLike(SchemaChild, Container):
    """Base class for all queryable table-like objects (tables, views, materialized views, dynamic tables)"""
    
    # Subclasses that can be checked for type mismatches
    _TABLE_LIKE_TYPES: ClassVar[list[type["TableLike"]]] = []
    
    def __init_subclass__(cls, **kwargs: object) -> None:
        """Register subclasses for type mismatch detection"""
        super().__init_subclass__(**kwargs)
        # Only register concrete classes with SHOW_PLURAL defined
        if hasattr(cls, 'SHOW_PLURAL') and cls.SHOW_PLURAL:
            TableLike._TABLE_LIKE_TYPES.append(cls)
    
    def exists(self) -> bool:
        """Check if object exists, warning if it exists as a different table-like type"""
        from snowlib.models.base.show import Show
        show = Show(self._context)
        
        if show.exists(self.__class__, self.name, container=self.container):
            return True
        
        # Check if it exists as a different table-like type
        for alt_class in TableLike._TABLE_LIKE_TYPES:
            if alt_class is self.__class__:
                continue
            if show.exists(alt_class, self.name, container=self.container):
                warnings.warn(
                    f"'{self.fqn}' does not exist as a {self.__class__.__name__}, but exists as a {alt_class.__name__}. Consider using session.{alt_class.__name__.lower()}.from_name() instead.",
                    UserWarning,
                    stacklevel=2,
                )
                break
        
        return False
    
    def read(
        self,
        columns: Optional[list[str]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Read data from table-like object into DataFrame"""
        from snowlib.primitives import Executor
        
        query = SafeQuery("SELECT")
        
        if columns:
            col_identifiers = ", ".join([f"IDENTIFIER(%s)" for _ in columns])
            query._parts.append(col_identifiers)
            query._bindings.extend(columns)
        else:
            query._parts.append("*")
        
        query._parts.append(f"FROM {self.fqn}")
        
        query.when(limit, "LIMIT %s", limit)
        
        sql, bindings = query.as_tuple()
        
        executor = Executor(self._context)
        return executor.run(sql, bindings=bindings).to_df()
    
    def drop(self, if_exists: bool = False) -> None:
        """Drop this object from Snowflake"""
        from snowlib.primitives import execute_sql
        
        object_type = self.SHOW_PLURAL.rstrip('S')
        
        if if_exists:
            sql = f"DROP {object_type} IF EXISTS {self.fqn}"
        else:
            sql = f"DROP {object_type} {self.fqn}"
        
        execute_sql(sql, context=self._context)
    
    @property
    def columns(self) -> list['Column']:
        """Get current columns by querying Snowflake via SHOW COLUMNS"""
        from ..column import Column
        from snowlib.utils.identifiers import is_valid_identifier
        
        results = self._show_children(Column)
        name_col = Column.SHOW_NAME_COLUMN
        
        columns = []
        for row in results:
            column_name = row[name_col]
            if is_valid_identifier(column_name):
                columns.append(
                    Column(
                        self._database_name,
                        self._schema_name,
                        self._name,
                        column_name,
                        self._context
                    )
                )
            else:
                msg = (
                    f"Skipping column with quoted identifier: {column_name!r}. "
                    "Models layer only supports unquoted identifiers. "
                    "Use primitives layer for quoted identifiers."
                )
                warnings.warn(msg, UserWarning, stacklevel=2)
        
        return columns
    
    def column(self, name: str) -> 'Column':
        """Get a column by name without checking existence"""
        from ..column import Column
        return Column(
            self._database_name,
            self._schema_name,
            self._name,
            name,
            self._context
        )
    
    def has_column(self, name: str) -> bool:
        """Check if column exists in this object"""
        from ..column import Column
        from ..base.show import Show
        
        show = Show(self._context)
        return show.exists(Column, name, container=self)