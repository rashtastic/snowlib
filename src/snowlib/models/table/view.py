"""View class - read-only query-defined object"""

from typing import Optional, ClassVar

from snowlib.models.table.base import TableLike


class View(TableLike):
    """Represents a Snowflake view with read-only query-defined projections of data"""
    
    SHOW_PLURAL: ClassVar[str] = "VIEWS"
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    
    @property
    def definition(self) -> Optional[str]:
        """Get the view's SELECT definition from metadata"""
        metadata = self.metadata
        if metadata:
            return metadata.get('text')
        return None
