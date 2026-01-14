"""Table-like Snowflake objects (tables, views, materialized views, dynamic tables)."""

from snowlib.models.table.base import TableLike
from snowlib.models.table.table import Table, WriteMethod
from snowlib.models.table.view import View
from snowlib.models.table.materialized_view import MaterializedView
from snowlib.models.table.dynamic_table import DynamicTable

__all__ = [
    "TableLike",
    "Table",
    "WriteMethod",
    "View",
    "MaterializedView",
    "DynamicTable",
]
