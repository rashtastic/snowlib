"""Execute Snowflake SHOW commands with safe parameter binding."""

import warnings
from typing import TYPE_CHECKING, Any, Optional

from snowlib.context import SnowflakeContext
from snowlib.primitives import Executor
from snowlib.utils.identifiers import is_valid_identifier
from snowlib.utils.query import SafeQuery

if TYPE_CHECKING:
    from .core import SnowflakeObject

# The most rows one SHOW returns. Kinds that take LIMIT ... FROM raise an error
# past it unless paged; the others silently drop every row past it.
SHOW_ROW_LIMIT = 10_000

# Kinds that take LIMIT <rows> FROM '<name>', mapped to the container type in
# which their rows are ordered by name alone. Paging is only correct there: a
# wider scope sorts by schema (or database) first, and FROM skips by name.
PAGED_KINDS: dict[str, Optional[str]] = {
    "DATABASES": None,
    "SCHEMAS": "DATABASE",
    "TABLES": "SCHEMA",
    "VIEWS": "SCHEMA",
    "DYNAMIC TABLES": "SCHEMA",
}


class Show:
    """Execute SHOW commands to query Snowflake object metadata."""

    def __init__(self, context: SnowflakeContext):
        """Initialize Show with a Snowflake context."""
        self._context = context

    @property
    def executor(self):
        """Get an Executor instance for running queries."""
        # Don't cache in case context changes
        return Executor(context=self._context)

    def execute(
        self,
        object_class: type["SnowflakeObject"],
        container: Optional["SnowflakeObject"] = None,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Execute a SHOW command and return results as list of dicts.

        Without a limit, kinds that support it are paged past SHOW's
        10,000-row maximum; kinds that do not warn when they reach it.
        """
        kind = object_class.SHOW_PLURAL
        # IN takes DATABASE, SCHEMA, TABLE or VIEW: a dynamic table's columns are
        # shown IN TABLE and a materialized view's IN VIEW.
        container_type = container._object_keyword().split()[-1] if container else None
        if isinstance(starts_with, str):
            starts_with = starts_with.upper()

        def page(rows: Optional[int], after: Optional[str]) -> list[dict[str, Any]]:
            query = SafeQuery(f"SHOW {kind}")
            query.when(like, "LIKE %s", like)
            if container:
                query.when(
                    container, f"IN {container_type} IDENTIFIER(%s)", container.fqn
                )
            query.when(starts_with, "STARTS WITH %s", starts_with)
            query.when(rows, "LIMIT %s", rows)
            query.when(after, "FROM %s", after)
            sql, bindings = query.as_tuple()
            df = self.executor.run_with_result_scan(sql, bindings=bindings).to_df()
            if df.empty:
                return []
            records: list[dict[str, Any]] = df.to_dict("records")  # type: ignore[assignment]
            return records

        if limit:
            return page(limit, None)

        if kind in PAGED_KINDS and PAGED_KINDS[kind] == container_type:
            name_col = object_class.SHOW_NAME_COLUMN
            records = page(SHOW_ROW_LIMIT, None)
            last_page = records
            while len(last_page) == SHOW_ROW_LIMIT:
                after = last_page[-1][name_col]
                last_page = [
                    r for r in page(SHOW_ROW_LIMIT, after) if r[name_col] != after
                ]
                records.extend(last_page)
            return records

        records = page(None, None)
        if len(records) >= SHOW_ROW_LIMIT:
            scope = f" IN {container_type} {container.fqn}" if container else ""
            warnings.warn(
                f"SHOW {kind}{scope} returned {len(records):,} rows, the most SHOW "
                + "returns, so the list is likely incomplete; the Information "
                + "Schema has no such limit.",
                UserWarning,
                stacklevel=3,
            )
        return records

    def get_metadata(
        self,
        object_class: type["SnowflakeObject"],
        name: str,
        container: Optional["SnowflakeObject"] = None,
    ) -> Optional[dict[str, Any]]:
        """Get metadata dict for an object with exact name match, or None if not found."""

        if not is_valid_identifier(name):
            msg = (
                f"Invalid identifier name: {name!r}. The name must be a valid Snowflake identifier "
                f"(letters, digits, underscores, starting with letter or underscore). "
                f"Wildcards (%, _) are not allowed for exact matching."
            )
            raise ValueError(msg)

        results = self.execute(object_class, container=container, like=name)
        if not results:
            return None

        # Find exact match (case-insensitive)
        name_col = object_class.SHOW_NAME_COLUMN
        name_upper = name.upper()
        for row in results:
            if row[name_col].upper() == name_upper:
                return row

        return None

    def exists(
        self,
        object_class: type["SnowflakeObject"],
        name: str,
        container: Optional["SnowflakeObject"] = None,
    ) -> bool:
        """Check if an object with the exact name exists."""
        return self.get_metadata(object_class, name, container) is not None
