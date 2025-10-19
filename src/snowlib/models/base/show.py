"""Execute Snowflake SHOW commands with safe parameter binding."""

from typing import TYPE_CHECKING, Optional, Any

from snowlib.context import SnowflakeContext
from snowlib.primitives import Executor
from snowlib.utils.query import SafeQuery
from snowlib.utils.identifiers import is_valid_identifier


if TYPE_CHECKING:
    from .core import SnowflakeObject


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
        """Execute a SHOW command and return results as list of dicts."""

        query = SafeQuery(f"SHOW {object_class.SHOW_PLURAL}")

        query.when(like, "LIKE %s", like)

        if container:
            container_type = container.__class__.__name__.upper()
            query.when(container, f"IN {container_type} IDENTIFIER(%s)", container.fqn)

        if isinstance(starts_with, str):
            starts_with = starts_with.upper()
        query.when(starts_with, "STARTS WITH %s", starts_with)
        query.when(limit, "LIMIT %s", limit)

        sql, bindings = query.as_tuple()

        result = self.executor.run_with_result_scan(sql, bindings=bindings)
        df = result.to_df()
        if df.empty:
            return []

        # Type narrowing for mypy
        records: list[dict[str, Any]] = df.to_dict("records")  # type: ignore[assignment]
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
