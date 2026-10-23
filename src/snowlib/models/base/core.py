"""Core base classes for Snowflake objects"""

from abc import ABC
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from snowlib.context import SnowflakeContext

if TYPE_CHECKING:
    from .fqn import FQN


class SnowflakeObject(ABC):
    """Base class for all Snowflake objects with immutable identity and dynamic state"""

    SHOW_PLURAL: ClassVar[str]
    SHOW_NAME_COLUMN: ClassVar[str] = "name"
    # GET_DDL's name for this type, or None where GET_DDL has none (stages)
    GET_DDL_TYPE: ClassVar[Optional[str]] = None

    def __init__(self, context: SnowflakeContext):
        """Initialize with frozen connection context"""
        self._context = context
        self._fqn: FQN

    @property
    def context(self) -> SnowflakeContext:
        """The frozen context this object was created with"""
        return self._context

    @property
    def name(self) -> str:
        """Object name (unqualified)"""
        return self._fqn.name

    @property
    def fqn(self) -> str:
        """Fully qualified name for use in SQL"""
        return str(self._fqn)

    @property
    def _database_name(self) -> str:
        """Internal helper for database name (first part of FQN)"""
        return self._fqn.database or ""

    @property
    def _schema_name(self) -> str:
        """Internal helper for schema name (second part of FQN)"""
        return self._fqn.schema or ""

    @property
    def _table_name(self) -> str:
        """Internal helper for table name (third part of FQN)"""
        return self._fqn.table or ""

    @property
    def _name(self) -> str:
        """Internal helper for object name (last part of FQN)"""
        return self._fqn.name

    @property
    def container(self) -> Optional["SnowflakeObject"]:
        """Parent container object (Schema, Database, etc.) or None for top-level objects."""
        return None

    def exists(self) -> bool:
        """Check if object currently exists in Snowflake via SHOW command."""
        from .show import Show

        show = Show(self._context)
        return show.exists(self.__class__, self.name, container=self.container)

    @property
    def metadata(self) -> Optional[dict[str, Any]]:
        """Get object metadata from SHOW command or None if not found."""
        from .show import Show

        show = Show(self._context)
        return show.get_metadata(self.__class__, self.name, container=self.container)

    @classmethod
    def _object_keyword(cls) -> str:
        """Singular object keyword for DESCRIBE and DROP: DYNAMIC TABLES -> DYNAMIC TABLE."""
        plural = cls.SHOW_PLURAL
        return plural[:-1] if plural.endswith("S") else plural

    def ddl(self) -> str:
        """Get this object's DDL from GET_DDL, with fully qualified names."""
        from snowlib.primitives import Executor
        from snowlib.utils.ddl import quote_ident

        if self.GET_DDL_TYPE is None:
            raise NotImplementedError(
                f"GET_DDL does not support {self.__class__.__name__}"
            )
        name = ".".join(quote_ident(p) for p in self.fqn.split("."))
        df = (
            Executor(context=self._context)
            .run(
                "SELECT GET_DDL(%s, %s, TRUE) AS ddl",
                bindings=[self.GET_DDL_TYPE, name],
            )
            .to_df()
        )
        return str(df["ddl"].iloc[0])

    def describe(self) -> "Any":  # Returns pd.DataFrame
        """Get object metadata via DESCRIBE command as DataFrame."""
        sql = f"DESCRIBE {self._object_keyword()} {self.fqn}"

        from snowlib.primitives import Executor

        executor = Executor(context=self._context)
        result = executor.run_with_result_scan(sql)

        return result.to_df()

    def __repr__(self) -> str:
        """String representation showing type and fully qualified name."""
        return f"{self.__class__.__name__}({self.fqn!r})"

    def __str__(self) -> str:
        """String representation is the fully qualified name."""
        return self.fqn

    def __eq__(self, other: object) -> bool:
        """Objects are equal if they have same type and FQN (case-insensitive)."""
        if not isinstance(other, SnowflakeObject):
            return False
        return type(self) is type(other) and self.fqn.upper() == other.fqn.upper()

    def __hash__(self) -> int:
        """Hash based on class type and FQN for use in sets/dicts."""
        return hash((type(self).__name__, self.fqn.upper()))

    @classmethod
    def from_name(
        cls,
        name: str,
        context: SnowflakeContext,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        **kwargs: Any,
    ) -> "SnowflakeObject":
        """Parse qualified name and resolve missing parts from context."""
        # Get object type from subclass's _type (will be set during __init__)
        # For this class method, we need to instantiate to know the type
        # This is a base implementation - subclasses should override
        msg = (
            f"{cls.__name__}.from_name() not implemented. "
            "Subclasses should provide their own implementation."
        )
        raise NotImplementedError(msg)


class Container(SnowflakeObject):
    """Base class for objects that can contain other objects (Database, Schema)."""

    def __init__(self, context: SnowflakeContext):
        """Initialize container with context."""
        super().__init__(context)
        # Lazy-initialize Show when first needed
        self._show: Any | None = None  # Any to avoid circular import

    @property
    def _show_instance(self) -> Any:  # Returns Show, but can't import it here
        """Lazy-initialized Show instance."""
        if self._show is None:
            from .show import Show

            self._show = Show(self._context)
        return self._show

    def _show_children(
        self, child_class: type["SnowflakeObject"], **filters: Any
    ) -> list[dict[str, Any]]:
        """Show child objects of a specific type via SHOW command."""
        return self._show_instance.execute(child_class, container=self, **filters)
