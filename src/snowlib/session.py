"""Context-bound session for snowlib operations"""

from collections.abc import Sequence
from typing import Any, Generic, Optional, Protocol, TypeVar

import pandas as pd

from snowlib.context import SnowflakeContext
from snowlib.models import (
    Database,
    DynamicTable,
    MaterializedView,
    Schema,
    Show,
    Stage,
    Table,
    View,
)
from snowlib.models.base import SnowflakeObject
from snowlib.primitives import AsyncQuery, Executor, QueryResult


class HasFromName(Protocol):
    """Protocol for model classes that have from_name classmethod"""

    @classmethod
    def from_name(
        cls,
        name: str,
        context: SnowflakeContext,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        **kwargs: Any,
    ) -> Any: ...


T = TypeVar("T", bound=HasFromName)


class BoundModel(Generic[T]):
    """A model class bound to a context for convenient instantiation"""

    def __init__(self, model_class: type[T], context: SnowflakeContext):
        self._model_class = model_class
        self._context = context

    def __call__(self, *args: Any) -> Any:
        """Instantiate the model with bound context"""
        return self._model_class(*args, self._context)  # type: ignore[call-arg]

    def from_name(
        self,
        name: str,
        default_database: Optional[str] = None,
        default_schema: Optional[str] = None,
        **kwargs: Any,
    ) -> T:
        """Call the model's from_name classmethod with bound context"""
        return self._model_class.from_name(
            name,
            self._context,
            default_database=default_database,
            default_schema=default_schema,
            **kwargs,
        )

    def __repr__(self) -> str:
        return f"BoundModel({self._model_class.__name__})"


class Session:
    """Context-bound wrapper providing convenient access to snowlib operations"""

    def __init__(
        self,
        profile: Optional[str] = None,
        context: Optional[SnowflakeContext] = None,
        config_path: Optional[str] = None,
        **overrides: Any,
    ):
        """Initialize session with a profile name or existing context"""
        if profile is None and context is None:
            raise ValueError("Session requires either 'profile' or 'context'")
        if profile is not None and context is not None:
            raise ValueError("Provide either 'profile' or 'context', not both")

        if context is not None:
            self._context = context
            self._owns_context = False
        else:
            self._context = SnowflakeContext(
                profile=profile, config_path=config_path, **overrides
            )
            self._owns_context = True

    @property
    def context(self) -> SnowflakeContext:
        """Access the underlying SnowflakeContext"""
        return self._context

    # Primitives

    def execute_sql(
        self, sql: str, bindings: Optional[Sequence[Any]] = None, arrow: bool = True
    ) -> QueryResult:
        """Execute SQL and return a QueryResult"""
        return Executor(self._context).run(sql, bindings=bindings, arrow=arrow)

    def query(
        self, sql: str, bindings: Optional[Sequence[Any]] = None, arrow: bool = True
    ) -> pd.DataFrame:
        """Execute SQL and return results as a DataFrame"""
        return Executor(self._context).run(sql, bindings=bindings, arrow=arrow).to_df()

    def execute_sql_async(
        self, sql: str, bindings: Optional[Sequence[Any]] = None
    ) -> AsyncQuery:
        """Execute SQL asynchronously and return an AsyncQuery"""
        return Executor(self._context).run_async(sql, bindings=bindings)

    def execute_block(self, sql: str) -> list[QueryResult]:
        """Execute a block of SQL statements and return a list of QueryResults"""
        return Executor(self._context).run_block(sql)

    def show(
        self,
        kind: str,
        in_: Optional[SnowflakeObject] = None,
        like: Optional[str] = None,
    ) -> pd.DataFrame:
        """SHOW <kind> [IN <object>] as a DataFrame: session.show("tasks", in_=schema)"""
        return Show(self._context).rows(kind, container=in_, like=like)

    # Model factories (bound models with from_name support)

    @property
    def database(self) -> BoundModel[Database]:
        """Bound Database model: session.database("NAME") or session.database.from_name("NAME")"""
        return BoundModel(Database, self._context)

    @property
    def schema(self) -> BoundModel[Schema]:
        """Bound Schema model: session.schema("DB", "NAME") or session.schema.from_name("DB.NAME")"""
        return BoundModel(Schema, self._context)

    @property
    def table(self) -> BoundModel[Table]:
        """Bound Table model: session.table("DB", "SCHEMA", "NAME") or session.table.from_name("DB.SCHEMA.NAME")"""
        return BoundModel(Table, self._context)

    @property
    def view(self) -> BoundModel[View]:
        """Bound View model: session.view("DB", "SCHEMA", "NAME") or session.view.from_name("DB.SCHEMA.NAME")"""
        return BoundModel(View, self._context)

    @property
    def materialized_view(self) -> BoundModel[MaterializedView]:
        """Bound MaterializedView model"""
        return BoundModel(MaterializedView, self._context)

    @property
    def dynamic_table(self) -> BoundModel[DynamicTable]:
        """Bound DynamicTable model"""
        return BoundModel(DynamicTable, self._context)

    @property
    def stage(self) -> BoundModel[Stage]:
        """Bound Stage model: session.stage("DB", "SCHEMA", "NAME") or session.stage.from_name("DB.SCHEMA.NAME")"""
        return BoundModel(Stage, self._context)

    # Lifecycle

    def close(self) -> None:
        """Close the session and underlying context if owned"""
        if self._owns_context:
            self._context.close()

    def __enter__(self) -> "Session":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit"""
        self.close()


def create_session(
    profile: Optional[str] = None,
    context: Optional[SnowflakeContext] = None,
    config_path: Optional[str] = None,
    **overrides: Any,
) -> Session:
    """Create a context-bound session for snowlib operations"""
    return Session(
        profile=profile, context=context, config_path=config_path, **overrides
    )
