"""Unit tests for Show paging past SHOW's 10,000-row maximum."""

import warnings
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snowlib.context import SnowflakeContext
from snowlib.models import Column, Database, Schema, Stage
from snowlib.models.base.show import SHOW_ROW_LIMIT, Show
from snowlib.models.table import DynamicTable, MaterializedView, Table, View


@pytest.fixture
def mock_ctx() -> SnowflakeContext:
    return MagicMock(spec=SnowflakeContext)


def fake_snowflake(names: list[str]):
    """An executor answering SHOW as Snowflake does: LIMIT n, FROM after a name."""
    calls: list[tuple[str, tuple]] = []

    def run_with_result_scan(sql, bindings=()):
        calls.append((sql, tuple(bindings)))
        rows = sorted(names)
        values = list(bindings)
        if "FROM %s" in sql:
            after = values[-1]
            rows = [n for n in rows if n > after]
        if "LIMIT %s" in sql:
            limit_index = -2 if "FROM %s" in sql else -1
            rows = rows[: values[limit_index]]
        result = MagicMock()
        result.to_df.return_value = pd.DataFrame({"name": rows})
        return result

    executor = MagicMock()
    executor.run_with_result_scan.side_effect = run_with_result_scan
    return executor, calls


def show_with(names, mock_ctx):
    executor, calls = fake_snowflake(names)
    patcher = patch("snowlib.models.base.show.Executor", return_value=executor)
    patcher.start()
    return Show(mock_ctx), calls, patcher


@pytest.mark.parametrize("count", [0, 3, SHOW_ROW_LIMIT - 1])
def test_single_page(count, mock_ctx):
    names = [f"T{i:05d}" for i in range(count)]
    show, calls, patcher = show_with(names, mock_ctx)
    try:
        rows = show.execute(Table, container=Schema("DB", "SCH", context=mock_ctx))
    finally:
        patcher.stop()
    assert [r["name"] for r in rows] == names
    assert len(calls) == 1
    assert "LIMIT %s" in calls[0][0]


@pytest.mark.parametrize(
    ("count", "queries"),
    [(SHOW_ROW_LIMIT, 2), (SHOW_ROW_LIMIT + 1, 2), (2 * SHOW_ROW_LIMIT + 5, 3)],
)
def test_pages_past_the_limit(count, queries, mock_ctx):
    names = [f"T{i:05d}" for i in range(count)]
    show, calls, patcher = show_with(names, mock_ctx)
    try:
        rows = show.execute(Table, container=Schema("DB", "SCH", context=mock_ctx))
    finally:
        patcher.stop()
    assert [r["name"] for r in rows] == names
    assert len(calls) == queries
    sql, bindings = calls[1]
    assert sql.endswith("LIMIT %s FROM %s")
    assert bindings[-2:] == (SHOW_ROW_LIMIT, names[SHOW_ROW_LIMIT - 1])


def test_schemas_page_within_a_database(mock_ctx):
    names = [f"S{i:05d}" for i in range(SHOW_ROW_LIMIT + 2)]
    show, calls, patcher = show_with(names, mock_ctx)
    try:
        rows = show.execute(Schema, container=Database("DB", context=mock_ctx))
    finally:
        patcher.stop()
    assert len(rows) == len(names)
    assert len(calls) == 2


def test_caller_limit_is_one_query(mock_ctx):
    names = [f"T{i:05d}" for i in range(SHOW_ROW_LIMIT + 5)]
    show, calls, patcher = show_with(names, mock_ctx)
    try:
        rows = show.execute(
            Table, container=Schema("DB", "SCH", context=mock_ctx), limit=50
        )
    finally:
        patcher.stop()
    assert len(rows) == 50
    assert len(calls) == 1
    assert "FROM %s" not in calls[0][0]


def test_tables_in_a_database_are_not_paged(mock_ctx):
    """Database-wide output sorts by schema first, so FROM by name would skip rows."""
    show, calls, patcher = show_with(["A", "B"], mock_ctx)
    try:
        show.execute(View, container=Database("DB", context=mock_ctx))
    finally:
        patcher.stop()
    assert len(calls) == 1
    assert "LIMIT" not in calls[0][0]


@pytest.mark.parametrize(
    ("object_class", "make_container"),
    [
        (Stage, lambda ctx: Schema("DB", "SCH", context=ctx)),
        (Column, lambda ctx: Table("DB", "SCH", "T", context=ctx)),
    ],
)
def test_unpaged_kind_warns_at_the_limit(object_class, make_container, mock_ctx):
    names = [f"N{i:05d}" for i in range(SHOW_ROW_LIMIT)]
    show, calls, patcher = show_with(names, mock_ctx)
    try:
        with pytest.warns(UserWarning, match="likely incomplete"):
            rows = show.execute(object_class, container=make_container(mock_ctx))
    finally:
        patcher.stop()
    assert len(rows) == SHOW_ROW_LIMIT
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("make_container", "expected"),
    [
        (lambda ctx: Table("DB", "SCH", "T", context=ctx), "IN TABLE"),
        (lambda ctx: View("DB", "SCH", "V", context=ctx), "IN VIEW"),
        (lambda ctx: DynamicTable("DB", "SCH", "DT", context=ctx), "IN TABLE"),
        (lambda ctx: MaterializedView("DB", "SCH", "MV", context=ctx), "IN VIEW"),
        (lambda ctx: Schema("DB", "SCH", context=ctx), "IN SCHEMA"),
        (lambda ctx: Database("DB", context=ctx), "IN DATABASE"),
    ],
)
def test_container_keyword(make_container, expected, mock_ctx):
    show, calls, patcher = show_with(["A"], mock_ctx)
    try:
        show.execute(Column, container=make_container(mock_ctx))
    finally:
        patcher.stop()
    assert f"{expected} IDENTIFIER(%s)" in calls[0][0]


class TestRows:
    """Show.rows: any listed kind, as a DataFrame."""

    @pytest.mark.parametrize(
        ("kind", "sql"),
        [
            ("tasks", "SHOW TASKS IN SCHEMA IDENTIFIER(%s)"),
            ("user functions", "SHOW USER FUNCTIONS IN SCHEMA IDENTIFIER(%s)"),
            ("User  Procedures", "SHOW USER PROCEDURES IN SCHEMA IDENTIFIER(%s)"),
            ("dynamic tables", "SHOW DYNAMIC TABLES IN SCHEMA IDENTIFIER(%s) LIMIT %s"),
        ],
    )
    def test_kind_sql(self, kind, sql, mock_ctx):
        show, calls, patcher = show_with(["A", "B"], mock_ctx)
        try:
            df = show.rows(kind, container=Schema("DB", "SCH", context=mock_ctx))
        finally:
            patcher.stop()
        assert isinstance(df, pd.DataFrame)
        assert df["name"].tolist() == ["A", "B"]
        assert calls[0][0] == sql
        assert calls[0][1][0] == "DB.SCH"

    def test_tasks_warn_at_the_limit(self, mock_ctx):
        """TASKS is not a verified LIMIT ... FROM kind, so it warns instead."""
        names = [f"T{i:05d}" for i in range(SHOW_ROW_LIMIT)]
        show, _calls, patcher = show_with(names, mock_ctx)
        try:
            with pytest.warns(UserWarning, match="likely incomplete"):
                show.rows("tasks", container=Schema("DB", "SCH", context=mock_ctx))
        finally:
            patcher.stop()

    def test_tasks_raise_at_the_limit_on_request(self, mock_ctx):
        names = [f"T{i:05d}" for i in range(SHOW_ROW_LIMIT)]
        show, _calls, patcher = show_with(names, mock_ctx)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                with pytest.raises(RuntimeError, match="likely incomplete"):
                    show.rows(
                        "tasks",
                        container=Schema("DB", "SCH", context=mock_ctx),
                        on_limit="raise",
                    )
        finally:
            patcher.stop()

    def test_raise_below_the_limit_returns_rows(self, mock_ctx):
        show, _calls, patcher = show_with(["A", "B"], mock_ctx)
        try:
            df = show.rows("tasks", on_limit="raise")
        finally:
            patcher.stop()
        assert df["name"].tolist() == ["A", "B"]

    def test_unknown_on_limit_is_rejected(self, mock_ctx):
        show, _calls, patcher = show_with(["A"], mock_ctx)
        try:
            with pytest.raises(ValueError, match="on_limit"):
                show.rows("tasks", on_limit="ignore")
        finally:
            patcher.stop()

    def test_empty_result_keeps_columns(self, mock_ctx):
        show, _calls, patcher = show_with([], mock_ctx)
        try:
            df = show.rows("tasks")
        finally:
            patcher.stop()
        assert df.empty
        assert list(df.columns) == ["name"]

    def test_frame_is_returned_as_fetched(self, mock_ctx):
        """No round trip through Python objects, so dtypes are SHOW's own."""
        fetched = pd.DataFrame(
            {
                "name": pd.Series(["A", "B"], dtype="object"),
                "rows": pd.Series([1, None], dtype="Int64"),
                "created_on": pd.to_datetime(["2026-01-01", "2026-01-02"]).as_unit(
                    "us"
                ),
            }
        )
        executor = MagicMock()
        executor.run_with_result_scan.return_value.to_df.return_value = fetched
        with patch("snowlib.models.base.show.Executor", return_value=executor):
            df = Show(mock_ctx).rows("tasks")
        pd.testing.assert_frame_equal(df, fetched)

    def test_paged_rows_are_one_frame(self, mock_ctx):
        names = [f"T{i:05d}" for i in range(SHOW_ROW_LIMIT + 3)]
        show, calls, patcher = show_with(names, mock_ctx)
        try:
            df = show.rows("tables", container=Schema("DB", "SCH", context=mock_ctx))
        finally:
            patcher.stop()
        assert df["name"].tolist() == names
        assert df.index.tolist() == list(range(len(names)))
        assert len(calls) == 2

    @pytest.mark.parametrize(
        "kind", ["grants", "tables; drop table x", "TABLES IN ACCOUNT", ""]
    )
    def test_unknown_kind_is_rejected(self, kind, mock_ctx):
        with pytest.raises(ValueError, match="Unsupported SHOW kind"):
            Show(mock_ctx).rows(kind)

    def test_session_show(self, mock_ctx):
        from snowlib.session import Session

        _show, calls, patcher = show_with(["A"], mock_ctx)
        try:
            session = Session(context=mock_ctx)
            df = session.show("tasks", in_=session.database("DB").schema("SCH"))
        finally:
            patcher.stop()
        assert df["name"].tolist() == ["A"]
        assert calls[0][0] == "SHOW TASKS IN SCHEMA IDENTIFIER(%s)"

    def test_session_show_passes_on_limit(self, mock_ctx):
        from snowlib.session import Session

        names = [f"T{i:05d}" for i in range(SHOW_ROW_LIMIT)]
        _show, _calls, patcher = show_with(names, mock_ctx)
        try:
            with pytest.raises(RuntimeError, match="likely incomplete"):
                Session(context=mock_ctx).show("tasks", on_limit="raise")
        finally:
            patcher.stop()


def test_unpaged_kind_below_the_limit_is_quiet(mock_ctx):
    show, _calls, patcher = show_with(["A", "B"], mock_ctx)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            rows = show.execute(Stage, container=Schema("DB", "SCH", context=mock_ctx))
    finally:
        patcher.stop()
    assert len(rows) == 2
