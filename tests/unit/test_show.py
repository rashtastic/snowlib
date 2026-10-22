"""Unit tests for Show paging past SHOW's 10,000-row maximum."""

import warnings
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snowlib.context import SnowflakeContext
from snowlib.models import Column, Database, Schema, Stage
from snowlib.models.base.show import SHOW_ROW_LIMIT, Show
from snowlib.models.table import Table, View


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


def test_unpaged_kind_below_the_limit_is_quiet(mock_ctx):
    show, _calls, patcher = show_with(["A", "B"], mock_ctx)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            rows = show.execute(Stage, container=Schema("DB", "SCH", context=mock_ctx))
    finally:
        patcher.stop()
    assert len(rows) == 2
