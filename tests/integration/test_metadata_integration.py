"""Integration tests for metadata access: Session.show, GET_DDL, Schema.columns and paging.

Objects are created in the test schema under one unique prefix and dropped
afterwards.
"""

import uuid
from collections.abc import Iterator
from unittest.mock import patch

import pytest

from snowlib.context import SnowflakeContext
from snowlib.models import DynamicTable, Schema, Table, View
from snowlib.models.base.show import Show
from snowlib.primitives import Executor, execute_sql
from snowlib.session import Session
from snowlib.utils.ddl import DdlTarget, arg_types_from_show

pytestmark = pytest.mark.integration


class Objects:
    """Names of the objects created for this module."""

    def __init__(self, database: str, schema: str):
        prefix = f"SNOWLIB_META_{uuid.uuid4().hex[:8].upper()}"
        self.database = database.upper()
        self.schema = schema.upper()
        self.prefix = prefix
        self.table = f"{prefix}_T"
        self.view = f"{prefix}_V"
        self.function = f"{prefix}_F"
        self.paged = [f"{prefix}_P{c}" for c in "ABC"]
        self.has_function = False

    def fqn(self, name: str) -> str:
        return f"{self.database}.{self.schema}.{name}"


@pytest.fixture(scope="module")
def ctx(test_profile) -> Iterator[SnowflakeContext]:
    context = SnowflakeContext(profile=test_profile)
    yield context
    context.close()


@pytest.fixture(scope="module")
def objects(ctx, test_database, test_schema) -> Iterator[Objects]:
    o = Objects(test_database, test_schema)
    statements = [
        f"create table {o.fqn(o.table)} (id int, name string, payload variant)",
        f"create view {o.fqn(o.view)} as select id, name from {o.fqn(o.table)}",
        *[f"create table {o.fqn(name)} (id int)" for name in o.paged],
    ]
    try:
        for sql in statements:
            execute_sql(sql, context=ctx)
        try:
            execute_sql(
                f"create function {o.fqn(o.function)}(x number, s varchar) "
                + "returns number as 'x + length(s)'",
                context=ctx,
            )
            o.has_function = True
        except Exception as e:  # noqa: BLE001
            print(f"function not created, its tests will skip: {e}")
            o.has_function = False
        yield o
    finally:
        drops = [
            f"drop view if exists {o.fqn(o.view)}",
            f"drop table if exists {o.fqn(o.table)}",
            *[f"drop table if exists {o.fqn(name)}" for name in o.paged],
            f"drop function if exists {o.fqn(o.function)}(number, varchar)",
        ]
        for sql in drops:
            execute_sql(sql, context=ctx)


@pytest.fixture(scope="module")
def session(ctx) -> Session:
    return Session(context=ctx)


@pytest.fixture(scope="module")
def schema(ctx, objects) -> Schema:
    return Schema(objects.database, objects.schema, ctx)


class TestSessionShow:
    def test_tables(self, session, schema, objects):
        df = session.show("tables", in_=schema, like=objects.table)
        assert df["name"].tolist() == [objects.table]
        assert df["is_dynamic"].tolist() == ["N"]

    def test_views(self, session, schema, objects):
        df = session.show("views", in_=schema, like=objects.view)
        assert df["name"].tolist() == [objects.view]

    def test_user_functions(self, session, schema, objects):
        if not objects.has_function:
            pytest.skip("test role cannot create functions")
        df = session.show("user functions", in_=schema, like=objects.function)
        assert df["name"].tolist() == [objects.function]
        assert arg_types_from_show(df["arguments"].iloc[0]) == ["NUMBER", "VARCHAR"]

    def test_kind_with_no_objects_is_empty(self, session, schema, objects):
        df = session.show("sequences", in_=schema, like=f"{objects.prefix}%")
        assert df.empty


class TestDdl:
    def test_table_ddl(self, ctx, objects):
        ddl = Table(objects.database, objects.schema, objects.table, ctx).ddl()
        assert "TABLE" in ddl.upper()
        assert objects.fqn(objects.table) in ddl.upper()
        assert "PAYLOAD" in ddl.upper()

    def test_view_ddl(self, ctx, objects):
        ddl = View(objects.database, objects.schema, objects.view, ctx).ddl()
        assert "VIEW" in ddl.upper()
        assert objects.table in ddl.upper()

    def test_schema_ddl(self, schema):
        assert "SCHEMA" in schema.ddl().upper()

    def test_batch(self, session, objects):
        targets = [
            DdlTarget("TABLE", objects.database, objects.schema, objects.table),
            DdlTarget("VIEW", objects.database, objects.schema, objects.view),
        ]
        if objects.has_function:
            targets.append(
                DdlTarget(
                    "FUNCTION",
                    objects.database,
                    objects.schema,
                    objects.function,
                    ["NUMBER", "VARCHAR"],
                )
            )
        df = session.get_ddl(targets)
        assert df["object_type"].tolist() == [t.object_type for t in targets]
        names = df["object_name"].tolist()
        assert names[:2] == [objects.table, objects.view]
        if objects.has_function:
            assert names[2] == f"{objects.function}(NUMBER, VARCHAR)"
            assert "FUNCTION" in df["ddl"].iloc[2].upper()
        assert all(isinstance(d, str) and d for d in df["ddl"])


class TestSchemaColumns:
    def test_columns(self, schema, objects):
        df = schema.columns()
        assert df.columns[0] == "object_fqn"
        table_cols = df[df["object_fqn"] == objects.fqn(objects.table)]
        assert table_cols["column_name"].tolist() == ["ID", "NAME", "PAYLOAD"]
        view_cols = df[df["object_fqn"] == objects.fqn(objects.view)]
        assert view_cols["column_name"].tolist() == ["ID", "NAME"]


class TestDynamicTables:
    def test_matches_show_tables(self, session, schema):
        dynamic = schema.dynamic_tables
        assert all(isinstance(t, DynamicTable) for t in dynamic)
        flagged = session.show("tables", in_=schema)
        flagged_names = set(flagged.loc[flagged["is_dynamic"] == "Y", "name"])
        assert {t.name for t in dynamic} == flagged_names


class TestPaging:
    def test_from_resumes_after_the_last_name(self, ctx, schema, objects):
        """With a page size of 1, three tables take three pages and a fourth, empty one."""
        real = Executor.run_with_result_scan
        with patch("snowlib.models.base.show.SHOW_ROW_LIMIT", 1):
            with patch.object(
                Executor, "run_with_result_scan", autospec=True, side_effect=real
            ) as spy:
                rows = Show(ctx).execute(
                    Table, container=schema, like=f"{objects.prefix}_P%"
                )
        assert [r["name"] for r in rows] == objects.paged
        sqls = [c.args[1] for c in spy.call_args_list]
        assert len(sqls) == 4
        assert all(s.endswith("LIMIT %s FROM %s") for s in sqls[1:])

    def test_schema_tables_includes_created(self, schema, objects):
        names = {t.name for t in schema.tables}
        assert {objects.table, *objects.paged} <= names
