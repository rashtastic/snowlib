"""Unit tests for GET_DDL: names, signatures, the batch query and model ddl()."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from snowlib.context import SnowflakeContext
from snowlib.models import Database, Schema, Stage
from snowlib.models.table import DynamicTable, MaterializedView, Table, View
from snowlib.utils.ddl import (
    DdlTarget,
    arg_types_from_show,
    ddl_name,
    ddl_query,
    object_label,
    quote_ident,
)


@pytest.fixture
def mock_ctx() -> SnowflakeContext:
    return MagicMock(spec=SnowflakeContext)


class TestNames:
    def test_quote_ident_keeps_case_and_escapes(self):
        assert quote_ident("my_task") == '"my_task"'
        assert quote_ident('odd"name') == '"odd""name"'

    @pytest.mark.parametrize(
        ("arguments", "expected"),
        [
            ("F() RETURN NUMBER", []),
            ("F(ARRAY, [BOOLEAN]) RETURN OBJECT", ["ARRAY", "BOOLEAN"]),
            ("F(NUMBER(38,0), VARCHAR) RETURN VARCHAR", ["NUMBER(38,0)", "VARCHAR"]),
            ("P(VECTOR(FLOAT, 3)) RETURN TABLE ()", ["VECTOR(FLOAT, 3)"]),
        ],
    )
    def test_arg_types_from_show(self, arguments, expected):
        assert arg_types_from_show(arguments) == expected

    @pytest.mark.parametrize(
        ("target", "label", "name"),
        [
            (DdlTarget("TABLE", "DB", "SCH", "T"), "T", '"DB"."SCH"."T"'),
            (DdlTarget("view", "DB", "SCH", "v"), "v", '"DB"."SCH"."v"'),
            (
                DdlTarget("DYNAMIC_TABLE", "DB", "SCH", "DT"),
                "DT",
                '"DB"."SCH"."DT"',
            ),
            (
                DdlTarget("FUNCTION", "DB", "SCH", "F", ["NUMBER", "VARCHAR"]),
                "F(NUMBER, VARCHAR)",
                '"DB"."SCH"."F"(NUMBER, VARCHAR)',
            ),
            (
                DdlTarget("PROCEDURE", "DB", "SCH", "P"),
                "P()",
                '"DB"."SCH"."P"()',
            ),
        ],
    )
    def test_label_and_name(self, target, label, name):
        assert object_label(target) == label
        assert ddl_name(target) == name

    def test_arg_types_rejected_for_a_table(self):
        with pytest.raises(ValueError, match="does not take argument types"):
            object_label(DdlTarget("TABLE", "DB", "SCH", "T", ["NUMBER"]))

    @pytest.mark.parametrize("object_type", ["STAGE", "DYNAMIC TABLE", "x'); --"])
    def test_unsupported_type_rejected(self, object_type):
        with pytest.raises(ValueError, match="does not support"):
            ddl_query([DdlTarget(object_type, "DB", "SCH", "T")])


class TestQuery:
    def test_one_query_for_many(self):
        sql, bindings = ddl_query(
            [
                DdlTarget("TASK", "DB", "SCH", "my_task"),
                DdlTarget("FUNCTION", "DB", "SCH", "F", ["NUMBER"]),
            ]
        )
        assert sql.count("UNION ALL") == 1
        assert sql.count("GET_DDL(%s, %s, TRUE)") == 2
        assert bindings == [
            "TASK",
            "my_task",
            "TASK",
            '"DB"."SCH"."my_task"',
            "FUNCTION",
            "F(NUMBER)",
            "FUNCTION",
            '"DB"."SCH"."F"(NUMBER)',
        ]

    def test_empty_is_an_error(self):
        with pytest.raises(ValueError, match="at least one"):
            ddl_query([])

    def test_session_get_ddl(self, mock_ctx):
        from snowlib.session import Session

        with patch("snowlib.session.Executor") as mock_executor_class:
            mock_executor_class.return_value.run.return_value.to_df.return_value = (
                pd.DataFrame(
                    {"object_type": ["TASK"], "object_name": ["T"], "ddl": ["x"]}
                )
            )
            df = Session(context=mock_ctx).get_ddl([DdlTarget("TASK", "DB", "S", "T")])
        assert df["ddl"].tolist() == ["x"]
        sql = mock_executor_class.return_value.run.call_args.args[0]
        assert "GET_DDL" in sql


class TestModelDdl:
    @pytest.mark.parametrize(
        ("make", "object_type", "name"),
        [
            (lambda ctx: Database("DB", context=ctx), "DATABASE", '"DB"'),
            (lambda ctx: Schema("DB", "SCH", context=ctx), "SCHEMA", '"DB"."SCH"'),
            (
                lambda ctx: Table("DB", "SCH", "T", context=ctx),
                "TABLE",
                '"DB"."SCH"."T"',
            ),
            (
                lambda ctx: View("DB", "SCH", "V", context=ctx),
                "VIEW",
                '"DB"."SCH"."V"',
            ),
            (
                lambda ctx: MaterializedView("DB", "SCH", "MV", context=ctx),
                "VIEW",
                '"DB"."SCH"."MV"',
            ),
            (
                lambda ctx: DynamicTable("DB", "SCH", "DT", context=ctx),
                "DYNAMIC_TABLE",
                '"DB"."SCH"."DT"',
            ),
        ],
    )
    @patch("snowlib.primitives.Executor")
    def test_ddl(self, mock_executor_class, make, object_type, name, mock_ctx):
        run = mock_executor_class.return_value.run
        run.return_value.to_df.return_value = pd.DataFrame({"ddl": ["create ..."]})
        assert make(mock_ctx).ddl() == "create ..."
        assert run.call_args.kwargs["bindings"] == [object_type, name]

    def test_stage_has_no_ddl(self, mock_ctx):
        with pytest.raises(NotImplementedError, match="Stage"):
            Stage("DB", "SCH", "STG", context=mock_ctx).ddl()
