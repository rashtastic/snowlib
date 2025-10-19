"""Unit tests for the models layer.

These tests mock the underlying primitives to test the logic of the
Snowflake object model without requiring a live connection.
"""
import pytest
from unittest.mock import MagicMock, patch

from snowlib.context import SnowflakeContext
from snowlib.models import Database, Schema
from snowlib.models.table import Table


@pytest.fixture
def mock_ctx() -> SnowflakeContext:
    """Provides a mocked SnowflakeContext."""
    ctx = MagicMock(spec=SnowflakeContext)
    ctx.cursor = MagicMock()
    return ctx


class TestDatabaseModel:
    """Unit tests for the Database model."""

    def test_database_init(self, mock_ctx):
        db = Database("TEST_DB", context=mock_ctx)
        assert db.name == "TEST_DB"
        assert db.fqn == "TEST_DB"
        assert db.context == mock_ctx

    @patch("snowlib.models.base.show.Show")
    def test_database_exists(self, mock_show_class, mock_ctx):
        db = Database("TEST_DB", context=mock_ctx)
        
        # Setup mock Show instance
        mock_show_instance = MagicMock()
        mock_show_instance.exists.return_value = True
        mock_show_class.return_value = mock_show_instance
        
        assert db.exists() is True
        mock_show_class.assert_called_once_with(mock_ctx)
        mock_show_instance.exists.assert_called_once_with(Database, "TEST_DB", container=None)

    @patch("snowlib.models.database.Container._show_children")
    def test_database_schemas(self, mock_show_children, mock_ctx):
        db = Database("TEST_DB", context=mock_ctx)
        mock_show_children.return_value = [{"name": "PUBLIC"}, {"name": "CUSTOM"}]

        schemas = db.schemas
        assert len(schemas) == 2
        assert isinstance(schemas[0], Schema)
        assert schemas[0].name == "PUBLIC"
        assert schemas[0].database.name == "TEST_DB"
        assert schemas[1].name == "CUSTOM"
        mock_show_children.assert_called_once_with(Schema)

    def test_database_schema(self, mock_ctx):
        db = Database("TEST_DB", context=mock_ctx)
        schema = db.schema("PUBLIC")
        assert isinstance(schema, Schema)
        assert schema.name == "PUBLIC"
        assert schema.database.name == "TEST_DB"


class TestSchemaModel:
    """Unit tests for the Schema model."""

    def test_schema_init(self, mock_ctx):
        schema = Schema("TEST_DB", "PUBLIC", context=mock_ctx)
        assert schema.name == "PUBLIC"
        assert schema.database.name == "TEST_DB"
        assert schema.fqn == "TEST_DB.PUBLIC"
        assert schema.context == mock_ctx

    @patch("snowlib.models.base.show.Show")
    def test_schema_exists(self, mock_show_class, mock_ctx):
        schema = Schema("TEST_DB", "PUBLIC", context=mock_ctx)
        
        # Setup mock Show instance
        mock_show_instance = MagicMock()
        mock_show_instance.exists.return_value = False
        mock_show_class.return_value = mock_show_instance
        
        assert schema.exists() is False
        mock_show_class.assert_called_once_with(mock_ctx)
        mock_show_instance.exists.assert_called_once()

    @patch("snowlib.models.schema.Container._show_children")
    def test_schema_tables(self, mock_show_children, mock_ctx):
        schema = Schema("TEST_DB", "PUBLIC", context=mock_ctx)
        mock_show_children.return_value = [{"name": "T1"}, {"name": "T2"}]

        tables = schema.tables
        assert len(tables) == 2
        assert isinstance(tables[0], Table)
        assert tables[0].name == "T1"
        assert tables[0].schema.name == "PUBLIC"
        assert tables[0].database.name == "TEST_DB"
        mock_show_children.assert_called_once_with(Table)


class TestTableModel:
    """Unit tests for the Table model."""

    def test_table_init(self, mock_ctx):
        table = Table("DB", "SCHEMA", "TABLE", context=mock_ctx)
        assert table.name == "TABLE"
        assert table.schema.name == "SCHEMA"
        assert table.database.name == "DB"
        assert table.fqn == "DB.SCHEMA.TABLE"

    @patch("snowlib.primitives.Executor")
    def test_table_read(self, mock_executor_class, mock_ctx):
        import pandas as pd
        from snowlib.primitives.result import QueryResult
        
        table = Table("DB", "SCHEMA", "TABLE", context=mock_ctx)
        
        # Setup mock Executor and QueryResult
        mock_executor = MagicMock()
        mock_result = MagicMock(spec=QueryResult)
        test_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        mock_result.to_df.return_value = test_df
        mock_executor.run.return_value = mock_result
        mock_executor_class.return_value = mock_executor
        
        # Call table.read()
        df = table.read(limit=10)
        
        # Assertions
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        mock_executor_class.assert_called_once_with(mock_ctx)
        mock_executor.run.assert_called_once()
        # Check that SQL contains expected parts
        call_args = mock_executor.run.call_args
        sql = call_args[0][0] if call_args[0] else call_args.args[0]
        assert "SELECT * FROM DB.SCHEMA.TABLE" in sql
        assert "LIMIT" in sql

    @patch("snowlib.primitives.execute_sql")
    def test_table_drop(self, mock_execute, mock_ctx):
        table = Table("DB", "SCHEMA", "TABLE", context=mock_ctx)
        table.drop()
        mock_execute.assert_called_once_with(
            "DROP TABLE DB.SCHEMA.TABLE", context=mock_ctx
        )
