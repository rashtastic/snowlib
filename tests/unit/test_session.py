"""Unit tests for Session class"""

import pytest
from unittest.mock import MagicMock, patch

from snowlib.session import Session, create_session, BoundModel
from snowlib.context import SnowflakeContext


class TestBoundModel:
    """Test BoundModel wrapper class"""
    
    def test_call_invokes_model_class(self):
        """BoundModel() invokes the wrapped model class with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_model_class = MagicMock()
        
        bound = BoundModel(mock_model_class, mock_ctx)
        bound("arg1", "arg2", "arg3")
        
        mock_model_class.assert_called_once_with("arg1", "arg2", "arg3", mock_ctx)
    
    def test_from_name_calls_classmethod(self):
        """BoundModel.from_name() calls the model's from_name classmethod"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_model_class = MagicMock()
        
        bound = BoundModel(mock_model_class, mock_ctx)
        bound.from_name("DB.SCHEMA.NAME")
        
        mock_model_class.from_name.assert_called_once_with(
            "DB.SCHEMA.NAME", mock_ctx,
            default_database=None, default_schema=None
        )
    
    def test_from_name_passes_defaults(self):
        """BoundModel.from_name() passes default_database and default_schema"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_model_class = MagicMock()
        
        bound = BoundModel(mock_model_class, mock_ctx)
        bound.from_name("NAME", default_database="DB", default_schema="SCH")
        
        mock_model_class.from_name.assert_called_once_with(
            "NAME", mock_ctx,
            default_database="DB", default_schema="SCH"
        )
    
    def test_repr(self):
        """BoundModel has useful repr"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        
        class FakeTable:
            pass
        
        bound = BoundModel(FakeTable, mock_ctx)
        assert repr(bound) == "BoundModel(FakeTable)"


class TestSessionInit:
    """Test Session initialization"""
    
    def test_requires_profile_or_context(self):
        """Session requires either profile or context"""
        with pytest.raises(ValueError, match="requires either"):
            Session()
    
    def test_rejects_both_profile_and_context(self):
        """Session rejects both profile and context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        with pytest.raises(ValueError, match="not both"):
            Session(profile="test", context=mock_ctx)
    
    def test_accepts_existing_context(self):
        """Session accepts an existing context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        assert session.context is mock_ctx
        assert session._owns_context is False
    
    @patch("snowlib.session.SnowflakeContext")
    def test_creates_context_from_profile(self, mock_ctx_class):
        """Session creates context from profile"""
        mock_ctx = MagicMock()
        mock_ctx_class.return_value = mock_ctx
        
        session = Session(profile="test")
        
        mock_ctx_class.assert_called_once_with(profile="test")
        assert session.context is mock_ctx
        assert session._owns_context is True
    
    @patch("snowlib.session.SnowflakeContext")
    def test_passes_overrides_to_context(self, mock_ctx_class):
        """Session passes overrides to context creation"""
        Session(profile="test", warehouse="MY_WH", role="MY_ROLE")
        
        mock_ctx_class.assert_called_once_with(
            profile="test", warehouse="MY_WH", role="MY_ROLE"
        )


class TestSessionLifecycle:
    """Test Session lifecycle management"""
    
    def test_close_owned_context(self):
        """Session closes context it owns"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        
        with patch("snowlib.session.SnowflakeContext", return_value=mock_ctx):
            session = Session(profile="test")
            session.close()
        
        mock_ctx.close.assert_called_once()
    
    def test_does_not_close_external_context(self):
        """Session does not close context it doesn't own"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        session.close()
        
        mock_ctx.close.assert_not_called()
    
    def test_context_manager(self):
        """Session works as context manager"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        
        with patch("snowlib.session.SnowflakeContext", return_value=mock_ctx):
            with Session(profile="test") as session:
                assert session.context is mock_ctx
        
        mock_ctx.close.assert_called_once()


class TestSessionPrimitives:
    """Test Session primitive methods"""
    
    @patch("snowlib.session.Executor")
    def test_execute_sql(self, mock_executor_class):
        """Session.execute_sql delegates to Executor"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor
        
        session = Session(context=mock_ctx)
        session.execute_sql("SELECT 1")
        
        mock_executor_class.assert_called_with(mock_ctx)
        mock_executor.run.assert_called_with("SELECT 1", bindings=None, arrow=True)
    
    @patch("snowlib.session.Executor")
    def test_execute_sql_with_bindings(self, mock_executor_class):
        """Session.execute_sql passes bindings"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor
        
        session = Session(context=mock_ctx)
        session.execute_sql("SELECT %s", bindings=[123])
        
        mock_executor.run.assert_called_with("SELECT %s", bindings=[123], arrow=True)
    
    @patch("snowlib.session.Executor")
    def test_query(self, mock_executor_class):
        """Session.query returns DataFrame"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_result = MagicMock()
        mock_executor = MagicMock()
        mock_executor.run.return_value = mock_result
        mock_executor_class.return_value = mock_executor
        
        session = Session(context=mock_ctx)
        session.query("SELECT 1")
        
        mock_result.to_df.assert_called_once()
    
    @patch("snowlib.session.Executor")
    def test_execute_block(self, mock_executor_class):
        """Session.execute_block delegates to Executor"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor
        
        session = Session(context=mock_ctx)
        session.execute_block("SELECT 1; SELECT 2;")
        
        mock_executor.run_block.assert_called_with("SELECT 1; SELECT 2;")


class TestSessionModelFactories:
    """Test Session model factory methods"""
    
    @patch("snowlib.session.Database")
    def test_database_factory_call(self, mock_database_class):
        """Session.database() creates Database with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.database("MY_DB")
        
        mock_database_class.assert_called_once_with("MY_DB", mock_ctx)
    
    @patch("snowlib.session.Database")
    def test_database_factory_from_name(self, mock_database_class):
        """Session.database.from_name() creates Database with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.database.from_name("MY_DB")
        
        mock_database_class.from_name.assert_called_once_with(
            "MY_DB", mock_ctx, default_database=None, default_schema=None
        )
    
    @patch("snowlib.session.Schema")
    def test_schema_factory_call(self, mock_schema_class):
        """Session.schema() creates Schema with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.schema("MY_DB", "MY_SCHEMA")
        
        mock_schema_class.assert_called_once_with("MY_DB", "MY_SCHEMA", mock_ctx)
    
    @patch("snowlib.session.Schema")
    def test_schema_factory_from_name(self, mock_schema_class):
        """Session.schema.from_name() creates Schema with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.schema.from_name("MY_DB.MY_SCHEMA")
        
        mock_schema_class.from_name.assert_called_once_with(
            "MY_DB.MY_SCHEMA", mock_ctx, 
            default_database=None, default_schema=None
        )
    
    @patch("snowlib.session.Table")
    def test_table_factory_call(self, mock_table_class):
        """Session.table() creates Table with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.table("MY_DB", "MY_SCHEMA", "MY_TABLE")
        
        mock_table_class.assert_called_once_with(
            "MY_DB", "MY_SCHEMA", "MY_TABLE", mock_ctx
        )
    
    @patch("snowlib.session.Table")
    def test_table_factory_from_name(self, mock_table_class):
        """Session.table.from_name() creates Table with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.table.from_name("MY_DB.MY_SCHEMA.MY_TABLE")
        
        mock_table_class.from_name.assert_called_once_with(
            "MY_DB.MY_SCHEMA.MY_TABLE", mock_ctx, 
            default_database=None, default_schema=None
        )
    
    @patch("snowlib.session.Table")
    def test_table_factory_from_name_with_defaults(self, mock_table_class):
        """Session.table.from_name() passes default_database and default_schema"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.table.from_name("MY_TABLE", default_database="DB", default_schema="SCH")
        
        mock_table_class.from_name.assert_called_once_with(
            "MY_TABLE", mock_ctx,
            default_database="DB", default_schema="SCH"
        )
    
    @patch("snowlib.session.View")
    def test_view_factory_call(self, mock_view_class):
        """Session.view() creates View with context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = Session(context=mock_ctx)
        
        session.view("MY_DB", "MY_SCHEMA", "MY_VIEW")
        
        mock_view_class.assert_called_once_with(
            "MY_DB", "MY_SCHEMA", "MY_VIEW", mock_ctx
        )


class TestCreateSession:
    """Test create_session factory function"""
    
    def test_create_session_with_context(self):
        """create_session works with existing context"""
        mock_ctx = MagicMock(spec=SnowflakeContext)
        session = create_session(context=mock_ctx)
        
        assert isinstance(session, Session)
        assert session.context is mock_ctx
    
    @patch("snowlib.session.SnowflakeContext")
    def test_create_session_with_profile(self, mock_ctx_class):
        """create_session works with profile"""
        session = create_session(profile="test")
        
        assert isinstance(session, Session)
        mock_ctx_class.assert_called_once_with(profile="test")
