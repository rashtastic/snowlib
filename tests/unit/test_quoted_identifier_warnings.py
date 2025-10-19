"""Tests for quoted identifier warnings in model listing methods"""
import warnings
from unittest.mock import Mock, patch

from snowlib.models import Database
from snowlib.models.schema import Schema
from snowlib.models.table import Table


class TestQuotedIdentifierWarnings:
    """Test that warnings are raised when skipping objects with quoted identifiers"""
    
    def test_database_schemas_warns_on_quoted_identifiers(self):
        """Database.schemas should warn when skipping schemas with quoted identifiers"""
        ctx = Mock()
        db = Database("TEST_DB", ctx)
        
        # Mock SHOW results with mixed valid/invalid names
        mock_results = [
            {"name": "VALID_SCHEMA"},
            {"name": "invalid.schema"},  # Has dot
            {"name": "ANOTHER_VALID"},
        ]
        
        with patch.object(db, '_show_children', return_value=mock_results):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                schemas = db.schemas
                
                # Should have 2 valid schemas
                assert len(schemas) == 2
                assert schemas[0].name == "VALID_SCHEMA"
                assert schemas[1].name == "ANOTHER_VALID"
                
                # Should have 1 warning
                assert len(w) == 1
                assert "invalid.schema" in str(w[0].message)
                assert "quoted identifier" in str(w[0].message).lower()
    
    def test_schema_tables_warns_on_quoted_identifiers(self):
        """Schema.tables should warn when skipping tables with quoted identifiers"""
        ctx = Mock()
        schema = Schema("TEST_DB", "PUBLIC", ctx)
        
        # Mock SHOW results with mixed valid/invalid names
        mock_results = [
            {"name": "VALID_TABLE"},
            {"name": "123invalid"},  # Starts with number
            {"name": "ANOTHER_VALID"},
        ]
        
        with patch.object(schema, '_show_children', return_value=mock_results):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                tables = schema.tables
                
                # Should have 2 valid tables
                assert len(tables) == 2
                assert tables[0].name == "VALID_TABLE"
                assert tables[1].name == "ANOTHER_VALID"
                
                # Should have 1 warning
                assert len(w) == 1
                assert "123invalid" in str(w[0].message)
    
    def test_schema_views_warns_on_quoted_identifiers(self):
        """Schema.views should warn when skipping views with quoted identifiers"""
        ctx = Mock()
        schema = Schema("TEST_DB", "PUBLIC", ctx)
        
        # Mock SHOW results with mixed valid/invalid names
        mock_results = [
            {"name": "VALID_VIEW"},
            {"name": "view-with-dash"},  # Has dash
            {"name": "ANOTHER_VALID"},
        ]
        
        with patch.object(schema, '_show_children', return_value=mock_results):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                views = schema.views
                
                # Should have 2 valid views
                assert len(views) == 2
                assert views[0].name == "VALID_VIEW"
                assert views[1].name == "ANOTHER_VALID"
                
                # Should have 1 warning
                assert len(w) == 1
                assert "view-with-dash" in str(w[0].message)
    
    def test_table_columns_warns_on_quoted_identifiers(self):
        """TableLike.columns should warn when skipping columns with quoted identifiers"""
        ctx = Mock()
        table = Table("TEST_DB", "PUBLIC", "MY_TABLE", ctx)
        
        # Mock SHOW results with mixed valid/invalid names
        mock_results = [
            {"column_name": "VALID_COLUMN"},
            {"column_name": "column with spaces"},  # Has spaces
            {"column_name": "ANOTHER_VALID"},
        ]
        
        with patch.object(table, '_show_children', return_value=mock_results):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                columns = table.columns
                
                # Should have 2 valid columns
                assert len(columns) == 2
                assert columns[0].name == "VALID_COLUMN"
                assert columns[1].name == "ANOTHER_VALID"
                
                # Should have 1 warning
                assert len(w) == 1
                assert "column with spaces" in str(w[0].message)
