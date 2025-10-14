"""Unit tests for io.names module - name parsing and validation.

These are fast unit tests that don't require Snowflake connection.
"""

import pytest
from unittest.mock import Mock, patch

from snowlib.io.names import (
    validate_identifier,
    parse_table_name,
    resolve_table_name,
    format_qualified_name,
)


@pytest.fixture
def test_database():
    return "O_CRI"

@pytest.fixture
def test_schema():
    return "PUBLIC"


class TestValidateIdentifier:
    """Test validate_identifier function."""
    
    def test_valid_simple_name(self):
        """Test valid simple identifier."""
        assert validate_identifier("my_table") is True
    
    def test_valid_with_numbers(self):
        """Test valid identifier with numbers."""
        assert validate_identifier("table_123") is True
    
    def test_valid_starts_with_underscore(self):
        """Test identifier starting with underscore."""
        assert validate_identifier("_private_table") is True
    
    def test_valid_uppercase(self):
        """Test uppercase identifier."""
        assert validate_identifier("MY_TABLE") is True
    
    def test_valid_mixed_case(self):
        """Test mixed case identifier."""
        assert validate_identifier("MyTable_123") is True
    
    def test_invalid_starts_with_number(self):
        """Test identifier starting with number."""
        with pytest.raises(ValueError, match="must start with letter or underscore"):
            validate_identifier("123_table")
    
    def test_invalid_special_characters(self):
        """Test identifier with special characters."""
        with pytest.raises(ValueError, match="must start with letter or underscore"):
            validate_identifier("my-table")
        
        with pytest.raises(ValueError, match="must start with letter or underscore"):
            validate_identifier("my.table")
    
    def test_invalid_empty_string(self):
        """Test empty identifier."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_identifier("")
    
    def test_invalid_too_long(self):
        """Test identifier that's too long."""
        long_name = "a" * 256
        with pytest.raises(ValueError, match="too long"):
            validate_identifier(long_name)
    
    def test_max_length_valid(self):
        """Test identifier at max length (255 chars)."""
        max_name = "a" * 255
        assert validate_identifier(max_name) is True


class TestParseTableName:
    """Test parse_table_name function."""
    
    def test_fully_qualified(self, test_database, test_schema):
        """Test parsing fully qualified name."""
        db, schema, table = parse_table_name(f"{test_database}.{test_schema}.my_table")
        assert db == test_database
        assert schema == test_schema
        assert table == "my_table"
    
    def test_fully_qualified_lowercase(self, test_database, test_schema):
        """Test parsing fully qualified name in lowercase."""
        db, schema, table = parse_table_name(f"{test_database.lower()}.{test_schema.lower()}.my_table")
        assert db == test_database.lower()
        assert schema == test_schema.lower()
        assert table == "my_table"
    
    def test_fully_qualified_mixed_case(self, test_database, test_schema):
        """Test parsing fully qualified name with mixed case."""
        # Mix case of test_database and test_schema for testing
        mixed_db = ''.join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(test_database))
        mixed_schema = ''.join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(test_schema))
        
        db, schema, table = parse_table_name(f"{mixed_db}.{mixed_schema}.MY_table")
        assert db == mixed_db
        assert schema == mixed_schema
        assert table == "MY_table"
    
    def test_partial_schema_table(self, test_database, test_schema):
        """Test parsing schema.table with default database."""
        db, schema, table = parse_table_name(
            f"{test_schema}.my_table",
            default_database=test_database
        )
        assert db == test_database
        assert schema == "PUBLIC"
        assert table == "my_table"
    
    def test_partial_schema_table_missing_database(self, test_schema):
        """Test parsing schema.table without default database."""
        with pytest.raises(ValueError, match="Database required"):
            parse_table_name(f"{test_schema}.my_table")
    
    def test_just_table_with_defaults(self, test_database, test_schema):
        """Test parsing just table name with defaults."""
        db, schema, table = parse_table_name(
            "my_table",
            default_schema=test_schema,
            default_database=test_database
        )
        assert db == test_database
        assert schema == test_schema
        assert table == "my_table"
    
    def test_just_table_missing_schema(self, test_database):
        """Test parsing just table without default schema."""
        with pytest.raises(ValueError, match="Schema required"):
            parse_table_name("my_table", default_database=test_database)
    
    def test_just_table_missing_database(self, test_schema):
        """Test parsing just table without default database."""
        with pytest.raises(ValueError, match="Database required"):
            parse_table_name("my_table", default_schema=test_schema)
    
    def test_empty_table_name(self):
        """Test empty table name."""
        with pytest.raises(ValueError, match="cannot be empty"):
            parse_table_name("")
    
    def test_too_many_parts(self):
        """Test table name with too many parts."""
        with pytest.raises(ValueError, match="Invalid table name"):
            parse_table_name("a.b.c.d")
    
    def test_invalid_identifier_in_name(self, test_database, test_schema):
        """Test table name with invalid identifier."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            parse_table_name(f"{test_database}.{test_schema}.123invalid")


class TestResolveTableName:
    """Test resolve_table_name function."""
    
    def test_fully_qualified_no_context_needed(self, test_database, test_schema):
        """Test fully qualified name doesn't need context."""
        # Mock shouldn't be called since fully qualified
        with patch('snowlib.io.names.get_current_database') as mock_db:
            with patch('snowlib.io.names.get_current_schema') as mock_schema:
                db, schema, table = resolve_table_name(
                    f"{test_database}.{test_schema}.my_table",
                    context="test"
                )
                
                assert db == test_database
                assert schema == test_schema
                assert table == "my_table"
                
                # Verify get_current_* were not called
                mock_db.assert_not_called()
                mock_schema.assert_not_called()
    
    def test_partial_name_gets_database_from_context(self, test_database, test_schema):
        """Test partial name queries context for database."""
        with patch('snowlib.io.names.get_current_database', return_value=test_database):
            db, schema, table = resolve_table_name(
                f"{test_schema}.my_table",
                context="test"
            )
            
            assert db == test_database
            assert schema == test_schema
            assert table == "my_table"
    
    def test_just_table_gets_both_from_context(self, test_database, test_schema):
        """Test minimal name queries context for database and schema."""
        with patch('snowlib.io.names.get_current_database', return_value=test_database):
            with patch('snowlib.io.names.get_current_schema', return_value=test_schema):
                db, schema, table = resolve_table_name("my_table", context="test")
                
                assert db == test_database
                assert schema == test_schema
                assert table == "my_table"
    
    def test_missing_database_in_context(self, test_schema):
        """Test error when context has no database."""
        with patch('snowlib.io.names.get_current_database', return_value=None):
            with pytest.raises(ValueError, match="Cannot resolve database"):
                resolve_table_name(f"{test_schema}.my_table", context="test")
    
    def test_missing_schema_in_context(self, test_database):
        """Test error when context has no schema."""
        with patch('snowlib.io.names.get_current_database', return_value=test_database):
            with patch('snowlib.io.names.get_current_schema', return_value=None):
                with pytest.raises(ValueError, match="Cannot resolve schema"):
                    resolve_table_name("my_table", context="test")
    
    def test_empty_table_name(self):
        """Test empty table name."""
        with pytest.raises(ValueError, match="cannot be empty"):
            resolve_table_name("", context="test")
    
    def test_passes_context_to_get_current(self, test_database, test_schema):
        """Test that context is passed to get_current_* functions."""
        mock_ctx = Mock()
        
        with patch('snowlib.io.names.get_current_database', return_value=test_database) as mock_db:
            with patch('snowlib.io.names.get_current_schema', return_value=test_schema) as mock_schema:
                resolve_table_name("my_table", context=mock_ctx)
                
                # Verify context was passed
                mock_db.assert_called_once_with(context=mock_ctx)
                mock_schema.assert_called_once_with(context=mock_ctx)


class TestFormatQualifiedName:
    """Test format_qualified_name function."""
    
    def test_format_uppercase(self, test_database, test_schema):
        """Test formatting uppercases all parts."""
        result = format_qualified_name(test_database.lower(), test_schema.lower(), "my_table")
        assert result == f"{test_database.upper()}.{test_schema.upper()}.MY_TABLE"
    
    def test_format_already_uppercase(self, test_database, test_schema):
        """Test formatting with already uppercase names."""
        result = format_qualified_name(test_database.upper(), test_schema.upper(), "MY_TABLE")
        assert result == f"{test_database.upper()}.{test_schema.upper()}.MY_TABLE"
    
    def test_format_mixed_case(self, test_database, test_schema):
        """Test formatting with mixed case."""
        mixed_db = ''.join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(test_database))
        mixed_schema = ''.join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(test_schema))
        result = format_qualified_name(mixed_db, mixed_schema, "My_TaBLe")
        assert result == f"{test_database.upper()}.{test_schema.upper()}.MY_TABLE"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
