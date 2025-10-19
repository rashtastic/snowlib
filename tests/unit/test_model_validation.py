import pytest

from snowlib.models import Database, Schema, Table
TEST_DB = "TEST_DB"
TEST_SCHEMA = "TEST_SCHEMA"


@pytest.fixture(scope="module")
def ctx():
    """
    Use a dummy context for these tests since we don't need a real connection.
    Validation happens before any connection is used.
    """
    return None


class TestModelIdentifierValidation:
    """
    Tests that model classes (`Database`, `Schema`, `Table`, etc.)
    correctly validate their identifiers upon creation by leveraging the FQN class.

    These tests confirm that the `ValueError` from `FQN` is propagated
    correctly through the model constructors.
    """

    def test_database_invalid_name_raises_error(self, ctx):
        """Verify Database constructor rejects invalid unquoted identifiers."""
        with pytest.raises(ValueError, match="Invalid identifier at position 0: 'invalid-db-name'"):
            Database("invalid-db-name", ctx)

    def test_schema_invalid_name_raises_error(self, ctx):
        """Verify Schema constructor rejects invalid unquoted identifiers."""
        with pytest.raises(ValueError, match="Invalid identifier at position 1: 'invalid-schema-name'"):
            Schema(TEST_DB, "invalid-schema-name", ctx)

    def test_table_invalid_name_raises_error(self, ctx):
        """Verify Table constructor rejects invalid unquoted identifiers."""
        with pytest.raises(ValueError, match="Invalid identifier at position 2: 'invalid-table-name'"):
            Table(TEST_DB, TEST_SCHEMA, "invalid-table-name", ctx)

    def test_valid_identifiers_work_correctly(self, ctx):
        """Verify that valid identifiers are accepted without error."""
        try:
            Database("VALID_DB", ctx)
            Schema("VALID_DB", "VALID_SCHEMA", ctx)
            Table("VALID_DB", "VALID_SCHEMA", "VALID_TABLE", ctx)
        except ValueError:
            pytest.fail("Valid identifiers were incorrectly rejected.")

