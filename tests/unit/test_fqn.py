"""Test FQN class"""

import pytest
from snowlib.models.base.fqn import FQN


def test_fqn_single_part():
    """Test FQN with single part (database)"""
    fqn = FQN.from_parts("my_db")
    assert str(fqn) == "MY_DB"
    assert fqn.database == "MY_DB"
    assert fqn.name == "MY_DB"
    assert len(fqn) == 1


def test_fqn_two_parts():
    """Test FQN with two parts (database.schema)"""
    fqn = FQN.from_parts("my_db", "public")
    assert str(fqn) == "MY_DB.PUBLIC"
    assert fqn.database == "MY_DB"
    assert fqn.schema == "PUBLIC"
    assert fqn.name == "PUBLIC"
    assert len(fqn) == 2


def test_fqn_three_parts():
    """Test FQN with three parts (database.schema.table)"""
    fqn = FQN.from_parts("my_db", "public", "sales")
    assert str(fqn) == "MY_DB.PUBLIC.SALES"
    assert fqn.database == "MY_DB"
    assert fqn.schema == "PUBLIC"
    assert fqn.table == "SALES"
    assert fqn.name == "SALES"
    assert len(fqn) == 3


def test_fqn_four_parts():
    """Test FQN with four parts (database.schema.table.column)"""
    fqn = FQN.from_parts("my_db", "public", "sales", "amount")
    assert str(fqn) == "MY_DB.PUBLIC.SALES.AMOUNT"
    assert fqn.database == "MY_DB"
    assert fqn.schema == "PUBLIC"
    assert fqn.table == "SALES"
    assert fqn.column == "AMOUNT"
    assert fqn.name == "AMOUNT"
    assert len(fqn) == 4


def test_fqn_parse():
    """Test FQN.parse() from dotted string"""
    fqn = FQN.parse("my_db.public.sales")
    assert str(fqn) == "MY_DB.PUBLIC.SALES"
    assert fqn.database == "MY_DB"
    assert fqn.schema == "PUBLIC"
    assert fqn.name == "SALES"


def test_fqn_uppercasing():
    """Test automatic uppercasing"""
    fqn = FQN.from_parts("my_db", "Public", "SaLeS")
    assert str(fqn) == "MY_DB.PUBLIC.SALES"


def test_fqn_validates_identifiers():
    """Test validation of identifier rules"""
    with pytest.raises(ValueError, match="Invalid identifier"):
        FQN.from_parts("my-db")  # Hyphens not allowed
    
    with pytest.raises(ValueError, match="Invalid identifier"):
        FQN.from_parts("123db")  # Can't start with number
    
    with pytest.raises(ValueError, match="Invalid identifier"):
        FQN.from_parts("my db")  # Spaces not allowed
    
    with pytest.raises(ValueError, match="Invalid identifier"):
        FQN.from_parts("my;db")  # Semicolon not allowed


def test_fqn_empty_raises():
    """Test empty FQN raises error"""
    with pytest.raises(ValueError, match="at least one part"):
        FQN.from_parts()


def test_fqn_immutable():
    """Test FQN is immutable (frozen dataclass)"""
    fqn = FQN.from_parts("my_db", "public")
    with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
        fqn.parts = ("other_db", "other_schema")


def test_fqn_equality():
    """Test FQN equality (dataclass auto-generates __eq__)"""
    fqn1 = FQN.from_parts("my_db", "public", "sales")
    fqn2 = FQN.from_parts("MY_DB", "PUBLIC", "SALES")
    assert fqn1 == fqn2  # Case-insensitive due to uppercasing


def test_fqn_hashable():
    """Test FQN can be used in sets/dicts (dataclass frozen=True)"""
    fqn1 = FQN.from_parts("my_db", "public", "sales")
    fqn2 = FQN.from_parts("MY_DB", "PUBLIC", "SALES")
    
    fqn_set = {fqn1, fqn2}
    assert len(fqn_set) == 1  # Should be same due to equality
