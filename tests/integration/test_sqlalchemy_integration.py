"""Integration tests for the optional SQLAlchemy module."""

import pytest

# Skip all tests in this module if sqlalchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import text

from snowlib.sqlalchemy import create_engine_from_profile


def test_create_engine_and_connect(test_profile: str):
    """
    Tests creating a SQLAlchemy engine from a snowlib profile and running a simple query.
    """
    # 1. Create engine from the primary test profile
    engine = create_engine_from_profile(test_profile)

    # 2. Connect and execute a query
    with engine.connect() as connection:
        result = connection.execute(text("select 1 as my_col")).scalar_one()
        assert result == 1


def test_create_engine_with_second_profile(test_profile2: str):
    """
    Tests creating a SQLAlchemy engine with the second profile (e.g., externalbrowser).
    """
    # 1. Create engine from the second test profile
    engine = create_engine_from_profile(test_profile2)

    # 2. Connect and execute a query
    with engine.connect() as connection:
        result = connection.execute(text("select 1 as my_col")).scalar_one()
        assert result == 1
