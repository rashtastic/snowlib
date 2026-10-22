"""Integration tests for the optional SQLAlchemy module."""

import json

import pytest

# Skip all tests in this module if sqlalchemy is not installed
sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import text  # noqa: E402

from snowlib.sqlalchemy import create_engine_from_profile  # noqa: E402


def test_create_engine_and_connect(test_profile: str):
    """
    Tests creating a SQLAlchemy engine from a snowlib profile and running a simple query.
    """
    # 1. Create engine from the primary test profile
    engine = create_engine_from_profile(test_profile)

    # 2. Connect and execute a query
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select 1 as my_col")).scalar_one()
            assert result == 1
    finally:
        engine.dispose()


def test_create_engine_with_second_profile(test_profile2: str):
    """
    Tests creating a SQLAlchemy engine with the second profile (e.g., externalbrowser).
    """
    # 1. Create engine from the second test profile
    engine = create_engine_from_profile(test_profile2)

    # 2. Connect and execute a query
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select 1 as my_col")).scalar_one()
            assert result == 1
    finally:
        engine.dispose()


def test_variant_reads_back(test_profile: str):
    """
    An untyped VARIANT reads back as a JSON string under every snowflake-sqlalchemy
    version. A column typed VARIANT reads back as a dict under 2.0, which turns on
    enable_structured_type_json by default, and as a string under 1.x.
    """
    from importlib.metadata import version

    from snowflake.sqlalchemy import VARIANT

    sql = """select parse_json('{"a": 1, "b": [1, 2]}') as v"""
    expected = {"a": 1, "b": [1, 2]}

    engine = create_engine_from_profile(test_profile)
    try:
        with engine.connect() as connection:
            untyped = connection.execute(text(sql)).scalar_one()
            typed = connection.execute(text(sql).columns(v=VARIANT)).scalar_one()
    finally:
        engine.dispose()

    assert isinstance(untyped, str)
    assert json.loads(untyped) == expected

    if int(version("snowflake-sqlalchemy").split(".")[0]) >= 2:
        assert typed == expected
    else:
        assert isinstance(typed, str)
        assert json.loads(typed) == expected
