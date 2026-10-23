"""The installed snowflake-connector-python, for parameters that depend on its version."""

import re


def connector_version() -> tuple[int, int]:
    """(major, minor) of the installed snowflake-connector-python"""
    import snowflake.connector

    major, minor = re.match(r"(\d+)\.(\d+)", snowflake.connector.__version__).groups()  # type: ignore[union-attr]
    return int(major), int(minor)
