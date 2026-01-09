"""Utilities for detecting and serializing JSON-eligible pandas columns"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


def _is_scalar_na(value: Any) -> bool:
    """Check if a value is a scalar NA (None, np.nan, pd.NA) but not a collection"""
    if isinstance(value, (dict, list)):
        return False
    try:
        return bool(pd.isna(value))
    except (ValueError, TypeError):
        return False


def _is_json_serializable(value: Any) -> bool:
    """Check if a single value can be serialized to JSON

    Uses allow_nan=False to reject np.nan and np.inf inside structures.
    """
    try:
        json.dumps(value, allow_nan=False)
        return True
    except (TypeError, ValueError):
        return False


def is_json_eligible(series: pd.Series) -> bool:
    """Check if a pandas Series contains JSON-serializable objects

    A column is JSON-eligible if:
    - It has object dtype (contains Python objects)
    - All non-null values are dicts, lists, or other JSON-serializable structures
    - No values contain np.nan or np.inf inside nested structures

    Top-level null values (None, pd.NA, np.nan) are skipped and will become
    SQL NULL rather than JSON null.
    """
    if series.dtype != object:
        return False

    # Check that we have at least some non-null values that are dicts/lists
    non_null = series.dropna()
    if len(non_null) == 0:
        return False

    # Must have at least one dict or list to be considered JSON-eligible
    has_structured_data = False
    for value in non_null:
        if isinstance(value, (dict, list)):
            has_structured_data = True
            break

    if not has_structured_data:
        return False

    # Verify all non-null values are JSON-serializable
    for value in non_null:
        if not _is_json_serializable(value):
            return False

    return True


def serialize_json_column(series: pd.Series) -> pd.Series:
    """Serialize a JSON-eligible column to JSON strings

    Preserves top-level nulls as None (which become SQL NULL in Snowflake),
    while Python None inside dicts/lists becomes JSON null.
    """
    def safe_dumps(val: Any) -> str | None:
        if _is_scalar_na(val):
            return None
        return json.dumps(val)

    return series.apply(safe_dumps)


def prepare_json_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Detect JSON-eligible columns and serialize them

    Returns:
        A tuple of (modified DataFrame, list of column names that were serialized)

    The returned DataFrame has JSON-eligible columns converted to JSON strings.
    The caller should use PARSE_JSON() on these columns after loading to Snowflake.
    """
    df = df.copy()
    json_columns: list[str] = []

    for col in df.columns:
        if is_json_eligible(df[col]):
            df[col] = serialize_json_column(df[col])
            json_columns.append(str(col))

    return df, json_columns
