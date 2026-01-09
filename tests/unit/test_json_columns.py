"""Tests for JSON column detection and serialization utilities"""

import json
import math

import numpy as np
import pandas as pd
import pytest

from snowlib.utils.json_columns import (
    is_json_eligible,
    serialize_json_column,
    prepare_json_columns,
)


class TestIsJsonEligible:
    """Tests for is_json_eligible function"""

    def test_column_of_dicts_is_eligible(self):
        series = pd.Series([{"a": 1}, {"b": 2}])
        assert is_json_eligible(series) is True

    def test_column_of_lists_is_eligible(self):
        series = pd.Series([[1, 2], [3, 4]])
        assert is_json_eligible(series) is True

    def test_mixed_dicts_and_lists_is_eligible(self):
        series = pd.Series([{"a": 1}, [1, 2], {"b": [3, 4]}])
        assert is_json_eligible(series) is True

    def test_nested_structures_eligible(self):
        series = pd.Series([{"a": {"nested": [1, 2, 3]}}, {"b": [{"x": 1}]}])
        assert is_json_eligible(series) is True

    def test_column_with_top_level_none_is_eligible(self):
        series = pd.Series([{"a": 1}, None, {"b": 2}])
        assert is_json_eligible(series) is True

    def test_column_with_top_level_nan_is_eligible(self):
        series = pd.Series([{"a": 1}, np.nan, {"b": 2}])
        assert is_json_eligible(series) is True

    def test_column_with_top_level_pd_na_is_eligible(self):
        series = pd.Series([{"a": 1}, pd.NA, {"b": 2}])
        assert is_json_eligible(series) is True

    def test_nested_none_is_eligible(self):
        # Python None inside dicts becomes JSON null
        series = pd.Series([{"a": 1, "b": None}, {"c": None}])
        assert is_json_eligible(series) is True

    def test_nested_nan_is_not_eligible(self):
        # np.nan inside structures cannot be serialized to JSON
        series = pd.Series([{"a": 1, "b": float("nan")}])
        assert is_json_eligible(series) is False

    def test_nested_inf_is_not_eligible(self):
        # np.inf inside structures cannot be serialized to JSON
        series = pd.Series([{"a": float("inf")}])
        assert is_json_eligible(series) is False

    def test_nested_negative_inf_is_not_eligible(self):
        series = pd.Series([{"a": float("-inf")}])
        assert is_json_eligible(series) is False

    def test_string_column_not_eligible(self):
        series = pd.Series(["a", "b", "c"])
        assert is_json_eligible(series) is False

    def test_int_column_not_eligible(self):
        series = pd.Series([1, 2, 3])
        assert is_json_eligible(series) is False

    def test_float_column_not_eligible(self):
        series = pd.Series([1.1, 2.2, 3.3])
        assert is_json_eligible(series) is False

    def test_all_null_column_not_eligible(self):
        # No structured data to infer from
        series = pd.Series([None, None, None])
        assert is_json_eligible(series) is False

    def test_empty_series_not_eligible(self):
        series = pd.Series([], dtype=object)
        assert is_json_eligible(series) is False

    def test_column_with_non_serializable_objects_not_eligible(self):
        # Custom objects that aren't JSON serializable
        class CustomObj:
            pass

        series = pd.Series([CustomObj(), {"a": 1}])
        assert is_json_eligible(series) is False

    def test_column_of_only_strings_not_eligible(self):
        # Even though strings are JSON-serializable, we only want dicts/lists
        series = pd.Series(["hello", "world"], dtype=object)
        assert is_json_eligible(series) is False


class TestSerializeJsonColumn:
    """Tests for serialize_json_column function"""

    def test_serializes_dicts_to_json_strings(self):
        series = pd.Series([{"a": 1}, {"b": 2}])
        result = serialize_json_column(series)
        assert result.tolist() == ['{"a": 1}', '{"b": 2}']

    def test_serializes_lists_to_json_strings(self):
        series = pd.Series([[1, 2], [3, 4]])
        result = serialize_json_column(series)
        assert result.tolist() == ["[1, 2]", "[3, 4]"]

    def test_preserves_top_level_none_as_none(self):
        series = pd.Series([{"a": 1}, None, {"b": 2}])
        result = serialize_json_column(series)
        assert result[0] == '{"a": 1}'
        assert result[1] is None
        assert result[2] == '{"b": 2}'

    def test_preserves_top_level_nan_as_none(self):
        series = pd.Series([{"a": 1}, np.nan, {"b": 2}])
        result = serialize_json_column(series)
        assert result[0] == '{"a": 1}'
        assert result[1] is None
        assert result[2] == '{"b": 2}'

    def test_nested_none_becomes_json_null(self):
        series = pd.Series([{"a": None}])
        result = serialize_json_column(series)
        assert result[0] == '{"a": null}'

    def test_nested_structures_serialized_correctly(self):
        series = pd.Series([{"outer": {"inner": [1, 2, 3]}}])
        result = serialize_json_column(series)
        parsed = json.loads(result[0])
        assert parsed == {"outer": {"inner": [1, 2, 3]}}


class TestPrepareJsonColumns:
    """Tests for prepare_json_columns function"""

    def test_detects_and_serializes_json_column(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "data": [{"a": 1}, {"b": 2}, {"c": 3}],
        })
        result_df, json_cols = prepare_json_columns(df)

        assert json_cols == ["data"]
        assert result_df["data"].tolist() == ['{"a": 1}', '{"b": 2}', '{"c": 3}']
        assert result_df["id"].tolist() == [1, 2, 3]

    def test_leaves_non_json_columns_unchanged(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        })
        result_df, json_cols = prepare_json_columns(df)

        assert json_cols == []
        assert result_df["id"].tolist() == [1, 2, 3]
        assert result_df["name"].tolist() == ["a", "b", "c"]

    def test_handles_multiple_json_columns(self):
        df = pd.DataFrame({
            "id": [1, 2],
            "payload": [{"x": 1}, {"x": 2}],
            "metadata": [["tag1"], ["tag2"]],
        })
        result_df, json_cols = prepare_json_columns(df)

        assert set(json_cols) == {"payload", "metadata"}

    def test_does_not_modify_original_dataframe(self):
        df = pd.DataFrame({
            "data": [{"a": 1}, {"b": 2}],
        })
        original_values = df["data"].tolist()

        prepare_json_columns(df)

        assert df["data"].tolist() == original_values

    def test_handles_empty_dataframe(self):
        df = pd.DataFrame()
        result_df, json_cols = prepare_json_columns(df)

        assert json_cols == []
        assert len(result_df) == 0

    def test_preserves_nulls_in_json_columns(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "data": [{"a": 1}, None, {"c": 3}],
        })
        result_df, json_cols = prepare_json_columns(df)

        assert json_cols == ["data"]
        assert result_df["data"][0] == '{"a": 1}'
        assert result_df["data"][1] is None
        assert result_df["data"][2] == '{"c": 3}'
