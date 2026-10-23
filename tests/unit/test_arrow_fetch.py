"""Unit tests for Arrow fetch options and the native fallback."""

import warnings
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from snowlib.primitives.result import QueryResult
from snowlib.utils import connector

pa = pytest.importorskip("pyarrow")


def cursor() -> Mock:
    c = Mock()
    c.description = [("ID",), ("TS",)]
    c.fetchall.return_value = [(1, "x"), (2, "y")]
    return c


class TestConnectorVersion:
    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            ("3.17.0", (3, 17)),
            ("4.8.0", (4, 8)),
            ("5.1.0", (5, 1)),
            ("4.8.1rc1", (4, 8)),
        ],
    )
    def test_parses(self, version, expected):
        with patch("snowflake.connector.__version__", version):
            assert connector.connector_version() == expected


class TestMicrosecondPrecision:
    @pytest.mark.parametrize(
        ("version", "passed"),
        [((3, 17), False), ((4, 2), False), ((4, 3), True), ((5, 1), True)],
    )
    def test_to_df(self, version, passed):
        c = cursor()
        c.fetch_pandas_all.return_value = pd.DataFrame({"ID": [1]})
        with patch("snowlib.primitives.result.connector_version", return_value=version):
            QueryResult(_cursor=c).to_df()
        kwargs = c.fetch_pandas_all.call_args.kwargs
        assert ("force_microsecond_precision" in kwargs) is passed
        if passed:
            assert kwargs["force_microsecond_precision"] is True

    @pytest.mark.parametrize(("version", "passed"), [((4, 2), False), ((4, 8), True)])
    def test_fetch_batches(self, version, passed):
        c = cursor()
        c.fetch_pandas_batches.return_value = iter([pd.DataFrame({"ID": [1]})])
        with patch("snowlib.primitives.result.connector_version", return_value=version):
            list(QueryResult(_cursor=c).fetch_batches())
        kwargs = c.fetch_pandas_batches.call_args.kwargs
        assert ("force_microsecond_precision" in kwargs) is passed


class TestBatchFallback:
    def test_failure_after_a_batch_raises(self):
        """A partly read cursor must not be refetched: rows would repeat or vanish."""

        def batches(**kwargs):
            yield pd.DataFrame({"ID": [1]})
            raise pa.ArrowInvalid("Schema at index 1 was different")

        c = cursor()
        c.fetch_pandas_batches.side_effect = batches
        got = []
        with pytest.raises(RuntimeError, match="after 1 batch") as excinfo:
            for df in QueryResult(_cursor=c).fetch_batches():
                got.append(df)
        assert len(got) == 1
        assert isinstance(excinfo.value.__cause__, pa.ArrowInvalid)
        c.fetchall.assert_not_called()

    def test_failure_before_any_batch_falls_back(self):
        c = cursor()
        c.fetch_pandas_batches.side_effect = pa.ArrowInvalid("Schema mismatch")
        with pytest.warns(UserWarning, match="Falling back to native fetch"):
            got = list(QueryResult(_cursor=c).fetch_batches())
        assert len(got) == 1
        assert got[0]["id"].tolist() == [1, 2]

    def test_not_supported_before_any_batch_falls_back_quietly(self):
        class NotSupportedError(Exception):
            pass

        c = cursor()
        c.fetch_pandas_batches.side_effect = NotSupportedError("DDL")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            got = list(QueryResult(_cursor=c).fetch_batches())
        assert got[0]["id"].tolist() == [1, 2]
