"""The ops tables' portable file (obs/ops_export.py): one type per column, and a round trip back to Python values."""

from datetime import UTC, date, datetime

import pytest

from dataplatform.obs.ops_export import column_types, export, restore

T = datetime(2026, 11, 9, 1, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("case", "rows", "expect"),
    [
        ("each kind", [{"a": "x", "b": 3, "c": 1.5, "d": True, "e": T, "f": date(2026, 11, 9)}],
         [("a", "text"), ("b", "integer"), ("c", "decimal"), ("d", "flag"), ("e", "time"), ("f", "date")]),
        ("type from the first non-empty value", [{"a": None}, {"a": 4}], [("a", "integer")]),
        ("never filled is text", [{"a": None}, {"a": None}], [("a", "text")]),
        ("integer widens to decimal", [{"a": 1}, {"a": 2.5}], [("a", "decimal")]),
        ("a flag is not an integer", [{"a": False}], [("a", "flag")]),
        ("columns in first-seen order, late ones too", [{"b": 1}, {"b": 2, "a": "x"}],
         [("b", "integer"), ("a", "text")]),
        ("no rows, no columns", [], []),
    ],
)
def test_column_types(case, rows, expect):
    assert column_types(rows) == expect


@pytest.mark.parametrize(
    ("case", "rows", "expect_rows"),
    [
        ("times and dates come back", [{"at": T, "day": date(2026, 11, 9)}], [(T, date(2026, 11, 9))]),
        ("missing values stay missing", [{"a": 1}, {"a": None}], [(1,), (None,)]),
        ("a column a row lacks is empty", [{"a": 1}, {"b": "x"}], [(1, None), (None, "x")]),
        ("integers in a decimal column become decimals", [{"a": 1}, {"a": 2.5}], [(1.0,), (2.5,)]),
        ("a tuple in a text column is carried as text", [{"a": "x"}, {"a": ("p", "q")}], [("x",), ('["p", "q"]',)]),
    ],
)
def test_round_trip(case, rows, expect_rows):
    _, back = restore(export({"t": rows})["t"])
    assert back == expect_rows


DECLARED = {"t": (("day", "text"), ("n", "decimal"))}


@pytest.mark.parametrize(
    ("case", "rows", "expect_columns"),
    [
        ("an empty table keeps its declared columns", [], [["day", "text"], ["n", "decimal"]]),
        ("declared types win over values", [{"day": "d", "n": 3}], [["day", "text"], ["n", "decimal"]]),
        ("an undeclared column is added after", [{"day": "d", "n": 1.0, "x": True}],
         [["day", "text"], ["n", "decimal"], ["x", "flag"]]),
    ],
)
def test_declared_columns(case, rows, expect_columns):
    assert export({"t": rows}, DECLARED)["t"]["columns"] == expect_columns
