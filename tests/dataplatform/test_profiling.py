"""Profiling and drift (X5, DPC-0051, B9): table-driven, no engine."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dataplatform.profiling import drift, merge, profile_rows

COLS = [("id", "string"), ("qty", "integer"), ("at", "timestamp")]
AT = datetime(2026, 11, 9, 10, tzinfo=UTC)


def test_profile_counts_and_ranges() -> None:
    p = profile_rows([{"id": "a", "qty": 3, "at": AT}, {"id": "b", "qty": None, "at": AT},
                      {"id": "", "qty": 7, "at": AT}], COLS)
    assert p["rows"] == 3
    assert p["columns"]["id"] == {"nulls": 1, "distinct": 2}  # text: counts only, never a value
    assert p["columns"]["qty"] == {"nulls": 1, "distinct": 2, "min": "3", "max": "7"}
    assert p["columns"]["at"]["min"] == AT.isoformat()


def test_merge_adds_and_widens() -> None:
    a = profile_rows([{"id": "a", "qty": 3, "at": AT}], COLS)
    b = profile_rows([{"id": "b", "qty": None, "at": AT}, {"id": "c", "qty": 9, "at": AT}], COLS)
    m = merge([a, b])
    assert m["rows"] == 3
    assert m["columns"]["qty"] == {"nulls": 1, "distinct": 1, "min": "3", "max": "9"}


def _p(rows: int, **nulls: int) -> dict[str, Any]:
    return {"rows": rows, "columns": {c: {"nulls": n, "distinct": 1} for c, n in nulls.items()}}


DRIFT_CASES = [
    ("no earlier profile: nothing to compare", _p(100, qty=0), None, []),
    ("steady: nothing found", _p(100, qty=2), _p(110, qty=3), []),
    ("a column goes mostly empty", _p(100, qty=60), _p(100, qty=5), ["qty empty in 60% of rows, was 5%"]),
    ("volume collapses", _p(10, qty=0), _p(100, qty=0), ["volume moved from 100 to 10 rows"]),
    ("volume triples", _p(300, qty=0), _p(100, qty=0), ["volume moved from 100 to 300 rows"]),
    ("a new column", _p(100, qty=0, colour=0), _p(100, qty=0), ["colour is new"]),
    ("a column stops coming", _p(100, qty=0), _p(100, qty=0, colour=0), ["colour no longer delivered"]),
]


@pytest.mark.parametrize(("case", "now", "before", "want"), DRIFT_CASES, ids=[c[0] for c in DRIFT_CASES])
def test_drift(case: str, now: dict[str, Any], before: dict[str, Any] | None, want: list[str]) -> None:
    assert drift(now, before) == want
