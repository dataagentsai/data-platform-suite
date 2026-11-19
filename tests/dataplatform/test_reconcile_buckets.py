"""Live source against silver by buckets (DPC-0052, REC-05, B9): table-driven, no engine."""

from __future__ import annotations

import pytest

from dataplatform.reconcile import bucket_of, bucket_totals, compare_buckets

ROWS = [{"order_id": f"SO{i}", "line_no": i % 3, "net_value": float(i)} for i in range(40)]
KEYS = ("order_id", "line_no")
MEASURES = {"lines": None, "net_value": "net_value"}


def test_bucket_is_stable_and_in_range() -> None:
    assert bucket_of(("SO1", 1), 16) == bucket_of(("SO1", 1), 16)
    assert all(0 <= bucket_of((f"k{i}",), 8) < 8 for i in range(100))


def test_totals_add_up() -> None:
    t = bucket_totals(ROWS, KEYS, MEASURES, 4)
    assert sum(b["lines"] for b in t.values()) == 40
    assert sum(b["net_value"] for b in t.values()) == sum(r["net_value"] for r in ROWS)


def _drop(rows, key):
    return [r for r in rows if r["order_id"] != key]


def _change(rows, key, value):
    return [{**r, "net_value": value} if r["order_id"] == key else r for r in rows]


CASES = [
    ("equal sides: nothing found", ROWS, {}, 0, set()),
    ("a row missing in silver: its bucket, both measures", _drop(ROWS, "SO7"), {}, 2, {"lines", "net_value"}),
    ("a changed value: its bucket, the value only", _change(ROWS, "SO7", 99.0), {}, 1, {"net_value"}),
    ("within tolerance: nothing found", _change(ROWS, "SO7", 7.4), {"net_value": 0.5}, 0, set()),
]


@pytest.mark.parametrize(("case", "silver", "tol", "n", "measures"), CASES, ids=[c[0] for c in CASES])
def test_compare_names_the_bucket(case, silver, tol, n, measures) -> None:
    found = compare_buckets(bucket_totals(ROWS, KEYS, MEASURES), bucket_totals(silver, KEYS, MEASURES), tol)
    assert len(found) == n
    assert {m for _, m, _, _ in found} == measures
    if found:
        assert {b for b, *_ in found} == {bucket_of(("SO7", 7 % 3), 16)}
