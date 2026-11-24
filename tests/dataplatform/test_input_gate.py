"""The input gate for a silver we do not build (gold/input_gate.py). Each row of a table is one way a provider's table can be
wrong, or one way it can be right; the gate must name the finding, and say pass, warn or fail.

Rules proven here (DAC): GATE-02 (a contract is checked on every read), VOID-01 (a bad input does not publish).
"""

from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from silver_helpers import ts

from dataplatform.gold.input_gate import InputContract, check

pytestmark = pytest.mark.usefixtures("spark")

NOW = datetime(2026, 11, 9, 2, 0)  # two hours after the helper's time zero (silver_helpers.T0)
DDL = "id string, amount double, vf timestamp, vt timestamp"
BASE = InputContract("erp.sales_order", (("id", "string"), ("amount", "number"), ("vf", "timestamp"), ("vt", "timestamp")),
                     keys=("id",), valid_from="vf", valid_to="vt", fresh_column="vf", max_age=timedelta(days=2))


def df_of(spark, rows, ddl=DDL):
    return spark.createDataFrame(rows, ddl)


def codes(result):
    return sorted(f.code for f in result.findings)


GOOD = [("a", 1.0, ts(0), ts(60)), ("a", 2.0, ts(60), None), ("b", 5.0, ts(0), None)]
WITH = lambda **kw: InputContract(**{**BASE.__dict__, **kw})  # noqa: E731


@pytest.mark.parametrize(
    ("case", "rows", "contract", "ddl", "last_rows", "status", "want"),
    [
        ("a sound history passes", GOOD, BASE, DDL, None, "pass", []),
        ("a version with no start", GOOD + [("c", 1.0, None, None)], BASE, DDL, None, "fail", ["VERSION-NO-START"]),
        ("a version that ends before it starts", GOOD + [("c", 1.0, ts(10), ts(5))], BASE, DDL, None, "fail", ["VERSION-ENDS-BEFORE-START"]),
        ("versions that overlap", [("a", 1.0, ts(0), ts(90)), ("a", 2.0, ts(60), None)], BASE, DDL, None, "fail", ["VERSION-OVERLAP"]),
        ("a gap between versions", [("a", 1.0, ts(0), ts(30)), ("a", 2.0, ts(60), None)], BASE, DDL, None, "fail", ["VERSION-GAP"]),
        ("a gap the contract allows", [("a", 1.0, ts(0), ts(30)), ("a", 2.0, ts(60), None)], WITH(gaps_allowed=True), DDL, None, "pass", []),
        ("two current versions of a key", [("a", 1.0, ts(0), None), ("a", 2.0, ts(10), None)], BASE, DDL, None, "fail", ["KEY-DUPLICATE", "VERSION-OVERLAP"]),
        ("an empty key", GOOD + [(None, 1.0, ts(0), None)], BASE, DDL, None, "fail", ["KEY-EMPTY"]),
        ("a column the contract needs is gone", [("a", ts(0), None)], BASE, "id string, vf timestamp, vt timestamp", None, "fail", ["COLUMN-MISSING"]),
        ("a column changed type", [("a", "x", ts(0), None)], BASE, "id string, amount string, vf timestamp, vt timestamp", None, "fail", ["TYPE-CHANGED"]),
        ("an extra column is fine", [("a", 1.0, ts(0), None, "x")], BASE, DDL + ", note string", None, "pass", []),
        ("too old", [("a", 1.0, ts(-60 * 24 * 10), None)], BASE, DDL, None, "fail", ["STALE"]),
        ("too old, but the dataset may publish through it", [("a", 1.0, ts(-60 * 24 * 10), None)],
         WITH(warn_only=frozenset({"STALE"})), DDL, None, "warn", ["STALE"]),
        ("the table shrank", GOOD, BASE, DDL, 10, "fail", ["VOLUME-DROP"]),
        ("a small drop is within the tolerance", GOOD, BASE, DDL, 3, "pass", []),
        ("no history columns in the contract", [("a", 1.0, ts(0), ts(60))],
         WITH(valid_from="", valid_to="", fresh_column="", max_age=None), DDL, None, "pass", []),
    ],
)
def test_the_gate(spark, case, rows, contract, ddl, last_rows, status, want):
    got = check(df_of(spark, rows, ddl), contract, now=NOW + timedelta(minutes=10), last_rows=last_rows)
    assert got.status == status, (case, got)
    assert codes(got) == sorted(want), (case, got.findings)


def test_a_missing_column_does_not_hide_behind_other_checks(spark):
    """With a column gone the gate reports that and stops: checking keys or history on it would only add noise."""
    got = check(df_of(spark, [("a", ts(0), None)], "id string, vf timestamp, vt timestamp"), BASE, now=NOW)
    assert codes(got) == ["COLUMN-MISSING"]


def test_the_gate_never_changes_the_table(spark):
    df = df_of(spark, GOOD)
    before = sorted(map(tuple, df.collect()), key=str)
    check(df, BASE, now=NOW)
    assert sorted(map(tuple, df.collect()), key=str) == before
