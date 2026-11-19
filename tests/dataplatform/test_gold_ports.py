"""Port contract tables (M5, ADR-020) for gold's edges: GoldTables (the read and write edges) and GoldReader, run
against every adapter bound to them (today the Delta folders on local Spark). An adapter is bound only when it passes
every row."""

import pytest

pytest.importorskip("pyspark")
from gold_helpers import d

from dataplatform.gold.plan import plan_from_manifest

COLS = [("id", "string"), ("day", "date"), ("hour", "timestamp"), ("qty", "bigint"), ("_is_complete", "boolean"),
        ("_run_id", "string")]
SCHEMA = "id string, day date, hour timestamp, qty bigint, _is_complete boolean, _run_id string"


def fact(column="day", unit="day"):
    return plan_from_manifest({"kind": "gold_fact", "name": "fact_t", "domain": "test", "grain": ["id"], "inputs": [],
                               "window": {"column": column, "unit": unit}, "build": {"kind": "sql", "ref": "x"}}, COLS)


DIM = plan_from_manifest({"kind": "gold_dimension", "name": "dim_t", "domain": "test", "grain": ["t_sk"],
                          "inputs": [{"dataset": "a.b"}]}, [("t_sk", "bigint"), ("code", "string")], {"a.b": ["code"]})


def adapters(spark, tmp):
    from dataplatform.adapters.spark_gold import DeltaGoldTables, SparkGoldReader

    return {"delta folders": (DeltaGoldTables(spark, tmp / "silver", tmp / "gold"),
                              SparkGoldReader(spark, tmp / "gold"))}


ADAPTERS = ["delta folders"]


def rows(spark, *items):
    from datetime import UTC, datetime

    return spark.createDataFrame([(i, d(day), datetime(2026, 11, day, h, tzinfo=UTC), q, True, "r")
                                  for i, day, h, q in items], SCHEMA)


def table(t, p):
    return sorted((r["id"], r["qty"]) for r in t.gold(p).collect())


def case_first_write_creates(spark, t):
    t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1), ("b", 11, 1, 2)), ["2026-11-10", "2026-11-11"])
    return table(t, fact()) == [("a", 1), ("b", 2)]


def case_only_named_windows_change(spark, t):
    t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1), ("b", 11, 1, 2)), ["2026-11-10", "2026-11-11"])
    t.replace_windows(fact(), rows(spark, ("c", 11, 1, 3)), ["2026-11-11"])
    return table(t, fact()) == [("a", 1), ("c", 3)]


def case_a_named_window_without_rows_is_emptied(spark, t):
    t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1), ("b", 11, 1, 2)), ["2026-11-10", "2026-11-11"])
    t.replace_windows(fact(), rows(spark), ["2026-11-11"])
    return table(t, fact()) == [("a", 1)]


def case_same_rows_twice_change_nothing(spark, t):
    for _ in range(2):
        t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1), ("b", 10, 2, 2)), ["2026-11-10"])
    return table(t, fact()) == [("a", 1), ("b", 2)]


def case_hour_windows_by_instant(spark, t):
    p = fact("hour", "hour")
    t.replace_windows(p, rows(spark, ("a", 10, 1, 1), ("b", 10, 2, 2)), ["2026-11-10T01:00:00Z", "2026-11-10T02:00:00Z"])
    t.replace_windows(p, rows(spark, ("c", 10, 2, 3)), ["2026-11-10T02:00:00Z"])
    return table(t, p) == [("a", 1), ("c", 3)]


def case_refuses_what_is_not_a_window(spark, t):
    try:
        t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1)), ["2026-11-10' OR 1=1 --"])
    except ValueError:
        return t.gold(fact()) is None
    return False


def case_dimension_is_replaced_whole(spark, t):
    t.write_dimension(DIM, spark.createDataFrame([(1, "a"), (2, "b")], "t_sk bigint, code string"))
    t.write_dimension(DIM, spark.createDataFrame([(1, "a")], "t_sk bigint, code string"))
    return [r["code"] for r in t.gold(DIM).collect()] == ["a"]


def case_unwritten_silver_is_empty_with_its_columns(spark, t):
    df = t.silver("a.never", [("id", "string"), ("qty", "bigint")])
    return df.count() == 0 and {"id", "qty", "_is_current", "_is_deleted", "_valid_from", "_valid_to"} <= set(df.columns)


CASES = {
    "the first replace creates the table": case_first_write_creates,
    "only the named windows change": case_only_named_windows_change,
    "a named window without rows is emptied": case_a_named_window_without_rows_is_emptied,
    "the same rows twice change nothing (DUP-02)": case_same_rows_twice_change_nothing,
    "hour windows are replaced by instant": case_hour_windows_by_instant,
    "a name that is not a window is refused before any write": case_refuses_what_is_not_a_window,
    "a dimension is replaced whole": case_dimension_is_replaced_whole,
    "silver never written reads as empty, with its columns": case_unwritten_silver_is_empty_with_its_columns,
}


@pytest.mark.parametrize("adapter", ADAPTERS)
@pytest.mark.parametrize("case", CASES)
def test_gold_tables_contract(spark, tmp_path, adapter, case):
    t, _ = adapters(spark, tmp_path)[adapter]
    assert CASES[case](spark, t)


@pytest.mark.parametrize("adapter", ADAPTERS)
@pytest.mark.parametrize(
    ("case", "time_zone", "want"),
    [("in the engine's zone (UTC)", None, {"2026-11-10": 2}),
     ("in the business zone a late UTC hour is the next day", "Asia/Kolkata", {"2026-11-10": 1, "2026-11-11": 1})],
)
def test_gold_reader_contract(spark, tmp_path, adapter, case, time_zone, want):
    t, reader = adapters(spark, tmp_path)[adapter]
    t.replace_windows(fact(), rows(spark, ("a", 10, 1, 1), ("b", 10, 20, 2)), ["2026-11-10"])
    got = reader.rows("SELECT date_format(DATE_TRUNC('DAY', hour), 'yyyy-MM-dd') AS d, COUNT(1) AS n FROM fact_t "
                      "GROUP BY 1", time_zone=time_zone)
    assert {r["d"]: r["n"] for r in got} == want
