"""Gold on local Spark (W1d): each fact's build() as a table of cases (D18), and the harness's engine operations
(the common one-to-one pipeline, keys as of the event, corrections, window digests). Table-driven; one shared Spark
(conftest.py)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from gold_helpers import FOREVER, T0, d, silver, ts
from pyspark.sql import functions as F

from dataplatform.gold import keys as K
from dataplatform.gold.plan import plan_from_manifest
from dataplatform.meta.port import Correction

MODELS = Path(__file__).resolve().parents[2] / "models/tyre"
TZ = "Asia/Kolkata"
SOL = [("order_id", "string"), ("line_no", "bigint"), ("dealer_code", "string"), ("material_code", "string"),
       ("order_ts", "timestamp"), ("promised_date", "date"), ("ordered_qty", "bigint"), ("net_value", "double"),
       ("line_status", "string")]
DLV = [("order_id", "string"), ("line_no", "bigint"), ("delivered_qty", "bigint"), ("delivered_ts", "timestamp")]
SELL = [("dealer_code", "string"), ("sale_month", "date"), ("material_code", "string"), ("units_sold", "bigint")]
PROD = [("production_id", "string"), ("press_id", "string"), ("material_code", "string"), ("cured_ts", "timestamp"),
        ("tyres_cured", "bigint")]
INSP = [("inspection_id", "string"), ("production_id", "string"), ("tyres_scrapped", "bigint"), ("result", "string")]


@pytest.fixture(scope="module")
def engine(spark, engine_name):
    """The gold engine adapter of the run's engine (conftest.py --engine): the same cases on Spark and DuckDB."""
    if engine_name == "duckdb":
        from dataplatform.adapters.duckdb_gold import DuckGoldEngine

        return DuckGoldEngine(spark, MODELS)
    from dataplatform.adapters.spark_gold import SparkGoldEngine

    return SparkGoldEngine(spark, MODELS)


def plan(name, kind, ref, window, unit):
    return plan_from_manifest({"kind": "gold_fact", "name": name, "domain": "sales", "grain": [],
                               "inputs": [], "window": {"column": window, "unit": unit},
                               "build": {"kind": kind, "ref": ref}}, [(window, "date")])


def line(o, promised, qty=10, status="open", order_ts=None, **kw):
    return {"order_id": o, "line_no": 10, "dealer_code": "D-1", "material_code": "M-1",
            "order_ts": order_ts or ts(1), "promised_date": promised, "ordered_qty": qty, "net_value": 100.0,
            "line_status": status, **kw}


def dlv(o, qty, at):
    return {"order_id": o, "line_no": 10, "delivered_qty": qty, "delivered_ts": at}


# ---------------------------------------------------------------------------------------------------- fact_otif
OTIF_CASES = {
    "on time and in full": ([line("A", d(12))], [dlv("A", 10, ts(30))], {"A": (d(12), 10, True, True, 1)}),
    "late by a day": ([line("A", d(12))], [dlv("A", 10, ts(100))], {"A": (d(12), 10, False, True, 0)}),
    "counted on the local date (19:00 UTC is the next day in India)":
        ([line("A", d(12))], [dlv("A", 10, datetime(2026, 11, 12, 19, 0, tzinfo=UTC))],
         {"A": (d(12), 10, False, True, 0)}),
    "partial delivery is not in full": ([line("A", d(12))], [dlv("A", 6, ts(30))], {"A": (d(12), 6, True, False, 0)}),
    "two deliveries make it in full": ([line("A", d(12))], [dlv("A", 6, ts(30)), dlv("A", 4, ts(40))],
                                       {"A": (d(12), 10, True, True, 1)}),
    "not delivered yet": ([line("A", d(12))], [], {"A": (d(12), 0, False, False, 0)}),
    "a cancelled line is left out": ([line("A", d(12), status="cancelled")], [dlv("A", 10, ts(30))], {}),
    "a deleted line is left out": ([line("A", d(12), _is_deleted=True)], [], {}),
    "the date first promised, not the rescheduled one": (
        [line("A", d(12), _seq_from=1, _is_current=False, _valid_to=ts(5)), line("A", d(14), _seq_from=2,
                                                                                 _valid_from=ts(5))],
        [dlv("A", 10, ts(100))], {"A": (d(12), 10, False, True, 0)}),
    "a late change (zero-length version) does not move the first promise": (
        [line("A", d(12), _seq_from=1, _is_current=False, _valid_to=ts(5)),
         line("A", d(13), _seq_from=3, _valid_from=ts(5)),
         line("A", d(14), _seq_from=2, _is_current=False, _valid_from=ts(5), _valid_to=ts(5))],
        [dlv("A", 10, ts(30))], {"A": (d(12), 10, True, True, 1)}),
    "a deleted delivery does not count": ([line("A", d(12))], [{**dlv("A", 10, ts(30)), "_is_deleted": True}],
                                          {"A": (d(12), 0, False, False, 0)}),
}


@pytest.mark.parametrize("case", OTIF_CASES)
def test_fact_otif_build(spark, engine, case):
    lines, deliveries, want = OTIF_CASES[case]
    p = plan("fact_otif", "sql", "gold/sales/fact_otif.sql", "promised_date", "day")
    out = engine.build(p, {"erp__sales_order_line": silver(spark, SOL, lines),
                           "erp__delivery": silver(spark, DLV, deliveries)}, {"business_tz": TZ})
    got = {r["order_id"]: (r["promised_date"], r["delivered_qty"], r["on_time"], r["in_full"], r["on_time_in_full"])
           for r in out.collect()}
    assert got == want


# ---------------------------------------------------------------------------------------------------- fact_sales
def sell(dealer, month, material, units, **kw):
    return {"dealer_code": dealer, "sale_month": month, "material_code": material, "units_sold": units, **kw}


OCT, NOV = d(1).replace(month=10), d(1)
SALES_CASES = {
    "sell-in by the local month (31 Oct 19:00 UTC is November in India)": (
        [line("A", d(5), qty=7, order_ts=datetime(2026, 10, 31, 19, 0, tzinfo=UTC))], [],
        {("D-1", "M-1", NOV): (7, 0, 100.0)}),
    "a cancelled line is not sold in": ([line("A", d(12), status="cancelled")], [], {}),
    "sell-out from the current file only": (
        [], [sell("D-2", OCT, "M-2", 50, _is_current=False), sell("D-2", OCT, "M-2", 60)],
        {("D-2", "M-2", OCT): (0, 60, 0.0)}),
    "both sides meet on one row": (
        [line("A", d(5), qty=7, order_ts=ts(30))], [sell("D-1", NOV, "M-1", 3)], {("D-1", "M-1", NOV): (7, 3, 100.0)}),
    "lines add up per dealer, material and month": (
        [line("A", d(5), qty=7), line("B", d(6), qty=5)], [], {("D-1", "M-1", NOV): (12, 0, 200.0)}),
}


@pytest.mark.parametrize("case", SALES_CASES)
def test_fact_sales_build(spark, engine, case):
    lines, files, want = SALES_CASES[case]
    p = plan("fact_sales", "sql", "gold/sales/fact_sales.sql", "sale_month", "month")
    out = engine.build(p, {"erp__sales_order_line": silver(spark, SOL, lines),
                           "partner__dealer_sellout": silver(spark, SELL, files)}, {"business_tz": TZ})
    got = {(r["dealer_code"], r["material_code"], r["sale_month"]): (r["units_sold_in"], r["units_sold_out"],
                                                                     r["net_value"]) for r in out.collect()}
    assert got == want


# ---------------------------------------------------------------------------------------------------- fact_scrap
def prod(pid, press, at, cured, **kw):
    return {"production_id": pid, "press_id": press, "material_code": "M-1", "cured_ts": at, "tyres_cured": cured,
            **kw}


def insp(pid, scrapped, result="scrap"):
    return {"inspection_id": f"IN-{pid}", "production_id": pid, "tyres_scrapped": scrapped, "result": result}


H10 = datetime(2026, 11, 12, 10, 0, tzinfo=UTC)
SCRAP_CASES = {
    "an hour with its inspection": ([prod("P1", "X", H10.replace(minute=55), 30)], [insp("P1", 2)],
                                    {("X", H10): (30, 2, True)}),
    "the inspection has not arrived": ([prod("P1", "X", H10.replace(minute=55), 30)], [], {("X", H10): (30, 0, False)}),
    "rework is not scrap": ([prod("P1", "X", H10.replace(minute=50), 30)], [insp("P1", 0, "rework")],
                            {("X", H10): (30, 0, True)}),
    "two presses, one hour": ([prod("P1", "X", H10, 30), prod("P2", "Y", H10, 20)], [insp("P1", 1), insp("P2", 3)],
                              {("X", H10): (30, 1, True), ("Y", H10): (20, 3, True)}),
    "a deleted production is left out": ([prod("P1", "X", H10, 30, _is_deleted=True)], [insp("P1", 1)], {}),
    "one hour waits if one of its rows waits": (
        [prod("P1", "X", H10, 30), prod("P2", "X", H10.replace(minute=40), 20)], [insp("P1", 1)],
        {("X", H10): (50, 1, False)}),
}


@pytest.mark.parametrize("case", SCRAP_CASES)
def test_fact_scrap_build(spark, engine, case):
    productions, inspections, want = SCRAP_CASES[case]
    p = plan("fact_scrap", "dataframe", "gold.manufacturing.scrap:build", "cure_hour", "hour")
    out = engine.build(p, {"mes__production": silver(spark, PROD, productions),
                           "lims__inspection_result": silver(spark, INSP, inspections)}, {"business_tz": TZ})
    rows = out.select("press_id", F.date_format("cure_hour", "yyyy-MM-dd HH:mm").alias("h"), "tyres_cured",
                      "tyres_scrapped", "_input_complete").collect()
    got = {(r["press_id"], r["h"]): (r["tyres_cured"], r["tyres_scrapped"], r["_input_complete"]) for r in rows}
    assert got == {(k[0], k[1].strftime("%Y-%m-%d %H:%M")): v for k, v in want.items()}


# ---------------------------------------------------------------------------------------------------- the common pipeline
DIM = plan_from_manifest({"kind": "gold_dimension", "name": "dim_dealer", "domain": "sales", "grain": ["dealer_sk"],
                          "inputs": [{"dataset": "dealer_api.dealer"}]},
                         [("dealer_sk", "bigint"), ("dealer_code", "string"), ("region", "string"),
                          ("valid_from", "timestamp"), ("valid_to", "timestamp"), ("is_current", "boolean")],
                         {"dealer_api.dealer": ["dealer_code"]})
DEALERS = [("dealer_code", "string"), ("region", "string")]
VERSIONS = [{"dealer_code": "D-1", "region": "north", "_valid_from": T0, "_valid_to": ts(48), "_is_current": False},
            {"dealer_code": "D-1", "region": "south", "_valid_from": ts(48), "_valid_to": FOREVER},
            {"dealer_code": "D-2", "region": "west", "_valid_from": ts(10), "_valid_to": FOREVER}]


@pytest.fixture(scope="module")
def dim(spark, engine):
    return engine.hold(engine.dimension(DIM, silver(spark, DEALERS, VERSIONS), "run-1"))


@pytest.mark.parametrize(
    ("case", "code", "start", "region", "valid_from", "valid_to"),
    [
        ("the first version reaches back to the beginning", "D-1", T0, "north", "1900-01-01 00:00",
         "2026-11-11 00:00"),
        ("a later version starts at its change", "D-1", ts(48), "south", "2026-11-11 00:00", "9999-12-31 00:00"),
        ("a single version", "D-2", ts(10), "west", "1900-01-01 00:00", "9999-12-31 00:00"),
    ],
)
def test_dimension_keys_and_validity(dim, case, code, start, region, valid_from, valid_to):
    """The key is keys.surrogate_key of (source, business key, version start): the same number in Python and Spark."""
    sk = K.surrogate_key("dealer_api.dealer", (code,), start)
    [row] = dim.where(F.col("dealer_sk") == sk).select(
        "dealer_code", "region", F.date_format("valid_from", "yyyy-MM-dd HH:mm").alias("f"),
        F.date_format("valid_to", "yyyy-MM-dd HH:mm").alias("t")).collect()
    assert (row["dealer_code"], row["region"], row["f"], row["t"]) == (code, region, valid_from, valid_to)


def test_dimension_has_the_reserved_members_and_unique_keys(engine, dim):
    reserved = {r["dealer_sk"]: r["dealer_code"] for r in dim.where("dealer_sk < 0").collect()}
    assert reserved == {K.UNKNOWN: "unknown", K.NOT_APPLICABLE: "not applicable"}
    assert engine.duplicates(dim, ("dealer_sk",)) == 0


def test_a_rebuild_gives_the_same_keys(spark, engine, dim):
    again = engine.dimension(DIM, silver(spark, DEALERS, list(reversed(VERSIONS))), "run-2")
    assert sorted(r[0] for r in dim.select("dealer_sk").collect()) == sorted(
        r[0] for r in again.select("dealer_sk").collect())


FACT = plan_from_manifest({"kind": "gold_fact", "name": "fact_x", "domain": "sales", "grain": ["id"], "inputs": [],
                           "dimensions": [{"name": "dim_dealer", "key": "dealer_sk", "business_key": ["dealer_code"],
                                           "as_of": "t"}],
                           "window": {"column": "day", "unit": "day"}, "build": {"kind": "sql", "ref": "x"}},
                          [("id", "string"), ("dealer_sk", "bigint"), ("dealer_code", "string"), ("day", "date"),
                           ("_is_complete", "boolean"), ("_run_id", "string")])
V1, V2 = K.surrogate_key("dealer_api.dealer", ("D-1",), T0), K.surrogate_key("dealer_api.dealer", ("D-1",), ts(48))


@pytest.mark.parametrize(
    ("case", "dealer", "t", "key"),
    [
        ("an event in the first version", "D-1", ts(1), V1),
        ("an event after the change: the new version (ADR-005)", "D-1", ts(50), V2),
        ("exactly at the change: the new version (half open)", "D-1", ts(48), V2),
        ("before the member's first version: still that member", "D-1", ts(-100), V1),
        ("a member that has not arrived: unknown", "D-9", ts(1), K.UNKNOWN),
        ("no business key: not applicable", None, ts(1), K.NOT_APPLICABLE),
    ],
)
def test_keys_as_of_the_event(spark, engine, dim, case, dealer, t, key):
    facts = spark.createDataFrame([("1", dealer, t, d(10))], "id string, dealer_code string, t timestamp, day date")
    [row] = engine.add_keys(FACT, facts, {"dim_dealer": dim}).collect()
    assert row["dealer_sk"] == key


def test_no_fact_row_is_dropped_or_multiplied(spark, engine, dim):
    """Count in = count out over every kind of row at once (left join + unknown member, ADR-004)."""
    rows = [(str(i), ["D-1", "D-2", "D-9", None][i % 4], ts(i % 97 - 20), d(10)) for i in range(400)]
    facts = spark.createDataFrame(rows, "id string, dealer_code string, t timestamp, day date")
    keyed = engine.add_keys(FACT, facts, {"dim_dealer": dim})
    assert engine.count(keyed) == engine.count(facts) == 400
    assert engine.unknown_rows(FACT, keyed) == {"dealer_sk": 100}


def test_corrections_overlay_only_their_keys(spark, engine):
    frame = silver(spark, SOL, [line("A", d(12)), line("B", d(12))])
    fixed = engine.overlay(frame, [Correction({"order_id": "A", "line_no": 10}, "promised_date", "2026-11-13")],
                           ("order_id", "line_no"))
    assert {r["order_id"]: r["promised_date"] for r in fixed.collect()} == {"A": d(13), "B": d(12)}
    assert engine.count(fixed) == 2


def test_window_digests(spark, engine):
    cols = "id string, dealer_sk bigint, dealer_code string, day date, _is_complete boolean, _run_id string"
    a = spark.createDataFrame([("1", 5, "D", d(10), True, "r1"), ("2", 5, "D", d(11), False, "r1")], cols)
    same = spark.createDataFrame([("2", 5, "D", d(11), False, "r2"), ("1", 5, "D", d(10), True, "r2")], cols)
    other = spark.createDataFrame([("1", 6, "D", d(10), True, "r1"), ("2", 5, "D", d(11), False, "r1")], cols)
    na, ns, no = (engine.window_numbers(FACT, f, "2026-11-01", set()) for f in (a, same, other))
    assert {w: v[1] for w, v in na.items()} == {w: v[1] for w, v in ns.items()}  # order and run id do not count
    assert na["2026-11-10"][1] != no["2026-11-10"][1] and na["2026-11-11"][1] == no["2026-11-11"][1]
    assert (na["2026-11-10"][0].complete, na["2026-11-11"][0].complete) == (True, False)
    assert set(engine.window_numbers(FACT, a, "2026-11-11", set())) == {"2026-11-11"}
    assert set(engine.window_numbers(FACT, a, "￿", {"2026-11-10"})) == {"2026-11-10"}


@pytest.mark.parametrize(
    ("case", "window_done", "input_done", "flag"),
    [("window and row complete", True, True, True), ("the window waits", False, True, False),
     ("only this row waits for its input (one plant's lab file)", True, False, False),
     ("a build that says nothing about its rows", True, None, True)],
)
def test_finish_flags_rows(spark, engine, case, window_done, input_done, flag):
    cols = "id string, dealer_sk bigint, dealer_code string, day date"
    facts = spark.createDataFrame([("1", 5, "D", d(10))], cols)
    if input_done is not None:
        facts = facts.withColumn("_input_complete", F.lit(input_done))
    [row] = engine.finish(FACT, facts, {"2026-11-10": window_done}, "r1").collect()
    assert (row["_is_complete"], row["_run_id"], "_input_complete" in row.asDict()) == (flag, "r1", False)
