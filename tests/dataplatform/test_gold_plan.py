"""The gold kernel without an engine (W1d): plans from gold manifests (ADR-003, -006 linter rules), hash surrogate keys
(ADR-004), windows, completeness (ADR-018) and restatements (ADR-017). Table-driven; no Spark."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataplatform.gold import keys as K
from dataplatform.gold import windows as W
from dataplatform.gold.plan import GoldPlanError, order, plan_from_manifest

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_slice = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre slice)")
TZ = "Asia/Kolkata"
COLS = [("order_id", "string"), ("dealer_sk", "bigint"), ("dealer_code", "string"), ("day", "date"),
        ("qty", "bigint"), ("_is_complete", "boolean"), ("_run_id", "string")]
FACT = {"kind": "gold_fact", "name": "fact_x", "domain": "sales", "pipeline": "modelled", "grain": ["order_id"],
        "inputs": [{"dataset": "src.order"}],
        "dimensions": [{"name": "dim_d", "key": "dealer_sk", "business_key": ["dealer_code"], "as_of": "day"}],
        "window": {"column": "day", "unit": "day", "lookback": 3}, "build": {"kind": "sql", "ref": "x.sql"},
        "checks": {"grain_unique": True, "orphan_rate_max": 0.01,
                   "reconcile": {"measures": [{"name": "qty_total", "column": "qty"}], "tolerance": {"qty_total": 0}}}}
DIM = {"kind": "gold_dimension", "name": "dim_d", "domain": "sales", "pipeline": "one_to_one", "grain": ["dealer_sk"],
       "inputs": [{"dataset": "src.dealer"}], "history": "scd2"}
DIM_COLS = [("dealer_sk", "bigint"), ("dealer_code", "string"), ("region", "string")]


def with_(doc, **kw):
    return {**doc, **kw}


@pytest.mark.parametrize(
    ("case", "doc", "cols", "problem"),
    [
        ("one-to-one with a build", with_(DIM, build={"kind": "sql", "ref": "d.sql"}), DIM_COLS, "declares no build"),
        ("modelled without a build", with_(FACT, build={}), COLS, "needs a build reference"),
        ("unknown build kind", with_(FACT, build={"kind": "notebook", "ref": "n"}), COLS, "build kind"),
        ("modelled without a window", with_(FACT, window=None), COLS, "needs a window"),
        ("window unit unknown", with_(FACT, window={"column": "day", "unit": "week"}), COLS, "window unit"),
        ("window column not in contract", with_(FACT, window={"column": "dt", "unit": "day"}), COLS, "window column"),
        ("dimension key not in contract", with_(FACT, dimensions=[{"name": "dim_d", "key": "d_sk"}]), COLS,
         "dimension keys"),
        ("materialised view with two inputs", with_(FACT, materialize="materialized_view",
                                                    inputs=[{"dataset": "a.b"}, {"dataset": "c.d"}]), COLS,
         "materialised view"),
        ("unknown kind", with_(FACT, kind="gold_view"), COLS, "kind"),
        ("on_incomplete unknown", with_(FACT, completeness={"on_incomplete": "drop"}), COLS, "on_incomplete"),
        ("dimension key missing in its input", DIM, DIM_COLS, "primary key"),
    ],
)
def test_the_linter_refuses(case, doc, cols, problem):
    with pytest.raises(GoldPlanError, match=problem):
        plan_from_manifest(doc, cols, {})


def test_a_fact_plan():
    p = plan_from_manifest(FACT, COLS)
    assert (p.table, p.pipeline, p.window.unit, p.measure_columns, p.reconcile.measures) == (
        "gold_sales.fact_x", "modelled", "day", ("qty",), (("qty_total", "qty"),))
    assert p.dimensions[0].as_of == "day" and p.orphan_rate_max == 0.01


def test_a_dimension_takes_its_inputs_key():
    p = plan_from_manifest(DIM, DIM_COLS, {"src.dealer": ["dealer_code"]})
    assert (p.surrogate, p.business_key) == ("dealer_sk", ("dealer_code",))


def test_order_puts_dimensions_first_and_refuses_unknown_ones():
    f, d = plan_from_manifest(FACT, COLS), plan_from_manifest(DIM, DIM_COLS, {"src.dealer": ["dealer_code"]})
    assert [p.name for p in order([f, d])] == ["dim_d", "fact_x"]
    with pytest.raises(GoldPlanError, match="no gold manifest"):
        order([f])


@needs_slice
def test_every_tyre_gold_manifest_is_a_plan():
    import yaml

    from dataplatform import contract as odcs

    keys = {"dealer_api.dealer": ["dealer_code"], "erp.material": ["material_code"], "mes.press": ["press_id"]}
    plans = []
    for path in sorted((SLICE / "manifest/gold").glob("*.yaml")):
        m = yaml.safe_load(path.read_text())
        c = yaml.safe_load((path.parent / m["contract"]).read_text())
        plans.append(plan_from_manifest(m, odcs.columns(c), keys))
    by = {p.name: p for p in order(plans)}
    assert [p.kind for p in order(plans)][:3] == ["gold_dimension"] * 3
    assert (by["fact_otif"].build_kind, by["fact_sales"].window.unit, by["fact_scrap"].build_kind) == (
        "sql", "month", "dataframe")
    assert by["fact_otif"].corrections and not by["fact_scrap"].corrections


def test_the_gold_kernel_names_no_product():
    """R1: the kernel (dataplatform/gold) imports no engine and names no product."""
    root = Path(__file__).resolve().parents[2] / "src/dataplatform/gold"
    for f in root.glob("*.py"):
        words = set(__import__("re").findall(r"[a-z_]+", f.read_text().lower()))
        assert not words & {"pyspark", "databricks", "delta", "spark", "deltalake", "unity"}, f.name


# ---------------------------------------------------------------------------------------------------- keys (ADR-004)
T0 = datetime(2026, 11, 9, 10, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("case", "a", "b", "same"),
    [
        ("same inputs, same key", ("erp.material", ("M1",), T0), ("erp.material", ("M1",), T0), True),
        ("another version start", ("erp.material", ("M1",), T0), ("erp.material", ("M1",), T0.replace(hour=11)), False),
        ("another member", ("erp.material", ("M1",), T0), ("erp.material", ("M2",), T0), False),
        ("another source", ("erp.material", ("M1",), T0), ("mes.material", ("M1",), T0), False),
        ("a naive time is UTC", ("x.y", ("1",), T0), ("x.y", ("1",), T0.replace(tzinfo=None)), True),
        ("a zone does not change the instant", ("x.y", ("1",), T0),
         ("x.y", ("1",), T0.astimezone(__import__("zoneinfo").ZoneInfo(TZ))), True),
    ],
)
def test_surrogate_keys_are_deterministic(case, a, b, same):
    assert (K.surrogate_key(*a) == K.surrogate_key(*b)) is same


def test_a_key_is_positive_and_never_a_reserved_member():
    for i in range(500):
        k = K.surrogate_key("x.y", (f"D-{i:04d}",), T0)
        assert 0 < k < 2**60
    assert K.key_text("x.y", ("a", None, 3), T0) == "x.y|a||3|2026-11-09T10:30:00.000000Z"


@pytest.mark.parametrize(
    ("case", "key", "found", "member"),
    [("found", ("D-1",), 77, 77), ("not arrived yet", ("D-1",), None, K.UNKNOWN),
     ("no business key", (None,), None, K.NOT_APPLICABLE), ("empty key", (), 5, K.NOT_APPLICABLE)],
)
def test_member(case, key, found, member):
    assert K.member(key, found) == member


# ---------------------------------------------------------------------------------------------------- windows
@pytest.mark.parametrize(
    ("case", "fn", "args", "want"),
    [
        ("day start is business midnight", W.start, ("2026-11-12", "day", TZ), "2026-11-11T18:30:00+00:00"),
        ("day end", W.end, ("2026-11-12", "day", TZ), "2026-11-12T18:30:00+00:00"),
        ("month end", W.end, ("2026-10-01", "month", TZ), "2026-10-31T18:30:00+00:00"),
        ("december rolls the year", W.end, ("2026-12-01", "month", TZ), "2026-12-31T18:30:00+00:00"),
        ("hour end", W.end, ("2026-11-12T05:00:00Z", "hour", TZ), "2026-11-12T06:00:00+00:00"),
        ("instant to business day", W.of, (datetime(2026, 11, 12, 19, 0, tzinfo=UTC), "day", TZ), "2026-11-13"),
        ("instant to month", W.of, (datetime(2026, 10, 31, 19, 0, tzinfo=UTC), "month", TZ), "2026-11-01"),
        ("instant to hour", W.of, (datetime(2026, 11, 12, 5, 59, tzinfo=UTC), "hour", TZ), "2026-11-12T05:00:00Z"),
        ("day lookback", W.range_start, ("2026-11-22", "day", 35, TZ), "2026-10-18"),
        ("month lookback", W.range_start, ("2026-11-22", "month", 3, TZ), "2026-08-01"),
        ("hour lookback from the day's end", W.range_start, ("2026-11-22", "hour", 48, TZ), "2026-11-20T18:00:00Z"),
    ],
)
def test_window_arithmetic(case, fn, args, want):
    got = fn(*args)
    assert (got.astimezone(UTC).isoformat() if isinstance(got, datetime) else got) == want


DAY_END = W.day_end("2026-11-12", TZ)


@pytest.mark.parametrize(
    ("case", "window", "unit", "mark", "complete"),
    [
        ("the day is loaded", "2026-11-12", "day", DAY_END, True),
        ("a later day is not", "2026-11-13", "day", DAY_END, False),
        ("nothing loaded yet", "2026-11-12", "day", None, False),
        ("the month is not over", "2026-11-01", "month", DAY_END, False),
        ("an hour of the day", "2026-11-12T17:00:00Z", "hour", DAY_END, True),
        ("an hour after the day", "2026-11-12T18:00:00Z", "hour", DAY_END, False),
    ],
)
def test_caught_up(case, window, unit, mark, complete):
    assert W.caught_up(window, unit, TZ, mark) is complete


OCT = datetime(2026, 10, 1, tzinfo=__import__("zoneinfo").ZoneInfo(TZ))


@pytest.mark.parametrize(
    ("case", "feeds", "expected", "complete", "missing"),
    [
        ("every dealer sent October", {"D-1": OCT, "D-2": OCT}, {"D-1", "D-2"}, True, []),
        ("one dealer late (LATE-01)", {"D-1": OCT}, {"D-1", "D-2"}, False, ["D-2"]),
        ("a dealer still on September", {"D-1": OCT, "D-2": OCT.replace(month=9)}, {"D-1", "D-2"}, False, ["D-2"]),
        ("a feed nobody expects does not count", {"D-1": OCT, "D-9": None}, {"D-1"}, True, []),
    ],
)
def test_feeds_caught_up(case, feeds, expected, complete, missing):
    assert W.feeds_caught_up("2026-10-01", "month", TZ, feeds, expected) == (complete, missing)


N = W.Numbers
END = W.end("2026-10-01", "month", TZ)
AFTER, BEFORE = datetime(2026, 11, 12, tzinfo=UTC), datetime(2026, 10, 20, tzinfo=UTC)


@pytest.mark.parametrize(
    ("case", "before", "after", "published", "feed", "restated"),
    [
        ("first publication", None, N(5, (("q", 1.0),), True), AFTER, False, False),
        ("same numbers", N(5, (("q", 1.0),), True), N(5, (("q", 1.0),), True), AFTER, False, False),
        ("a complete window changes (a correction)", N(5, (("q", 1.0),), True), N(5, (("q", 2.0),), True), AFTER,
         False, True),
        ("a late member's file completes an ended month (LATE-01)", N(5, (("q", 1.0),), False),
         N(6, (("q", 2.0),), True), AFTER, True, True),
        ("an input's ordinary lag completes an ended hour", N(5, (("q", 1.0),), False), N(5, (("q", 2.0),), True),
         AFTER, False, False),
        ("a provisional number of an ended month changes", N(5, (("q", 1.0),), False), N(6, (("q", 2.0),), False),
         AFTER, True, False),
        ("an open period completes", N(5, (("q", 1.0),), False), N(6, (("q", 2.0),), True), BEFORE, True, False),
        ("only the flag moves", N(5, (("q", 1.0),), False), N(5, (("q", 1.0),), True), AFTER, True, False),
        ("every row moved out of a complete window", N(5, (("q", 1.0),), True), None, AFTER, False, True),
        ("every row moved out of a provisional window", N(5, (("q", 1.0),), False), None, AFTER, False, False),
    ],
)
def test_is_restatement(case, before, after, published, feed, restated):
    assert W.is_restatement(before, after, END, published, feed) is restated
