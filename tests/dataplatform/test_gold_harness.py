"""The gold harness on local Spark and Delta folders (W1d): the properties ADR-003..006, -017 and -018 promise, run on a
small domain (dealers type 2, orders by day, monthly dealer files). Table-driven where the cases are alike.

    no fact row dropped           count built = count published, orphans on -1 included
    replace, never append         a rerun changes nothing; a changed window is replaced whole, others untouched
    late dimension (LATE-02)      -1 then re-keyed when the member arrives, whatever the lookback
    corrections (ADR-017)         applied only for windows their plan names; logged as HIST-05 restatements
    completeness (ADR-018)        a window waits for every input, per-feed inputs for every expected member
    late member file (LATE-01)    the month completes and is restated; a resend changes nothing
    checks before publish         a fan-out or a duplicate grain holds the last good windows

Rules proven here (DAC): GOLD-01 (replace, never append), JOIN-01 (no fact row lost or multiplied by a key join),
COMP-01 (complete only when every input caught up), PUB-01 (a failed check publishes nothing), DEL-03 (a deleted
source row leaves gold).
"""

import pytest

pytest.importorskip("pyspark")
from gold_helpers import DEALER, FEED, ORDER, d, shop_store, silver, silver_loaded, ts

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.gold.harness import run_gold
from dataplatform.meta.port import BackfillPlan, Correction
from dataplatform.obs import steps as S

TZ = "Asia/Kolkata"
DAYS = [f"2026-11-{x:02d}" for x in range(9, 16)]


class Shop:
    """One small platform: a memory store, silver folders written by the test, gold folders the harness writes."""

    def __init__(self, spark, tmp, monthly=False):
        from dataplatform.adapters.spark_gold import (
            DeltaGoldTables,
            SparkGoldEngine,
            SparkGoldReader,
        )

        self.spark, self.tmp = spark, tmp
        (tmp / "models").mkdir()
        self.store = shop_store(MemoryMetaStore(), tmp / "models", monthly)
        self.tables = DeltaGoldTables(spark, tmp / "silver", tmp / "gold")
        self.engine = SparkGoldEngine(spark, tmp / "models")
        self.reader = SparkGoldReader(spark, tmp / "gold")
        self.runs = 0

    def silver(self, name, cols, rows):
        silver(self.spark, cols, rows).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .save(str(self.tmp / "silver" / f"src__{name}"))

    def run(self, as_of, only=None):
        self.runs += 1
        return run_gold(self.store, self.tables, self.engine, "shop", "dev", f"gold-{self.runs}", as_of, TZ, only=only)

    def facts(self, table="fact_orders"):
        return sorted((dict(r) for r in self.reader.rows(f"SELECT * FROM {table}")),
                      key=lambda r: tuple(str(r[k]) for k in sorted(r)))

    def steps(self, run):
        return [s for s in self.store.steps if s["run_id"] == run]


def order(o, dealer, day, qty=1, hours=None):
    return {"order_id": o, "dealer_code": dealer, "day": d(day), "qty": qty,
            "order_ts": ts(hours if hours is not None else (day - 9) * 24 + 1)}


DEALERS = [{"dealer_code": "D-1", "region": "north"}, {"dealer_code": "D-2", "region": "south"}]


@pytest.fixture
def shop(spark, tmp_path):
    s = Shop(spark, tmp_path)
    s.silver("dealer", DEALER, DEALERS)
    silver_loaded(s.store, "src.dealer", DAYS[:1])
    return s


def test_no_fact_row_is_dropped(shop):
    rows = [order(f"O{i}", ["D-1", "D-2", "D-9", None][i % 4], 9 + i % 3, i) for i in range(40)]
    shop.silver("order", ORDER, rows)
    silver_loaded(shop.store, "src.order", DAYS[:3])
    report = shop.run(DAYS[2])
    t = report.by_name()["fact_orders"]
    assert t.status == "succeeded", t.reason
    got = shop.facts()
    assert len(got) == 40 and sum(r["qty"] for r in got) == sum(r["qty"] for r in rows)
    assert {r["dealer_sk"] for r in got if r["dealer_code"] == "D-9"} == {-1}
    assert {r["dealer_sk"] for r in got if r["dealer_code"] is None} == {-2}
    assert t.counts["orphan_rows"] == 10


def test_replace_never_append(shop):
    shop.silver("order", ORDER, [order("A", "D-1", 9), order("B", "D-1", 10), order("C", "D-2", 11)])
    silver_loaded(shop.store, "src.order", DAYS[:3])
    first = shop.run(DAYS[2]).by_name()["fact_orders"]
    before = shop.facts()
    again = shop.run(DAYS[2]).by_name()["fact_orders"]
    assert (first.counts["windows_replaced"], again.counts["windows_replaced"]) == (3, 0)
    assert shop.facts() == before  # the rerun wrote nothing: not even the run id moved
    # B is deleted and C changes: their windows are replaced whole; A's window keeps the first run's rows
    shop.silver("order", ORDER, [order("A", "D-1", 9), {**order("B", "D-1", 10), "_is_deleted": True},
                                 order("C", "D-2", 11, qty=5)])
    third = shop.run(DAYS[2]).by_name()["fact_orders"]
    after = {r["order_id"]: r for r in shop.facts()}
    assert sorted(third.replaced) == ["2026-11-10", "2026-11-11"]
    assert set(after) == {"A", "C"} and after["C"]["qty"] == 5
    assert after["A"]["_run_id"] == "gold-1" and after["C"]["_run_id"] == "gold-3"


def test_a_late_member_is_rekeyed_outside_the_lookback(shop):
    shop.silver("order", ORDER, [order("A", "D-3", 9), order("B", "D-1", 12), order("C", "D-2", 12)])
    silver_loaded(shop.store, "src.order", DAYS[:1])
    t = shop.run(DAYS[0]).by_name()["fact_orders"]
    assert [r["dealer_sk"] for r in shop.facts() if r["order_id"] == "A"] == [-1]
    assert any(s["step"] == "detect.LATE-02" and s["status"] == "warned" for s in shop.steps("gold-1"))
    assert t.status == "succeeded"
    shop.silver("dealer", DEALER, [*DEALERS, {"dealer_code": "D-3", "region": "west", "_valid_from": ts(24 * 9)}])
    silver_loaded(shop.store, "src.order", DAYS[1:7])
    t = shop.run(DAYS[6]).by_name()["fact_orders"]  # day 9 is outside the 5-day lookback on the 15th
    assert "2026-11-09" in t.replaced
    assert [r["dealer_sk"] > 0 for r in shop.facts()] == [True, True, True]
    assert not any(s["step"] == "detect.LATE-02" for s in shop.steps("gold-2"))


@pytest.mark.parametrize(
    ("case", "value", "applied", "restated"),
    [
        ("inside the plan's windows: applied and restated", "2026-11-11", 1, ["2026-11-10", "2026-11-11"]),
        ("outside the plan's windows: not applied", "2026-11-12", 0, []),
    ],
)
def test_corrections_reach_only_the_planned_windows(shop, case, value, applied, restated):
    shop.silver("order", ORDER, [order("A", "D-1", 10), order("B", "D-1", 11)])
    silver_loaded(shop.store, "src.order", DAYS[:4])
    shop.run(DAYS[3])
    plan = BackfillPlan("bp-1", "src", "order", ("2026-11-10", "2026-11-11"), DAYS[4], "A was always due a day later",
                        "sales@example")
    shop.store.record_corrections(plan, [Correction({"order_id": "A"}, "day", value, "2026-11-10")])
    silver_loaded(shop.store, "src.order", DAYS[4:5])
    t = shop.run(DAYS[4]).by_name()["fact_orders"]
    assert (t.counts["corrections"], t.counts["corrections_not_applied"]) == (applied, 1 - applied)
    assert sorted(r.window for r in t.restated) == restated
    assert {r.detector for r in t.restated} <= {"HIST-05"}
    assert t.plans_applied == ["bp-1"]
    hist = [s for s in shop.steps("gold-2") if s["step"] == "detect.HIST-05"]
    assert hist and all(s["status"] == "warned" for s in hist)
    assert {r["order_id"]: str(r["day"]) for r in shop.facts()}["A"] == ("2026-11-11" if applied else "2026-11-10")
    # silver is untouched (ADR-017)
    assert shop.tables.silver("src.order", []).where("order_id = 'A'").first()["day"] == d(10)



@pytest.mark.parametrize(("case", "withdrawn", "day9_qty", "restated"), [
    ("a withdrawn batch: the old window is rebuilt and restated", True, 1, ["2026-11-09"]),
    ("no withdrawal: a window outside the lookback is left alone", False, 10, []),
])
def test_a_withdrawn_batch_rebuilds_its_windows_whatever_the_lookback(shop, case, withdrawn, day9_qty, restated):
    """VOID-01: the bad batch's rows leave silver (rebuilt from bronze); gold recomputes every window and replaces
    only those that changed, logged as restatements; no row is deleted by hand."""
    from datetime import date

    from dataplatform.recovery import withdraw

    shop.silver("order", ORDER, [order("A", "D-1", 9), order("BAD", "D-1", 9, qty=9), order("C", "D-2", 12)])
    silver_loaded(shop.store, "src.order", DAYS[:4])
    shop.run(DAYS[3])
    shop.silver("order", ORDER, [order("A", "D-1", 9), order("C", "D-2", 12)])  # silver after its rebuild
    if withdrawn:
        withdraw(shop.store, "src", "order", "2026-11-09:order", reason="corrupt file found a week late",
                 approved_by="ops@example", today=date.fromisoformat(DAYS[6]))
    silver_loaded(shop.store, "src.order", DAYS[4:7])
    t = shop.run(DAYS[6]).by_name()["fact_orders"]  # 2026-11-09 is outside the 5-day lookback
    assert t.status == "succeeded", t.reason
    assert sum(r["qty"] for r in shop.facts() if str(r["day"]) == "2026-11-09") == day9_qty, case
    assert sorted(r.window for r in t.restated) == restated
    rst = [x for x in shop.steps("gold-2") if x["step"] == "gold.restatement"]
    assert [("void-" in x["message"]) for x in rst] == [True] * len(restated)


def test_a_past_window_is_rebuilt_from_its_run_log(shop):
    """INSP-01: the run log names each input's version and the query; inspect rebuilds that run's window from them,
    after silver and gold have both moved on."""
    from dataplatform.gold.inspect import reproduce

    shop.silver("order", ORDER, [order("A", "D-1", 9, qty=2), order("B", "D-2", 9, qty=3)])
    silver_loaded(shop.store, "src.order", DAYS[:1])
    shop.run(DAYS[0])
    shop.silver("order", ORDER, [order("A", "D-1", 9, qty=20), order("B", "D-2", 9, qty=3)])  # silver moves on
    silver_loaded(shop.store, "src.order", DAYS[1:2])
    shop.run(DAYS[1])
    assert sum(r["qty"] for r in shop.facts()) == 23  # gold now publishes the new number
    kw = {"domain": "shop", "table": "fact_orders", "window": "2026-11-09", "tz": TZ}
    past = reproduce(shop.store, shop.tables, shop.engine, run_id="gold-1", **kw)
    assert sum(r["qty"] for r in past.keyed.collect()) == 5  # the first run's rows, from its own inputs
    assert not past.same and past.as_of == DAYS[0]
    assert set(past.versions) == {"src.order"} and "parameters bound by the engine" in past.query
    now = reproduce(shop.store, shop.tables, shop.engine, run_id="gold-2", **kw)
    assert now.same and now.versions["src.order"] > past.versions["src.order"]

def test_a_window_is_complete_only_when_every_input_caught_up(shop):
    shop.silver("order", ORDER, [order("A", "D-1", 9), order("B", "D-1", 10), order("C", "D-1", 12)])
    silver_loaded(shop.store, "src.order", DAYS[:2])  # loaded through the 10th
    t = shop.run(DAYS[1]).by_name()["fact_orders"]
    flags = {str(r["day"]): r["_is_complete"] for r in shop.facts()}
    assert flags == {"2026-11-09": True, "2026-11-10": True, "2026-11-12": False}
    assert t.incomplete == ["2026-11-12"]
    marks = shop.store.bookmarks("gold_shop", "fact_orders")
    assert marks["input.src.order"] == "2026-11-11T00:00:00+05:30"


def test_a_failed_check_holds_the_last_good_windows(shop):
    shop.silver("order", ORDER, [order("A", "D-1", 9)])
    silver_loaded(shop.store, "src.order", DAYS[:1])
    shop.run(DAYS[0])
    good = shop.facts()
    # two dealer versions that overlap would fan a fact row out: the key step fails and nothing is written
    shop.silver("dealer", DEALER, [*DEALERS, {"dealer_code": "D-1", "region": "east", "_valid_from": ts(1),
                                              "_row_key": "dup"}])
    shop.silver("order", ORDER, [order("A", "D-1", 9, qty=9)])
    t = shop.run(DAYS[0]).by_name()["fact_orders"]
    assert t.status == "failed" and "after the key lookup" in t.reason
    assert "the join to dim_dealer" in t.reason  # LEDG-01: the join that multiplied the rows is named
    assert shop.facts() == good


@pytest.mark.parametrize(("case", "dealers", "want"), [
    ("every dealer known", ["D-1", "D-2", "D-1"], {"matched": 3, "unknown": 0, "not_applicable": 0}),
    ("an unknown dealer and an order with none", ["D-1", "D-9", None, "D-2"],
     {"matched": 2, "unknown": 1, "not_applicable": 1}),
])
def test_the_row_ledger_counts_reads_and_each_join(shop, case, dealers, want):
    shop.silver("order", ORDER, [order(f"O{i}", x, 9) for i, x in enumerate(dealers)])
    silver_loaded(shop.store, "src.order", DAYS[:1])
    shop.run(DAYS[0])
    [led] = [x for x in shop.steps("gold-1") if x["step"] == "gold.ledger" and x["dataset"] == "fact_orders"]
    assert led["status"] == "succeeded", led["message"]
    c = led["counts"]
    assert {w: c[f"dealer_sk.{w}"] for w in want} == want, case
    assert c["built"] == c["after_keys"] == len(dealers)
    assert any(k.startswith("read.") for k in c)


def test_every_step_row_keeps_the_one_step_format(shop):
    shop.silver("order", ORDER, [order("A", "D-1", 9), order("B", "D-7", 9)])
    silver_loaded(shop.store, "src.order", DAYS[:1])
    shop.run(DAYS[0])
    rows = [s for s in shop.store.steps if S.kind_of(s["step"])]
    assert {s["step"] for s in rows} >= {"detect.KEY-01", "detect.LATE-02"}
    assert {s["step"]: S.violations(s["step"], s["status"], s["counts"], s["message"]) for s in rows
            if S.violations(s["step"], s["status"], s["counts"], s["message"])} == {}


# ---------------------------------------------------------------------------------------------------- per-feed inputs
OCT, SEP = d(1).replace(month=10), d(1).replace(month=9)


@pytest.fixture
def monthly(spark, tmp_path):
    s = Shop(spark, tmp_path, monthly=True)
    s.silver("dealer", DEALER, [{**x, "_valid_from": ts(-24 * 40)} for x in DEALERS])
    silver_loaded(s.store, "src.dealer", DAYS[:1])
    s.silver("order", ORDER, [])
    silver_loaded(s.store, "src.order", DAYS[:1])
    return s


def feed(store, day, dealers):
    silver_loaded(store, "src.feed", [day], {x: "2026-10-01T00:00:00+05:30" for x in dealers})


def test_a_late_member_file_completes_and_restates_the_month(monthly):
    monthly.silver("feed", FEED, [{"dealer_code": "D-1", "month": OCT, "units": 5}])
    feed(monthly.store, DAYS[0], ["D-1"])
    day1 = monthly.run(DAYS[0], only={"dim_dealer", "fact_monthly"}).by_name()["fact_monthly"]
    assert day1.incomplete == ["2026-10-01"] and not day1.restated
    assert "src.feed (1 feeds, e.g. D-2)" in next(s["message"] for s in monthly.steps("gold-1")
                                                  if s["step"] == "gold.completeness")
    monthly.silver("feed", FEED, [{"dealer_code": "D-1", "month": OCT, "units": 5},
                                  {"dealer_code": "D-2", "month": OCT, "units": 7}])
    feed(monthly.store, DAYS[4], ["D-2"])
    day5 = monthly.run(DAYS[4], only={"dim_dealer", "fact_monthly"}).by_name()["fact_monthly"]
    assert day5.incomplete == []
    assert [(r.window, r.detector) for r in day5.restated] == [("2026-10-01", "LATE-01")]
    assert any(s["step"] == "detect.LATE-01" for s in monthly.steps("gold-2"))
    [rst] = [s for s in monthly.steps("gold-2") if s["step"] == "gold.restatement"]  # RSTM-01
    assert "window=2026-10-01; reason=late data" in rst["message"]
    assert (rst["counts"]["units.before"], rst["counts"]["units.after"]) == (5, 12)
    resend = monthly.run(DAYS[5], only={"dim_dealer", "fact_monthly"}).by_name()["fact_monthly"]
    assert (resend.replaced, resend.restated) == ([], [])


def test_a_member_that_joined_later_owes_no_file_for_earlier_months(monthly):
    monthly.silver("dealer", DEALER, [*[{**x, "_valid_from": ts(-24 * 40)} for x in DEALERS],
                                      {"dealer_code": "D-9", "region": "west", "_valid_from": ts(24 * 12)}])
    monthly.silver("feed", FEED, [{"dealer_code": x, "month": OCT, "units": 1} for x in ("D-1", "D-2")])
    feed(monthly.store, DAYS[0], ["D-1", "D-2"])
    t = monthly.run(DAYS[0], only={"dim_dealer", "fact_monthly"}).by_name()["fact_monthly"]
    assert t.incomplete == []
