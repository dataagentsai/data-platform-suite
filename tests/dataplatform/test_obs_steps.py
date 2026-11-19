"""The one step format (ADR-024 rule 2): a contract test over every owner's step rows.

Each row of OWNERS is one owner that writes `detect.*`, `quality.*`, `recon.*` or `profile.*` rows, run on a small
input; every step row it writes must keep the one schema (obs/steps.violations). Owners not built yet are pending
with the session they wait for. A second table proves the contract refuses what it must.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.meta.port import StepRecord
from dataplatform.obs import steps as S
from dataplatform.obs.clock import ManualClock
from dataplatform.obs.recorder import RunRecorder
from dataplatform.obs.trace import LocalTracer

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
T0 = datetime(2026, 11, 9, tzinfo=UTC)


def recorder_rows():
    """W3's recorder: what any owner writes through RunRecorder (ds.check, ds.detect)."""
    from test_obs_recorder import DOMAIN, dataset

    from dataplatform.adapters.memory_runlog import StampingRunLog

    store = StampingRunLog(MemoryMetaStore(), ManualClock(T0))
    store.load_domain(DOMAIN)
    store.load_dataset(dataset())
    with RunRecorder(store, LocalTracer()).run("shop", "dev") as run, run.dataset("erp", "orders", "fp", {}) as ds:
        ds.check("quality", "not_null_id", passed=True, counts={"rows": 3})
        ds.check("quality", "positive_qty", passed=False)
        ds.check("recon", "source_vs_bronze", passed=False, message="sources to landing: 7 keys", warn_only=True)
        ds.check("profile", "row_count", passed=True, counts={"rows": 3})
        ds.detect("DEL-01", {"keys": 5})
        ds.detect("SCH-02", blocked=True)
    return store.run_report(run.run_id)["steps"]


def ingestion_rows():
    """W1b ingestion: one detect.<DAC id> row per detector that met the run (held, skipped or noticed)."""
    from dataplatform.ingest.pipelines import Result
    from dataplatform.ingest.runner import detect_rows
    from dataplatform.ingest.steps import Batch, Outcome

    b = Batch("2026-11-09", "f.csv", b"x")
    result = Result([Outcome(b, "k1", "skipped_duplicate", 0, "same bytes"),
                     Outcome(b, "k2", "rejected_contract", 0, "missing column defect_code"),
                     Outcome(b, "k3", "held_mass_delete", 0, "would delete 40%")], noticed={"DEL-01": 5})
    return [{"step": f"detect.{d}", "status": st, "counts": c, "message": m} for d, st, c, m in detect_rows(result)]


def fixture_rows():
    """W3's fixture runner on the tyre world (the rows the ops screens were built on)."""
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog (the tyre DWD)")
    from dataplatform.twin.runner import Harness

    h = Harness.build(SLICE, days=4)
    return [s for rid in h.fixture.out.run_ids for s in h.fixture.store.run_report(rid)["steps"]]


def gold_rows():
    """W1d gold harness: its detect.* rows (KEY-01, LATE-02, HIST-05, LATE-01) on the small shop domain."""
    pytest.importorskip("pyspark")
    import tempfile

    from gold_helpers import DEALER, ORDER, d, shop_store, silver, silver_loaded, ts

    from dataplatform.adapters.spark_gold import DeltaGoldTables, SparkGoldEngine
    from dataplatform.adapters.spark_silver import local_session
    from dataplatform.gold.harness import run_gold

    spark = local_session("dataplatform-tests")  # the session's shared Spark when one is running
    tmp = Path(tempfile.mkdtemp(prefix="gold-steps-"))
    (tmp / "models").mkdir()
    store = shop_store(MemoryMetaStore(), tmp / "models")
    for name, cols, rows in (("dealer", DEALER, [{"dealer_code": "D-1", "region": "north"}]),
                             ("order", ORDER, [{"order_id": "A", "dealer_code": "D-1", "day": d(9), "qty": 1,
                                                "order_ts": ts(1)},
                                               {"order_id": "B", "dealer_code": "D-9", "day": d(9), "qty": 1,
                                                "order_ts": ts(1)}])):
        silver(spark, cols, rows).write.format("delta").save(str(tmp / "silver" / f"src__{name}"))
        silver_loaded(store, f"src.{name}", ["2026-11-09"])
    run_gold(store, DeltaGoldTables(spark, tmp / "silver", tmp / "gold"), SparkGoldEngine(spark, tmp / "models"),
             "shop", "dev", "gold-steps", "2026-11-09", "Asia/Kolkata")
    return store.run_report("gold-steps")["steps"]


def pending(session):
    def rows():
        pytest.skip(f"pending: {session} does not write step rows yet")
    return rows


OWNERS = {
    "recorder (any owner through RunRecorder)": recorder_rows,
    "ingestion (W1b) detect rows": ingestion_rows,
    "fixture runner (W3)": fixture_rows,
    "silver (W1c) detect rows": pending("W1c silver (its detectors are judged on silver tables today)"),
    "gold harness (W1d)": gold_rows,
    "data expectation services (W1e)": pending("W1e DES"),
}


@pytest.mark.parametrize("owner", list(OWNERS))
def test_every_owner_keeps_the_one_step_format(owner):
    rows = [r for r in OWNERS[owner]() if S.kind_of(r["step"])]
    assert rows, f"{owner} wrote no owner step rows"
    bad = {r["step"]: S.violations(r["step"], r["status"], r["counts"], r["message"]) for r in rows}
    assert {k: v for k, v in bad.items() if v} == {}


@pytest.mark.parametrize(
    ("case", "row", "problem"),
    [
        ("a pipeline step is not an owner row", ("silver.dedupe", "succeeded", {}, ""), "is not <detect"),
        ("no id after the kind", ("quality.", "succeeded", {}, ""), "is not <detect"),
        ("an unknown status", ("quality.a", "ok", {}, ""), "status 'ok'"),
        ("counts are integers", ("recon.a", "warned", {"keys": 1.5}, "x"), "counts ['keys']"),
        ("a boolean is not a count", ("recon.a", "warned", {"ok": True}, "x"), "counts ['ok']"),
        ("a blocked step says why", ("detect.SCH-02", "failed", {}, ""), "says why"),
        ("a long message", ("profile.a", "succeeded", {}, "x" * 4001), "message"),
    ],
)
def test_the_contract_refuses(case, row, problem):
    assert any(problem in v for v in S.violations(*row))


@pytest.mark.parametrize(
    ("step", "status", "expect"),
    [
        ("detect.DEL-01", "warned", ("detect", "engine", "warned", "sources_ingestion")),
        ("quality.not_null", "failed", ("quality", "des", "blocked", "des_data_expectations")),
        ("recon.live_source_vs_silver", "succeeded", ("recon", "des", "passed", "des_data_expectations")),
        ("profile.row_count", "succeeded", ("profile", "des", "passed", "des_data_expectations")),
    ],
)
def test_owner_severity_and_screen(step, status, expect):
    owner = S.owner_of(step)
    assert (S.kind_of(step), owner, S.severity(status), S.SCREEN[owner]) == expect


def test_a_step_record_is_the_row():
    r = StepRecord("r", "erp", "orders", "detect.DEL-01", "warned", {"keys": 5}, "5 noticed")
    assert S.violations(r.step, r.status, r.counts, r.message) == []
