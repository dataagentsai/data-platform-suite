"""The silver run in the kernel (W1c): which batches it takes, what it records in the metadata store, and how a
failure leaves the bookmark. No engine: the stage runner and the tables are small fakes behind the ports.
Also the day-by-day judge of silver's incidents (DEL-01, SCD-02, LATE-03, DUP-04). Table-driven."""

import pytest
from silver_helpers import view

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.ingest.daybyday import Verdict
from dataplatform.meta import registry
from dataplatform.meta.port import DqRule, FileRecord
from dataplatform.silver.daybyday import SilverDay, judge
from dataplatform.silver.plan import plan_from_view
from dataplatform.silver.ports import StageError, StageResult
from dataplatform.silver.runner import BOOKMARK, SilverDataset, SilverReport, build_silver


class FakeStages:
    def __init__(self, fail=None, critical=0):
        self.fail, self.seen, self.released, self.critical, self.plans = fail, [], 0, critical, []

    def run(self, plan, bronze, current, run_id, as_of):
        if self.fail:
            raise self.fail
        self.seen.append(list(bronze))
        self.plans.append(plan)
        return StageResult(list(bronze), None, {"rows_in": len(bronze), "rows_out": len(bronze),
                                                 "quality.quarantined": self.critical, "duplicates.dropped": 1,
                                                 **({"quality.critical": self.critical} if self.critical else {})})

    def release(self, result):
        self.released += 1


class FakeTables:
    def __init__(self, fail_write=False):
        self.written, self.fail_write = [], fail_write

    def bronze(self, plan, batch_keys):
        return list(batch_keys)

    def current(self, plan):
        return None

    def reset(self, plan):
        self.written.append((plan.table, "reset", []))

    def write(self, plan, result):
        if self.fail_write:
            raise OSError("disk full")
        self.written.append((plan.table, result.txn, list(result.rows)))
        return len(result.rows)


def store_with(files, quality=None, dq_rules=()):
    s = MemoryMetaStore()
    s.register_settings(registry.load())
    s.load_domain({"domain": "t", "settings": {}, "manifest": {}, "fingerprint": "d"})
    v = view(quality=quality)
    s.load_dataset({
        "domain": "t", "source_name": "src", "source_env": "", "dataset_name": "things", "is_enabled": True,
        "kind": "dataset", "archetypes": [], "ingest_mode": "push", "ingest_read": "change_feed",
        "history_kind": "event", "load_pattern": "append", "contract_id": "c", "contract_version": "1",
        "fingerprint": "fp", "settings": {}, "columns": v["columns"], "business_keys": {"primary": ["id"]},
        "manifest": v["manifest"],
    })
    if dq_rules:
        s.set_dq_rules("src", "things", list(dq_rules))
    s.start_run("ingest-1", "t", "dev")
    s.start_dataset_run("ingest-1", "src", "things", "fp", {})
    for key, status in files:
        s.log_file(FileRecord("ingest-1", "src", "things", key, f"{key}.json", "2026-11-09", "c", None, 1, status))
    s.finish_dataset_run("ingest-1", "src", "things", "succeeded")
    s.finish_run("ingest-1")
    return s


def silver(store, stages=None, tables=None, run_id="silver-1", rebuild=None):
    stages, tables = stages or FakeStages(), tables or FakeTables()
    return build_silver(store, tables, stages, "t", "dev", run_id, "2026-11-09", rebuild=rebuild), stages, tables


@pytest.mark.parametrize(
    ("case", "files", "status", "taken"),
    [
        ("loaded batches are taken", [("b1", "loaded"), ("b2", "loaded")], "succeeded", ["b1", "b2"]),
        ("held and skipped batches are not", [("b1", "loaded"), ("b2", "held_mass_delete"),
                                              ("b3", "skipped_duplicate"), ("b4", "rejected_contract")],
         "succeeded", ["b1"]),
        ("a batch reloaded is taken once per run", [("b1", "loaded"), ("b1", "loaded")], "succeeded", ["b1"]),
        ("nothing new: skipped", [], "skipped", []),
        ("only held batches: nothing taken, the bookmark moves past them", [("b1", "rejected_volume")],
         "succeeded", []),
    ],
)
def test_which_batches_a_run_takes(case, files, status, taken):
    store = store_with(files)
    report, stages, _ = silver(store)
    assert report.datasets[0].status == status, case
    assert (stages.seen[0] if stages.seen else []) == taken
    assert store.bookmarks("src", "things").get(BOOKMARK, "0") == str(len(files))


def test_the_next_run_starts_after_the_bookmark():
    store = store_with([("b1", "loaded")])
    silver(store)
    store.start_run("ingest-2", "t", "dev")
    store.start_dataset_run("ingest-2", "src", "things", "fp", {})
    store.log_file(FileRecord("ingest-2", "src", "things", "b2", "b2.json", "2026-11-10", "c", None, 1, "loaded"))
    store.finish_dataset_run("ingest-2", "src", "things", "succeeded")
    store.finish_run("ingest-2")
    _, stages, tables = silver(store, run_id="silver-2")
    assert stages.seen == [["b2"]]
    assert tables.written[0][1] == 2


@pytest.mark.parametrize(
    ("case", "stages", "tables", "step", "error_class"),
    [
        ("a batch that breaks its contract", FakeStages(StageError("contract column(s) v missing")), FakeTables(),
         "silver.stages", "StageError"),
        ("a write that fails", FakeStages(), FakeTables(fail_write=True), "silver.write", "OSError"),
    ],
)
def test_a_failure_leaves_the_bookmark_and_is_logged(case, stages, tables, step, error_class):
    store = store_with([("b1", "loaded")])
    report, _, _ = silver(store, stages, tables)
    assert report.status == "failed" and report.datasets[0].status == "failed", case
    assert BOOKMARK not in store.bookmarks("src", "things")
    errors = store.run_report("silver-1")["errors"]
    assert [(e["step"], e["error_class"]) for e in errors] == [(step, error_class)]


def test_a_critical_rule_broken_holds_the_batch():
    """ADR-009: nothing written, the bookmark stays, the dataset is skipped (blocked) and the quality step failed."""
    store = store_with([("b1", "loaded")])
    report, stages, tables = silver(store, FakeStages(critical=2))
    assert report.datasets[0].status == "skipped"
    assert "critical" in report.datasets[0].reason
    assert tables.written == []
    assert BOOKMARK not in store.bookmarks("src", "things")
    assert stages.released == 1
    steps = {s["step"]: s for s in store.run_report("silver-1")["steps"]}
    assert steps["silver.quality"]["status"] == "failed"


def test_every_stage_with_counts_is_a_step_in_the_run_log():
    store = store_with([("b1", "loaded")])
    silver(store)
    steps = {s["step"]: s for s in store.run_report("silver-1")["steps"]}
    assert {"silver", "silver.quality", "silver.duplicates"} <= set(steps)
    assert steps["silver"]["counts"]["rows_written"] == 1
    assert steps["silver"]["counts"]["batches"] == 1


def test_a_plan_the_engine_cannot_run_fails_the_dataset():
    store = store_with([("b1", "loaded")])
    m = store.dataset_view("src", "things")["manifest"]
    m["history"]["kind"] = "lookup"
    store._get("src", "things")["manifest"] = m
    report, stages, _ = silver(store)
    assert report.datasets[0].status == "failed"
    assert "dataset kind" in report.datasets[0].reason
    assert stages.seen == []


# ---------------------------------------------------------------------------------------------------- incidents
class Versions:
    def __init__(self, by_key):
        self.by_key = by_key

    def versions(self, plan, key):
        return self.by_key.get(tuple(key.values()), [])


SOL = plan_from_view(view(columns=[("order_id", "string"), ("line_no", "integer"), ("promised_date", "date"),
                                   ("change_seq", "integer")], keys=("order_id", "line_no"), kind="master",
                          load={"history_order_by": "change_seq"}), "")
READ = plan_from_view(view(), "")


def v(seq, current, deleted=False, promised="2026-11-20"):
    return {"_seq_from": seq, "_is_current": current, "_is_deleted": deleted, "promised_date": promised}


def day(measures=None, counts=None):
    return SilverDay(8, "2026-11-16", SilverReport("s", "succeeded", [SilverDataset("src", "things", "succeeded",
                                                                                    counts or {})]),
                     measures or {})


@pytest.mark.parametrize(
    ("case", "finding", "versions", "sday", "verdict"),
    [
        ("DEL-01: every deleted key marked", {"detector": "DEL-01", "detail": {"keys": [["S1", 10], ["S2", 20]]}},
         {("S1", 10): [v(1, True, True)], ("S2", 20): [v(1, True, True)]}, day(), "handled"),
        ("DEL-01: a key removed instead of marked", {"detector": "DEL-01", "detail": {"keys": [["S1", 10]]}},
         {}, day(), "missed"),
        ("SCD-02: the late change in history, the newer current",
         {"detector": "SCD-02", "detail": {"key": ["S1", 10], "older_seq": 4, "newer_seq": 5,
                                            "current_promised_date": "2026-11-20"}},
         {("S1", 10): [v(1, False), v(4, False, promised="2026-11-19"), v(5, True)]}, day(), "handled"),
        ("SCD-02: the late change made current",
         {"detector": "SCD-02", "detail": {"key": ["S1", 10], "older_seq": 4, "newer_seq": 5,
                                            "current_promised_date": "2026-11-20"}},
         {("S1", 10): [v(1, False), v(5, False), v(4, True, promised="2026-11-19")]}, day(), "missed"),
    ],
)
def test_judge_on_what_silver_holds(case, finding, versions, sday, verdict):
    f = {"incident": "x", "dataset": "erp.line", **finding}
    got = judge(8, f, Versions(versions), {"erp.line": SOL}, sday)
    assert isinstance(got, Verdict) and got.verdict == verdict, case


@pytest.mark.parametrize(
    ("case", "finding", "sday", "verdict"),
    [
        ("LATE-03: every event hour equals the oracle", {"detector": "LATE-03", "detail": {"late_readings": 9}},
         day({"src.things": {"event_hours_differing": (0.0, 0.0)}}), "handled"),
        ("LATE-03: an hour short", {"detector": "LATE-03", "detail": {"late_readings": 9}},
         day({"src.things": {"event_hours_differing": (1.0, 0.0)}}), "missed"),
        ("DUP-04: replays dropped, silver equals the oracle", {"detector": "DUP-04",
                                                               "detail": {"replayed_readings": 3}},
         day({"src.things": {"row_count": (10.0, 10.0)}}, {"duplicates.replayed": 2, "duplicates.dropped": 1}),
         "handled"),
        ("DUP-04: a replay kept", {"detector": "DUP-04", "detail": {"replayed_readings": 3}},
         day({"src.things": {"row_count": (11.0, 10.0)}}, {"duplicates.dropped": 2}), "missed"),
    ],
)
def test_judge_reading_incidents(case, finding, sday, verdict):
    f = {"incident": "x", "dataset": "src.things", **finding}
    assert judge(13, f, Versions({}), {"src.things": READ}, sday).verdict == verdict, case


def test_detectors_that_are_not_silver_are_left_to_other_layers():
    f = {"incident": "x", "dataset": "src.things", "detector": "LATE-01", "detail": {}}
    assert judge(5, f, Versions({}), {"src.things": READ}, day()) is None


@pytest.mark.parametrize(
    ("case", "void", "rebuild", "taken", "reset"),
    [
        ("a voided batch is never taken", ["b2"], None, [["b1", "b3"]], False),
        ("a rebuild takes every good batch from the start", [], {"src.things"}, [["b1", "b2", "b3"]], True),
        ("a rebuild skips the voided batch", ["b2"], {"src.things"}, [["b1", "b3"]], True),
    ],
)
def test_recovery_by_rerun(case, void, rebuild, taken, reset):
    """ADR-025: void the bad batch, rebuild silver from bronze; the first run already took b1..b3 when rebuilding."""
    store = store_with([("b1", "loaded"), ("b2", "loaded"), ("b3", "loaded")])
    if rebuild:
        silver(store)  # silver already holds everything once
    for key in void:
        store.void_batch("src", "things", key, "short copy slipped past the guard", "ops@tyre.example")
    report, stages, tables = silver(store, run_id="silver-2", rebuild=rebuild)
    assert report.status == "succeeded", case
    assert stages.seen[-1:] == taken, case
    assert any(w[1] == "reset" for w in tables.written) is reset, case


@pytest.mark.parametrize(("case", "minutes_later", "status", "written"), [
    ("another run holds the silver lease: skipped, nothing written", 0, "skipped", 0),
    ("the holder crashed and its lease expired: this run takes it over", 5 * 60, "succeeded", 1),
])
def test_two_runs_never_write_one_table(case, minutes_later, status, written):
    """ORCH-04: the second run waits for nothing and writes nothing while the first holds the table."""
    from datetime import UTC, datetime, timedelta

    store = store_with([("b1", "loaded")])
    t0 = datetime(2026, 11, 15, 6, tzinfo=UTC)
    store.now = lambda: t0
    assert store.acquire_lease("silver:src.things", "manual-rerun", 3600) is None
    store.now = lambda: t0 + timedelta(minutes=minutes_later)
    report, _, tables = silver(store)
    [d] = report.datasets
    assert d.status == status, case
    assert len(tables.written) == written
    lease_steps = [s for s in store.steps if s["step"] == "orch.lease"]
    assert (len(lease_steps) == 1) is (status == "skipped")
    if lease_steps:
        assert "manual-rerun" in lease_steps[0]["message"] and lease_steps[0]["status"] == "failed"
    assert store.acquire_lease("silver:src.things", "next", 60) == (None if status == "succeeded" else "manual-rerun")


NEG = DqRule("v-sql-1", "v", "sql", {"query": "SELECT COUNT(*) FROM ${object} WHERE v < 0", "mustBe": 0}, "major",
             "quarantine")


@pytest.mark.parametrize(
    ("case", "quality", "want"),
    [
        ("the contract's action stands without an override", None, "quarantine"),
        ("the dataset says flag for major rules", {"actions": {"major": {"rows": "flag"}}}, "flag"),
        ("an override for another severity changes nothing", {"actions": {"minor": {"rows": "flag"}}}, "quarantine"),
    ],
)
def test_the_stages_get_the_contract_rules_with_the_datasets_actions(case, quality, want):
    store = store_with([("b1", "loaded")], quality=quality, dq_rules=[NEG])
    _, stages, _ = silver(store)
    assert {r.name: r.action for r in stages.plans[0].rules}["v-sql-1"] == want, case
