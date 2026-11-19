"""What housekeeping may remove from bronze (housekeeping.py, B8, RET-01)."""

from datetime import date, timedelta

import pytest

from dataplatform.housekeeping import Batch, plan_removals

TODAY = date(2026, 11, 15)


def d(days_ago):
    return TODAY - timedelta(days=days_ago)


def inc(key, days_ago):
    return Batch(key, d(days_ago))


@pytest.mark.parametrize(
    ("case", "batches", "window", "remove", "needs_baseline"),
    [
        ("everything inside the window stays", [inc("b1", 10), inc("b2", 5)], 30, [], False),
        ("change feed with no baseline: write one first, remove nothing",
         [inc("b1", 120), inc("b2", 100), inc("b3", 5)], 90, [], True),
        ("checked baseline before the cut-off: older batches go",
         [inc("b1", 200), inc("b2", 150), Batch("base", d(120), "baseline", True), inc("b3", 100), inc("b4", 5)],
         90, ["b1", "b2"], False),
        ("an unchecked baseline does not count",
         [inc("b1", 200), Batch("base", d(120), "baseline", False), inc("b3", 5)], 90, [], True),
        ("a baseline inside the window does not count yet",
         [inc("b1", 200), Batch("base", d(30), "baseline", True), inc("b3", 5)], 90, [], True),
        ("the newest qualifying baseline wins",
         [Batch("f1", d(300), "full", True), inc("b1", 250), Batch("f2", d(100), "full", True), inc("b2", 5)],
         90, ["f1", "b1"], False),
        ("daily full copies: each day is its own baseline",
         [Batch(f"c{n}", d(n), "full", True) for n in (40, 39, 36, 35, 34)], 35, ["c40", "c39", "c36"], False),
        ("no batches at all", [], 90, [], False),
    ],
)
def test_plan_removals(case, batches, window, remove, needs_baseline):
    p = plan_removals("erp.x", batches, TODAY, window)
    assert list(p.remove) == remove
    assert p.needs_baseline is needs_baseline


def test_the_baseline_itself_and_everything_after_it_stay():
    batches = [inc("b1", 200), Batch("base", d(120), "baseline", True), inc("b2", 119), inc("b3", 1)]
    p = plan_removals("erp.x", batches, TODAY, 90)
    assert p.keep_from == d(120)
    assert "base" not in p.remove


@pytest.mark.parametrize(("case", "batches", "window", "says"), [
    ("negative window", [], -1, "negative"),
    ("unknown load type", [Batch("b", d(1), "partial")], 30, "load type"),
])
def test_refused(case, batches, window, says):
    with pytest.raises(ValueError, match=says):
        plan_removals("erp.x", batches, TODAY, window)


# The job (run_housekeeping): file log in, bronze rows out, removed batches voided, one step per dataset.

from silver_helpers import view  # noqa: E402

from dataplatform.adapters.memory_bronze import MemoryBronze  # noqa: E402
from dataplatform.adapters.memory_meta import MemoryMetaStore  # noqa: E402
from dataplatform.housekeeping import STEP, batches_of, run_housekeeping  # noqa: E402
from dataplatform.meta import registry  # noqa: E402
from dataplatform.meta.port import FileRecord  # noqa: E402
from dataplatform.silver.runner import new_batches  # noqa: E402

COLS = [("id", "integer"), ("_batch_key", "string")]


def rows(key, *ids):
    return [{"id": i, "_batch_key": key} for i in ids]  # the lander stamps the batch key on every row


def world(files):
    """A store with one dataset src.things and its file log; bronze holds two rows per loaded batch."""
    s = MemoryMetaStore()
    s.register_settings(registry.load())
    s.load_domain({"domain": "t", "settings": {}, "manifest": {}, "fingerprint": "d"})
    v = view()
    s.load_dataset({
        "domain": "t", "source_name": "src", "source_env": "", "dataset_name": "things", "is_enabled": True,
        "kind": "dataset", "archetypes": [], "ingest_mode": "pull", "ingest_read": "full_copy",
        "history_kind": "event", "load_pattern": "append", "contract_id": "c", "contract_version": "1",
        "fingerprint": "fp", "settings": {}, "columns": v["columns"], "business_keys": {"primary": ["id"]},
        "manifest": v["manifest"],
    })
    s.start_run("ingest-1", "t", "dev")
    s.start_dataset_run("ingest-1", "src", "things", "fp", {})
    bronze = MemoryBronze()
    for key, days_ago, load_type, status in files:
        s.log_file(FileRecord("ingest-1", "src", "things", key, f"{key}.json", d(days_ago).isoformat(), "c", None, 2,
                              status, load_type=load_type))
        if status == "loaded":
            bronze.write("src.things", key, rows(key, 1, 2), COLS)
    s.finish_dataset_run("ingest-1", "src", "things", "succeeded")
    s.finish_run("ingest-1")
    return s, bronze


@pytest.mark.parametrize(
    ("case", "files", "window", "left", "step_status"),
    [
        ("daily copies, 1-day window: copies before yesterday's go",
         [("c3", 3, "full", "loaded"), ("c2", 2, "full", "loaded"), ("c1", 1, "full", "loaded"),
          ("c0", 0, "full", "loaded")], 1, ["c1", "c0"], "succeeded"),
        ("a held copy is no baseline",
         [("c3", 3, "full", "loaded"), ("c2", 2, "full", "held_mass_delete"), ("c0", 0, "full", "loaded")],
         1, ["c3", "c0"], "succeeded"),  # had c2 counted, c3 would go
        ("a change feed without a baseline keeps everything",
         [("i3", 3, "incremental", "loaded"), ("i0", 0, "incremental", "loaded")], 1, ["i3", "i0"], "warned"),
        ("a wide window keeps everything",
         [("c3", 3, "full", "loaded"), ("c0", 0, "full", "loaded")], 30, ["c3", "c0"], "succeeded"),
    ],
)
def test_run_housekeeping(case, files, window, left, step_status):
    s, bronze = world(files)
    [swept] = run_housekeeping(s, bronze, "t", env="dev", run_id="hk-1", today=TODAY, correction_days=window)
    assert sorted({r["_batch_key"] for r in bronze.tables["src.things"]}) == sorted(left)
    assert swept.rows_removed == 2 * len(swept.plan.remove)
    [step] = [x for x in s.steps if x["run_id"] == "hk-1"]
    assert (step["step"], step["status"]) == (STEP, step_status)
    assert step["counts"]["batches_removed"] == len(swept.plan.remove)
    taken, _ = new_batches(s, "src", "things", from_start=True)  # a rebuild skips what housekeeping removed
    assert sorted(taken) == sorted(left)


def test_a_second_run_removes_nothing_more():
    s, bronze = world([("c2", 2, "full", "loaded"), ("c1", 1, "full", "loaded"), ("c0", 0, "full", "loaded")])
    run_housekeeping(s, bronze, "t", env="dev", run_id="hk-1", today=TODAY, correction_days=1)
    [again] = run_housekeeping(s, bronze, "t", env="dev", run_id="hk-2", today=TODAY, correction_days=1)
    assert again.plan.remove == ()
    assert again.rows_removed == 0


def test_the_window_defaults_to_the_bronze_retention_setting():
    s, bronze = world([("c3", 3, "full", "loaded"), ("c0", 0, "full", "loaded")])
    [swept] = run_housekeeping(s, bronze, "t", env="dev", run_id="hk-1", today=TODAY)
    assert swept.plan.remove == ()
    assert s.steps[-1]["counts"]["correction_days"] == 1095


def test_batches_of_skips_void_and_repeats():
    files = [FileRecord("r", "s", "d", k, "f", "2026-11-01", "c", None, 1, st, load_type=lt)
             for k, st, lt in [("a", "loaded", "full"), ("a", "loaded", "full"), ("b", "void", "incremental"),
                               ("c", "skipped_duplicate", "incremental"), ("e", "loaded", "incremental")]]
    assert [(b.batch_key, b.checked) for b in batches_of(files)] == [("a", True), ("e", False)]


@pytest.mark.parametrize(("case", "keys", "removed"), [
    ("one batch", ["b1"], 2), ("none", [], 0), ("unknown batch", ["zz"], 0)])
def test_memory_bronze_remove(case, keys, removed):
    b = MemoryBronze()
    b.write("s.t", "b1", rows("b1", 1, 2), COLS)
    b.write("s.t", "b2", rows("b2", 3), COLS)
    assert b.remove("s.t", keys) == removed


def test_remove_refuses_an_unsafe_key():
    with pytest.raises(ValueError, match="safe batch key"):
        MemoryBronze().remove("s.t", ["x' OR '1'='1"])


class FakeBaseliner:
    """Logs a checked baseline at the cut-off (written) or refuses, like silver/baseline_writer.py."""

    def __init__(self, status):
        self.status, self.calls = status, []

    def write(self, store, run_id, view, cutoff, today):
        from dataplatform.housekeeping import BaselineOutcome

        self.calls.append(cutoff)
        if self.status == "written":
            store.log_file(FileRecord(run_id, view["source"], view["dataset"], "baseline-x", "baseline-x",
                                      cutoff.isoformat(), "c", None, 2, "loaded", load_type="baseline"))
        return BaselineOutcome(self.status, {}, self.status)


@pytest.mark.parametrize(("case", "status", "left", "step"), [
    ("a checked baseline lets the old batches go", "written", ["i1", "baseline-x"], "succeeded"),
    ("a refused baseline keeps everything", "refused", ["i3", "i2", "i1"], "failed"),
    ("not needed (events): everything stays", "not_needed", ["i3", "i2", "i1"], "skipped"),
])
def test_baseline_in_the_job(case, status, left, step):
    s, bronze = world([("i3", 3, "incremental", "loaded"), ("i2", 2, "incremental", "loaded"),
                       ("i1", 1, "incremental", "loaded")])
    b = FakeBaseliner(status)
    if status == "written":
        bronze.write("src.things", "baseline-x", rows("baseline-x", 1, 2), COLS)
    run_housekeeping(s, bronze, "t", env="dev", run_id="hk-1", today=TODAY, correction_days=1, baseliner=b)
    assert b.calls == [d(1)]
    assert sorted({r["_batch_key"] for r in bronze.tables["src.things"]}) == sorted(left)
    [base] = [x for x in s.steps if x["step"] == "housekeeping.baseline"]
    assert base["status"] == step


def test_a_normal_silver_run_skips_a_baseline_batch_and_a_rebuild_takes_it():
    s, _ = world([("i1", 1, "incremental", "loaded")])
    s.start_run("hk", "t", "dev")
    s.start_dataset_run("hk", "src", "things", "fp", {})
    s.log_file(FileRecord("hk", "src", "things", "baseline-x", "b", d(1).isoformat(), "c", None, 1, "loaded",
                          load_type="baseline"))
    assert new_batches(s, "src", "things")[0] == ["i1"]
    assert new_batches(s, "src", "things", from_start=True)[0] == ["i1", "baseline-x"]


def test_housekeeping_never_removes_while_a_load_writes_bronze():
    """ORCH-04: the lander and housekeeping share the bronze lease."""
    s, bronze = world([("c3", 3, "full", "loaded"), ("c0", 0, "full", "loaded")])
    s.acquire_lease("bronze:src.things", "ingest-9", 3600)
    [swept] = run_housekeeping(s, bronze, "t", env="dev", run_id="hk-1", today=TODAY, correction_days=1)
    assert swept.plan.remove == ()
    assert sorted({r["_batch_key"] for r in bronze.tables["src.things"]}) == ["c0", "c3"]
    assert [x["status"] for x in s.steps if x["step"] == "orch.lease"] == ["failed"]
