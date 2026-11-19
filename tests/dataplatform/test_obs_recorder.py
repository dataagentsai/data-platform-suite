"""The run recorder (G8, OPS-03): one id from start to finish, every outcome recorded through the store's port.

Table-driven: each row plays a small run against the in-memory store (with the run-log reader stamping times) and
checks one thing about what was recorded.
"""

from datetime import UTC, datetime

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_runlog import StampingRunLog
from dataplatform.meta.port import SettingDef
from dataplatform.obs.clock import ManualClock
from dataplatform.obs.recorder import RecorderError, RunRecorder
from dataplatform.obs.trace import LocalTracer, as_uuid

DEFS = [SettingDef("sla_freshness_hours", "dataset", "string"), SettingDef("sla_freshness_error_hours", "dataset",
                                                                            "string")]
DOMAIN = {"domain": "shop", "business_timezone": "UTC", "environments": ["dev"], "settings": {},
          "manifest": {"kind": "platform"}, "fingerprint": "d1"}


def dataset(name="orders"):
    return {"domain": "shop", "source_name": "erp", "source_env": "prod", "dataset_name": name, "is_enabled": True,
            "kind": "dataset", "archetypes": ["PA2"], "ingest_mode": "push", "ingest_read": "change_feed",
            "history_kind": "transaction", "load_pattern": "history", "contract_id": f"c-{name}",
            "contract_version": "0.1.0", "fingerprint": f"fp-{name}", "settings": {}, "columns": [],
            "business_keys": {}, "manifest": {"dataset_name": name}}

T0 = datetime(2026, 11, 9, 23, 0, tzinfo=UTC)


@pytest.fixture
def env():
    clock = ManualClock(T0)
    store = StampingRunLog(MemoryMetaStore(), clock)
    store.register_settings(DEFS)
    store.load_domain(DOMAIN)
    store.load_dataset(dataset())
    store.load_dataset(dataset("returns"))
    tracer = LocalTracer()
    return {"store": store, "tracer": tracer, "rec": RunRecorder(store, tracer), "clock": clock}


def happy(e):
    with e["rec"].run("shop", "dev", "schedule") as run, run.dataset("erp", "orders", "fp", {"a": 1}) as ds:
        with ds.step("land") as step:
            step.counts["rows"] = 3
        e["clock"].advance(30)
        ds.check("quality", "not_null", passed=True)
        ds.check("recon", "src_vs_silver", passed=False, message="sources to landing")
        ds.detect("DEL-01", {"keys": 5})
        ds.file("k1", "a.csv", "w1", "c", 3, "loaded")
        ds.succeed({"rows_written": 3}, watermark="9", bookmarks={"feed": "2026-11-09T23:00:00+00:00"})
    return run


def report(e, run):
    return e["store"].run_report(run.run_id)


def failing_step(e):
    with (pytest.raises(ZeroDivisionError), e["rec"].run("shop", "dev") as run,
          run.dataset("erp", "orders", "fp", {}) as ds, ds.step("land")):
        raise ZeroDivisionError("bad row")
    return run


def run_dies(e):
    def body(run):
        run.cm = run.dataset("erp", "orders", "fp", {})
        run.cm.__enter__()  # a dataset still open when the run dies
        raise RuntimeError("driver lost")

    holder = {}

    def play():
        with e["rec"].run("shop", "dev") as run:
            holder["run"] = run
            body(run)

    with pytest.raises(RuntimeError, match="driver lost"):
        play()
    return holder["run"]


def finished_twice(e):
    with e["rec"].run("shop", "dev") as run, run.dataset("erp", "orders", "fp", {}) as ds:
        ds.succeed()
        ds.fail("again")


def outcome(r):
    return [s["status"] for s in r["steps"]], r["datasets"][0]["status"], [x["error_class"] for x in r["errors"]]


def snapshot(e, run):
    return e["store"].snapshot("shop", T0, datetime(2026, 11, 10, tzinfo=UTC))


def hooked(e):
    seen = []
    e["rec"].on_finish = (seen.append,)
    run = happy(e)
    return seen == [run.run_id]


CASES = [
    ("run id is the root span's trace id",
     lambda e: (r := happy(e)).run_id == r.span.trace_id == e["tracer"].finished[-1].trace_id, True),
    ("every span of the run shares its trace id",
     lambda e: len({s.trace_id for s in (happy(e), e["tracer"].finished)[1]}), 1),
    ("span tree: steps, dataset, run", lambda e: (happy(e), [s.name for s in e["tracer"].finished])[1],
     ["step land", "dataset erp.orders", "run shop"]),
    ("run status from its datasets", lambda e: happy(e).status, "succeeded"),
    ("steps follow the naming convention", lambda e: [s["step"] for s in report(e, happy(e))["steps"]],
     ["land", "quality.not_null", "recon.src_vs_silver", "detect.DEL-01"]),
    ("check and detector verdicts", lambda e: [s["status"] for s in report(e, happy(e))["steps"]],
     ["succeeded", "succeeded", "failed", "warned"]),
    ("watermark moves on success", lambda e: (happy(e), e["store"].watermark("erp", "orders"))[1], "9"),
    ("a dataset left open succeeds when its block ends",
     lambda e: [d["status"] for d in report(e, _open_ds(e))["datasets"]], ["succeeded"]),
    ("a failing step is logged failed, its dataset failed, the error kept",
     lambda e: outcome(report(e, failing_step(e))),
     (["failed"], "failed", ["ZeroDivisionError"])),
    ("a dying run closes its open datasets as failed",
     lambda e: (lambda r: (r["run"]["status"], r["datasets"][0]["status"], r["errors"][-1]["step"]))(  # noqa: PLC3002
         report(e, run_dies(e))), ("failed", "failed", "run")),
    ("finishing a dataset twice is refused", finished_twice, RecorderError),
    ("unknown check kind refused",
     lambda e: _with_ds(e, lambda ds: ds.check("vibes", "x", passed=True)), ValueError),
    ("job tags carry the run id", lambda e: (r := happy(e)).job_tags()["dp_run_id"] == r.run_id, True),
    ("job tags name domain, data product and run", lambda e: sorted(happy(e).job_tags("sales")),
     ["dp_data_product", "dp_domain", "dp_env", "dp_run_id"]),
    ("a finished run pushes its id to the hooks", hooked, True),
    ("lineage gets the same id as a UUID", lambda e: (r := happy(e)).lineage_run_id() == as_uuid(r.run_id), True),
    ("log fields name run and trace", lambda e: (r := happy(e)).log_fields()["trace_id"] == r.run_id, True),
    ("reader: duration from the clock", lambda e: (r := happy(e), snapshot(e, r).datasets[0])[1].ended_at
     - T0, __import__("datetime").timedelta(seconds=30)),
    ("reader: steps, files and errors keep their run id",
     lambda e: (r := happy(e), {x.run_id for x in [*snapshot(e, r).steps, *snapshot(e, r).files]})[1] == {r.run_id},
     True),
]


def _open_ds(e):
    with e["rec"].run("shop", "dev") as run, run.dataset("erp", "orders", "fp", {}):
        pass
    return run


def _with_ds(e, fn):
    with e["rec"].run("shop", "dev") as run, run.dataset("erp", "orders", "fp", {}) as ds:
        fn(ds)


@pytest.mark.parametrize(("case", "act", "expect"), CASES, ids=[c[0] for c in CASES])
def test_recorder(env, case, act, expect):
    if isinstance(expect, type) and issubclass(expect, Exception):
        with pytest.raises(expect):
            act(env)
    else:
        assert act(env) == expect
