"""Contract table for the run-log reader port (ADR-020, M5): the stamping memory adapter and the PostgreSQL adapter
answer the same window the same way. Times differ by adapter (simulated clock vs the database's clock), so rows are
compared without them, and the times are checked for order instead.

PostgreSQL rows run when a local server answers (DP_OBS_TEST_DSN, default dbname=dp_obs_test; created if missing)
and are skipped otherwise. Each row starts from a freshly installed schema.
"""

import os
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_runlog import StampingRunLog
from dataplatform.adapters.postgres_runlog import parse
from dataplatform.meta.port import SettingDef
from dataplatform.obs.clock import ManualClock
from dataplatform.obs.recorder import RunRecorder
from dataplatform.obs.trace import LocalTracer

PG_DSN = os.environ.get("DP_OBS_TEST_DSN", "dbname=dp_obs_test")
T0 = datetime(2026, 11, 9, 17, 30, tzinfo=UTC)
TIMES = ("started_at", "ended_at", "at")
DOMAINS = [{"domain": d, "business_timezone": "UTC", "environments": ["dev"], "settings": {},
            "manifest": {"kind": "platform"}, "fingerprint": d} for d in ("shop", "bank")]


def ds_doc(domain, source, name):
    return {"domain": domain, "source_name": source, "source_env": "prod", "dataset_name": name, "is_enabled": True,
            "kind": "dataset", "archetypes": [], "ingest_mode": "push", "ingest_read": "change_feed",
            "history_kind": "transaction", "load_pattern": "history", "contract_id": "c", "contract_version": "1",
            "fingerprint": f"fp-{name}", "settings": {}, "columns": [], "business_keys": {}, "manifest": {}}


def seed(store):
    store.register_settings([SettingDef("cost_center", "dataset", "string")])
    for d in DOMAINS:
        store.load_domain(d)
    store.load_dataset(ds_doc("shop", "erp", "orders"))
    store.load_dataset(ds_doc("shop", "erp", "returns"))
    store.load_dataset(ds_doc("bank", "core", "accounts"))


def play(store, tick):
    """Two shop runs (one with a failure) and one bank run, through the recorder."""
    rec = RunRecorder(store, LocalTracer())
    with rec.run("shop", "dev", "schedule") as r1:
        with r1.dataset("erp", "orders", "fp", {}) as ds:
            with ds.step("land") as st:
                st.counts["rows"] = 4
            tick()
            ds.check("quality", "not_null", passed=True)
            ds.detect("DEL-01", {"keys": 2})
            ds.file("k1", "a.csv", "w1", "c1", 4, "loaded")
            ds.file("k1", "a2.csv", "w1", "c1", 4, "skipped_duplicate", reason="same batch")
            ds.succeed({"rows_written": 4}, watermark="10")
        with r1.dataset("erp", "returns", "fp", {}) as ds:
            ds.error("contract", "ContractMismatch", "column renamed")
            ds.fail("held")
    tick()
    with rec.run("bank", "dev") as r3, r3.dataset("core", "accounts", "fp", {}) as ds:
        ds.succeed()
    tick()
    with rec.run("shop", "dev", "manual", "reload") as r2, r2.dataset("erp", "orders", "fp", {}) as ds:
        ds.succeed({"rows_written": 1}, watermark="11")
    return [r1.run_id, r2.run_id]


def memory():
    clock = ManualClock(T0)
    store = StampingRunLog(MemoryMetaStore(), clock)
    seed(store)
    return store, store, lambda: clock.advance(60), (T0 - timedelta(minutes=1), T0 + timedelta(hours=1))


_pg = {}


def postgres():
    if "conn" not in _pg:
        try:
            import psycopg

            from dataplatform.adapters.postgres_meta import create_database

            create_database(PG_DSN)
            _pg["conn"] = psycopg.connect(PG_DSN, autocommit=True)
        except Exception as exc:
            _pg["conn"], _pg["why"] = None, str(exc)
    if _pg["conn"] is None:
        pytest.skip(f"no local PostgreSQL: {_pg['why']}")
    from dataplatform.adapters import postgres_runlog
    from dataplatform.adapters.postgres_meta import PostgresMetaStore, install

    conn = _pg["conn"]
    conn.execute("DROP SCHEMA IF EXISTS config, runtime CASCADE")
    conn.autocommit = False
    install(conn)
    postgres_runlog.install(conn)
    conn.autocommit = True
    store = PostgresMetaStore(conn)
    seed(store)
    now = datetime.now(UTC)
    return store, postgres_runlog.PostgresRunLogReader(conn), lambda: None, (now - timedelta(minutes=5),
                                                                              now + timedelta(hours=1))


def customer():
    """Level 2 of ADR-024 rule 6: the customer's own run log on the port. Pending until a customer's mapping and a
    sample of their rows exist (adapters/customer_runlog.py says how to fill it in)."""
    pytest.skip("pending: no customer run log connected yet (adapters/customer_runlog.py is an example skeleton)")


ADAPTERS = {"memory": memory, "postgres": postgres, "customer": customer}


def plain(rows):
    return [{k: v for k, v in asdict(r).items() if k not in TIMES and k != "run_id"} for r in rows]


def runs(snap, ids):
    return [(ids.index(r.run_id), r.trigger, r.run_type, r.status) for r in snap.runs]


def ordered(snap):
    ok = all(r.ended_at is not None and r.ended_at >= r.started_at for r in snap.runs)
    ok &= all(d.ended_at is not None and d.ended_at >= d.started_at for d in snap.datasets)
    by_run = {r.run_id: r for r in snap.runs}
    return ok and all(by_run[s.run_id].started_at <= s.at <= by_run[s.run_id].ended_at for s in snap.steps)


CASES = [
    ("only the domain's runs, in start order", runs,
     [(0, "schedule", "normal", "partial"), (1, "manual", "reload", "succeeded")]),
    ("dataset runs with status, counts and watermarks", lambda snap, ids: plain(snap.datasets),
     [{"source": "erp", "dataset": "orders", "status": "succeeded", "config_fingerprint": "fp",
       "watermark_before": None, "watermark_after": "10", "counts": {"rows_written": 4}, "message": ""},
      {"source": "erp", "dataset": "returns", "status": "failed", "config_fingerprint": "fp",
       "watermark_before": None, "watermark_after": None, "counts": {}, "message": "held"},
      {"source": "erp", "dataset": "orders", "status": "succeeded", "config_fingerprint": "fp",
       "watermark_before": "10", "watermark_after": "11", "counts": {"rows_written": 1}, "message": ""}]),
    ("steps in record order", lambda snap, ids: [(s.step, s.status) for s in snap.steps],
     [("land", "succeeded"), ("quality.not_null", "succeeded"), ("detect.DEL-01", "warned")]),
    ("files with outcome and reason", lambda snap, ids: [(f.file_name, f.status, f.reason) for f in snap.files],
     [("a.csv", "loaded", ""), ("a2.csv", "skipped_duplicate", "same batch")]),
    ("errors with dataset and class", lambda snap, ids: [(e.dataset, e.error_class) for e in snap.errors],
     [("returns", "ContractMismatch")]),
    ("every record keyed by a run in the window",
     lambda snap, ids: {x.run_id for x in [*snap.datasets, *snap.steps, *snap.files, *snap.errors]} <= set(ids),
     True),
    ("times are aware and in order", lambda snap, ids: ordered(snap), True),
]


@pytest.mark.parametrize("adapter", list(ADAPTERS))
@pytest.mark.parametrize(("case", "check", "expect"), CASES, ids=[c[0] for c in CASES])
def test_runlog_reader_contract(adapter, case, check, expect):
    store, reader, tick, (since, until) = ADAPTERS[adapter]()
    ids = play(store, tick)
    assert check(reader.snapshot("shop", since, until), ids) == expect


@pytest.mark.parametrize("adapter", list(ADAPTERS))
def test_window_excludes_runs_outside_it(adapter):
    store, reader, tick, (since, until) = ADAPTERS[adapter]()
    play(store, tick)
    assert reader.snapshot("shop", until, until + timedelta(days=1)).runs == []
    assert reader.snapshot("shop", since - timedelta(days=1), since).runs == []


def test_parse_reads_the_procedure_document():
    doc = {"runs": [{"run_id": "r", "domain": "shop", "env": "dev", "trigger": "t", "run_type": "normal",
                     "status": "running", "started_at": "2026-11-09T10:00:00+00:00", "ended_at": None}],
           "datasets": [], "steps": [], "files": [{"run_id": "r", "source": "s", "dataset": "d", "batch_key": "b",
                                                    "file_name": "f", "window": "w", "rows": "3", "status": "loaded",
                                                    "reason": "", "at": "2026-11-09T10:00:01.5+00:00"}],
           "errors": []}
    snap = parse(doc)
    assert (snap.runs[0].ended_at, snap.files[0].rows, snap.files[0].at.microsecond) == (None, 3, 500000)
