"""One ingestion pipeline per source type, run through the runner against in-memory sources, store and bronze.

Table-driven: each row is a short story (what the sources do, which runs happen) and what the store and bronze must
show afterwards. Covers idempotency (DUP-01, DUP-02), the run log flow, bookmarks and watermarks, the mass-delete
guard (DEL-04), schema stops (SCH-02), what is noticed (DEL-01, SCD-02, LATE-03, DUP-04), reload and backfill (HIST-04)
and restarting after a failed run without reading a batch twice.

Rules proven here (DAC): FILE-01 (every delivered file ends with a recorded outcome).
"""

import copy

import pytest

from dataplatform.adapters.files import FileLanding
from dataplatform.adapters.keyed_tokens import KeyedTokenizer
from dataplatform.adapters.local_movers import ChangeFeedMover, DropMover
from dataplatform.adapters.memory_bronze import MemoryBronze
from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_sources import MemoryFileDrop, MemoryRecords, MemorySourceDb, MemoryStream
from dataplatform.ingest.pipelines import Plan, Sources, stream_events
from dataplatform.ingest.runner import ingest
from dataplatform.ingest.steps import Ctx, Spec
from dataplatform.meta import registry
from dataplatform.source_ports import SourceChange, TableSpec

TZ = "Asia/Kolkata"
W1, W2, W3 = "2026-11-09", "2026-11-10", "2026-11-11"


def col(name, t="string", pk=None, cls=""):
    return {
        "name": name,
        "logical_type": t,
        "required": pk is not None,
        "primary_key_position": pk,
        "description": "",
        "classification": cls,
    }


DATASETS = {
    ("erp", "line"): (
        "push",
        "change_feed",
        [col("id", pk=1), col("qty", "integer"), col("change_seq", "integer")],
        {"watermark_column": "change_seq"},
        {"history_order_by": "change_seq"},
        {"min": 1},
    ),
    ("erp", "mat"): (
        "pull",
        "full_copy",
        [col("code", pk=1), col("name"), col("extracted_at", "timestamp")],
        {},
        {"mass_delete_max_share": 0.05},
        {"min": 1},
    ),
    ("lab", "insp"): (
        "push",
        "files",
        [col("id", pk=1), col("n", "integer")],
        {"file_format_data": "csv", "csv_delimiter": ";", "file_pattern": "insp_*.csv"},
        {"new_columns": "stop"},
        {"min": 1},
    ),
    ("api", "dealer"): (
        "pull",
        "pages",
        [col("code", pk=1), col("email", cls="restricted"), col("updated_at", "timestamp")],
        {"watermark_column": "updated_at"},
        {},
        {"min": 0},
    ),
    ("bus", "reading"): (
        "stream",
        "events",
        [col("reading_id", pk=1), col("event_ts", "timestamp"), col("v", "number")],
        {"watermark_column": "event_ts"},
        {},
        {"min": 1},
    ),
}
LINE = TableSpec("line", (("id", "string"), ("qty", "integer"), ("change_seq", "integer")), ("id",), change_feed=True)
MAT = TableSpec(
    "mat", (("code", "string"), ("name", "string"), ("extracted_at", "timestamp")), ("code",), full_copy=True
)


class Rig:
    def __init__(self, tmp):
        self.store = MemoryMetaStore()
        self.store.register_settings(registry.load())
        self.store.load_domain({"domain": "t", "settings": {}, "manifest": {}, "fingerprint": "d"})
        for (src, ds), spec in DATASETS.items():
            self.load(src, ds, *spec)
        self.db, self.drop, self.api, self.stream = MemorySourceDb(), MemoryFileDrop(), MemoryRecords(), MemoryStream(2)
        self.db.ensure("erp_db", [LINE, MAT])
        self.landing = FileLanding(tmp / "landing")
        self.bronze = MemoryBronze()
        self.sources = Sources(
            self.landing,
            self.db,
            self.api,
            self.stream,
            {"erp": "erp_db", "lab": "lab_drop", "api": "dealer_api", "bus": "bus"},
            TZ,
        )
        self.seq = 0
        self.runs = 0

    def load(self, src, ds, mode, read, columns, settings, load, volume):
        self.store.load_dataset(
            {
                "domain": "t",
                "source_name": src,
                "source_env": "",
                "dataset_name": ds,
                "is_enabled": True,
                "kind": "dataset",
                "archetypes": [],
                "ingest_mode": mode,
                "ingest_read": read,
                "history_kind": "",
                "load_pattern": "",
                "contract_id": ds,
                "contract_version": "1",
                "fingerprint": f"{ds}-{load}",
                "settings": settings,
                "columns": columns,
                "business_keys": {"primary": [columns[0]["name"]]},
                "manifest": {"load": load, "volume": {"expected_rows_per_delivery": volume}},
            }
        )

    # ---- what the sources do
    def change(self, op, id_, qty, seq=None, at="2026-11-09T05:00:00+00:00"):
        self.seq += 1
        s = seq if seq is not None else self.seq
        self.db.apply("erp_db", LINE, [SourceChange(op, {"id": id_, "qty": qty, "change_seq": s}, s, at)])

    def copy(self, at, codes):
        self.db.publish_copy("erp_db", MAT, at, [{"code": c, "name": f"m{c}"} for c in codes])

    def file(self, name, body, at="2026-11-09T06:00:00+00:00"):
        self.drop.put("lab_drop", "lab.insp", name, body, at)

    def dealer(self, code, at):
        self.api.upsert(
            "dealer_api", "dealer", code, {"code": code, "email": f"{code}@x.example", "updated_at": at}, at
        )

    def readings(self, items):
        self.stream.append(
            "reading",
            [(f"P{i % 3}", at, {"reading_id": rid, "event_ts": ts, "v": 1.0}) for i, (rid, ts, at) in enumerate(items)],
        )

    # ---- the platform
    def run(self, window=W1, run_type="normal", windows=(), silver=None):
        ChangeFeedMover(self.db, self.landing).move("erp_db", "erp", LINE, window)
        DropMover(self.drop, self.landing, TZ).move("lab_drop", "lab.insp", "lab", "insp")
        self.runs += 1
        return ingest(
            self.store,
            self.bronze,
            KeyedTokenizer(b"k" * 32),
            self.sources,
            "t",
            "dev",
            f"r{self.runs}",
            Plan(run_type, window, tuple(windows)),
            silver=silver,
        ).by_name()

    def rows(self, ds):
        return self.bronze.fingerprint(ds)["row_count"]


def ts(minute, second=0):
    return f"2026-11-09T05:{minute:02d}:{second:02d}+00:00"


# ---------------------------------------------------------------- change feed (push)
def test_change_feed_lands_and_notices(tmp_path):
    r = Rig(tmp_path)
    r.change("I", "A", 5)
    r.change("I", "B", 1)
    r.change("U", "A", 7, seq=10)
    r.change("U", "A", 6, seq=9)  # an older change committed after a newer one: SCD-02
    r.change("D", "B", 1)  # a hard delete: DEL-01
    d = r.run()["erp.line"]
    assert (d.status, d.counts["loaded"], d.counts["rows_loaded"]) == ("succeeded", 1, 5)
    assert d.noticed == {"DEL-01": 1, "SCD-02": 1}
    assert r.store.bookmarks("erp", "line") == {"feed_pos": "5"} and r.store.watermark("erp", "line") == "10"
    ops = sorted(x["_op"] for x in r.bronze.tables["erp.line"])
    assert ops == ["D", "I", "I", "U", "U"]  # bronze keeps every change as delivered (RAW-01)


@pytest.mark.parametrize(
    ("case", "second", "status", "rows"),
    [
        ("a rerun with nothing new is skipped and lands nothing", lambda r: r.run(), "skipped", 2),
        (
            "only the new changes are delivered next time",
            lambda r: (r.change("I", "C", 1), r.run(W2))[1],
            "succeeded",
            3,
        ),
    ],
)
def test_change_feed_second_run(tmp_path, case, second, status, rows):
    r = Rig(tmp_path)
    r.change("I", "A", 1)
    r.change("I", "B", 1)
    r.run()
    assert second(r)["erp.line"].status == status
    assert r.rows("erp.line") == rows


# ---------------------------------------------------------------- files (push)
GOOD = b"id;n\nA;1\nB;2\n"


@pytest.mark.parametrize(
    ("case", "files", "statuses", "rows"),
    [
        ("a file loads", [("insp_1.csv", GOOD)], ["loaded"], 2),
        (
            "a resend under a new name is skipped (DUP-01)",
            [("insp_1.csv", GOOD), ("insp_1_resent.csv", GOOD)],
            ["loaded", "skipped_duplicate"],
            2,
        ),
        ("a file the pattern does not name is left alone", [("other.csv", GOOD)], [], 0),
        (
            "a renamed column stops the file, naming it (SCH-02)",
            [("insp_1.csv", b"id;count\nA;1\n")],
            ["rejected_contract"],
            0,
        ),
        (
            "a new column stops the file when the manifest says stop",
            [("insp_1.csv", b"id;n;x\nA;1;y\n")],
            ["rejected_contract"],
            0,
        ),
    ],
)
def test_files(tmp_path, case, files, statuses, rows):
    r = Rig(tmp_path)
    for name, body in files:
        r.file(name, body)
    r.run()
    assert [f.status for f in r.store.dataset_files("lab", "insp")] == statuses
    assert r.rows("lab.insp") == rows


def test_a_stopped_file_names_the_column_and_fails_the_dataset(tmp_path):
    r = Rig(tmp_path)
    r.file("insp_1.csv", GOOD)
    r.file("insp_2.csv", b"id;count\nC;1\n", "2026-11-09T07:00:00+00:00")
    d = r.run()["lab.insp"]
    assert d.status == "failed" and "missing contract column(s) n" in d.reasons[0]
    report = r.store.run_report("r1")
    assert report["run"]["status"] == "partial"  # other datasets did fine
    assert [e["error_class"] for e in report["errors"]] == ["ContractBreak"]


# ---------------------------------------------------------------- full copy (pull)
@pytest.mark.parametrize(
    ("case", "copies", "statuses", "current_keys"),
    [
        ("the first copy loads", [("2026-11-08T19:30:00+00:00", "ABCDEFGHIJ")], ["loaded"], 10),
        (
            "only the newest of two copies is read",
            [("2026-11-08T19:30:00+00:00", "AB"), ("2026-11-08T20:30:00+00:00", "ABC")],
            ["loaded"],
            3,
        ),
    ],
)
def test_full_copy(tmp_path, case, copies, statuses, current_keys):
    r = Rig(tmp_path)
    for at, codes in copies:
        r.copy(at, codes)
    r.run()
    assert [f.status for f in r.store.dataset_files("erp", "mat")] == statuses
    assert r.rows("erp.mat") == current_keys


class SilverKeys:
    """The CurrentKeys port as silver would answer it (W1c): the current, not deleted keys, or None."""

    def __init__(self, keys):
        self.keys = keys

    def current_keys(self, source, dataset, keys):
        return None if self.keys is None else {(k,) for k in self.keys}


@pytest.mark.parametrize(
    ("case", "first", "silver", "second", "status"),
    [
        ("silver's keys are the comparison (DPC-0054): a short first copy is held", None, "ABCDEFGHIJ", "ABCDEF",
         "failed"),
        ("silver never written: the last accepted copy in bronze is the comparison", "ABCDEFGHIJ", None, "ABCDEF",
         "failed"),
        ("keys silver already marked deleted do not count as missing", "ABCDEFGHIJ", "ABCDEF", "ABCDEF",
         "succeeded"),
        ("a copy as long as silver loads", None, "ABCDEF", "ABCDEFG", "succeeded"),
    ],
)
def test_mass_delete_guard_compares_with_silver(tmp_path, case, first, silver, second, status):
    r = Rig(tmp_path)
    if first:
        r.copy("2026-11-08T19:30:00+00:00", first)
        r.run(W1)
    r.copy("2026-11-09T19:30:00+00:00", second)
    assert r.run(W2, silver=SilverKeys(silver))["erp.mat"].status == status, case


def test_mass_delete_is_held_then_the_next_good_copy_loads(tmp_path):
    r = Rig(tmp_path)
    r.copy("2026-11-08T19:30:00+00:00", "ABCDEFGHIJ")
    r.run(W1)
    r.copy("2026-11-09T19:30:00+00:00", "ABCDEF")  # 40% missing: DEL-04
    d = r.run(W2)["erp.mat"]
    assert d.status == "failed" and d.counts["held_mass_delete"] == 1 and "40.0%" in d.reasons[0]
    assert r.rows("erp.mat") == 10  # nothing appended
    r.copy("2026-11-10T19:30:00+00:00", "ABCDEFGHIJK")  # compared with the last accepted copy, not the held one
    assert r.run(W3)["erp.mat"].status == "succeeded" and r.rows("erp.mat") == 21


def test_backfill_retries_a_held_copy_after_the_manifest_changes(tmp_path):
    r = Rig(tmp_path)
    r.copy("2026-11-08T19:30:00+00:00", "ABCDEFGHIJ")
    r.run(W1)
    r.copy("2026-11-09T19:30:00+00:00", "ABCDEF")
    r.run(W2)
    r.load("erp", "mat", *DATASETS[("erp", "mat")][:4], {"mass_delete_max_share": 0.5}, {"min": 1})
    d = r.run(W3, "backfill", [W2])["erp.mat"]
    assert d.counts["loaded"] == 1 and r.rows("erp.mat") == 16
    assert r.store.run_report("r3")["run"]["run_type"] == "backfill"


# ---------------------------------------------------------------- pages (pull)
def test_pages_from_the_cursor_with_personal_data_tokenised(tmp_path):
    r = Rig(tmp_path)
    for i in range(450):
        r.dealer(f"D{i:04d}", ts(i // 60, i % 60))
    d = r.run()["api.dealer"]
    assert (d.counts["loaded"], d.counts["rows_loaded"]) == (3, 450)  # pages of 200
    assert not [x for x in r.bronze.tables["api.dealer"] if "@" in str(x["email"])]
    r.dealer("D0001", ts(59))  # one change since the cursor
    assert r.run(W2)["api.dealer"].counts["rows_loaded"] == 1
    assert r.store.watermark("api", "dealer").startswith("2026-11-09T05:59:00")


# ---------------------------------------------------------------- stream (trigger-once)
def test_stream_reads_each_shard_from_its_checkpoint(tmp_path):
    r = Rig(tmp_path)
    r.readings([(f"R{i}", ts(1, i), ts(1, i)) for i in range(30)])
    d = r.run()["bus.reading"]
    assert d.counts["rows_loaded"] == 30 and set(r.store.bookmarks("bus", "reading")) == set(r.stream.shards("reading"))
    r.readings([("R30", ts(2), ts(2))])
    assert r.run(W2)["bus.reading"].counts["rows_loaded"] == 1  # never read twice


@pytest.mark.parametrize(
    ("case", "items", "noticed"),
    [
        ("in order", [(f"R{i}", ts(1, i), ts(1, i)) for i in range(9)], {}),
        (
            "a reading 30 s behind (LATE-03)",
            [("R1", ts(1, 0), ts(1, 0)), ("R2", ts(1, 40), ts(1, 40)), ("R3", ts(1, 10), ts(1, 41))],
            {"LATE-03": 1},
        ),
        ("small jitter is not late", [("R1", ts(1, 2), ts(1, 2)), ("R2", ts(1, 1), ts(1, 3))], {}),
        (
            "a replay is counted, and kept in bronze (DUP-04)",
            [("R1", ts(1, 0), ts(1, 0)), ("R1", ts(1, 0), ts(2, 0))],
            {"DUP-04": 1},
        ),
    ],
)
def test_stream_notices(tmp_path, case, items, noticed):
    r = Rig(tmp_path)
    r.stream.append(
        "reading", [("same-press", at, {"reading_id": rid, "event_ts": t, "v": 1.0}) for rid, t, at in items]
    )
    d = r.run()["bus.reading"]
    assert d.noticed == noticed and d.counts["rows_loaded"] == len(items)


def test_a_failed_run_never_makes_the_next_read_a_batch_twice(tmp_path):
    """ORCH-03: the file log commits each batch, so after a run fails midway the next run starts after the last batch
    loaded (though bookmarks stayed) and nothing is loaded twice."""
    r = Rig(tmp_path)
    r.readings([(f"R{i}", ts(1, i), ts(1, i)) for i in range(10)])
    view = r.store.dataset_view("bus", "reading")
    r.store.start_run("crash", "t", "dev")
    r.store.start_dataset_run("crash", "bus", "reading", "fp", view)
    stream_events(
        Ctx(Spec.from_view(view), r.store, r.bronze, KeyedTokenizer(b"k" * 32), "crash"), r.sources, Plan("normal", W1)
    )
    r.store.finish_dataset_run("crash", "bus", "reading", "failed")  # bookmarks stay where they were
    assert r.store.bookmarks("bus", "reading") == {}
    assert r.run()["bus.reading"].status == "skipped" and r.rows("bus.reading") == 10


# ---------------------------------------------------------------- reload (DUP-02, HIST-04) and the run log
def everything(r):
    r.change("I", "A", 1)
    r.copy("2026-11-08T19:30:00+00:00", "ABC")
    r.file("insp_1.csv", GOOD)
    r.dealer("D1", ts(1))
    r.readings([("R1", ts(1), ts(1)), ("R2", ts(2), ts(2))])


def fingerprints(r):
    return {
        t: (r.bronze.fingerprint(t)["row_count"], sorted(map(str, (x["_batch_key"] for x in rows))))
        for t, rows in r.bronze.tables.items()
    }


def test_reload_of_a_window_changes_nothing(tmp_path):
    r = Rig(tmp_path)
    everything(r)
    r.run(W1)
    before = copy.deepcopy(fingerprints(r))
    report = r.run(W2, "reload", [W1])
    assert fingerprints(r) == before  # DUP-02: a rerun leaves every table as it was
    assert {k: v.counts["loaded"] for k, v in report.items()} == {
        "api.dealer": 0,
        "bus.reading": len(r.stream.shards("reading")),
        "erp.line": 1,
        "erp.mat": 1,
        "lab.insp": 1,
    }
    assert r.store.run_report("r2")["run"]["run_type"] == "reload"
    assert report["api.dealer"].status == "skipped"  # an interface keeps no history: bronze is its replay point
    assert "replay point" in next(s["message"] for s in r.store.run_report("r2")["steps"] if s["dataset"] == "dealer")


def test_every_run_goes_through_the_run_log(tmp_path):
    r = Rig(tmp_path)
    everything(r)
    r.run(W1)
    rep = r.store.run_report("r1")
    assert rep["run"]["status"] == "succeeded"
    assert sorted(f"{d['source']}.{d['dataset']}" for d in rep["datasets"]) == sorted(f"{s}.{d}" for s, d in DATASETS)
    assert all(d["config_fingerprint"] for d in rep["datasets"])
    steps = [s["step"] for s in rep["steps"]]
    assert sorted(s for s in steps if s != "profile.batches") == sorted(
        f"ingest.{m}.{rd}" for m, rd, *_ in DATASETS.values())
    assert steps.count("profile.batches") == len(DATASETS)  # every dataset that loaded is profiled (B9)
    assert len(rep["files"]) == 6  # one change delivery, one copy, one file, one page, two shards
