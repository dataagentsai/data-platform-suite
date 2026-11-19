"""FIXTURE - a tiny fake runner that fills the run log until W1b-W1e produce real runs.

Nothing here ingests anything. For each world day it writes what a correct platform's run WOULD record, through the
real recorder into the real store port: one run per day with every enabled dataset, the rows the oracle says reach
bronze, the files a file drop delivers, and, for each incident the world planted that day, the detector step the
engine is expected to record (EFFECTS below). It also writes the telemetry a run would leave (a job with its tags,
lineage edges) and some readers of the gold tables. Day 2 has one manual re-run of erp.delivery that takes five times
as long, so the performance screen has a retry and a slow-down to show.

Alerts are pushed (ADR-024): the recorder calls Monitor.run_finished as each run ends; at the day's last minute the
scheduler's one wake-up runs a lateness check only if one is due, and the telemetry is copied once for the day.

What this proves: the recorder, the run log reader, the ops builder, freshness, alerts and the dashboards work
together on a realistic week. What it does NOT prove: that the engine detects anything. The scenarios
(dataplatform.twin) mark every check that needs the engine as pending until W1b-W1e land, then this module is
replaced by the real runs (the scenario runner's `obs` checks switch from this fixture to the engine's run log).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from dataplatform.adapters.memory_system import MemorySystemTables
from dataplatform.meta.port import MetadataStore
from dataplatform.obs.alerts import Alert, AlertConfig
from dataplatform.obs.clock import ManualClock
from dataplatform.obs.monitor import MemoryNotifier, Monitor
from dataplatform.obs.ops import Tables
from dataplatform.obs.recorder import DatasetHandle, RunRecorder
from dataplatform.obs.runlog import RunLogReader
from dataplatform.obs.system import AccessRow, JobRunRow, LineageRow
from dataplatform.obs.trace import Tracer

FIXTURE = True  # every row this module writes is synthetic


@dataclass(frozen=True)
class Effect:
    """What the run log shows when a detector meets its incident (the engine's expected behaviour, per DAC)."""

    detect: bool = True  # a detect.<id> step
    blocked: bool = False  # the detect step failed: the load stopped
    file_status: str = ""  # a file record held back with this status
    dataset_status: str = "succeeded"
    recon_failed: str = ""  # a failed recon.<entry> step instead of a detect step
    error_class: str = ""


EFFECTS: dict[str, Effect] = {
    "LATE-03": Effect(),
    "DEL-01": Effect(),
    "LATE-01": Effect(detect=False),  # seen as a lagging bookmark from the due day (freshness), see the scenario
    "DUP-01": Effect(file_status="skipped_duplicate"),
    "SCH-02": Effect(blocked=True, file_status="rejected_contract", error_class="ContractMismatch"),
    "SCD-02": Effect(),
    "DEL-04": Effect(blocked=True, file_status="rejected_volume", dataset_status="skipped"),
    "HIST-05": Effect(),
    "LATE-02": Effect(),
    "REC-05": Effect(detect=False, recon_failed="live_source_vs_silver"),
    "DUP-04": Effect(),
}
FILE_DROPS = ("lims.inspection_result", "partner.dealer_sellout")
RECON_DAILY = ("erp.delivery", "erp.sales_order_line")
OTHER_DEALERS = ("D-0001", "D-0002", "D-0003")
GOLD = ("fact_otif", "fact_sales", "fact_scrap")
RERUN = (2, "erp", "delivery")  # (day, source, dataset): a manual re-run, five times its usual time
DATA_PRODUCT = {"erp": "sales", "partner": "sales", "dealer_api": "sales"}  # job tags; others: manufacturing


@dataclass
class FixtureRun:
    run_ids: list[str] = field(default_factory=list)
    sent: list[Alert] = field(default_factory=list)


def _counts(detail: dict[str, Any]) -> dict[str, int]:
    out = {}
    for k, v in detail.items():
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            out[k] = v
        elif isinstance(v, list):
            out[k] = len(v)
    return out


class FixtureRunner:
    def __init__(self, store: MetadataStore, reader: RunLogReader, tracer: Tracer, clock: ManualClock,
                 domain: str, env: str, start: datetime, calendar: list[dict[str, Any]], config: AlertConfig):
        self.store, self.clock, self.domain, self.env, self.start = store, clock, domain, env, start
        self.system = MemorySystemTables()
        self.notifier = MemoryNotifier()
        self.monitor = Monitor(reader, self.system, store, domain, config, self.notifier, since=start)
        self.recorder = RunRecorder(store, tracer, on_finish=(self._pushed,))
        self.calendar = {c["id"]: c for c in calendar}
        self.jobs = 0
        self.out = FixtureRun()

    def day_start(self, day: int) -> datetime:
        return self.start + timedelta(days=day - 1)

    def _pushed(self, run_id: str) -> None:
        self.out.sent.extend(self.monitor.run_finished(run_id, self.clock.now()).sent)

    def run_day(self, day: int, oracle: dict[str, Any]) -> Tables:
        self.clock.set(self.day_start(day) + timedelta(hours=23))
        findings: dict[str, list[dict[str, Any]]] = {}
        for f in oracle["findings"]:
            findings.setdefault(f["dataset"], []).append(f)
        refs = [r for r in self.store.datasets(self.domain) if r.is_enabled]
        with self.recorder.run(self.domain, self.env, trigger="schedule") as run:
            for ref in refs:
                name = f"{ref.source}.{ref.dataset}"
                with run.dataset(ref.source, ref.dataset, ref.fingerprint, {"fixture": True}) as ds:
                    self._dataset(ds, name, day, oracle, findings.get(name, []))
        self.out.run_ids.append(run.run_id)
        if day == RERUN[0]:
            self._rerun(oracle)
        self._readers(day)
        self.clock.set(self.day_start(day) + timedelta(hours=23, minutes=59))
        woke = self.monitor.wake(self.clock.now())
        if woke:
            self.out.sent.extend(woke.sent)
        self.monitor.copy_system(self.clock.now())
        return self.monitor.tables(self.clock.now())

    def _rerun(self, oracle: dict[str, Any]) -> None:
        _, source, dataset = RERUN
        ref = next(r for r in self.store.datasets(self.domain) if (r.source, r.dataset) == (source, dataset))
        with self.recorder.run(self.domain, self.env, trigger="manual") as run, \
                run.dataset(source, dataset, ref.fingerprint, {"fixture": True}) as ds:
            started = self.clock.now()
            with ds.step("land") as step:
                step.counts["rows"] = 0
            self.clock.advance(5 * (30 + int(oracle["bronze"].get(f"{source}.{dataset}", 0)) / 200))
            ds.succeed({"rows_read": 0, "rows_written": 0})
            self._system(ds, f"{source}.{dataset}", started)
        self.out.run_ids.append(run.run_id)

    def _dataset(self, ds: DatasetHandle, name: str, day: int, oracle: dict[str, Any],
                 found: list[dict[str, Any]]) -> None:
        started = self.clock.now()
        read, written = int(oracle["sources"].get(name, 0)), int(oracle["bronze"].get(name, 0))
        with ds.step("land") as step:
            step.counts["rows"] = read
        self.clock.advance(20 + written / 200)
        effects = [(f, EFFECTS[f["detector"]]) for f in found]
        if name in FILE_DROPS and written:
            ds.file(f"{day}:{name}", f"{ds.dataset}_{day:02d}.csv", f"day-{day:02d}", f"c{day}", written, "loaded")
        status = "succeeded"
        for f, eff in effects:
            status = self._effect(ds, f, eff, read, written) or status
        for entry in [e.recon_failed for _, e in effects if e.recon_failed] or (
                ["live_source_vs_silver"] if name in RECON_DAILY else []):
            failed = any(e.recon_failed == entry for _, e in effects)
            ds.check("recon", entry, passed=not failed, message="sources to landing" if failed else "")
        with ds.step("bronze") as step:
            step.counts["rows"] = written
        self.clock.advance(10)
        counts = {"rows_read": read, "rows_written": written}
        if status == "skipped":
            ds.skip("blocked: the load stopped before bronze", counts)
        else:
            ds.succeed(counts, watermark=self.clock.now().isoformat(), bookmarks=self._bookmarks(name, day))
        self._system(ds, name, started)

    def _effect(self, ds: DatasetHandle, f: dict[str, Any], eff: Effect, read: int, written: int) -> str | None:
        detail = f.get("detail") or {}
        if eff.file_status:
            rows = int(detail.get("rows_delivered", max(read - written, 0)))
            ds.file(f"held:{f['incident']}", str(detail.get("file") or detail.get("first_file") or f["incident"]),
                    "held", "x", rows, eff.file_status, reason=f"{f['detector']}: {f['incident']}")
        if eff.error_class:
            ds.error("contract", eff.error_class, f"{f['incident']}: {detail}")
        if eff.detect:
            ds.detect(f["detector"], _counts(detail), f["incident"], blocked=eff.blocked)
        if f["detector"] == "HIST-05":  # the correction restates published windows (RSTM-01, gold/harness.py)
            windows = detail.get("windows") or [f"day-{self.calendar.get(f['incident'], {}).get('day', 0):02d}"]
            for w in windows[:3]:
                with ds.step("gold.restatement") as st:
                    st.counts.update({"rows.before": 12, "rows.after": 12, "net_value.before": 1000,
                                      "net_value.after": 940})
                    st.warn(f"window={w}; reason=correction plan {f['incident']}; detector=HIST-05")
        return eff.dataset_status if eff.dataset_status != "succeeded" else None

    def _bookmarks(self, name: str, day: int) -> dict[str, str]:
        if name != "partner.dealer_sellout":
            return {}
        late = self.calendar.get("sellout-file-late", {})
        params = late.get("params") or {}
        dealer, arrives = str(params.get("dealer", "D-0042")), int(late.get("day", 5))
        month_end = (self.start.replace(day=1) - timedelta(seconds=1))  # the month before the clock starts
        before = month_end.replace(day=1) - timedelta(seconds=1)
        marks = {d: month_end.isoformat() for d in OTHER_DEALERS} if day == 1 else {}
        if day == 1:
            marks[dealer] = before.isoformat()
        if day == arrives:
            marks[dealer] = month_end.isoformat()
        return marks

    def _system(self, ds: DatasetHandle, name: str, started: datetime) -> None:
        self.jobs += 1
        job = f"job-{self.jobs:05d}"
        run_id, ended = ds.run.run_id, self.clock.now()
        tags = ds.run.job_tags(DATA_PRODUCT.get(ds.source, "manufacturing"))
        self.system.job_rows.append(JobRunRow(job, name, run_id, started, ended,
                                              "succeeded" if ds.status != "failed" else "failed", tags))
        flat = name.replace(".", "__")
        bronze, silver = f"{self.domain}_dev.bronze.{flat}", f"{self.domain}_dev.silver.{flat}"
        self.system.lineage_rows += [LineageRow(ended, f"{self.domain}_dev.landing.{name}", bronze, run_id, job),
                                     LineageRow(ended, bronze, silver, run_id, job)]

    def _readers(self, day: int) -> None:
        at = self.day_start(day) + timedelta(hours=10)
        for n in range(2 + day // 2):
            for t in GOLD[: 1 + n % len(GOLD)]:
                self.system.access_rows.append(AccessRow(at + timedelta(minutes=n), f"analyst{n}@tyre.example",
                                                         "read", f"{self.domain}_prod.gold.{t}"))
