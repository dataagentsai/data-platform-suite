"""StampingRunLog: any metadata store, plus the run-log reader port (dataplatform.obs.runlog) in process.

It passes every call through to the store it wraps, unchanged, and stamps each run-log write with the clock port's
time. So the in-memory store (which keeps no times) can answer "which runs started this week, and how long did they
take", and a simulated week runs on simulated time. The PostgreSQL reader (postgres_runlog.py) answers the same port
from the store's own timestamps; both pass tests/dataplatform/test_obs_runlog.py (M5).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from dataplatform.meta.port import (
    BackfillPlan,
    Column,
    Correction,
    DatasetRef,
    FileRecord,
    MetadataStore,
    Setting,
    SettingDef,
    StepRecord,
)
from dataplatform.obs.clock import Clock
from dataplatform.obs.runlog import (
    DatasetRunRow,
    ErrorRow,
    FileRow,
    RunLogSnapshot,
    RunRow,
    StepRow,
)


class StampingRunLog:
    def __init__(self, inner: MetadataStore, clock: Clock) -> None:
        self.inner, self.clock = inner, clock
        self.order: list[str] = []
        self.run_times: dict[str, list[datetime | None]] = {}
        self.ds_times: dict[tuple[str, str, str], list[datetime | None]] = {}
        self.step_times: dict[str, list[datetime]] = {}
        self.file_times: dict[str, list[datetime]] = {}
        self.error_times: dict[str, list[datetime]] = {}

    # ---- configuration: passed through ----
    def register_settings(self, settings: list[SettingDef]) -> None:
        self.inner.register_settings(settings)

    def load_domain(self, doc: dict[str, Any]) -> None:
        self.inner.load_domain(doc)

    def load_dataset(self, doc: dict[str, Any]) -> None:
        self.inner.load_dataset(doc)

    def settings_registry(self) -> list[SettingDef]:
        return self.inner.settings_registry()

    def datasets(self, domain: str) -> list[DatasetRef]:
        return self.inner.datasets(domain)

    def settings(self, source: str, dataset: str) -> list[Setting]:
        return self.inner.settings(source, dataset)

    def columns(self, source: str, dataset: str) -> list[Column]:
        return self.inner.columns(source, dataset)

    def business_keys(self, source: str, dataset: str, kind: str = "primary") -> list[str]:
        return self.inner.business_keys(source, dataset, kind)

    def dataset_view(self, source: str, dataset: str) -> dict[str, Any]:
        return self.inner.dataset_view(source, dataset)

    # ---- run log: passed through, stamped after the store accepts the write ----
    def start_run(self, run_id: str, domain: str, env: str, trigger: str = "manual",
                  run_type: str = "normal") -> None:
        self.inner.start_run(run_id, domain, env, trigger, run_type)
        self.order.append(run_id)
        self.run_times[run_id] = [self.clock.now(), None]

    def start_dataset_run(self, run_id: str, source: str, dataset: str, config_fingerprint: str,
                          config: dict[str, Any]) -> str | None:
        before = self.inner.start_dataset_run(run_id, source, dataset, config_fingerprint, config)
        self.ds_times[(run_id, source, dataset)] = [self.clock.now(), None]
        return before

    def log_step(self, step: StepRecord) -> None:
        self.inner.log_step(step)
        self.step_times.setdefault(step.run_id, []).append(self.clock.now())

    def log_file(self, file: FileRecord) -> None:
        self.inner.log_file(file)
        self.file_times.setdefault(file.run_id, []).append(self.clock.now())

    def batch_status(self, source: str, dataset: str, batch_key: str) -> str | None:
        return self.inner.batch_status(source, dataset, batch_key)

    def log_error(self, run_id: str, source: str | None, dataset: str | None, step: str, error_class: str,
                  message: str) -> None:
        self.inner.log_error(run_id, source, dataset, step, error_class, message)
        self.error_times.setdefault(run_id, []).append(self.clock.now())

    def finish_dataset_run(self, run_id: str, source: str, dataset: str, status: str,
                           counts: dict[str, int] | None = None, watermark: str | None = None,
                           bookmarks: dict[str, str] | None = None, message: str = "") -> None:
        self.inner.finish_dataset_run(run_id, source, dataset, status, counts, watermark, bookmarks, message)
        self.ds_times[(run_id, source, dataset)][1] = self.clock.now()

    def finish_run(self, run_id: str) -> str:
        status = self.inner.finish_run(run_id)
        self.run_times[run_id][1] = self.clock.now()
        return status

    def watermark(self, source: str, dataset: str) -> str | None:
        return self.inner.watermark(source, dataset)

    def bookmarks(self, source: str, dataset: str) -> dict[str, str]:
        return self.inner.bookmarks(source, dataset)

    def reset_watermark(self, source: str, dataset: str, value: str | None, reason: str,
                        changed_by: str) -> None:
        self.inner.reset_watermark(source, dataset, value, reason, changed_by)

    def run_report(self, run_id: str) -> dict[str, Any]:
        return self.inner.run_report(run_id)

    def dataset_files(self, source: str, dataset: str, window: str | None = None) -> list[FileRecord]:
        return self.inner.dataset_files(source, dataset, window)

    # ---- corrections (ADR-017): passed through, not stamped (they are not run-log rows) ----
    def record_corrections(self, plan: BackfillPlan, corrections: list[Correction]) -> None:
        self.inner.record_corrections(plan, corrections)

    def corrections(self, source: str, dataset: str, as_of: str | None = None) -> list[Correction]:
        return self.inner.corrections(source, dataset, as_of)

    def backfill_plans(self, source: str, dataset: str) -> list[BackfillPlan]:
        return self.inner.backfill_plans(source, dataset)

    # ---- the reader port ----
    # ---- 13 Nov additions to the store port: passed through ----
    def acquire_lease(self, table: str, run_id: str, ttl_seconds: int) -> str | None:
        return self.inner.acquire_lease(table, run_id, ttl_seconds)

    def release_lease(self, table: str, run_id: str) -> None:
        self.inner.release_lease(table, run_id)

    def void_batch(self, source: str, dataset: str, batch_key: str, reason: str, approved_by: str) -> None:
        self.inner.void_batch(source, dataset, batch_key, reason, approved_by)

    def set_dq_rules(self, source: str, dataset: str, rules: list[Any]) -> None:
        self.inner.set_dq_rules(source, dataset, rules)

    def dq_rules(self, source: str, dataset: str) -> list[Any]:
        return self.inner.dq_rules(source, dataset)

    def log_dq_results(self, results: list[Any]) -> None:
        self.inner.log_dq_results(results)

    def dq_results(self, run_id: str, source: str | None = None, dataset: str | None = None) -> list[Any]:
        return self.inner.dq_results(run_id, source, dataset)

    def save_profile(self, run_id: str, source: str, dataset: str, profile: dict[str, Any]) -> None:
        self.inner.save_profile(run_id, source, dataset, profile)

    def last_profile(self, source: str, dataset: str) -> dict[str, Any] | None:
        return self.inner.last_profile(source, dataset)

    def snapshot(self, domain: str, since: datetime, until: datetime) -> RunLogSnapshot:
        out = RunLogSnapshot()
        for run_id in self.order:
            started, ended = self.run_times[run_id]
            if started is None or not since <= started < until:
                continue
            report = self.inner.run_report(run_id)
            if report["run"]["domain"] != domain:
                continue
            self._add(out, run_id, report, started, ended)
        return out

    def _add(self, out: RunLogSnapshot, run_id: str, report: dict[str, Any], started: datetime,
             ended: datetime | None) -> None:
        r = report["run"]
        out.runs.append(RunRow(run_id, r["domain"], r["env"], r["trigger"], r["run_type"], r["status"], started,
                               ended))
        for d in report["datasets"]:
            t0, t1 = self.ds_times[(run_id, d["source"], d["dataset"])]
            if t0 is None:  # never: stamped when the store accepted the start
                continue
            out.datasets.append(DatasetRunRow(run_id, d["source"], d["dataset"], d["status"],
                                              d["config_fingerprint"], d["watermark_before"],
                                              d["watermark_after"], dict(d["counts"]), d["message"], t0, t1))
        for s, at in zip(report["steps"], self.step_times.get(run_id, []), strict=True):
            out.steps.append(StepRow(run_id, s["source"], s["dataset"], s["step"], s["status"], dict(s["counts"]),
                                     s["message"], at))
        for f, at in zip(report["files"], self.file_times.get(run_id, []), strict=True):
            out.files.append(FileRow(run_id, f["source"], f["dataset"], f["batch_key"], f["file_name"],
                                     f["window"], f["rows"], f["status"], f["reason"], at))
        for e, at in zip(report["errors"], self.error_times.get(run_id, []), strict=True):
            out.errors.append(ErrorRow(run_id, e["source"], e["dataset"], e["step"], e["error_class"],
                                       e["message"], at))
