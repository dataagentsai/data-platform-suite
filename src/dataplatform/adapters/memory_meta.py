"""The metadata store in memory, for unit tests. Same port and the same refusals as the PostgreSQL adapter; the
contract test table (tests/dataplatform/test_meta_store.py) runs every row against both (M5)."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from dataplatform.meta.port import (
    DATASET_RUN_END,
    DQ_ACTIONS,
    DQ_SEVERITIES,
    FILE_STATUSES,
    KEY_KINDS,
    LOAD_TYPES,
    MIN_REASON,
    RUN_TYPES,
    SCOPES,
    STEP_STATUSES,
    VOID,
    BackfillPlan,
    Column,
    Correction,
    DatasetRef,
    DqResult,
    DqRule,
    FileRecord,
    InvalidArgument,
    NotFound,
    RunStateError,
    Setting,
    SettingDef,
    StepRecord,
    UnknownSetting,
    redact,
    run_status,
)

Key = tuple[str, str]


def _plain(x: Any) -> Any:
    return json.loads(json.dumps(x, default=str))


class MemoryMetaStore:
    def __init__(self) -> None:
        self.registry: dict[tuple[str, str], SettingDef] = {}
        self.domains: dict[str, dict[str, Any]] = {}
        self.ds: dict[Key, dict[str, Any]] = {}
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.ds_runs: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.steps: list[dict[str, Any]] = []
        self.files: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.marks: dict[Key, str | None] = {}
        self.books: dict[Key, dict[str, str]] = {}
        self.audit: list[dict[str, Any]] = []
        self.plans: dict[str, BackfillPlan] = {}
        self.fixes: list[Correction] = []
        self.dq: dict[Key, list[DqRule]] = {}
        self.dq_log: list[DqResult] = []
        self.voids: dict[tuple[str, str, str], dict[str, str]] = {}
        self.leases: dict[str, tuple[str, datetime]] = {}  # table -> (run id, expires at): ORCH-04
        self.now: Callable[[], datetime] = lambda: datetime.now(UTC)  # tests set their own clock
        self.profiles: list[tuple[str, str, str, dict[str, Any]]] = []

    # ---- configuration ----
    def register_settings(self, settings: list[SettingDef]) -> None:
        for d in settings:
            if d.scope not in SCOPES:
                raise InvalidArgument(f"scope {d.scope!r} is not one of {SCOPES}")
            self.registry[(d.scope, d.key)] = d

    def _known(self, key: str, scopes: tuple[str, ...]) -> bool:
        return any((s, key) in self.registry for s in scopes)

    def load_domain(self, doc: dict[str, Any]) -> None:
        for k in doc.get("settings") or {}:
            if not self._known(k, ("platform", "dataset")):
                raise UnknownSetting(f"setting '{k}' is not registered")
        self.domains[doc["domain"]] = _plain(doc)

    def load_dataset(self, doc: dict[str, Any]) -> None:
        if doc["domain"] not in self.domains:
            raise NotFound(f"domain {doc['domain']} is not loaded")
        for k in doc.get("settings") or {}:
            if not self._known(k, ("dataset",)):
                raise UnknownSetting(f"setting '{k}' is not registered")
        names = {c["name"] for c in doc.get("columns") or []}
        for kind, cols in (doc.get("business_keys") or {}).items():
            if kind not in KEY_KINDS:
                raise InvalidArgument(f"business key kind {kind!r} is not one of {KEY_KINDS}")
            if set(cols) - names:
                raise InvalidArgument(f"business key {kind} names a column the dataset does not have")
        self.ds[(doc["source_name"], doc["dataset_name"])] = _plain(doc)

    def _get(self, source: str, dataset: str) -> dict[str, Any]:
        try:
            return self.ds[(source, dataset)]
        except KeyError:
            raise NotFound(f"dataset {source}.{dataset} is not loaded") from None

    def settings_registry(self) -> list[SettingDef]:
        return sorted(self.registry.values(), key=lambda d: (d.scope, d.key))

    def datasets(self, domain: str) -> list[DatasetRef]:
        return [DatasetRef(d["domain"], d["source_name"], d["dataset_name"], d["is_enabled"], d["kind"],
                           d["ingest_mode"], d["fingerprint"])
                for _, d in sorted(self.ds.items()) if d["domain"] == domain]

    def settings(self, source: str, dataset: str) -> list[Setting]:
        d = self._get(source, dataset)
        dom = self.domains[d["domain"]].get("settings") or {}
        out = []
        for (scope, key), defn in sorted(self.registry.items(), key=lambda kv: kv[0][1]):
            if scope != "dataset":
                continue
            if key in d["settings"]:
                out.append(Setting(key, d["settings"][key], "dataset"))
            elif key in dom:
                out.append(Setting(key, dom[key], "domain"))
            else:
                out.append(Setting(key, defn.default, "default"))
        return out

    def columns(self, source: str, dataset: str) -> list[Column]:
        return [Column(c["name"], c["logical_type"], c["required"], c["primary_key_position"], c["description"],
                       c.get("classification", ""))
                for c in self._get(source, dataset)["columns"]]

    def business_keys(self, source: str, dataset: str, kind: str = "primary") -> list[str]:
        if kind not in KEY_KINDS:
            raise InvalidArgument(f"business key kind {kind!r} is not one of {KEY_KINDS}")
        return list((self._get(source, dataset).get("business_keys") or {}).get(kind) or [])

    def dataset_view(self, source: str, dataset: str) -> dict[str, Any]:
        d = self._get(source, dataset)
        merged = self.settings(source, dataset)
        return {
            "domain": d["domain"], "source": source, "dataset": dataset, "source_env": d["source_env"],
            "is_enabled": d["is_enabled"], "kind": d["kind"], "archetypes": d["archetypes"],
            "ingest_mode": d["ingest_mode"], "ingest_read": d["ingest_read"], "history_kind": d["history_kind"],
            "load_pattern": d["load_pattern"], "contract_id": d["contract_id"],
            "contract_version": d["contract_version"], "fingerprint": d["fingerprint"],
            "settings": {s.key: s.value for s in merged if s.value is not None},
            "origins": {s.key: s.origin for s in merged if s.value is not None},
            "columns": [c.__dict__ for c in self.columns(source, dataset)],
            "business_keys": {k: self.business_keys(source, dataset, k) for k in KEY_KINDS},
            "manifest": copy.deepcopy(d["manifest"]),
        }

    # ---- run log ----
    def _run(self, run_id: str) -> dict[str, Any]:
        if run_id not in self.runs:
            raise NotFound(f"run {run_id} was never started")
        return self.runs[run_id]

    def _ds_run(self, run_id: str, source: str, dataset: str) -> dict[str, Any]:
        self._run(run_id)
        try:
            return self.ds_runs[(run_id, source, dataset)]
        except KeyError:
            raise RunStateError(f"{source}.{dataset} was not started in run {run_id}") from None

    def start_run(self, run_id: str, domain: str, env: str, trigger: str = "manual", run_type: str = "normal") -> None:
        if run_type not in RUN_TYPES:
            raise InvalidArgument(f"run type {run_type!r} is not one of {RUN_TYPES}")
        if domain not in self.domains:
            raise NotFound(f"domain {domain} is not loaded")
        if run_id in self.runs:
            raise RunStateError(f"run {run_id} was already started")
        self.runs[run_id] = {"run_id": run_id, "domain": domain, "env": env, "trigger": trigger,
                             "run_type": run_type, "status": "running"}

    def start_dataset_run(self, run_id: str, source: str, dataset: str, config_fingerprint: str,
                          config: dict[str, Any]) -> str | None:
        if self._run(run_id)["status"] != "running":
            raise RunStateError(f"run {run_id} is already finished")
        self._get(source, dataset)
        if (run_id, source, dataset) in self.ds_runs:
            raise RunStateError(f"{source}.{dataset} was already started in run {run_id}")
        self.snapshots.setdefault(config_fingerprint, _plain(config))
        before = self.marks.get((source, dataset))
        self.ds_runs[(run_id, source, dataset)] = {
            "source": source, "dataset": dataset, "status": "running", "config_fingerprint": config_fingerprint,
            "watermark_before": before, "watermark_after": None, "counts": {}, "message": ""}
        return before

    def log_step(self, step: StepRecord) -> None:
        if step.status not in STEP_STATUSES:
            raise InvalidArgument(f"step status {step.status!r} is not one of {STEP_STATUSES}")
        self._ds_run(step.run_id, step.source, step.dataset)
        self.steps.append({"run_id": step.run_id, "source": step.source, "dataset": step.dataset, "step": step.step,
                           "status": step.status, "counts": dict(step.counts), "message": redact(step.message)})

    def log_file(self, file: FileRecord) -> None:
        if file.status not in FILE_STATUSES:
            raise InvalidArgument(f"file status {file.status!r} is not one of {FILE_STATUSES}")
        if file.load_type not in LOAD_TYPES:
            raise InvalidArgument(f"load type {file.load_type!r} is not one of {LOAD_TYPES}")
        self._ds_run(file.run_id, file.source, file.dataset)
        self.files.append({**file.__dict__, "reason": redact(file.reason)})

    def batch_status(self, source: str, dataset: str, batch_key: str) -> str | None:
        if (source, dataset, batch_key) in self.voids:
            return VOID
        hits = [f for f in self.files if (f["source"], f["dataset"], f["batch_key"]) == (source, dataset, batch_key)]
        return hits[-1]["status"] if hits else None

    def acquire_lease(self, table: str, run_id: str, ttl_seconds: int) -> str | None:
        if ttl_seconds <= 0:
            raise InvalidArgument("a lease needs a positive time to live")
        now = self.now()
        held = self.leases.get(table)
        if held is None or held[1] < now or held[0] == run_id:
            self.leases[table] = (run_id, now + timedelta(seconds=ttl_seconds))
            return None
        return held[0]

    def release_lease(self, table: str, run_id: str) -> None:
        if self.leases.get(table, ("", None))[0] == run_id:
            del self.leases[table]

    def void_batch(self, source: str, dataset: str, batch_key: str, reason: str, approved_by: str) -> None:
        self._get(source, dataset)
        if len(reason.strip()) < MIN_REASON or not approved_by.strip():
            raise InvalidArgument(f"voiding a batch needs who approved it and a reason of at least {MIN_REASON} "
                                  "characters")
        status = self.batch_status(source, dataset, batch_key)
        if status == VOID:
            raise RunStateError(f"batch {batch_key} is already void")
        if status != "loaded":
            raise InvalidArgument(f"batch {batch_key} never loaded (status {status}); only a loaded batch is voided")
        self.voids[(source, dataset, batch_key)] = {"reason": reason, "approved_by": approved_by}

    def log_error(self, run_id: str, source: str | None, dataset: str | None, step: str, error_class: str,
                  message: str) -> None:
        self._run(run_id)
        self.errors.append({"run_id": run_id, "source": source, "dataset": dataset, "step": step,
                            "error_class": error_class, "message": redact(message)})

    def finish_dataset_run(self, run_id: str, source: str, dataset: str, status: str,
                           counts: dict[str, int] | None = None, watermark: str | None = None,
                           bookmarks: dict[str, str] | None = None, message: str = "") -> None:
        if status not in DATASET_RUN_END:
            raise InvalidArgument(f"dataset run status {status!r} is not one of {DATASET_RUN_END}")
        r = self._ds_run(run_id, source, dataset)
        if r["status"] != "running":
            raise RunStateError(f"{source}.{dataset} in run {run_id} is already {r['status']}")
        if status == "succeeded":
            if watermark is not None:
                self.marks[(source, dataset)] = watermark
            self.books.setdefault((source, dataset), {}).update(bookmarks or {})
        r.update(status=status, counts=dict(counts or {}), message=redact(message),
                 watermark_after=self.marks.get((source, dataset)))

    def finish_run(self, run_id: str) -> str:
        run = self._run(run_id)
        if run["status"] != "running":
            raise RunStateError(f"run {run_id} is already {run['status']}")
        mine = [r for (rid, _, _), r in self.ds_runs.items() if rid == run_id]
        if any(r["status"] == "running" for r in mine):
            raise RunStateError(f"run {run_id} still has datasets running")
        run["status"] = run_status([r["status"] for r in mine])
        return str(run["status"])

    def watermark(self, source: str, dataset: str) -> str | None:
        self._get(source, dataset)
        return self.marks.get((source, dataset))

    def bookmarks(self, source: str, dataset: str) -> dict[str, str]:
        self._get(source, dataset)
        return dict(sorted(self.books.get((source, dataset), {}).items()))

    def reset_watermark(self, source: str, dataset: str, value: str | None, reason: str, changed_by: str) -> None:
        self._get(source, dataset)
        if len(reason.strip()) < MIN_REASON:
            raise InvalidArgument(f"a watermark reset needs a reason of at least {MIN_REASON} characters")
        if not changed_by.strip():
            raise InvalidArgument("a watermark reset needs who made it")
        self.audit.append({"source": source, "dataset": dataset, "old": self.marks.get((source, dataset)),
                           "new": value, "reason": reason, "changed_by": changed_by})
        self.marks[(source, dataset)] = value

    def run_report(self, run_id: str) -> dict[str, Any]:
        run = self._run(run_id)
        pick = ("source", "dataset", "status", "config_fingerprint", "watermark_before", "watermark_after", "counts",
                "message")
        report: dict[str, Any] = _plain({
            "run": {k: run[k] for k in ("run_id", "domain", "env", "trigger", "run_type", "status")},
            "datasets": [{k: r[k] for k in pick} for (rid, _, _), r in self.ds_runs.items() if rid == run_id],
            "steps": [{k: v for k, v in s.items() if k != "run_id"} for s in self.steps if s["run_id"] == run_id],
            "files": [{k: v for k, v in f.items() if k != "run_id"} for f in self.files if f["run_id"] == run_id],
            "errors": [{k: v for k, v in e.items() if k != "run_id"} for e in self.errors if e["run_id"] == run_id],
        })
        return report

    def dataset_files(self, source: str, dataset: str, window: str | None = None) -> list[FileRecord]:
        self._get(source, dataset)
        return [FileRecord(**{**f, "status": VOID}) if (source, dataset, f["batch_key"]) in self.voids
                else FileRecord(**f) for f in self.files
                if (f["source"], f["dataset"]) == (source, dataset) and window in (None, f["window"])]

    # ---- corrections ----
    def record_corrections(self, plan: BackfillPlan, corrections: list[Correction]) -> None:
        d = self._get(plan.source, plan.dataset)
        if plan.plan_id in self.plans:
            raise RunStateError(f"backfill plan {plan.plan_id} was already recorded")
        if len(plan.reason.strip()) < MIN_REASON or not plan.recorded_by.strip():
            raise InvalidArgument(f"a backfill plan needs who recorded it and a reason of at least {MIN_REASON} "
                                  "characters")
        if not plan.windows:
            raise InvalidArgument("a backfill plan names the windows it rebuilds")
        primary = sorted((d.get("business_keys") or {}).get("primary") or [])
        names = {c["name"] for c in d["columns"]}
        for c in corrections:
            if sorted(c.key) != primary:
                raise InvalidArgument(f"a correction's key must be the primary key {primary}")
            if c.field not in names:
                raise InvalidArgument(f"{c.field} is not a column of {plan.source}.{plan.dataset}")
        self.plans[plan.plan_id] = plan
        self.fixes += [Correction(dict(c.key), c.field, c.value, c.was, c.applies_from, c.applies_to, plan.plan_id)
                       for c in corrections]

    def corrections(self, source: str, dataset: str, as_of: str | None = None) -> list[Correction]:
        self._get(source, dataset)
        out = []
        for c in self.fixes:
            plan = self.plans[c.plan_id]
            if (plan.source, plan.dataset) != (source, dataset):
                continue
            active = as_of is not None and as_of >= plan.activate_on
            out.append(Correction(dict(c.key), c.field, c.value, c.was, c.applies_from, c.applies_to, c.plan_id,
                                  "active" if active else "recorded"))
        return out

    def backfill_plans(self, source: str, dataset: str) -> list[BackfillPlan]:
        self._get(source, dataset)
        return [p for _, p in sorted(self.plans.items()) if (p.source, p.dataset) == (source, dataset)]

    # ---- data quality ----
    def set_dq_rules(self, source: str, dataset: str, rules: list[DqRule]) -> None:
        columns = {c.name for c in self.columns(source, dataset)}
        ids = [r.rule_id for r in rules]
        if len(ids) != len(set(ids)):
            raise InvalidArgument("a rule id appears twice")
        for r in rules:
            if not r.rule_id.strip():
                raise InvalidArgument("a rule needs an id")
            if r.column and r.column not in columns:
                raise InvalidArgument(f"{r.column} is not a column of {source}.{dataset}")
            if r.severity not in DQ_SEVERITIES or r.action not in DQ_ACTIONS:
                raise InvalidArgument(f"rule {r.rule_id}: severity {r.severity!r} or action {r.action!r} unknown")
        self.dq[(source, dataset)] = [DqRule(r.rule_id, r.column, r.check, dict(r.params), r.severity, r.action,
                                             r.enabled) for r in sorted(rules, key=lambda r: r.rule_id)]

    def dq_rules(self, source: str, dataset: str) -> list[DqRule]:
        self._get(source, dataset)
        return list(self.dq.get((source, dataset), []))

    def log_dq_results(self, results: list[DqResult]) -> None:
        for r in results:  # check every row before keeping any (one call, all or nothing)
            self._ds_run(r.run_id, r.source, r.dataset)
            if r.rule_id not in {x.rule_id for x in self.dq.get((r.source, r.dataset), [])}:
                raise InvalidArgument(f"{r.rule_id} is not a rule of {r.source}.{r.dataset}")
            if r.checked < 0 or r.failed < 0 or r.failed > r.checked:
                raise InvalidArgument(f"rule {r.rule_id}: failed {r.failed} of {r.checked} is not possible")
            if r.action not in DQ_ACTIONS:
                raise InvalidArgument(f"rule {r.rule_id}: action {r.action!r} unknown")
        self.dq_log += [DqResult(r.run_id, r.source, r.dataset, r.rule_id, r.checked, r.failed, r.action,
                                 redact(r.detail)) for r in results]

    def dq_results(self, run_id: str, source: str | None = None, dataset: str | None = None) -> list[DqResult]:
        self._run(run_id)
        return [r for r in self.dq_log if r.run_id == run_id and (source is None or r.source == source)
                and (dataset is None or r.dataset == dataset)]

    # ---- profiles ----
    def save_profile(self, run_id: str, source: str, dataset: str, profile: dict[str, Any]) -> None:
        self._ds_run(run_id, source, dataset)
        self.profiles.append((run_id, source, dataset, json.loads(json.dumps(profile, default=str))))

    def last_profile(self, source: str, dataset: str) -> dict[str, Any] | None:
        self._get(source, dataset)
        mine = [p for r, s, d, p in self.profiles if (s, d) == (source, dataset)]
        return mine[-1] if mine else None
