"""What the housekeeping job may remove from bronze (B8; DAC RET-01, DPC-0116). Pure and product-free: it decides,
adapters act (archive, delete by batch key, expire snapshots) and the run log records.

Keep rule: keep everything from the older of (the latest checked baseline) and (today minus the correction window).
A baseline is a complete picture of the table: a full copy from the source, or a baseline batch the platform writes
from silver; only a baseline whose row count and key totals were checked against silver counts. When no checked
baseline is newer than the cut-off, the job must write and check one before removing anything.

    plan_removals     the pure decision for one dataset
    batches_of        the file log as Batches: loaded, not void, one per batch key
    run_housekeeping  the job: plan, write a baseline where one is owed (Baseliner), remove through the bronze port,
                      void the removed batches, log each dataset

Decisions (B8, 15 Nov): the removed delivery's archive copy is the landing archive (ADR-029 keeps it as long as bronze),
so the job writes no second archive. A removed batch is voided with reason "housekeeping": the file log stays
append-only and every later read, a rebuild included, skips it. A loaded full copy counts as checked: silver took it
through the key compare and the mass-delete guard; a held one never loaded. Snapshot expiry is the table's upkeep
(predictive optimization on Databricks), not this job. The correction window defaults to the dataset's
bronze_retention_days setting (ADR-029).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Protocol

from dataplatform.leases import LEASE_SECONDS, busy
from dataplatform.meta.port import GOLD_KINDS, FileRecord, MetadataStore, StepRecord
from dataplatform.retention import Retention, window_day

LOAD_TYPES = ("incremental", "full", "baseline")


@dataclass(frozen=True)
class Batch:
    batch_key: str
    delivered: date
    load_type: str = "incremental"  # incremental | full (from the source) | baseline (written from silver)
    checked: bool = False  # a full or baseline batch whose counts and key totals matched silver


@dataclass(frozen=True)
class Plan:
    dataset: str
    keep_from: date | None  # nothing older than this stays; None = keep everything
    remove: tuple[str, ...]  # batch keys to archive and then remove, oldest first
    needs_baseline: bool  # write and check a baseline first; nothing is removed this run
    reason: str


def plan_removals(dataset: str, batches: Iterable[Batch], today: date, correction_days: int) -> Plan:
    if correction_days < 0:
        raise ValueError(f"{dataset}: correction window {correction_days} days is negative")
    bad = sorted({b.load_type for b in batches if b.load_type not in LOAD_TYPES})
    if bad:
        raise ValueError(f"{dataset}: load type(s) {bad} are not {LOAD_TYPES}")
    ordered = sorted(batches, key=lambda b: (b.delivered, b.batch_key))
    cutoff = today - timedelta(days=correction_days)
    baselines = [b for b in ordered if b.load_type in ("full", "baseline") and b.checked and b.delivered <= cutoff]
    if not ordered or ordered[0].delivered >= cutoff:
        return Plan(dataset, None, (), False, "nothing older than the correction window")
    if not baselines:
        return Plan(dataset, None, (), True,
                    f"no checked baseline on or before {cutoff}: write and check one before removing anything")
    base = baselines[-1]  # the newest checked baseline inside the allowed range
    keep_from = base.delivered
    remove = tuple(b.batch_key for b in ordered if b.delivered < keep_from)
    return Plan(dataset, keep_from, remove, False,
                f"keep from {keep_from} (checked {base.load_type} {base.batch_key}); {len(remove)} batch(es) older")


class BronzeRemover(Protocol):
    """The bronze writer's removal edge (MemoryBronze, DeltaBronze, StoreBronzeWriter)."""

    def remove(self, table: str, batch_keys: list[str]) -> int:
        """Delete the rows of these batches in one transaction; returns the rows removed."""


@dataclass(frozen=True)
class BaselineOutcome:
    status: str  # written | refused | not_needed
    counts: dict[str, int]
    message: str


class Baseliner(Protocol):
    """Writes and checks a baseline batch for one dataset (silver/baseline_writer.py: a compaction of bronze, replayed
    through the processors against silver). Only a checked baseline is logged to the file log, as load type baseline,
    window = the cut-off."""

    def write(self, store: MetadataStore, run_id: str, view: dict[str, Any], cutoff: date,
              today: date) -> BaselineOutcome:
        """Write, check and log one baseline; a baseline that fails its check is removed again and refused."""


STEP = "housekeeping.bronze"
BASELINE_STEP = "housekeeping.baseline"
REASON = "housekeeping: older than the checked baseline {base} and the {days}-day correction window"


def batches_of(files: Iterable[FileRecord]) -> list[Batch]:
    """Loaded batches, one per batch key (the first loaded record), void ones left out."""
    seen: dict[str, Batch] = {}
    for f in files:
        if f.status == "loaded" and f.batch_key not in seen:
            checked = f.load_type in ("full", "baseline")
            seen[f.batch_key] = Batch(f.batch_key, window_day(f.window), f.load_type, checked)
    return list(seen.values())


@dataclass(frozen=True)
class Swept:
    dataset: str
    plan: Plan
    rows_removed: int
    baseline: BaselineOutcome | None = None


def run_housekeeping(store: MetadataStore, bronze: BronzeRemover, domain: str, *, env: str, run_id: str, today: date,
                     correction_days: int | None = None, baseliner: Baseliner | None = None) -> list[Swept]:
    """One housekeeping run over a domain's source datasets. Each dataset gets its plan; removals go through the bronze
    port, then each removed batch is voided; one step per dataset records the plan. A dataset that needs a baseline gets
    one from the baseliner (when given) and is planned again; without one, or when its check fails, it is logged as
    warned and loses nothing."""
    store.start_run(run_id, domain, env, "housekeeping")
    out = []
    for ref in store.datasets(domain):
        if not ref.is_enabled or ref.kind in GOLD_KINDS:
            continue
        out.append(_sweep(store, bronze, run_id, today, (ref.source, ref.dataset),
                          correction_days=correction_days, baseliner=baseliner))
    store.finish_run(run_id)
    return out


def _sweep(store: MetadataStore, bronze: BronzeRemover, run_id: str, today: date, ref: tuple[str, str], *,
           correction_days: int | None, baseliner: Baseliner | None = None) -> Swept:
    source, dataset = ref
    name = f"{source}.{dataset}"
    view: dict[str, Any] = store.dataset_view(source, dataset)
    days = correction_days
    if days is None:
        days = Retention.from_settings(view.get("settings") or {}).bronze_days
    store.start_dataset_run(run_id, source, dataset, view["fingerprint"], view)
    lease = f"bronze:{name}"  # the lander's lease too: housekeeping never removes while a load writes (ORCH-04)
    holder = store.acquire_lease(lease, run_id, LEASE_SECONDS)
    if holder is not None:
        msg = busy(store, run_id, source, dataset, lease, holder)
        return Swept(name, Plan(name, None, (), False, msg), 0)
    try:
        return _sweep_leased(store, bronze, run_id, today, (name, view, days), baseliner=baseliner)
    finally:
        store.release_lease(lease, run_id)


def _sweep_leased(store: MetadataStore, bronze: BronzeRemover, run_id: str, today: date,
                  what: tuple[str, dict[str, Any], int], *, baseliner: Baseliner | None) -> Swept:
    name, view, days = what
    source, dataset = view["source"], view["dataset"]
    plan = plan_removals(name, batches_of(store.dataset_files(source, dataset)), today, days)
    got = None
    if plan.needs_baseline and baseliner is not None:
        got = baseliner.write(store, run_id, view, today - timedelta(days=days), today)
        status = {"written": "succeeded", "refused": "failed"}.get(got.status, "skipped")
        store.log_step(StepRecord(run_id, source, dataset, BASELINE_STEP, status, got.counts, got.message))
        if got.status == "written":
            plan = plan_removals(name, batches_of(store.dataset_files(source, dataset)), today, days)
    removed = bronze.remove(name, list(plan.remove)) if plan.remove else 0
    reason = REASON.format(base=plan.keep_from, days=days)
    for key in plan.remove:
        store.void_batch(source, dataset, key, reason, "housekeeping")
    status = "warned" if plan.needs_baseline else "succeeded"
    counts = {"batches_removed": len(plan.remove), "rows_removed": removed, "correction_days": days}
    store.log_step(StepRecord(run_id, source, dataset, STEP, status, counts, plan.reason))
    store.finish_dataset_run(run_id, source, dataset, "succeeded", counts=counts, message=plan.reason)
    return Swept(name, plan, removed, got)
