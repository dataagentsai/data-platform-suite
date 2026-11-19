"""One ingestion run over a domain, every step recorded in the metadata store (ADR-010):

    start_run(run_id, domain, env, trigger, run_type)
      for each enabled dataset (config from the store's merged view, never from files):
        start_dataset_run(config fingerprint + snapshot)
        the dataset's pipeline -> log_file per batch (inside the lander steps)
        log_step (counts and what was noticed), log_error per held batch
        finish_dataset_run(status, counts, watermark, bookmarks)     watermark and bookmarks move only on success
    finish_run -> succeeded | partial | failed

A dataset run is `failed` when a batch was held (contract, volume, mass delete): DPC-0013 and DEL-04 say a held
batch is never a success, and the dataset stays blocked for that run. It is `skipped` when nothing new arrived.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dataplatform import profiling
from dataplatform.ingest.pipelines import PIPELINES, Plan, Result, Sources
from dataplatform.ingest.ports import BronzeWriter, CurrentKeys, Tokenizer
from dataplatform.ingest.steps import Ctx, Spec, flush
from dataplatform.leases import LEASE_SECONDS, busy
from dataplatform.meta.port import GOLD_KINDS, MetadataStore, StepRecord

HELD = ("rejected_contract", "rejected_volume", "held_mass_delete")
# the detector (DAC id) that holds or skips a batch: one `detect.<id>` step row each (ADR-024 one step format)
DETECTOR = {
    "skipped_duplicate": "DUP-01",
    "rejected_contract": "SCH-02",
    "rejected_volume": "ING-01",  # the volume guard is one of the entry steps every way into bronze passes
    "held_mass_delete": "DEL-04",
}
ERROR_CLASS = {
    "rejected_contract": "ContractBreak",
    "rejected_volume": "VolumeOutOfRange",
    "held_mass_delete": "MassDelete",
}


@dataclass
class DatasetReport:
    source: str
    dataset: str
    status: str
    counts: dict[str, int]
    noticed: dict[str, int]
    reasons: list[str] = field(default_factory=list)


@dataclass
class RunReport:
    run_id: str
    status: str
    datasets: list[DatasetReport]

    def by_name(self) -> dict[str, DatasetReport]:
        return {f"{d.source}.{d.dataset}": d for d in self.datasets}


def _profile_step(store: MetadataStore, run_id: str, spec: Any, result: Any, normal: bool) -> None:
    """The run's profile (B9, DPC-0051): logged as a step and compared with the last one the store kept; drift makes
    the step `warned` and names what moved. A normal run's profile is kept for the next comparison."""
    loaded = [o.profile for o in result.outcomes if o.status == "loaded" and o.profile]
    if not loaded:
        return
    now = profiling.merge(loaded)
    found = profiling.drift(now, store.last_profile(spec.source, spec.dataset))
    counts = {"rows": now["rows"], **{f"{c}.{k}": v for c, m in now["columns"].items() for k, v in m.items()
                                      if k in ("nulls", "distinct")}}
    store.log_step(StepRecord(run_id, spec.source, spec.dataset, "profile.batches", "warned" if found else "succeeded",
                              counts, "; ".join(found)[:4000]))
    if normal:  # a reload or backfill is not the dataset's usual shape: it is not kept as the next baseline
        store.save_profile(run_id, spec.source, spec.dataset, now)


def counts_of(result: Result) -> dict[str, int]:
    out = {s: 0 for s in ("loaded", "skipped_duplicate", *HELD)}
    for o in result.outcomes:
        out[o.status] += 1
    out["rows_loaded"] = sum(o.rows for o in result.outcomes if o.status == "loaded")
    return out


def status_of(result: Result) -> str:
    if any(o.status in HELD for o in result.outcomes):
        return "failed"
    return "succeeded" if result.outcomes else "skipped"


def detect_rows(result: Result) -> list[tuple[str, str, dict[str, int], str]]:
    """(detector, status, counts, message) per detector that met this run: a held batch is `failed` (blocked), a
    skipped duplicate or anything noticed is `warned` (handled or flagged for the layer that applies it)."""
    held: dict[str, list[str]] = {}
    for o in result.outcomes:
        if o.status in DETECTOR:
            held.setdefault(o.status, []).append(f"{o.batch.name}: {o.reason}")
    out = [
        (DETECTOR[st], "warned" if st == "skipped_duplicate" else "failed", {"batches": len(why)}, "; ".join(why))
        for st, why in sorted(held.items())
    ]
    out += [(det, "warned", {"rows": n}, f"{n} noticed") for det, n in sorted(result.noticed.items())]
    return out


def _detect_steps(store: MetadataStore, run_id: str, spec: Spec, result: Result) -> None:
    for det, status, counts, message in detect_rows(result):
        store.log_step(StepRecord(run_id, spec.source, spec.dataset, f"detect.{det}", status, counts, message[:4000]))


def ingest(
    store: MetadataStore,
    writer: BronzeWriter,
    tokenizer: Tokenizer,
    sources: Sources,
    domain: str,
    env: str,
    run_id: str,
    plan: Plan,
    trigger: str = "schedule",
    only: set[str] | None = None,
    silver: CurrentKeys | None = None,
) -> RunReport:
    """Run every enabled dataset of a domain (or the named `source.dataset`s) once, by the plan's run type."""
    store.start_run(run_id, domain, env, trigger, plan.run_type)
    reports = []
    for ref in store.datasets(domain):
        name = f"{ref.source}.{ref.dataset}"
        if not ref.is_enabled or ref.kind in GOLD_KINDS or (only is not None and name not in only):
            continue
        reports.append(
            _one(store, writer, tokenizer, sources, run_id, plan, store.dataset_view(ref.source, ref.dataset), silver)
        )
    return RunReport(run_id, store.finish_run(run_id), reports)


def _one(
    store: MetadataStore,
    writer: BronzeWriter,
    tokenizer: Tokenizer,
    sources: Sources,
    run_id: str,
    plan: Plan,
    view: dict[str, Any],
    silver: CurrentKeys | None = None,
) -> DatasetReport:
    spec = Spec.from_view(view)
    store.start_dataset_run(run_id, spec.source, spec.dataset, spec.fingerprint, view)
    lease = f"bronze:{spec.source}.{spec.dataset}"  # shared with housekeeping (ORCH-04)
    holder = store.acquire_lease(lease, run_id, LEASE_SECONDS)
    if holder is not None:
        msg = busy(store, run_id, spec.source, spec.dataset, lease, holder)
        return DatasetReport(spec.source, spec.dataset, "skipped", {}, {}, [msg])
    try:
        return _one_leased(store, writer, tokenizer, sources, run_id, plan, view, silver, spec)
    finally:
        store.release_lease(lease, run_id)


def _one_leased(  # noqa: PLR0917 -- the runner's own arguments, passed through to the dataset's run
    store: MetadataStore,
    writer: BronzeWriter,
    tokenizer: Tokenizer,
    sources: Sources,
    run_id: str,
    plan: Plan,
    view: dict[str, Any],
    silver: CurrentKeys | None,
    spec: Spec,
) -> DatasetReport:
    pipeline = PIPELINES.get((spec.mode, spec.read))
    if pipeline is None:
        msg = f"no ingestion pipeline for ingest mode {spec.mode!r} and read {spec.read!r}"
        store.log_error(run_id, spec.source, spec.dataset, "ingest", "NoPipeline", msg)
        store.finish_dataset_run(run_id, spec.source, spec.dataset, "failed", message=msg)
        return DatasetReport(spec.source, spec.dataset, "failed", {}, {}, [msg])
    try:
        ctx = Ctx(spec, store, writer, tokenizer, run_id, silver=silver, pending=[])
        result = pipeline(ctx, sources, plan)
        flush(ctx)  # the run's new batches in one bronze commit, then logged
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        store.log_error(run_id, spec.source, spec.dataset, "ingest", type(exc).__name__, msg)
        store.finish_dataset_run(run_id, spec.source, spec.dataset, "failed", message=msg)
        return DatasetReport(spec.source, spec.dataset, "failed", {}, {}, [msg])
    counts, status = counts_of(result), status_of(result)
    reasons = [f"{o.batch.name}: {o.reason}" for o in result.outcomes if o.status in HELD]
    for o in result.outcomes:
        if o.status in HELD:
            store.log_error(
                run_id, spec.source, spec.dataset, "land", ERROR_CLASS[o.status], f"{o.batch.name}: {o.reason}"
            )
    noticed = "; ".join([f"{k}: {v}" for k, v in sorted(result.noticed.items())] + result.notes)
    step_status = "warned" if status == "succeeded" and result.noticed else status
    store.log_step(
        StepRecord(
            run_id,
            spec.source,
            spec.dataset,
            f"ingest.{spec.mode}.{spec.read}",
            step_status,
            {**counts, **{f"noticed.{k}": v for k, v in result.noticed.items()}},
            noticed or ("nothing new" if status == "skipped" else ""),
        )
    )
    _detect_steps(store, run_id, spec, result)
    _profile_step(store, run_id, spec, result, plan.run_type == "normal")
    store.finish_dataset_run(
        run_id,
        spec.source,
        spec.dataset,
        status,
        counts,
        result.watermark if plan.run_type == "normal" else None,
        result.bookmarks if plan.run_type == "normal" else None,
        "; ".join(reasons)[:4000],
    )
    return DatasetReport(spec.source, spec.dataset, status, counts, dict(result.noticed), reasons)
