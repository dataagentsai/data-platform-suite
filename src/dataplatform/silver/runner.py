"""The silver run: for each enabled dataset, the bronze batches loaded since silver last ran go through the thirteen
stages and into silver, and every step is recorded in the metadata store under the run id (ADR-010).

    start_run -> per dataset: start_dataset_run -> read edge -> stages -> write edge -> log_step per stage
              -> finish_dataset_run(bookmark silver.files) -> finish_run

Where silver starts: the bookmark `silver.files` is the number of the dataset's file-log records (oldest first)
already taken; the next run takes the `loaded` batches after it. It moves only when the dataset run succeeds, so a
failed run is taken again whole next time; the stages are idempotent (a change seen twice changes nothing), so a
batch reloaded into bronze is simply taken again. Corrections do not reach silver (ADR-017): silver keeps what the
source said.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any

from dataplatform.leases import LEASE_SECONDS, busy
from dataplatform.meta.port import GOLD_KINDS, MetadataStore, StepRecord
from dataplatform.silver.contract_rules import contract_rules, with_overrides
from dataplatform.silver.plan import STAGES, PlanError, Rule, plan_from_view
from dataplatform.silver.ports import SilverTables, StageError, StageRunner

BOOKMARK = "silver.files"


@dataclass
class SilverDataset:
    source: str
    dataset: str
    status: str  # succeeded | failed | skipped
    counts: dict[str, int] = field(default_factory=dict)
    batches: int = 0
    reason: str = ""
    seconds: float = 0.0

    @property
    def name(self) -> str:
        return f"{self.source}.{self.dataset}"


@dataclass
class SilverReport:
    run_id: str
    status: str
    datasets: list[SilverDataset]

    def by_name(self) -> dict[str, SilverDataset]:
        return {d.name: d for d in self.datasets}


def new_batches(store: MetadataStore, source: str, dataset: str, from_start: bool = False) -> tuple[list[str], int]:
    """(batch keys loaded since the bookmark, in file-log order without repeats; the new bookmark). `from_start`
    takes every loaded batch from the first (a rebuild, ADR-025). A voided batch reads as `void` and is never taken.
    A baseline batch (housekeeping, B8) holds what silver already has: only a rebuild takes it."""
    files = store.dataset_files(source, dataset)
    start = 0 if from_start else int(store.bookmarks(source, dataset).get(BOOKMARK, "0"))
    keys: list[str] = []
    for f in files[start:]:
        if f.load_type == "baseline" and not from_start:
            continue
        if f.status == "loaded" and f.batch_key not in keys:
            keys.append(f.batch_key)
    return keys, len(files)


def build_silver(
    store: MetadataStore,
    tables: SilverTables,
    stages: StageRunner,
    domain: str,
    env: str,
    run_id: str,
    as_of: str,
    token_pattern: str = "",
    only: set[str] | None = None,
    row_filters: dict[str, tuple[Rule, ...]] | None = None,
    rebuild: set[str] | None = None,
) -> SilverReport:
    """Run silver for every enabled dataset of a domain (or the named `source.dataset`s). `as_of` is the business
    date of the run (the late lookback counts back from it). Datasets named in `rebuild` (ADR-025, recovery by rerun)
    lose their silver table and take every good bronze batch again, oldest first; voided batches stay out."""
    store.start_run(run_id, domain, env, "silver")
    out = []
    for ref in store.datasets(domain):
        name = f"{ref.source}.{ref.dataset}"
        if not ref.is_enabled or ref.kind in GOLD_KINDS or (only is not None and name not in only):
            continue
        view = store.dataset_view(ref.source, ref.dataset)
        t0 = time.monotonic()
        one = _one(store, tables, stages, run_id, as_of, view, token_pattern, (row_filters or {}).get(name, ()),
                   name in (rebuild or set()))
        one.seconds = round(time.monotonic() - t0, 1)
        out.append(one)
    return SilverReport(run_id, store.finish_run(run_id), out)


def _fail(store: MetadataStore, run_id: str, view: dict[str, Any], step: str, exc: Exception) -> SilverDataset:
    msg = f"{type(exc).__name__}: {exc}"
    store.log_error(run_id, view["source"], view["dataset"], step, type(exc).__name__, msg)
    store.finish_dataset_run(run_id, view["source"], view["dataset"], "failed", message=msg)
    return SilverDataset(view["source"], view["dataset"], "failed", reason=msg)


def _one(  # noqa: PLR0917 -- the runner's own arguments, passed through to the dataset's run
    store: MetadataStore,
    tables: SilverTables,
    stages: StageRunner,
    run_id: str,
    as_of: str,
    view: dict[str, Any],
    token_pattern: str,
    row_filters: tuple[Rule, ...],
    rebuild: bool = False,
) -> SilverDataset:
    """One dataset under its silver writer lease (ORCH-04): a second run finds it busy and writes nothing."""
    source, dataset = view["source"], view["dataset"]
    lease = f"silver:{source}.{dataset}"
    holder = store.acquire_lease(lease, run_id, LEASE_SECONDS)
    if holder is not None:
        store.start_dataset_run(run_id, source, dataset, view["fingerprint"], view)
        return SilverDataset(source, dataset, "skipped", reason=busy(store, run_id, source, dataset, lease, holder))
    try:
        return _one_leased(store, tables, stages, run_id, as_of, view, token_pattern, row_filters, rebuild)
    finally:
        store.release_lease(lease, run_id)


def _one_leased(  # noqa: PLR0911 -- one early return per outcome the run log records (plan, nothing new, held, write)
    store: MetadataStore,
    tables: SilverTables,
    stages: StageRunner,
    run_id: str,
    as_of: str,
    view: dict[str, Any],
    token_pattern: str,
    row_filters: tuple[Rule, ...],
    rebuild: bool = False,
) -> SilverDataset:
    source, dataset = view["source"], view["dataset"]
    store.start_dataset_run(run_id, source, dataset, view["fingerprint"], view)
    try:
        plan = plan_from_view(view, token_pattern, row_filters=row_filters)
    except PlanError as exc:
        return _fail(store, run_id, view, "silver.plan", exc)
    contract, untranslated = contract_rules(store.dq_rules(source, dataset))  # DQ-01: the contract's row rules run
    plan = replace(plan, rules=plan.rules + with_overrides(contract, dict(plan.quality_actions)))
    if untranslated:
        store.log_step(StepRecord(run_id, source, dataset, "silver.contract_rules", "succeeded",
                                  {"translated": len(contract)}, "; ".join(untranslated)))
    if rebuild:
        tables.reset(plan)
        store.log_step(StepRecord(run_id, source, dataset, "silver.rebuild", "succeeded", {},
                                  "silver removed; every good bronze batch taken again (ADR-025)"))
    keys, mark = new_batches(store, source, dataset, from_start=rebuild)
    if not keys:
        start = int(store.bookmarks(source, dataset).get(BOOKMARK, "0"))
        if mark == start:  # nothing new in the file log
            store.log_step(StepRecord(run_id, source, dataset, "silver", "skipped", {}, "no new bronze batches"))
            store.finish_dataset_run(run_id, source, dataset, "skipped")
            return SilverDataset(source, dataset, "skipped")
        # only batches the lander held or skipped: nothing to take, and the bookmark moves past them (a bookmark
        # moves only with a succeeded dataset run)
        msg = f"{mark - start} new file record(s), none loaded: nothing to take"
        store.log_step(StepRecord(run_id, source, dataset, "silver", "succeeded", {"batches": 0}, msg))
        store.finish_dataset_run(run_id, source, dataset, "succeeded", {"batches": 0}, bookmarks={BOOKMARK: str(mark)})
        return SilverDataset(source, dataset, "succeeded", {"batches": 0}, reason=msg)
    try:
        result = stages.run(plan, tables.bronze(plan, keys), tables.current(plan), run_id, as_of)
    except StageError as exc:
        return _fail(store, run_id, view, "silver.stages", exc)
    result.txn = mark
    critical = int(result.counts.get("quality.critical", 0))
    if critical:  # ADR-009: a critical rule broken holds the whole batch; the bookmark stays, so it stays blocked
        stages.release(result)                       # until the batch is voided or resent
        names = ", ".join(r.name for r in plan.rules if r.severity == "critical" and r.action == "quarantine")
        msg = (f"held: {critical} row(s) broke a critical rule ({names}); nothing written, the bookmark stays; void "
               "or resend the batch to go on (ADR-009)")
        store.log_step(StepRecord(run_id, source, dataset, "silver.quality", "failed", dict(result.counts), msg))
        store.finish_dataset_run(run_id, source, dataset, "skipped", dict(result.counts), message=msg)
        return SilverDataset(source, dataset, "skipped", dict(result.counts), reason=msg)
    try:
        written = tables.write(plan, result)
    except Exception as exc:  # the write edge's own errors: nothing is committed, the run is taken again next time
        stages.release(result)
        return _fail(store, run_id, view, "silver.write", exc)
    stages.release(result)
    counts = {**result.counts, "batches": len(keys), "rows_written": written}
    for stage in STAGES:
        mine = {k.split(".", 1)[1]: v for k, v in counts.items() if k.startswith(f"{stage}.")}
        if mine:
            status = "warned" if any(v for k, v in mine.items() if k in ("quarantined", "emptied")) else "succeeded"
            store.log_step(StepRecord(run_id, source, dataset, f"silver.{stage}", status, mine))
    store.log_step(StepRecord(run_id, source, dataset, "silver", "succeeded", counts))
    store.finish_dataset_run(run_id, source, dataset, "succeeded", counts, bookmarks={BOOKMARK: str(mark)})
    return SilverDataset(source, dataset, "succeeded", counts, len(keys))
