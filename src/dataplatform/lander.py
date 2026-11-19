"""The lander: landing -> bronze. Our own code (decision D11); the mover that filled landing is swappable.

For each delivery not yet loaded, in window order:
  1. batch key = window + content checksum (DPC-0024). Already loaded -> skip; a resend changes nothing (DUP-01).
  2. columns checked against the contract (DPC-0008). A missing required column stops this delivery (SCH-02).
  3. row count checked against the declared count and the manifest's volume band (DPC-0013).
  4. rows written to bronze with load metadata (DPC-0022): batch key, run id, file name, window.
  5. one batch record and one run-log row, both carrying the run id.
"""
from __future__ import annotations

from typing import Any

from dataplatform import contract as odcs
from dataplatform.keys import batch_key, config_fingerprint
from dataplatform.model import LandOutcome, RunRecord
from dataplatform.ports import LandingStore, MetadataStore, RunLog, TableWriter

LOAD_COLUMNS = [("_batch_key", "string"), ("_run_id", "string"), ("_source_file", "string"), ("_window", "string")]
CHANGE_COLUMNS = [("op", "string")]      # change-feed columns the source adds; bronze keeps them


def land(cfg: dict[str, Any], contract: dict[str, Any], landing: LandingStore, meta: MetadataStore,
         writer: TableWriter, runlog: RunLog, run_id: str, day: int) -> list[LandOutcome]:
    source, dataset = cfg["source_name"], cfg["dataset_name"]
    allowed_extra = {c for c, _ in CHANGE_COLUMNS}
    columns = odcs.columns(contract) + CHANGE_COLUMNS + LOAD_COLUMNS
    band = cfg.get("volume", {}).get("expected_rows_per_delivery", {})
    outcomes: list[LandOutcome] = []

    for d in sorted(landing.deliveries(source, dataset), key=lambda d: (d.window, d.name)):
        key = batch_key(d.window, d.checksum)
        file_id = f"file:{d.window}/{d.name}"
        if meta.batch_status(file_id) is not None:      # this file was handled by an earlier run
            continue
        if meta.batch_status(key) == "loaded":          # new file name, same window and bytes: a resend
            reason = f"same window and bytes as batch {key}"
            meta.record_batch(file_id, d, "skipped_duplicate", run_id, reason)
            outcomes.append(LandOutcome(d, key, "skipped_duplicate", 0, reason))
            continue
        rows = landing.read(d)
        observed = set().union(*(r.keys() for r in rows)) if rows else set()
        verdict = odcs.check_columns(contract, observed, allowed_extra)
        outcome = _decide(d, key, rows, verdict, band)
        if outcome.status == "loaded":
            stamped = [{**r, "_batch_key": key, "_run_id": run_id, "_source_file": d.name, "_window": d.window}
                       for r in rows]
            writer.append(cfg["bronze_table"], stamped, columns)
        if outcome.status == "loaded":
            meta.record_batch(key, d, "loaded", run_id)
        meta.record_batch(file_id, d, outcome.status, run_id, outcome.reason)
        outcomes.append(outcome)

    status = "failed" if any(o.status.startswith("rejected") for o in outcomes) else "succeeded"
    counts = {s: sum(o.status == s for o in outcomes) for s in
              ("loaded", "skipped_duplicate", "rejected_contract", "rejected_volume")}
    counts["rows_loaded"] = sum(o.rows for o in outcomes if o.status == "loaded")
    runlog.write(RunRecord(run_id, day, "land", source, dataset, status, config_fingerprint(cfg), counts,
                           "; ".join(f"{o.delivery.name}: {o.reason}" for o in outcomes if o.reason)))
    return outcomes


def _decide(d, key, rows, verdict, band) -> LandOutcome:
    if not verdict.ok:
        return LandOutcome(d, key, "rejected_contract", 0, verdict.reason)
    if len(rows) != d.declared_rows:
        return LandOutcome(d, key, "rejected_volume", 0,
                           f"delivery manifest says {d.declared_rows} rows, file has {len(rows)}")
    lo, hi = band.get("min", 0), band.get("max", float("inf"))
    if not lo <= len(rows) <= hi:
        return LandOutcome(d, key, "rejected_volume", 0, f"{len(rows)} rows is outside the expected {lo}-{hi}")
    note = f"new column(s) kept in bronze only: {', '.join(verdict.extra)}" if verdict.extra else ""
    return LandOutcome(d, key, "loaded", len(rows), note)
