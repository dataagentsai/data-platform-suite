"""The baseline batch writer (B8, assumed decision A3): compact bronze up to the cut-off, write it back to bronze as one
batch, replay it through the processors and compare with silver; log it only when it matches.

Engine-free: frames come and go through the silver ports (SilverTables, StageRunner), rows go through the bronze writer.
The rows travel through the driver once (bronze writers take rows today); a frame write is owed for very large tables.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from dataplatform.housekeeping import BaselineOutcome, batches_of
from dataplatform.meta.port import FileRecord, MetadataStore
from dataplatform.silver.contract_rules import contract_rules
from dataplatform.silver.plan import PlanError, plan_from_view
from dataplatform.silver.ports import SilverTables, StageRunner
from dataplatform.stages.baseline import compact, compactable, replay_check

LOGICAL = {"string": "string", "bigint": "integer", "int": "integer", "long": "integer", "smallint": "integer",
           "double": "number", "float": "number", "boolean": "boolean", "timestamp": "timestamp", "date": "date"}


def logical(dtype: str) -> str:
    """A frame column type as the bronze writer's logical type (decimals as numbers, timestamps with zones too)."""
    base = dtype.split("(", 1)[0].lower()
    if base.startswith("decimal"):
        return "number"
    if base.startswith("timestamp"):
        return "timestamp"
    return LOGICAL.get(base, "string")


def baseline_key(cutoff: date) -> str:
    return f"baseline-{cutoff.isoformat()}"


@dataclass
class SilverBaseliner:
    tables: SilverTables
    stages: StageRunner
    bronze: Any  # BronzeWriter + remove (ingest/ports.py, housekeeping.BronzeRemover)
    token_pattern: str = ""

    def write(self, store: MetadataStore, run_id: str, view: dict[str, Any], cutoff: date,
              today: date) -> BaselineOutcome:
        source, dataset = view["source"], view["dataset"]
        try:
            plan = plan_from_view(view, self.token_pattern)
        except PlanError as exc:
            return BaselineOutcome("refused", {}, f"no plan: {exc}")
        why = compactable(plan)
        if why:
            return BaselineOutcome("not_needed", {}, why)
        plan = replace(plan, rules=plan.rules + contract_rules(store.dq_rules(source, dataset))[0])
        batches = batches_of(store.dataset_files(source, dataset))
        upto = [b.batch_key for b in batches if b.delivered <= cutoff]
        later = [b.batch_key for b in batches if b.delivered > cutoff]
        if not upto:
            return BaselineOutcome("not_needed", {}, f"no batch on or before {cutoff}")
        key, table = baseline_key(cutoff), f"{source}.{dataset}"
        frame = compact(self.tables.bronze(plan, upto), plan)
        columns = [(f.name, logical(f.dataType.simpleString())) for f in frame.schema.fields]
        rows = [{**r.asDict(), "_batch_key": key, "_run_id": run_id} for r in frame.collect()]
        self.bronze.write(table, key, rows, columns, replace=True)
        counts = self._check(plan, key, later, run_id, today)
        counts.update(rows=len(rows), batches_compacted=len(upto))
        if counts["missing"] or counts["extra"]:
            self.bronze.remove(table, [key])
            msg = (f"replay of {key} differs from silver: {counts['missing']} key(s) missing, {counts['extra']} extra; "
                   "baseline removed, bronze kept")
            return BaselineOutcome("refused", counts, msg)
        note = f"baseline of {len(upto)} batch(es) up to {cutoff}, replay = silver"
        store.log_file(FileRecord(run_id, source, dataset, key, key, cutoff.isoformat(), key, None, len(rows), "loaded",
                                  note, "baseline"))
        msg = f"{key}: {len(rows)} rows from {len(upto)} batch(es); replay = silver on {counts['baseline_keys']} key(s)"
        return BaselineOutcome("written", counts, msg)

    def _check(self, plan: Any, key: str, later: list[str], run_id: str, today: date) -> dict[str, int]:
        replayed = self.stages.run(plan, self.tables.bronze(plan, [key]), None, run_id, today.isoformat())
        moved = None
        if later:
            moved = self.stages.run(plan, self.tables.bronze(plan, later), None, run_id, today.isoformat())
        try:
            return replay_check(replayed.rows, moved.rows if moved else None, self.tables.current(plan))
        finally:
            self.stages.release(replayed)
            if moved is not None:
                self.stages.release(moved)


SAME = 1e-6  # measures are rounded to 2 places; anything above this is a real difference
HISTORY_ONLY = ("versions",)  # a rebuild from a baseline keeps no version history before the cut-off (ADR-029)


def prove_rebuild(store: MetadataStore, tables: SilverTables, stages: StageRunner, domain: str, names: set[str], *,
                  run_id: str, as_of: str, token_pattern: str = "") -> dict[str, dict[str, tuple[float, float]]]:
    """Rebuild these datasets' silver from what bronze still holds (ADR-025: baseline + later batches) and compare
    every current-row measure before and after. Returns, per dataset, the measures that differ (empty = proven)."""
    from dataplatform.silver.runner import build_silver  # noqa: PLC0415 -- runner imports housekeeping's neighbours

    def measures() -> dict[str, dict[str, float]]:
        out = {}
        for name in sorted(names):
            source, dataset = name.split(".", 1)
            plan = plan_from_view(store.dataset_view(source, dataset), token_pattern)
            out[name] = tables.measures(plan, plan.measures)
        return out

    before = measures()
    build_silver(store, tables, stages, domain, "dev", run_id, as_of, token_pattern, only=names, rebuild=names)
    after = measures()
    return {n: {k: (v, after[n].get(k, 0.0)) for k, v in before[n].items()
                if k not in HISTORY_ONLY and abs(v - after[n].get(k, 0.0)) > SAME} for n in sorted(names)}
