"""Inspect mode (INSP-01): rebuild one window of a past gold run from what its run log names. Product-free.

Every fact run logs `gold.inputs` (each silver input and the version it read) and `gold.query` (the build's SQL
with its parameter values shown, or the dataframe function). `reproduce` reads those two rows, reads each input as
it was at that version (time travel through the GoldTables port), runs the same build and key lookup, and returns
the window's rows and numbers beside what gold publishes now. Nothing is written.

    reproduce(store, tables, engine, domain=..., run_id=..., table=..., window=..., tz=...)

The intermediate results come back as frames (built, keyed), so a developer can look at each step on demand.
Two limits, said plainly: dimensions are read as they are now (gold dimensions are rebuilt whole each run, so a
member that changed since then shows its new key), and correction overlays are not replayed (a window a correction
plan rebuilt shows the uncorrected numbers).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dataplatform.contract import TYPES
from dataplatform.gold.harness import AFTER_ALL, INPUTS_STEP, QUERY_STEP, plans
from dataplatform.gold.ports import Frame, GoldEngine, GoldTables
from dataplatform.gold.windows import Numbers
from dataplatform.meta.port import MetadataStore


class NotInspectable(ValueError):
    """The run log does not name what is needed (an older run, a dimension, a table the run did not build)."""


@dataclass
class Reproduced:
    table: str
    window: str
    run_id: str
    as_of: str
    query: str
    versions: dict[str, int]
    built: Frame  # the build's output before keys
    keyed: Frame  # after the key lookup, the window's rows only
    rebuilt: Numbers | None  # the window's numbers rebuilt from the run's inputs
    published: Numbers | None  # what gold publishes for the window now

    @property
    def same(self) -> bool:
        return _plain(self.rebuilt) == _plain(self.published)


def _plain(n: Numbers | None) -> tuple[int, tuple[tuple[str, float], ...]] | None:
    return None if n is None else (n.rows, n.sums)  # completeness is the run's flag, not the data


def _steps(store: MetadataStore, run_id: str, schema: str, table: str) -> dict[str, dict[str, Any]]:
    report = store.run_report(run_id)
    mine = [s for s in report.get("steps", []) if (s.get("source"), s.get("dataset")) == (schema, table)]
    return {s["step"]: s for s in mine if s["step"] in (INPUTS_STEP, QUERY_STEP)}


def reproduce(store: MetadataStore, tables: GoldTables, engine: GoldEngine, *, domain: str, run_id: str, table: str,
              window: str, tz: str, params: dict[str, str] | None = None) -> Reproduced:
    by_name = {p.name: p for p in plans(store, domain)}
    if table not in by_name or by_name[table].window is None:
        raise NotInspectable(f"{table} is not a fact of {domain}")
    p = by_name[table]
    steps = _steps(store, run_id, p.schema, p.name)
    if INPUTS_STEP not in steps:
        raise NotInspectable(f"run {run_id} logged no {INPUTS_STEP} step for {table}")
    versions = {k.split(".", 1)[1]: int(v) for k, v in steps[INPUTS_STEP]["counts"].items()
                if k.startswith("version.")}
    fields = dict(part.split("=", 1) for part in steps[INPUTS_STEP]["message"].split("; ") if "=" in part)
    frames = {}
    for i in p.inputs:
        cols = [(c.name, TYPES[c.logical_type]) for c in store.columns(i.source, i.name)]
        v = versions.get(i.dataset, -1)
        frames[f"{i.source}__{i.name}"] = tables.silver_at(i.dataset, v, cols) if v >= 0 else tables.silver(
            i.dataset, cols)
    built = engine.build(p, frames, {"business_tz": tz, **(params or {})})
    dims = {d.name: tables.gold(by_name[d.name]) for d in p.dimensions}
    keyed = engine.only_windows(p, engine.add_keys(p, built, dims), AFTER_ALL, {window})
    rebuilt = engine.window_numbers(p, keyed, AFTER_ALL, {window}).get(window)
    published = engine.window_numbers(p, tables.gold(p), AFTER_ALL, {window}).get(window)
    return Reproduced(table, window, run_id, fields.get("as_of", ""), steps.get(QUERY_STEP, {}).get("message", ""),
                      versions, built, keyed, rebuilt[0] if rebuilt else None, published[0] if published else None)
