"""Run the privacy scan over every enabled dataset's bronze and silver (B20 C15). Engine-free: frames come through
the silver ports. Returns (layer, dataset.column, values in clear) per restricted column."""

from __future__ import annotations

from dataplatform.meta.port import GOLD_KINDS, MetadataStore
from dataplatform.silver.plan import plan_from_view
from dataplatform.silver.ports import SilverTables
from dataplatform.silver.runner import new_batches
from dataplatform.stages.privacy import clear_counts

RESTRICTED = "restricted"


def scan(store: MetadataStore, tables: SilverTables, domain: str, pattern: str) -> list[tuple[str, str, int]]:
    out = []
    for ref in store.datasets(domain):
        if not ref.is_enabled or ref.kind in GOLD_KINDS:
            continue
        view = store.dataset_view(ref.source, ref.dataset)
        plan = plan_from_view(view, pattern)
        restricted = [c for c in plan.columns if c.classification == RESTRICTED]
        if not restricted:
            continue
        name = f"{ref.source}.{ref.dataset}"
        silver = tables.current(plan)
        if silver is not None:
            for col, n in clear_counts(silver, [c.name for c in restricted], pattern).items():
                out.append(("silver", f"{name}.{col}", n))
        keys, _ = new_batches(store, ref.source, ref.dataset, from_start=True)
        if keys:
            bronze = tables.bronze(plan, keys)
            for col, n in clear_counts(bronze, [c.source or c.name for c in restricted], pattern).items():
                out.append(("bronze", f"{name}.{col}", n))
    return out
