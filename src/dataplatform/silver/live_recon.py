"""Live source against silver (DPC-0052, DAC REC-05): a source can lose rows silently (a change feed that skips
changes during maintenance), so every day the live table is compared with silver's current rows. Pure and product-free.

By buckets first: both sides add up rows per business-key bucket (dataplatform.reconcile.bucket_of; the engine
computes silver's side, only totals come back). Only buckets that differ are drilled into: their keys on both sides,
so the finding names the exact keys missing from silver or extra in it, and the hop (sources to landing).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from dataplatform.reconcile import bucket_of, bucket_totals, compare_buckets
from dataplatform.silver.plan import SilverPlan

STEP = "recon.live_source_vs_silver"
BUCKETS = 16


class BucketedSilver(Protocol):
    def bucket_totals(self, plan: SilverPlan, measures: dict[str, str | None],
                      buckets: int) -> dict[int, dict[str, float]]: ...

    def keys_in_buckets(self, plan: SilverPlan, buckets: int, wanted: set[int]) -> set[tuple[str, ...]]: ...


@dataclass(frozen=True)
class LiveRecon:
    dataset: str
    source_rows: int
    silver_rows: int
    buckets_differing: tuple[int, ...] = ()
    missing: tuple[tuple[str, ...], ...] = ()  # in the live source, not in silver
    extra: tuple[tuple[str, ...], ...] = field(default_factory=tuple)  # in silver, not in the live source

    @property
    def ok(self) -> bool:
        return not self.buckets_differing

    def message(self) -> str:
        if self.ok:
            return f"live source {self.source_rows} rows = silver {self.silver_rows} rows in every bucket"
        hop = "sources to landing" if self.missing and not self.extra else "between source and silver"
        show = ", ".join("/".join(k) for k in self.missing[:10])
        return (f"live source {self.source_rows} rows, silver {self.silver_rows}; {len(self.buckets_differing)} "
                f"bucket(s) differ; {len(self.missing)} key(s) missing from silver, {len(self.extra)} extra; hop: "
                f"{hop}; missing: {show}")


def _text(key: Iterable[Any]) -> tuple[str, ...]:
    return tuple("" if v is None else str(v) for v in key)


def reconcile_live(dataset: str, source_rows: list[Mapping[str, Any]], plan: SilverPlan, silver: BucketedSilver,
                   buckets: int = BUCKETS) -> LiveRecon:
    keys = plan.keys
    left = bucket_totals([dict(r) for r in source_rows], keys, {"rows": None}, buckets)
    right = silver.bucket_totals(plan, {"rows": None}, buckets)
    total = int(sum(b["rows"] for b in right.values()))
    bad = sorted({b for b, _, _, _ in compare_buckets(left, right)})
    if not bad:
        return LiveRecon(dataset, len(source_rows), total)
    wanted = set(bad)
    src = {_text(r.get(k) for k in keys) for r in source_rows
           if bucket_of(tuple(r.get(k) for k in keys), buckets) in wanted}
    sil = silver.keys_in_buckets(plan, buckets, wanted)
    return LiveRecon(dataset, len(source_rows), total, tuple(bad), tuple(sorted(src - sil)), tuple(sorted(sil - src)))
