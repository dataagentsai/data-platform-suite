"""dedup group — row-level deduplication.

Unlike the column processors, dedup operates on the whole pyarrow
Table at once. It runs AFTER the column groups (so dedup respects
post-PII / post-cast equivalence) and BEFORE ``_record_hash``
recompute (so the hash is over the surviving rows only).

| Mode                          | What it does |
|-------------------------------|--------------|
| ``none`` (default)            | No-op |
| ``exact``                     | Drop perfectly identical rows within one run |
| ``business_key_first``        | Keep first per business_key |
| ``business_key_latest``       | Keep latest per business_key by ``order_by`` |
| ``flag``                      | Add ``_is_duplicate`` column without dropping |

Bronze-upsert (Bronze MERGE instead of APPEND) is intentionally NOT
in this module — it would break Bronze byte-faithfulness. Customers
who want it must opt in via a separate config + ADR.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pyarrow as pa


DedupMode = Literal["none", "exact", "business_key_first", "business_key_latest", "flag"]


@dataclass(frozen=True)
class DedupConfig:
    """Resolved dedup settings for one Silver run.

    None / empty means "no dedup". The engine no-ops cleanly.
    """

    mode: DedupMode = "none"
    business_keys: tuple[str, ...] = ()
    order_by: str | None = None


@dataclass(frozen=True)
class DedupResult:
    """Outcome of one dedup pass."""

    rows_in: int
    rows_out: int
    rows_dropped: int
    flagged_only: int = 0
    """When ``mode='flag'``, count of rows tagged as duplicates
    (rows weren't dropped). 0 for other modes."""

    @property
    def fired(self) -> bool:
        return self.rows_dropped > 0 or self.flagged_only > 0


# ──────────────────────────────────────────────────────────────────────


def apply_dedup(table: pa.Table, config: DedupConfig) -> tuple[pa.Table, DedupResult]:
    """Apply the dedup mode to a pyarrow Table.

    Returns ``(deduped_table, result)``. The returned Table is fresh
    (input never mutated). For modes that need ``business_keys`` /
    ``order_by`` but aren't given them, the call no-ops with a clear
    DedupResult — caller decides whether to surface a warning.
    """
    rows_in = table.num_rows

    if config.mode == "none" or rows_in == 0:
        return table, DedupResult(rows_in=rows_in, rows_out=rows_in, rows_dropped=0)

    if config.mode == "exact":
        return _dedup_exact(table)

    if config.mode in {"business_key_first", "business_key_latest"}:
        if not config.business_keys:
            return table, DedupResult(rows_in=rows_in, rows_out=rows_in, rows_dropped=0)
        keep: Literal["first", "latest"] = (
            "first" if config.mode == "business_key_first" else "latest"
        )
        return _dedup_by_keys(table, config.business_keys, order_by=config.order_by, keep=keep)

    if config.mode == "flag":
        if not config.business_keys:
            return table, DedupResult(rows_in=rows_in, rows_out=rows_in, rows_dropped=0)
        return _flag_duplicates(table, config.business_keys)

    return table, DedupResult(rows_in=rows_in, rows_out=rows_in, rows_dropped=0)


# ──────────────────────────────────────────────────────────────────────
# Mode implementations
# ──────────────────────────────────────────────────────────────────────


def _dedup_exact(table: pa.Table) -> tuple[pa.Table, DedupResult]:
    """Drop perfectly identical rows. Uses pyarrow's group_by on all
    columns; each group's first row is kept."""
    # Build a tuple-key per row across all columns, mark first-seen.
    columns_data = [table.column(c).to_pylist() for c in table.column_names]
    seen: set[tuple[object, ...]] = set()
    keep_indices: list[int] = []
    for i in range(table.num_rows):
        key = tuple(col[i] for col in columns_data)
        if key in seen:
            continue
        seen.add(key)
        keep_indices.append(i)
    kept = table.take(keep_indices)
    rows_in = table.num_rows
    return kept, DedupResult(
        rows_in=rows_in,
        rows_out=kept.num_rows,
        rows_dropped=rows_in - kept.num_rows,
    )


def _dedup_by_keys(
    table: pa.Table,
    business_keys: tuple[str, ...],
    *,
    order_by: str | None,
    keep: Literal["first", "latest"],
) -> tuple[pa.Table, DedupResult]:
    """Keep one row per business_key tuple.

    * ``keep='first'`` — preserves the first row in input order
    * ``keep='latest'`` — preserves the row with the max ``order_by``
      value (or the last row in input order if ``order_by`` is None)
    """
    rows_in = table.num_rows
    key_lists = [table.column(k).to_pylist() for k in business_keys]
    order_values: list[object] | None = (
        table.column(order_by).to_pylist() if order_by and order_by in table.column_names else None
    )

    chosen_idx: dict[tuple[object, ...], int] = {}
    for i in range(rows_in):
        key = tuple(c[i] for c in key_lists)
        if key not in chosen_idx:
            chosen_idx[key] = i
            continue
        if keep == "first":
            continue  # already have one
        # 'latest'
        if order_values is None:
            chosen_idx[key] = i  # last-in-source-order wins
        else:
            current = order_values[chosen_idx[key]]
            new = order_values[i]
            if new is not None and (current is None or new > current):  # type: ignore[operator]
                chosen_idx[key] = i

    keep_indices = sorted(chosen_idx.values())
    kept = table.take(keep_indices)
    return kept, DedupResult(
        rows_in=rows_in,
        rows_out=kept.num_rows,
        rows_dropped=rows_in - kept.num_rows,
    )


def _flag_duplicates(
    table: pa.Table, business_keys: tuple[str, ...]
) -> tuple[pa.Table, DedupResult]:
    """Add ``_is_duplicate`` column without dropping rows.

    First occurrence of each business_key tuple is False; subsequent
    occurrences are True. Lets downstream investigate without losing data.
    """
    import pyarrow as pa

    rows_in = table.num_rows
    key_lists = [table.column(k).to_pylist() for k in business_keys]
    seen: set[tuple[object, ...]] = set()
    flags: list[bool] = []
    flagged_count = 0
    for i in range(rows_in):
        key = tuple(c[i] for c in key_lists)
        is_dup = key in seen
        flags.append(is_dup)
        if is_dup:
            flagged_count += 1
        else:
            seen.add(key)
    new_array = pa.array(flags, type=pa.bool_())
    flagged_table = table.append_column("_is_duplicate", new_array)
    return flagged_table, DedupResult(
        rows_in=rows_in,
        rows_out=rows_in,
        rows_dropped=0,
        flagged_only=flagged_count,
    )


__all__ = [
    "DedupConfig",
    "DedupMode",
    "DedupResult",
    "apply_dedup",
]
