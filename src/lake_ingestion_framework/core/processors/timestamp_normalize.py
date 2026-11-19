"""timestamp_normalize group — TZ + format consistency.

| Processor              | Default | What it does |
|------------------------|---------|--------------|
| utc_normalize          | on      | tz-aware → UTC |
| epoch_to_timestamp     | on (when BIGINT → TIMESTAMP cast set) | auto-detects ms/s precision |
| iso8601_parse          | on (for STRING → TIMESTAMP) | parse with try_cast fallback |

Operates on TIMESTAMP / DATE / BIGINT columns based on context. A
single column can route through multiple processors in this group
when its bronze type and silver target differ (e.g. BIGINT epoch
seconds → TIMESTAMP; STRING ISO → TIMESTAMP).
"""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


_TIMESTAMP_TYPES = {"TIMESTAMP", "DATETIME", "TIMESTAMPTZ"}
_BIGINT_TYPES = {"BIGINT", "INT8", "LONG"}
_STRING_TYPES = {"STRING", "VARCHAR", "TEXT"}

# Threshold: epoch seconds should be < 10^11 (~5138 AD); anything
# larger we treat as milliseconds.
_EPOCH_MS_THRESHOLD = 10**11


def _normalize_type(t: str | None) -> str:
    return (t or "").upper().split("(", 1)[0].strip()


# ──────────────────────────────────────────────────────────────────────


class UtcNormalize:
    """Convert tz-aware timestamps to UTC. Naive timestamps pass
    through unchanged (use ``assume_timezone`` config + the SilverPipeline's
    own UTC tagging for those)."""

    name = "utc_normalize"
    group = ProcessorGroup.TIMESTAMP_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        bronze = _normalize_type(ctx.bronze_data_type)
        silver = _normalize_type(ctx.silver_data_type)
        return bronze in _TIMESTAMP_TYPES or silver in _TIMESTAMP_TYPES

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        src = table.column(col)
        if not pa.types.is_timestamp(src.type):
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        # Already UTC tz-aware → no-op
        if str(src.type.tz or "").upper() in {"UTC", "+00:00"}:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        # tz-aware but not UTC → cast preserves wall time, swaps tz
        if src.type.tz:
            new_array = src.cast(pa.timestamp(src.type.unit, tz="UTC"))
            idx = table.schema.get_field_index(col)
            return table.set_column(idx, col, new_array), ProcessorRunResult(
                self.group,
                self.name,
                col,
                fired=True,
                rows_affected=table.num_rows,
            )
        # Naive timestamp — caller can use assume_timezone (Wave 4+); skip for now.
        return table, ProcessorRunResult(self.group, self.name, col, fired=False)


class EpochToTimestamp:
    """BIGINT (epoch s/ms) → TIMESTAMP. Auto-detects precision."""

    name = "epoch_to_timestamp"
    group = ProcessorGroup.TIMESTAMP_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        bronze = _normalize_type(ctx.bronze_data_type)
        silver = _normalize_type(ctx.silver_data_type)
        return bronze in _BIGINT_TYPES and silver in _TIMESTAMP_TYPES

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        src = table.column(col)
        if not pa.types.is_integer(src.type):
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        values = src.to_pylist()
        out: list[datetime | None] = []
        for v in values:
            if v is None:
                out.append(None)
                continue
            n = int(v)
            if abs(n) >= _EPOCH_MS_THRESHOLD:
                # milliseconds
                out.append(datetime.fromtimestamp(n / 1000, tz=UTC))
            else:
                out.append(datetime.fromtimestamp(n, tz=UTC))
        new_array = pa.array(out, type=pa.timestamp("us", tz="UTC"))
        idx = table.schema.get_field_index(col)
        return table.set_column(idx, col, new_array), ProcessorRunResult(
            self.group,
            self.name,
            col,
            fired=True,
            rows_affected=len(out),
        )


class Iso8601Parse:
    """STRING → TIMESTAMP via ISO-8601 parsing.

    Uses ``datetime.fromisoformat`` with a small fallback for the
    ``Z`` suffix (Python ≤3.10 doesn't accept it). Bad values become
    NULL — same fail-open posture as ``safe_cast``.
    """

    name = "iso8601_parse"
    group = ProcessorGroup.TIMESTAMP_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        bronze = _normalize_type(ctx.bronze_data_type)
        silver = _normalize_type(ctx.silver_data_type)
        return bronze in _STRING_TYPES and silver in _TIMESTAMP_TYPES

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        src = table.column(col)
        if not pa.types.is_string(src.type):
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        out: list[datetime | None] = []
        for raw in src.to_pylist():
            if raw is None:
                out.append(None)
                continue
            s = str(raw).strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                out.append(None)
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)  # noqa: UP017 — match stdlib name
            out.append(dt.astimezone(UTC))
        new_array = pa.array(out, type=pa.timestamp("us", tz="UTC"))
        idx = table.schema.get_field_index(col)
        return table.set_column(idx, col, new_array), ProcessorRunResult(
            self.group,
            self.name,
            col,
            fired=True,
            rows_affected=len(out),
        )


TIMESTAMP_NORMALIZE_DEFAULT_ON = (UtcNormalize(), EpochToTimestamp(), Iso8601Parse())
TIMESTAMP_NORMALIZE_OPT_IN: dict[str, object] = {}


__all__ = [
    "EpochToTimestamp",
    "Iso8601Parse",
    "TIMESTAMP_NORMALIZE_DEFAULT_ON",
    "TIMESTAMP_NORMALIZE_OPT_IN",
    "UtcNormalize",
]
