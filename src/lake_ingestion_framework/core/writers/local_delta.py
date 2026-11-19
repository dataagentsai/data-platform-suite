"""Local Delta writer — turns ``QueryBatch`` streams into Delta tables on disk.

Used by the ADF Mimic Copy executor (per ``docs/adf_mimic_design.md``).
The writer:

1. Consumes ``Iterator[QueryBatch]`` from a connector's ``read_query`` /
   ``read_file`` / ``read_endpoint``.
2. Enriches each row with framework metadata columns (``_run_id``,
   ``_loaded_at``, ``_record_hash``).
3. Builds a ``pyarrow.Table`` per batch and appends it to the target
   Delta table via ``deltalake.write_deltalake``.
4. Tracks counts and bytes for the run-log / stats.

Why ``deltalake`` (Rust binding) and not ``delta-spark``:

- No JVM. No Spark startup cost (Spark cold-start is 10-30 s; deltalake
  imports in milliseconds).
- Pure Python wheels with bundled native binaries; no system-level
  install.
- The local mimic exists exactly so we can run end-to-end *without*
  heavyweight runtimes — pulling in Spark for local writes defeats the
  purpose.

Type mapping is deferred. Per Phase 0b's lean scope, we let pyarrow
infer types from the row tuples + columns. Cloud-parity type maps land
in a follow-up phase (ADR 0009) when first schema-mismatch arises in
practice. Until then: integers, floats, strings, booleans, timestamps,
and dates flow through correctly via Arrow's inference; complex types
(arrays, structs) are a known edge that we'll tighten when needed.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lake_ingestion_framework.core.exceptions import (
    SinkUnavailableError,
    WriteFailedError,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_LOADED_AT as _LOADED_AT_COL,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_RECORD_HASH as _RECORD_HASH_COL,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_RUN_ID as _RUN_ID_COL,
)
from lake_ingestion_framework.core.metadata_cols import (
    compute_record_hash,
)
from lake_ingestion_framework.core.source_connectors.protocol import QueryBatch


@dataclass(frozen=True)
class WriteStats:
    """Counts emitted by ``LocalDeltaWriter.write_batched``."""

    rows_written: int = 0
    batches_written: int = 0
    columns: tuple[str, ...] = field(default_factory=tuple)


class LocalDeltaWriter:
    """Append a ``QueryBatch`` stream into a local Delta table.

    Lifecycle:

        writer = LocalDeltaWriter(table_path=Path("/tmp/lakes/.../orders"))
        stats = writer.write_batched(connector.read_query(...), run_id="run-123")

    The writer creates the Delta table on first write (no separate
    schema definition). Subsequent writes append; schema evolution
    beyond add-column is out of scope (see CLAUDE.md).
    """

    def __init__(
        self,
        table_path: Path,
        *,
        partition_by: list[str] | None = None,
    ) -> None:
        self._table_path = table_path
        self._partition_by = partition_by or []

    def write_batched(
        self,
        batches: Iterable[QueryBatch],
        *,
        run_id: str,
        hash_columns: list[str] | None = None,
    ) -> WriteStats:
        """Write each batch to the target table and return cumulative stats.

        Each batch is enriched with three metadata columns (``_run_id``,
        ``_loaded_at``, ``_record_hash``) before being converted to an
        Arrow table and appended to the target Delta path.

        ``hash_columns`` selects which source columns participate in the
        per-row hash. Default is all source columns (excludes the
        injected metadata columns themselves).
        """
        deltalake = _lazy_import_deltalake()
        pa = _lazy_import_pyarrow()
        loaded_at = datetime.now(UTC)

        rows_total = 0
        batches_total = 0
        columns_seen: tuple[str, ...] = ()

        for batch in batches:
            if not batch.rows:
                continue
            arrow_table = self._build_arrow_table(
                pa,
                batch=batch,
                run_id=run_id,
                loaded_at=loaded_at,
                hash_columns=hash_columns,
            )
            try:
                deltalake.write_deltalake(
                    str(self._table_path),
                    arrow_table,
                    mode="append",
                    partition_by=self._partition_by or None,
                )
            except Exception as exc:  # deltalake error types are version-dependent
                raise WriteFailedError(f"Delta write failed for {self._table_path}: {exc}") from exc

            rows_total += len(batch.rows)
            batches_total += 1
            if not columns_seen:
                columns_seen = (
                    *batch.columns,
                    _RUN_ID_COL,
                    _LOADED_AT_COL,
                    _RECORD_HASH_COL,
                )

        return WriteStats(
            rows_written=rows_total,
            batches_written=batches_total,
            columns=columns_seen,
        )

    def _build_arrow_table(
        self,
        pa: Any,
        *,
        batch: QueryBatch,
        run_id: str,
        loaded_at: datetime,
        hash_columns: list[str] | None,
    ) -> Any:
        """Convert one ``QueryBatch`` to a pyarrow Table with metadata cols."""
        # Indices of columns participating in the row-hash. Default: all
        # source columns. Custom subset lets callers exclude noisy fields
        # (large blobs, audit timestamps) where stable change-detection
        # matters more than a literal full-row hash.
        if hash_columns is None:
            hash_indices = list(range(len(batch.columns)))
        else:
            col_to_idx = {c: i for i, c in enumerate(batch.columns)}
            hash_indices = [col_to_idx[c] for c in hash_columns if c in col_to_idx]

        per_column_data: dict[str, list[Any]] = {c: [] for c in batch.columns}
        run_ids: list[str] = []
        loaded_ats: list[datetime] = []
        record_hashes: list[str] = []

        for row in batch.rows:
            for idx, col in enumerate(batch.columns):
                per_column_data[col].append(row[idx] if idx < len(row) else None)
            run_ids.append(run_id)
            loaded_ats.append(loaded_at)
            record_hashes.append(_compute_record_hash(row, hash_indices))

        # Build columnar arrays — pyarrow infers types from the Python values
        arrays = [pa.array(per_column_data[c]) for c in batch.columns]
        names = list(batch.columns)

        arrays.append(pa.array(run_ids, type=pa.string()))
        names.append(_RUN_ID_COL)
        arrays.append(pa.array(loaded_ats, type=pa.timestamp("us", tz="UTC")))
        names.append(_LOADED_AT_COL)
        arrays.append(pa.array(record_hashes, type=pa.string()))
        names.append(_RECORD_HASH_COL)

        return pa.Table.from_arrays(arrays, names=names)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _compute_record_hash(row: tuple[Any, ...], indices: list[int]) -> str:
    """Hash the values at ``indices`` from ``row``.

    Thin adapter over ``metadata_cols.compute_record_hash`` — extracts
    the indexed values into a tuple before hashing. Identical recipe
    across Bronze / Silver / Gold so change detection is portable.
    """
    selected = tuple(row[i] if i < len(row) else None for i in indices)
    return compute_record_hash(selected)


def _lazy_import_deltalake() -> Any:
    """Import deltalake on first use; surface a clean error if missing.

    Lazy import so the framework stays importable without the [local]
    extra — same pattern as every connector module.
    """
    try:
        import deltalake  # — lazy by design
    except ImportError as exc:
        raise SinkUnavailableError(
            "deltalake not installed. Install with: pip install lake-ingest[local]"
        ) from exc
    return deltalake


def _lazy_import_pyarrow() -> Any:
    """Import pyarrow on first use; surface a clean error if missing.

    Lazy import so the framework stays importable without the [local]
    extra — same pattern as every connector module.
    """
    try:
        import pyarrow as pa  # — lazy by design
    except ImportError as exc:
        raise SinkUnavailableError(
            "pyarrow not installed. Install with: pip install lake-ingest[local]"
        ) from exc
    return pa


# Iterator alias to avoid an unused-import warning if callers want a
# precise type hint for the batches argument.
_BatchIterator = Iterator[QueryBatch]


__all__ = ["LocalDeltaWriter", "WriteStats"]
