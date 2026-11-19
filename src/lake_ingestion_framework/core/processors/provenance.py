"""provenance group — framework audit columns.

| Processor              | Default | Notes |
|------------------------|---------|-------|
| silver_run_id          | on      | already shipped (in SilverPipeline._enrich) |
| silver_loaded_at       | on      | already shipped |
| record_hash            | on      | already shipped |
| source_file_uri        | on (file sources) | NEW — per-row file URI |
| ingestion_batch_id     | on      | NEW — correlates rows ingested in same batch |

Wave 1 formalises the existing Silver enrich into named processors so
they show up in ``processors describe`` output. The original logic
stays in ``SilverPipeline._enrich`` for now; the engine just
*describes* them — actual move to engine-driven application happens
in a follow-up commit (mechanical refactor, no behaviour change).

Two new processors land here as data shapes:

* ``source_file_uri`` — adds a ``_source_file_uri`` STRING column.
  Default-on for file-based source types (sftp / s3 / https_pull /
  azure_blob); silent no-op otherwise. Wave 1 ships the marker; the
  populated value comes from the connector when file ingestion lands
  metadata about which file each row came from.
* ``ingestion_batch_id`` — adds a ``_ingestion_batch_id`` STRING
  column. Default-on; value comes from the orchestration layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


_SHIPPED_NAMES = ("silver_run_id", "silver_loaded_at", "record_hash")


class _ShippedProvenance:
    """Marker class for the three already-shipped Silver-enrich
    processors (``_silver_run_id`` / ``_silver_loaded_at`` /
    ``_record_hash``). The engine reports them as fired but the actual
    column-add lives in ``SilverPipeline._enrich`` for now."""

    group = ProcessorGroup.PROVENANCE

    def __init__(self, name: str) -> None:
        self.name = name

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        # These run table-wide, not column-by-column. The engine
        # invokes the provenance group exactly once per run, not per
        # column — see ProcessorEngine.apply_provenance_group.
        return False

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        return table, ProcessorRunResult(self.group, self.name, ctx.silver_column_name, fired=True)


PROVENANCE_DEFAULT_ON = tuple(_ShippedProvenance(name) for name in _SHIPPED_NAMES)


__all__ = [
    "PROVENANCE_DEFAULT_ON",
]
