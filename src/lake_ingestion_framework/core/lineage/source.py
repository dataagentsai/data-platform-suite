"""Source → Bronze emitter (Phase 7-D).

Emits one OL ``COMPLETE`` event per ingestion run — closing the upstream
gap so the framework's owned arc is **Source → Bronze → Silver → Gold**.

Why this lives outside the writer
---------------------------------

``LocalDeltaWriter`` consumes ``QueryBatch`` streams without knowing the
source identity (a deliberate design — same writer drives postgres,
mysql, REST, SFTP). The orchestrator (CLI today, scheduler later)
holds both halves of the picture: the source connector's identity AND
the writer's :class:`WriteStats`. So the orchestrator assembles
:class:`SourceRunStats` post-write and calls this emitter.

Each event carries:

- Input ``Dataset``: the source — e.g., ``postgres://prod-db/public.orders``
- Output ``Dataset``: the Bronze Delta table with a ``SchemaDatasetFacet``
- ``SqlJobFacet`` on the job when ``query_text`` is supplied (OL has a
  standard facet for this — design §6.2 / column-lineage seed)
- Custom ``sourceRunStats`` facet — ``rows_written`` /
  ``batches_written`` / ``duration_seconds`` (cost-free numbers from
  ``LocalDeltaWriter``)
- Job namespace ``lake-ingest.source`` so consumers can slice
  ingestion events from Silver/dbt events

The dataset names follow the same convention as Silver/Gold: callers
pass ``<schema>.<table>`` (e.g., ``bronze.orders``) so the
Bronze→Silver→Gold graph stitches without a downstream rename.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lake_ingestion_framework.core.lineage._ol import (
    PRODUCER,
    lazy_import_openlineage,
    serialize,
)

if TYPE_CHECKING:
    from lake_ingestion_framework.core.lineage.sinks import LineageSink


_JOB_NAMESPACE = "lake-ingest.source"


@dataclass(frozen=True)
class SourceRunStats:
    """Identity + counts for one Source → Bronze ingestion run.

    Built by the orchestrator (CLI / scheduler) by combining the
    source connector's identity with :class:`LocalDeltaWriter`'s
    :class:`WriteStats`. The writer doesn't know the source identity
    so it can't do this itself.
    """

    run_id: str
    """Run ID — same value passed to ``LocalDeltaWriter.write_batched``."""

    source_namespace: str
    """OL namespace for the source — e.g.,
    ``postgres://prod-db.example.com:5432`` or
    ``salesforce://prod.my.salesforce.com``."""

    source_name: str
    """Source dataset name — e.g., ``public.orders`` (Postgres
    schema.table) or ``Account`` (Salesforce object)."""

    bronze_namespace: str
    """OL namespace for the Bronze target — typically
    ``delta://<storage_account>`` or ``databricks://<workspace>``."""

    bronze_name: str
    """Bronze dataset name — conventionally ``bronze.<table>`` so the
    downstream Silver/Gold graph stitches by name."""

    rows_written: int
    columns: tuple[str, ...]
    """Bronze-side columns, in write order. Includes the framework's
    metadata cols (``_run_id`` / ``_loaded_at`` / ``_record_hash``)."""

    batches_written: int = 0
    duration_seconds: float = 0.0
    query_text: str | None = None
    """Optional source query (SQL, REST URL, file glob). Emitted as an
    ``SqlJobFacet`` when present — feeds future column-lineage SQL
    parsing without requiring callers to opt in."""

    source_columns: tuple[str, ...] = field(default_factory=tuple)
    """Columns on the *source* side. Often equal to ``columns`` minus
    the Bronze metadata cols. Surfaced as a SchemaDatasetFacet on the
    input dataset so column-level lineage queries (future phase) can
    map source → Bronze without re-querying the source."""


def emit_source_run_event(
    stats: SourceRunStats,
    sink: LineageSink,
) -> dict[str, Any]:
    """Emit one OL ``COMPLETE`` event for a Source → Bronze ingestion.

    Returns the emitted event dict for inspection. The sink's ``emit``
    is invoked exactly once per call.

    Skips silently with a no-op if ``stats.run_id`` or the source/bronze
    identifiers are empty — same defensive shape as the Silver emitter.
    """
    if not stats.run_id or not stats.source_name or not stats.bronze_name:
        return {}

    ol = lazy_import_openlineage()
    event_time = datetime.now(UTC).isoformat()
    job_name = f"source/{stats.bronze_name}"
    run_id = _stable_run_id(stats.run_id, job_name)

    bronze_facets: dict[str, Any] = {}
    if stats.columns:
        bronze_facets["schema"] = ol.SchemaDatasetFacet(
            fields=[ol.SchemaDatasetFacetFields(name=col, type="unknown") for col in stats.columns]
        )

    source_facets: dict[str, Any] = {}
    if stats.source_columns:
        source_facets["schema"] = ol.SchemaDatasetFacet(
            fields=[
                ol.SchemaDatasetFacetFields(name=col, type="unknown")
                for col in stats.source_columns
            ]
        )

    job_facets: dict[str, Any] = {}
    if stats.query_text:
        # The SQL facet only models SQL strictly; for non-SQL sources
        # (REST / SFTP) we still attach it via an OL "documentation"
        # job facet so downstream consumers see the query body somewhere.
        # Future Source-emitter polish can switch to ``sql:`` for SQL
        # connectors and ``documentation:`` for non-SQL.
        job_facets["documentation"] = ol.DocumentationJobFacet(description=stats.query_text)

    event = ol.RunEvent(
        eventType=ol.RunState.COMPLETE,
        eventTime=event_time,
        run=ol.Run(runId=run_id),
        job=ol.Job(namespace=_JOB_NAMESPACE, name=job_name, facets=job_facets),
        producer=PRODUCER,
        inputs=[
            ol.InputDataset(
                namespace=stats.source_namespace,
                name=stats.source_name,
                facets=source_facets,
            )
        ],
        outputs=[
            ol.OutputDataset(
                namespace=stats.bronze_namespace,
                name=stats.bronze_name,
                facets=bronze_facets,
            )
        ],
    )

    payload: dict[str, Any] = json.loads(serialize(ol, event))
    payload.setdefault("run", {}).setdefault("facets", {})["sourceRunStats"] = {
        "_producer": PRODUCER,
        "_schemaURL": (
            "https://github.com/anthropics/lake-ingest/"
            "blob/main/docs/lineage_design.md#custom-facets"
        ),
        "rows_written": stats.rows_written,
        "batches_written": stats.batches_written,
        "duration_seconds": stats.duration_seconds,
    }

    sink.emit(payload)
    return payload


def _stable_run_id(framework_run_id: str, job_name: str) -> str:
    """Hash framework_run_id + job_name into a UUID-shaped string.

    OL ``runId`` must be a UUID. We derive deterministically so a
    re-emit (e.g., for backfill) produces the same OL runId.
    """
    digest = hashlib.sha256(f"{framework_run_id}/{job_name}".encode()).digest()
    return str(uuid.UUID(bytes=digest[:16]))
