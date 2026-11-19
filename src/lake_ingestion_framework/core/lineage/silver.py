"""Silver pipeline → OpenLineage emitter.

Called *after* a successful ``SilverPipeline.run()`` to push one
``COMPLETE`` OL event per run. The pipeline itself stays pure — it
returns :class:`SilverRunStats` and doesn't import lineage code; the
caller (CLI, future scheduler integration) orchestrates both the run
and the emit.

Each event records:

- Input ``Dataset``: the Bronze Delta path (from ``stats.bronze_path``)
- Output ``Dataset``: the Silver Delta path with a ``SchemaDatasetFacet``
  carrying every column written
- ``runId`` derived deterministically from ``stats.run_id``
- A custom run-stats facet carrying ``rows_read`` / ``rows_written`` /
  ``rows_inserted`` / ``rows_updated`` / ``rows_unchanged`` /
  ``silver_version_after`` / ``duration_seconds`` — every cost-free
  metric the pipeline already captures
- ``eventTime`` = wall-clock ``datetime.now(UTC)`` (real run, not a
  deterministic-from-manifest case like the dbt emitter)

The job namespace is ``lake-ingest.silver`` (vs ``dbt`` for the manifest
emitter) so consumers can group framework-emitted events separately
from dbt-emitted ones.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lake_ingestion_framework.core.lineage._ol import (
    PRODUCER,
    lazy_import_openlineage,
    serialize,
)

if TYPE_CHECKING:
    from lake_ingestion_framework.core.lineage.sinks import LineageSink
    from lake_ingestion_framework.core.silver.pipeline import SilverRunStats


_DEFAULT_NAMESPACE = "delta://lakehouse"
_JOB_NAMESPACE = "lake-ingest.silver"


def emit_silver_run_event(
    stats: SilverRunStats,
    sink: LineageSink,
    *,
    namespace: str = _DEFAULT_NAMESPACE,
    silver_dataset_name: str | None = None,
    bronze_dataset_name: str | None = None,
) -> dict[str, Any]:
    """Emit one OL ``COMPLETE`` event for a successful Silver run.

    Returns the emitted event as a dict so the caller (or tests) can
    inspect what landed in the sink. The sink's ``emit`` is invoked
    exactly once per call.

    Dataset naming — **must match downstream consumers**:

    - The Gold dbt-manifest emitter names datasets ``<schema>.<table>``
      (e.g. ``silver.customers``, ``gold.dim_customer``). For the
      Bronze→Silver→Gold graph to stitch together, ``silver_dataset_name``
      should match the Gold project's source declaration (the
      ``GoldSource(name=...)`` value, prefixed with ``silver.``).
    - Both arguments default to ``<silver_path>.name`` /
      ``<bronze_path>.name`` — fine for filesystem-only local mode
      where downstream consumers see the same paths.
    - In production the orchestrator/CLI should pass explicit names
      that match the warehouse table identifiers.

    Skips silently with a no-op if ``stats.bronze_path`` or
    ``stats.run_id`` aren't populated — these were added in the same
    phase as this emitter; pre-phase pipelines (or stats constructed
    directly in tests) won't carry them and shouldn't crash.
    """
    if not stats.bronze_path or not stats.run_id:
        # Nothing to correlate — skip rather than emit a malformed event.
        return {}

    ol = lazy_import_openlineage()
    event_time = datetime.now(UTC).isoformat()
    job_name = _job_name(stats.silver_path)
    run_id = _stable_run_id(stats.run_id, job_name)

    output = ol.OutputDataset(
        namespace=namespace,
        name=silver_dataset_name or _dataset_name(stats.silver_path),
        facets=_output_facets(stats, ol),
    )
    bronze_input = ol.InputDataset(
        namespace=namespace,
        name=bronze_dataset_name or _dataset_name(stats.bronze_path),
        facets={},
    )

    event = ol.RunEvent(
        eventType=ol.RunState.COMPLETE,
        eventTime=event_time,
        run=ol.Run(runId=run_id),
        job=ol.Job(namespace=_JOB_NAMESPACE, name=job_name, facets={}),
        producer=PRODUCER,
        inputs=[bronze_input],
        outputs=[output],
    )

    payload: dict[str, Any] = json.loads(serialize(ol, event))
    # Custom run-stats facet — not part of the OL spec but consumers
    # like Marquez surface unknown facets in the UI; UC ignores them.
    # Carrying these here lets downstream queries answer "how many rows
    # did silver-run-42 ingest?" without re-reading the run_log table.
    payload.setdefault("run", {}).setdefault("facets", {})["silverRunStats"] = {
        "_producer": PRODUCER,
        "_schemaURL": (
            "https://github.com/anthropics/lake-ingest/"
            "blob/main/docs/lineage_design.md#custom-facets"
        ),
        "rows_read": stats.rows_read,
        "rows_written": stats.rows_written,
        "rows_inserted": stats.rows_inserted,
        "rows_updated": stats.rows_updated,
        "rows_unchanged": stats.rows_unchanged,
        "silver_version_after": stats.silver_version_after,
        "duration_seconds": stats.duration_seconds,
        "strategy": stats.strategy.value,
    }

    # Phase 6 — DQ results facet. Only emit when DQ actually ran;
    # the absence of the facet means "no rules were configured at any
    # injection point for this dataset". Marquez surfaces unknown
    # facets in the UI; consumers can filter on _producer.
    if stats.dq_rules_evaluated > 0:
        payload["run"]["facets"]["silverDqResults"] = {
            "_producer": PRODUCER,
            "_schemaURL": (
                "https://github.com/anthropics/lake-ingest/"
                "blob/main/docs/lineage_design.md#dq-facet"
            ),
            "rules_evaluated": stats.dq_rules_evaluated,
            "rules_passed": stats.dq_rules_passed,
            "rules_failed": stats.dq_rules_failed,
            "rules_errored": stats.dq_rules_errored,
            "violations_logged": stats.dq_violations_logged,
            "rows_quarantined": stats.dq_rows_quarantined,
        }

    sink.emit(payload)
    return payload


def _output_facets(stats: SilverRunStats, ol: Any) -> dict[str, Any]:
    facets: dict[str, Any] = {}
    if stats.silver_columns:
        facets["schema"] = ol.SchemaDatasetFacet(
            fields=[
                ol.SchemaDatasetFacetFields(name=col, type="unknown")
                for col in stats.silver_columns
            ]
        )
    return facets


def _job_name(silver_path: Path) -> str:
    """Job name = the Silver path's stem.

    Two Silver runs writing the same target produce events for the same
    job; runId distinguishes them. Same convention as dbt model jobs.
    """
    return f"silver/{silver_path.name}"


def _dataset_name(path: Path) -> str:
    """Use the path stem as the dataset name.

    Realm-agnostic: in production, datasets sit under ADLS/UC tables
    and customers typically override the OL namespace+name pair via the
    emitter's ``namespace`` arg. For the file-backed local mode the
    path stem is honest and unambiguous.
    """
    return path.name


def _stable_run_id(framework_run_id: str, job_name: str) -> str:
    """Hash the framework's run_id + job_name into a UUID-shaped string.

    OL ``runId`` is required to be a UUID. We derive deterministically
    so a re-emit (e.g., replaying for backfill) for the same logical
    run produces the same OL runId — keeps lineage idempotent.
    """
    digest = hashlib.sha256(f"{framework_run_id}/{job_name}".encode()).digest()
    return str(uuid.UUID(bytes=digest[:16]))
