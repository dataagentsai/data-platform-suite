"""Lineage layer — emits OpenLineage events at framework boundaries.

Phase 7-A: dbt manifest → OL events (Gold side).
Phase 7-B: SilverPipeline → OL events (Bronze→Silver hop).

Public emitters:

- :func:`emit_from_dbt_manifest` — reads ``target/manifest.json``,
  emits one ``COMPLETE`` event per Gold model + snapshot
- :func:`emit_silver_run_event` — called after a successful
  ``SilverPipeline.run()``, emits one ``COMPLETE`` event with
  Bronze→Silver edges + run-stats facet

Sinks (where events go) live in :mod:`.sinks`. The default everywhere
is :class:`.sinks.JsonLinesSink` — JSONL is the canonical OpenLineage
wire format.

Out of scope (later lineage phases): Source emitter, Spark-listener
Gold path, semantic / activation / vector emitters, HTTP transport
to Marquez / UC native push, ``meta.lineage_*`` SQL persistence,
column-level edges via SQL parsing, MCP server, drift / PII / impact.
"""

from lake_ingestion_framework.core.lineage._ol import LineageEmitError
from lake_ingestion_framework.core.lineage.dbt_manifest import (
    LineageEmitConfig,
    LineageEmitStats,
    emit_from_dbt_manifest,
)
from lake_ingestion_framework.core.lineage.graph import (
    DatasetNode,
    JobReference,
    LineageEdge,
    LineageGraph,
    LineageGraphError,
)
from lake_ingestion_framework.core.lineage.silver import emit_silver_run_event
from lake_ingestion_framework.core.lineage.sinks import JsonLinesSink, LineageSink
from lake_ingestion_framework.core.lineage.source import (
    SourceRunStats,
    emit_source_run_event,
)

__all__ = [
    "DatasetNode",
    "JobReference",
    "JsonLinesSink",
    "LineageEdge",
    "LineageEmitConfig",
    "LineageEmitError",
    "LineageEmitStats",
    "LineageGraph",
    "LineageGraphError",
    "LineageSink",
    "SourceRunStats",
    "emit_from_dbt_manifest",
    "emit_silver_run_event",
    "emit_source_run_event",
]
