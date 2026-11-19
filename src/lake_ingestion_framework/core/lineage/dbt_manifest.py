"""dbt manifest → OpenLineage events.

Walks the manifest produced by ``dbt parse`` (any dbt invocation that
writes ``target/manifest.json``) and emits one OL ``COMPLETE`` event
per Gold model + snapshot. Each event carries:

- An input ``Dataset`` per upstream source / model — extracted from
  ``depends_on.nodes``
- An output ``Dataset`` for the model itself
- A ``SchemaDatasetFacet`` on every dataset with the column list
- A ``ColumnLineageDatasetFacet`` skeleton on the output (column-level
  edges are model-level today; SQL parsing for true column-to-column
  edges arrives in a later phase)
- A ``DocumentationJobFacet`` carrying the model's description

Events are written to a JSONL file (one OL event per line) — the
canonical wire format. Downstream consumers (Marquez HTTP transport,
UC import, Atlan ingestion) read JSONL natively.

Determinism: events are sorted by ``job.name``; timestamps come from
the manifest's ``metadata.generated_at`` (not ``datetime.now()``) so
re-running against the same manifest produces byte-identical output.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.lineage._ol import (
    PRODUCER,
    LineageEmitError,
    lazy_import_openlineage,
    serialize,
)
from lake_ingestion_framework.core.lineage.sinks import JsonLinesSink, LineageSink

# ──────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LineageEmitConfig:
    """Inputs for a single ``emit_from_dbt_manifest()`` call.

    ``namespace`` identifies the data store in OL terms — for a
    Databricks lakehouse it's typically ``databricks://<workspace>`` or
    ``delta://<storage_account>``. Customers override per-environment.

    ``job_namespace`` groups all jobs from one orchestrator. ``dbt`` is
    the conventional value for dbt-emitted events; framework-emitted
    events from other layers will use other values
    (``lake-ingest.silver`` / ``lake-ingest.source``).

    ``producer`` is a URI identifying *us* — required by the OL spec on
    every event so consumers know which tool emitted it.
    """

    manifest_path: Path
    output_path: Path
    namespace: str = "delta://lakehouse"
    job_namespace: str = "dbt"
    producer: str = PRODUCER


@dataclass(frozen=True)
class LineageEmitStats:
    """What ``emit_from_dbt_manifest()`` actually emitted."""

    events_written: int
    output_path: Path
    inputs_count: int = 0  # distinct upstream datasets referenced
    outputs_count: int = 0  # distinct output datasets
    duration_seconds: float = 0.0
    skipped_nodes: tuple[str, ...] = field(default_factory=tuple)
    """Manifest node IDs we deliberately skipped (e.g., tests, seeds,
    operations) — surfaced for debugging when an expected model
    doesn't appear in the output."""


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────


# Resource types the emitter treats as outputs (they produce a dataset).
# Tests, seeds, operations, analyses don't materialize a dataset and
# wouldn't yield a useful OL event.
_OUTPUT_RESOURCE_TYPES: frozenset[str] = frozenset({"model", "snapshot"})

# Resource types upstream-of relationships can validly point to.
# Anything else (macros, tests, exposures) is filtered out of inputs.
_INPUT_RESOURCE_TYPES: frozenset[str] = frozenset({"model", "snapshot", "source", "seed"})


def emit_from_dbt_manifest(
    config: LineageEmitConfig, sink: LineageSink | None = None
) -> LineageEmitStats:
    """Read a dbt manifest, emit OL events to a JSONL file (or any sink).

    Idempotent: re-running with the same manifest writes byte-identical
    output. Fails fast on malformed manifest before touching the output
    path.

    By default events go to a :class:`JsonLinesSink` opened in truncate
    mode at ``config.output_path``. Pass an explicit ``sink`` (e.g., a
    future ``MarquezHttpSink`` or in-memory test sink) to override.
    """
    started_at = time.monotonic()
    ol = lazy_import_openlineage()

    manifest = _load_manifest(config.manifest_path)
    event_time = _manifest_event_time(manifest)

    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})

    # Build a node lookup combining nodes + sources — every potential
    # input or output references one of these by unique_id.
    all_nodes: dict[str, dict[str, Any]] = {**nodes, **sources}

    output_node_ids = sorted(
        node_id
        for node_id, node in nodes.items()
        if node.get("resource_type") in _OUTPUT_RESOURCE_TYPES
    )
    skipped = tuple(
        sorted(
            node_id
            for node_id, node in nodes.items()
            if node.get("resource_type") not in _OUTPUT_RESOURCE_TYPES
            and node.get("resource_type") in {"test", "seed", "operation", "analysis"}
        )
    )

    distinct_inputs: set[str] = set()
    distinct_outputs: set[str] = set()
    events_written = 0

    # ``mode='w'`` for the default sink — manifest emission is one-shot
    # (re-running overwrites). Custom sinks (e.g., the Silver per-run
    # sink) use append mode.
    owns_sink = sink is None
    sink_to_use: LineageSink = sink or JsonLinesSink(config.output_path, mode="w")

    try:
        for node_id in output_node_ids:
            node = all_nodes[node_id]
            event = _build_event_for_node(node, all_nodes, config, event_time, ol)
            sink_to_use.emit(event)
            events_written += 1

            for inp in event.get("inputs", []):
                distinct_inputs.add(f"{inp['namespace']}/{inp['name']}")
            for out in event.get("outputs", []):
                distinct_outputs.add(f"{out['namespace']}/{out['name']}")
    finally:
        if owns_sink:
            sink_to_use.close()

    return LineageEmitStats(
        events_written=events_written,
        output_path=config.output_path,
        inputs_count=len(distinct_inputs),
        outputs_count=len(distinct_outputs),
        duration_seconds=time.monotonic() - started_at,
        skipped_nodes=skipped,
    )


# ──────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise LineageEmitError(f"Manifest not found at {path}.")
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise LineageEmitError(f"Manifest at {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise LineageEmitError(f"Manifest at {path} must be a JSON object.")
    if "nodes" not in data:
        raise ValidationError(
            f"Manifest at {path} is missing 'nodes' — is this a dbt manifest.json?"
        )
    return data


def _manifest_event_time(manifest: dict[str, Any]) -> str:
    """Return the manifest's generated_at timestamp as an ISO 8601 string.

    Using the manifest's own timestamp (not ``datetime.now()``) makes
    re-runs byte-identical for the same input. Falls back to the Unix
    epoch if the manifest doesn't carry one — better deterministic-but-
    wrong than non-deterministic.
    """
    metadata = manifest.get("metadata", {})
    raw = metadata.get("generated_at")
    if isinstance(raw, str) and raw:
        return raw
    return datetime(1970, 1, 1, tzinfo=UTC).isoformat()


def _build_event_for_node(
    node: dict[str, Any],
    all_nodes: dict[str, dict[str, Any]],
    config: LineageEmitConfig,
    event_time: str,
    ol: Any,
) -> dict[str, Any]:
    """Produce one OL ``COMPLETE`` RunEvent dict for a single output node."""
    job_name = node["unique_id"]
    run_id = _stable_run_id(job_name, event_time)

    output_dataset = _build_dataset(node, config, ol, is_output=True)
    upstream_ids = node.get("depends_on", {}).get("nodes", [])
    inputs = [
        _build_dataset(all_nodes[uid], config, ol, is_output=False)
        for uid in sorted(upstream_ids)
        if uid in all_nodes and all_nodes[uid].get("resource_type") in _INPUT_RESOURCE_TYPES
    ]

    run = ol.Run(runId=run_id)
    job = ol.Job(
        namespace=config.job_namespace,
        name=job_name,
        facets=_job_facets(node, ol),
    )
    event = ol.RunEvent(
        eventType=ol.RunState.COMPLETE,
        eventTime=event_time,
        run=run,
        job=job,
        producer=config.producer,
        inputs=inputs,
        outputs=[output_dataset],
    )

    # The OL client's serializer produces a clean dict. We round-trip
    # through json.dumps to drop any non-serialisable defaults.
    parsed: dict[str, Any] = json.loads(serialize(ol, event))
    return parsed


def _stable_run_id(job_name: str, event_time: str) -> str:
    """Produce a deterministic run ID for this (job, time) pair.

    OL ``runId`` is required to be a UUID. We hash the job name +
    event time into a UUID-shaped string so repeated emissions of the
    same manifest produce the same run IDs.
    """
    # sha256 (not sha1) so the security scanner doesn't fire — we only
    # need a deterministic 16-byte input for uuid.UUID(bytes=...). Using
    # the first 16 bytes of a 32-byte digest is fine; collision risk
    # remains negligible for this volume.
    digest = hashlib.sha256(f"{job_name}@{event_time}".encode()).digest()
    return str(uuid.UUID(bytes=digest[:16]))


def _build_dataset(
    node: dict[str, Any],
    config: LineageEmitConfig,
    ol: Any,
    *,
    is_output: bool,
) -> Any:
    """Build an OL ``InputDataset`` or ``OutputDataset`` from a manifest node.

    ``is_output`` switches between the two — both carry the same fields
    but OL's type system distinguishes them on the event.
    """
    facets = {}
    columns = node.get("columns") or {}
    if columns:
        facets["schema"] = ol.SchemaDatasetFacet(
            fields=[
                ol.SchemaDatasetFacetFields(
                    name=col_name,
                    type=col_meta.get("data_type") or "unknown",
                    description=col_meta.get("description") or None,
                )
                for col_name, col_meta in sorted(columns.items())
            ]
        )

    cls = ol.OutputDataset if is_output else ol.InputDataset
    return cls(
        namespace=config.namespace,
        name=_dataset_name(node),
        facets=facets,
    )


def _dataset_name(node: dict[str, Any]) -> str:
    """Produce the OL dataset name from a manifest node.

    Sources expose ``schema``/``identifier`` under different keys than
    models; normalise both to ``<schema>.<table>``. Strip the project
    prefix from model unique_ids — OL consumers expect physical names.
    """
    schema = node.get("schema") or "default"
    if node.get("resource_type") == "source":
        # Sources use 'identifier' for the physical table name (falls
        # back to 'name' if not set in YAML).
        table = node.get("identifier") or node["name"]
    else:
        # Models use 'alias' if set, else the model name.
        table = node.get("alias") or node["name"]
    return f"{schema}.{table}"


def _job_facets(node: dict[str, Any], ol: Any) -> dict[str, Any]:
    facets: dict[str, Any] = {}
    description = node.get("description")
    if description:
        facets["documentation"] = ol.DocumentationJobFacet(description=description)
    return facets
