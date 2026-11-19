"""Unit tests for the Source → Bronze OL emitter (Phase 7-D).

Covers ``emit_source_run_event`` against synthetic ``SourceRunStats``.
The Source emitter has no real-pipeline integration today (the
orchestrator assembles stats post-write), so unit tests cover the full
behavior surface; the end-to-end smoke chains a hand-crafted Source
event into the real Silver+Gold trail.
"""

from __future__ import annotations

import re
from typing import Any

from lake_ingestion_framework.core.lineage import (
    SourceRunStats,
    emit_source_run_event,
)


class _CapturingSink:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def close(self) -> None:
        pass


def _stats(**overrides: Any) -> SourceRunStats:
    base: dict[str, Any] = {
        "run_id": "ingest-run-42",
        "source_namespace": "postgres://prod-db:5432",
        "source_name": "public.orders",
        "bronze_namespace": "delta://lakehouse",
        "bronze_name": "bronze.orders",
        "rows_written": 1000,
        "columns": (
            "order_id",
            "amount",
            "updated_at",
            "_run_id",
            "_loaded_at",
            "_record_hash",
        ),
        "source_columns": ("order_id", "amount", "updated_at"),
        "batches_written": 4,
        "duration_seconds": 2.3,
    }
    base.update(overrides)
    return SourceRunStats(**base)


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_emits_one_event_per_call() -> None:
    sink = _CapturingSink()
    emit_source_run_event(_stats(), sink)
    assert len(sink.events) == 1


def test_event_uses_complete_state_and_source_namespace() -> None:
    sink = _CapturingSink()
    emit_source_run_event(_stats(), sink)
    event = sink.events[0]
    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "lake-ingest.source"
    assert event["job"]["name"] == "source/bronze.orders"


def test_event_carries_source_input_with_schema() -> None:
    """Source-side schema facet is what enables future column-level
    lineage queries without re-querying the source DB."""
    sink = _CapturingSink()
    emit_source_run_event(_stats(), sink)
    inputs = sink.events[0]["inputs"]
    assert len(inputs) == 1
    assert inputs[0]["namespace"] == "postgres://prod-db:5432"
    assert inputs[0]["name"] == "public.orders"
    field_names = [f["name"] for f in inputs[0]["facets"]["schema"]["fields"]]
    assert field_names == ["order_id", "amount", "updated_at"]


def test_event_carries_bronze_output_with_full_schema() -> None:
    """Bronze schema includes the framework's metadata cols — that's
    what later layers will see."""
    sink = _CapturingSink()
    emit_source_run_event(_stats(), sink)
    output = sink.events[0]["outputs"][0]
    assert output["namespace"] == "delta://lakehouse"
    assert output["name"] == "bronze.orders"
    field_names = [f["name"] for f in output["facets"]["schema"]["fields"]]
    assert "order_id" in field_names
    assert "_run_id" in field_names
    assert "_record_hash" in field_names


def test_event_attaches_query_as_documentation_facet() -> None:
    sink = _CapturingSink()
    emit_source_run_event(
        _stats(query_text="SELECT id FROM orders WHERE updated_at > %(wm)s"),
        sink,
    )
    doc = sink.events[0]["job"]["facets"]["documentation"]
    assert "SELECT id" in doc["description"]


def test_event_omits_documentation_facet_when_no_query() -> None:
    sink = _CapturingSink()
    emit_source_run_event(_stats(query_text=None), sink)
    assert "documentation" not in sink.events[0]["job"]["facets"]


def test_event_carries_source_run_stats_facet() -> None:
    sink = _CapturingSink()
    emit_source_run_event(
        _stats(rows_written=12345, batches_written=3, duration_seconds=4.2),
        sink,
    )
    facet = sink.events[0]["run"]["facets"]["sourceRunStats"]
    assert facet["rows_written"] == 12345
    assert facet["batches_written"] == 3
    assert facet["duration_seconds"] == 4.2


def test_run_id_is_deterministic_uuid() -> None:
    sink_a = _CapturingSink()
    sink_b = _CapturingSink()
    emit_source_run_event(_stats(), sink_a)
    emit_source_run_event(_stats(), sink_b)

    a_run = sink_a.events[0]["run"]["runId"]
    b_run = sink_b.events[0]["run"]["runId"]

    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert uuid_re.match(a_run)
    assert a_run == b_run


def test_returns_emitted_payload_for_inspection() -> None:
    sink = _CapturingSink()
    payload = emit_source_run_event(_stats(), sink)
    assert payload["eventType"] == "COMPLETE"


# ──────────────────────────────────────────────────────────────────────
# Defensive paths
# ──────────────────────────────────────────────────────────────────────


def test_skips_when_run_id_missing() -> None:
    sink = _CapturingSink()
    payload = emit_source_run_event(_stats(run_id=""), sink)
    assert payload == {}
    assert sink.events == []


def test_skips_when_source_name_missing() -> None:
    sink = _CapturingSink()
    payload = emit_source_run_event(_stats(source_name=""), sink)
    assert payload == {}
    assert sink.events == []


def test_skips_when_bronze_name_missing() -> None:
    sink = _CapturingSink()
    payload = emit_source_run_event(_stats(bronze_name=""), sink)
    assert payload == {}
    assert sink.events == []


def test_omits_schema_facets_when_columns_empty() -> None:
    """Source/Bronze emitted without schema cols should still produce
    a valid event — just no SchemaDatasetFacet on either dataset."""
    sink = _CapturingSink()
    emit_source_run_event(_stats(columns=(), source_columns=()), sink)
    event = sink.events[0]
    assert "schema" not in event["inputs"][0]["facets"]
    assert "schema" not in event["outputs"][0]["facets"]
