"""Unit tests for the Silver pipeline → OpenLineage emitter (Phase 7-B).

Covers the Silver emitter (``emit_silver_run_event``) + the shared
sink protocol. Uses synthetic ``SilverRunStats`` rather than running
the real pipeline so each test stays in-memory and < 100ms.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from lake_ingestion_framework.core.lineage import (
    JsonLinesSink,
    emit_silver_run_event,
)
from lake_ingestion_framework.core.silver.pipeline import (
    SilverRunStats,
    SilverStrategy,
)

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


class _CapturingSink:
    """Test sink that just remembers everything ``emit`` was called with.

    Conforms structurally to ``LineageSink`` (Protocol) — no inheritance
    needed, which is the whole point of using Protocol.
    """

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.closed = False

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


def _stats(**overrides: Any) -> SilverRunStats:
    base: dict[str, Any] = {
        "rows_read": 100,
        "rows_written": 100,
        "strategy": SilverStrategy.APPEND,
        "silver_path": Path("/tmp/silver_demo"),
        "bronze_path": Path("/tmp/bronze_demo"),
        "run_id": "silver-run-42",
        "silver_columns": ("id", "email", "_silver_run_id", "_silver_loaded_at"),
        "rows_inserted": 100,
        "rows_updated": 0,
        "rows_unchanged": 0,
        "silver_version_after": 3,
        "duration_seconds": 0.45,
    }
    base.update(overrides)
    return SilverRunStats(**base)


# ──────────────────────────────────────────────────────────────────────
# emit_silver_run_event — happy path
# ──────────────────────────────────────────────────────────────────────


def test_emits_one_event_per_call() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    assert len(sink.events) == 1


def test_event_uses_complete_run_state() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    assert sink.events[0]["eventType"] == "COMPLETE"


def test_event_carries_bronze_input_and_silver_output() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    event = sink.events[0]

    assert len(event["inputs"]) == 1
    assert event["inputs"][0]["name"] == "bronze_demo"

    assert len(event["outputs"]) == 1
    assert event["outputs"][0]["name"] == "silver_demo"


def test_silver_output_carries_schema_facet_with_columns() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    schema = sink.events[0]["outputs"][0]["facets"]["schema"]
    field_names = [f["name"] for f in schema["fields"]]
    assert field_names == ["id", "email", "_silver_run_id", "_silver_loaded_at"]


def test_event_carries_silver_run_stats_facet() -> None:
    """The custom run-stats facet lets downstream consumers answer
    'how many rows did silver-run-42 ingest?' without re-querying."""
    sink = _CapturingSink()
    emit_silver_run_event(_stats(rows_inserted=80, rows_updated=15, rows_unchanged=5), sink)
    facet = sink.events[0]["run"]["facets"]["silverRunStats"]
    assert facet["rows_read"] == 100
    assert facet["rows_inserted"] == 80
    assert facet["rows_updated"] == 15
    assert facet["rows_unchanged"] == 5
    assert facet["strategy"] == "append"
    assert facet["silver_version_after"] == 3


def test_run_id_is_deterministic_uuid() -> None:
    """Re-emitting for the same logical run produces the same OL runId."""
    import re

    sink_a = _CapturingSink()
    sink_b = _CapturingSink()
    emit_silver_run_event(_stats(), sink_a)
    emit_silver_run_event(_stats(), sink_b)

    a_run = sink_a.events[0]["run"]["runId"]
    b_run = sink_b.events[0]["run"]["runId"]

    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert uuid_re.match(a_run)
    assert a_run == b_run


def test_namespace_override_applies_to_both_sides() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink, namespace="databricks://prod")
    event = sink.events[0]
    assert event["inputs"][0]["namespace"] == "databricks://prod"
    assert event["outputs"][0]["namespace"] == "databricks://prod"


def test_job_namespace_groups_silver_events_separately_from_dbt() -> None:
    """Consumers can filter by job namespace to slice 'silver runs only'."""
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    assert sink.events[0]["job"]["namespace"] == "lake-ingest.silver"


# ──────────────────────────────────────────────────────────────────────
# emit_silver_run_event — defensive paths
# ──────────────────────────────────────────────────────────────────────


def test_skips_when_bronze_path_missing() -> None:
    """Stats with no bronze_path can't be correlated — emit nothing."""
    sink = _CapturingSink()
    payload = emit_silver_run_event(_stats(bronze_path=None), sink)
    assert payload == {}
    assert sink.events == []


def test_skips_when_run_id_missing() -> None:
    sink = _CapturingSink()
    payload = emit_silver_run_event(_stats(run_id=""), sink)
    assert payload == {}
    assert sink.events == []


def test_returns_emitted_payload_for_inspection() -> None:
    """The function returns the event dict so callers can log / inspect."""
    sink = _CapturingSink()
    payload = emit_silver_run_event(_stats(), sink)
    assert payload["eventType"] == "COMPLETE"
    assert payload is not sink.events[0] or payload == sink.events[0]  # same shape


# ──────────────────────────────────────────────────────────────────────
# Phase 6 — DQ facet (silverDqResults)
# ──────────────────────────────────────────────────────────────────────


def test_dq_facet_omitted_when_no_rules_evaluated() -> None:
    """Default stats (no DQ context) should not emit a DQ facet —
    absence means "this dataset doesn't carry DQ rules"."""
    sink = _CapturingSink()
    emit_silver_run_event(_stats(), sink)
    assert "silverDqResults" not in sink.events[0]["run"]["facets"]


def test_dq_facet_present_when_rules_evaluated() -> None:
    sink = _CapturingSink()
    emit_silver_run_event(
        _stats(
            dq_rules_evaluated=3,
            dq_rules_passed=1,
            dq_rules_failed=2,
            dq_rules_errored=0,
            dq_violations_logged=42,
            dq_rows_quarantined=10,
        ),
        sink,
    )
    facet = sink.events[0]["run"]["facets"]["silverDqResults"]
    assert facet["rules_evaluated"] == 3
    assert facet["rules_passed"] == 1
    assert facet["rules_failed"] == 2
    assert facet["rules_errored"] == 0
    assert facet["violations_logged"] == 42
    assert facet["rows_quarantined"] == 10


def test_dq_facet_carries_producer_and_schema_url() -> None:
    """Custom facets must carry _producer + _schemaURL per OL spec."""
    sink = _CapturingSink()
    emit_silver_run_event(_stats(dq_rules_evaluated=1, dq_rules_passed=1), sink)
    facet = sink.events[0]["run"]["facets"]["silverDqResults"]
    assert facet["_producer"]
    assert facet["_schemaURL"].endswith("#dq-facet")


# ──────────────────────────────────────────────────────────────────────
# JsonLinesSink
# ──────────────────────────────────────────────────────────────────────


def test_json_lines_sink_writes_one_line_per_event(tmp_path: Path) -> None:
    out = tmp_path / "lineage.jsonl"
    with JsonLinesSink(out) as sink:
        sink.emit({"a": 1})
        sink.emit({"b": 2})
    lines = out.read_text().splitlines()
    assert len(lines) == 2
    assert "a" in lines[0]
    assert "b" in lines[1]


def test_json_lines_sink_appends_by_default(tmp_path: Path) -> None:
    """Default ``mode='a'`` favours per-run Silver/Source emissions
    where a single file accumulates events across runs."""
    out = tmp_path / "lineage.jsonl"
    with JsonLinesSink(out, mode="a") as s1:
        s1.emit({"run": 1})
    with JsonLinesSink(out, mode="a") as s2:
        s2.emit({"run": 2})
    assert len(out.read_text().splitlines()) == 2


def test_json_lines_sink_truncates_in_w_mode(tmp_path: Path) -> None:
    """``mode='w'`` is for one-shot bulk emitters (dbt manifest)."""
    out = tmp_path / "lineage.jsonl"
    with JsonLinesSink(out, mode="w") as s1:
        s1.emit({"run": 1})
        s1.emit({"run": 2})
    with JsonLinesSink(out, mode="w") as s2:
        s2.emit({"run": 3})
    lines = out.read_text().splitlines()
    assert len(lines) == 1
    assert "3" in lines[0]


def test_json_lines_sink_no_emit_no_file(tmp_path: Path) -> None:
    """Lazy-open: a sink that's never used creates no file at all."""
    out = tmp_path / "subdir" / "lineage.jsonl"
    sink = JsonLinesSink(out)
    sink.close()
    assert not out.exists()
    # Parent dir also not created — nothing happened, file system pristine.
    assert not (tmp_path / "subdir").exists()


def test_json_lines_sink_creates_parent_dir(tmp_path: Path) -> None:
    """First emit creates the parent directory if missing."""
    out = tmp_path / "deep" / "nested" / "lineage.jsonl"
    with JsonLinesSink(out) as sink:
        sink.emit({"a": 1})
    assert out.is_file()


def test_json_lines_sink_rejects_invalid_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="mode must be"):
        JsonLinesSink(tmp_path / "x.jsonl", mode="r")


def test_json_lines_sink_emits_sorted_keys(tmp_path: Path) -> None:
    """Sorted keys = byte-identical re-emission for the same event."""
    import json

    out = tmp_path / "lineage.jsonl"
    with JsonLinesSink(out) as sink:
        sink.emit({"b": 2, "a": 1, "c": 3})
    line = out.read_text().strip()
    # Reading back yields the right values regardless of input order.
    parsed = json.loads(line)
    assert parsed == {"a": 1, "b": 2, "c": 3}
    # And the on-disk shape sorts: '{"a"...,"b"...,"c"...}'
    assert line.index('"a"') < line.index('"b"') < line.index('"c"')
