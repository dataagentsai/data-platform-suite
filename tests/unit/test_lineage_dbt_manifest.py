"""Unit tests for the dbt manifest → OpenLineage emitter (Phase 7-A).

Synthetic manifest fixtures cover:

- A model wired to one Silver source and one upstream Gold model
- A snapshot wrapping a model
- Tests / seeds / analyses get filtered out of the output set
- Determinism: re-runs are byte-identical
- Validation: malformed manifests fail fast before touching the FS
- Schema facets: column lists land on output datasets
- Documentation facets: model descriptions surface as OL job facets

The full-shape end-to-end test against a real ``dbt parse`` output
lives in ``tests/smoke/test_gold_dbt_parse.py``'s sibling smoke; this
file uses synthetic manifests so it runs in milliseconds without dbt.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.lineage import (
    LineageEmitConfig,
    emit_from_dbt_manifest,
)
from lake_ingestion_framework.core.lineage.dbt_manifest import LineageEmitError

# ──────────────────────────────────────────────────────────────────────
# Synthetic manifest fixtures
# ──────────────────────────────────────────────────────────────────────


def _source(
    unique_id: str, schema: str, name: str, columns: dict[str, dict[str, Any]] | None = None
) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "source",
        "source_name": "silver",
        "schema": schema,
        "database": "main",
        "name": name,
        "identifier": name,
        "columns": columns or {},
    }


def _model(
    unique_id: str,
    name: str,
    schema: str,
    *,
    depends_on_nodes: list[str] | None = None,
    columns: dict[str, dict[str, Any]] | None = None,
    description: str = "",
) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "model",
        "name": name,
        "schema": schema,
        "database": "main",
        "depends_on": {"nodes": depends_on_nodes or []},
        "columns": columns or {},
        "description": description,
    }


def _snapshot(unique_id: str, name: str, *, depends_on_nodes: list[str]) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "snapshot",
        "name": name,
        "schema": "gold_snapshots",
        "database": "main",
        "depends_on": {"nodes": depends_on_nodes},
        "columns": {},
        "description": "",
    }


def _test_node(unique_id: str) -> dict[str, Any]:
    return {
        "unique_id": unique_id,
        "resource_type": "test",
        "name": unique_id,
        "schema": "gold",
        "depends_on": {"nodes": []},
        "columns": {},
    }


def _write_manifest(path: Path, *, nodes: dict[str, Any], sources: dict[str, Any]) -> None:
    manifest = {
        "metadata": {"generated_at": "2026-04-15T12:00:00.123456Z"},
        "nodes": nodes,
        "sources": sources,
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")


def _read_events(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_emits_one_event_per_model_and_snapshot(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders"),
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
                columns={"order_id": {"data_type": "BIGINT"}},
            ),
            "snapshot.acme.snap_fct_orders": _snapshot(
                "snapshot.acme.snap_fct_orders",
                "snap_fct_orders",
                depends_on_nodes=["model.acme.fct_orders"],
            ),
            "test.acme.not_null_fct_orders_order_id": _test_node(
                "test.acme.not_null_fct_orders_order_id"
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    stats = emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))

    assert stats.events_written == 2  # fct_orders + snap_fct_orders; test filtered
    assert "test.acme.not_null_fct_orders_order_id" in stats.skipped_nodes


def test_uses_manifest_generated_at_for_event_time(tmp_path: Path) -> None:
    """Re-runs need byte-identical output. Pulling event_time from the
    manifest (not datetime.now()) is what makes that possible."""
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    events = _read_events(out)
    assert events[0]["eventTime"] == "2026-04-15T12:00:00.123456Z"


def test_idempotent_byte_identical_output(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
            ),
        },
    )
    out_a = tmp_path / "a.jsonl"
    out_b = tmp_path / "b.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out_a))
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out_b))
    assert out_a.read_bytes() == out_b.read_bytes()


# ──────────────────────────────────────────────────────────────────────
# Event shape
# ──────────────────────────────────────────────────────────────────────


def test_event_carries_input_dataset_for_each_upstream_node(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders"),
            "source.acme.silver.customers": _source(
                "source.acme.silver.customers", "silver", "customers"
            ),
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=[
                    "source.acme.silver.orders",
                    "source.acme.silver.customers",
                ],
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    events = _read_events(out)
    inputs = events[0]["inputs"]
    assert {i["name"] for i in inputs} == {"silver.orders", "silver.customers"}


def test_event_carries_output_dataset_with_schema_facet(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
                columns={
                    "order_id": {"data_type": "BIGINT", "description": "Primary key"},
                    "amount": {"data_type": "DECIMAL"},
                },
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    event = _read_events(out)[0]
    assert len(event["outputs"]) == 1
    output = event["outputs"][0]
    assert output["name"] == "gold.fct_orders"
    schema = output["facets"]["schema"]
    field_names = [f["name"] for f in schema["fields"]]
    assert "order_id" in field_names
    assert "amount" in field_names
    # description on the column rides through
    pk_field = next(f for f in schema["fields"] if f["name"] == "order_id")
    assert pk_field["description"] == "Primary key"


def test_event_carries_documentation_job_facet_for_described_models(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
                description="Daily orders fact.",
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    event = _read_events(out)[0]
    assert event["job"]["facets"]["documentation"]["description"] == "Daily orders fact."


def test_run_id_is_a_valid_uuid_and_deterministic(tmp_path: Path) -> None:
    """OL spec requires runId to be a UUID; ours is hash-derived so two
    runs of the same manifest produce the same UUID."""
    import re

    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
            ),
        },
    )
    out_a = tmp_path / "a.jsonl"
    out_b = tmp_path / "b.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out_a))
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out_b))

    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    a_run = _read_events(out_a)[0]["run"]["runId"]
    b_run = _read_events(out_b)[0]["run"]["runId"]
    assert uuid_re.match(a_run)
    assert a_run == b_run  # deterministic


# ──────────────────────────────────────────────────────────────────────
# Filtering
# ──────────────────────────────────────────────────────────────────────


def test_inputs_filter_to_model_source_snapshot_seed(tmp_path: Path) -> None:
    """``depends_on.nodes`` can include macro IDs etc. — only data nodes
    appear as OL inputs."""
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders": _model(
                "model.acme.fct_orders",
                "fct_orders",
                "gold",
                # Mix of valid + invalid input refs:
                depends_on_nodes=[
                    "source.acme.silver.orders",  # valid (source)
                    "macro.acme.gold_meta_cols",  # filtered (not in all_nodes anyway)
                    "model.acme.does_not_exist",  # filtered (missing from manifest)
                ],
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    inputs = _read_events(out)[0]["inputs"]
    assert {i["name"] for i in inputs} == {"silver.orders"}


# ──────────────────────────────────────────────────────────────────────
# Validation / error paths
# ──────────────────────────────────────────────────────────────────────


def test_missing_manifest_raises(tmp_path: Path) -> None:
    with pytest.raises(LineageEmitError, match="Manifest not found"):
        emit_from_dbt_manifest(
            LineageEmitConfig(
                manifest_path=tmp_path / "nope.json", output_path=tmp_path / "out.jsonl"
            )
        )


def test_invalid_json_manifest_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("not valid json", encoding="utf-8")
    with pytest.raises(LineageEmitError, match="not valid JSON"):
        emit_from_dbt_manifest(
            LineageEmitConfig(manifest_path=bad, output_path=tmp_path / "out.jsonl")
        )


def test_manifest_missing_nodes_raises(tmp_path: Path) -> None:
    """Not a dbt manifest — surface the ambiguity early rather than emit nothing."""
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"metadata": {}}), encoding="utf-8")
    with pytest.raises(ValidationError, match="missing 'nodes'"):
        emit_from_dbt_manifest(
            LineageEmitConfig(manifest_path=bad, output_path=tmp_path / "out.jsonl")
        )


def test_empty_manifest_emits_zero_events_no_error(tmp_path: Path) -> None:
    """A valid-but-empty manifest is fine — emit zero events.

    The lazy-open sink means no file is written when there's nothing
    to emit (no garbage zero-byte files). Callers should treat
    ``stats.events_written`` as the truth, not the filesystem.
    """
    manifest = tmp_path / "manifest.json"
    _write_manifest(manifest, sources={}, nodes={})
    out = tmp_path / "lineage.jsonl"
    stats = emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    assert stats.events_written == 0
    assert not out.exists()


# ──────────────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────────────


def test_stats_count_distinct_inputs_and_outputs(tmp_path: Path) -> None:
    """Two models reading the same source = 1 distinct input."""
    manifest = tmp_path / "manifest.json"
    _write_manifest(
        manifest,
        sources={
            "source.acme.silver.orders": _source("source.acme.silver.orders", "silver", "orders")
        },
        nodes={
            "model.acme.fct_orders_a": _model(
                "model.acme.fct_orders_a",
                "fct_orders_a",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
            ),
            "model.acme.fct_orders_b": _model(
                "model.acme.fct_orders_b",
                "fct_orders_b",
                "gold",
                depends_on_nodes=["source.acme.silver.orders"],
            ),
        },
    )
    out = tmp_path / "lineage.jsonl"
    stats = emit_from_dbt_manifest(LineageEmitConfig(manifest_path=manifest, output_path=out))
    assert stats.events_written == 2
    assert stats.inputs_count == 1
    assert stats.outputs_count == 2
