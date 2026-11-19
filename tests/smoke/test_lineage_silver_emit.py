"""End-to-end smoke: real Silver pipeline → real OL events.

Runs ``SilverPipeline.run()`` against a real Bronze Delta table on
disk, then feeds the resulting :class:`SilverRunStats` through
``emit_silver_run_event`` to a JSONL sink. Asserts the emitted event
is structurally sound, carries the right run-stats facet, and that
the schema facet reflects the columns we actually wrote.

This complements the unit tests (which use synthetic stats) by
proving the stats object the *real* pipeline produces is rich enough
to drive a usable lineage event end-to-end.

Pre-reqs: ``[local]`` (deltalake + pyarrow) + ``[lineage]`` (OL).
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

pytestmark = pytest.mark.smoke


def _bronze_table(rows: list[dict[str, object]]) -> pa.Table:
    """Bronze-shaped pyarrow table — columns mirror what `LocalDeltaWriter`
    would emit (business cols + Bronze metadata)."""
    return pa.table(
        {
            "id": [r["id"] for r in rows],
            "email": [r["email"] for r in rows],
            "tier": [r["tier"] for r in rows],
            "_run_id": ["bronze-run-1"] * len(rows),
            "_loaded_at": pa.array(["2026-04-01T00:00:00Z"] * len(rows), type=pa.string()),
            "_record_hash": [str(r["id"]) for r in rows],
        }
    )


def test_silver_pipeline_lineage_round_trip(tmp_path: Path) -> None:
    from deltalake import write_deltalake
    from lake_ingestion_framework.core.lineage import (
        JsonLinesSink,
        emit_silver_run_event,
    )
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    bronze_path = tmp_path / "bronze"
    silver_path = tmp_path / "silver"
    lineage_out = tmp_path / "lineage.jsonl"

    write_deltalake(
        str(bronze_path),
        _bronze_table(
            [
                {"id": 1, "email": "alice@x.com", "tier": "free"},
                {"id": 2, "email": "bob@x.com", "tier": "premium"},
            ]
        ),
        mode="overwrite",
    )

    stats = SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id="silver-smoke-1",
            strategy=SilverStrategy.APPEND,
        )
    )

    # Run-level invariants the lineage event will reflect:
    assert stats.bronze_path == bronze_path
    assert stats.silver_path == silver_path
    assert stats.run_id == "silver-smoke-1"
    assert stats.silver_columns, "Silver pipeline must populate column list"
    # Silver metadata cols ride along with the business cols.
    assert "_silver_run_id" in stats.silver_columns
    assert "_silver_loaded_at" in stats.silver_columns

    with JsonLinesSink(lineage_out, mode="a") as sink:
        emit_silver_run_event(stats, sink, namespace="databricks://smoke")

    lines = lineage_out.read_text().strip().splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])

    # OL-spec required fields.
    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "lake-ingest.silver"
    assert event["job"]["name"] == f"silver/{silver_path.name}"
    assert event["producer"].endswith("/lake-ingest")

    # Bronze→Silver edge.
    inputs = event["inputs"]
    outputs = event["outputs"]
    assert len(inputs) == 1
    assert len(outputs) == 1
    assert inputs[0]["namespace"] == "databricks://smoke"
    assert inputs[0]["name"] == bronze_path.name
    assert outputs[0]["name"] == silver_path.name

    # Schema facet on the output reflects the columns we wrote.
    schema = outputs[0]["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    assert "id" in field_names
    assert "email" in field_names
    assert "_silver_run_id" in field_names
    # Bronze-only columns must NOT appear (pipeline drops them before write).
    assert "_run_id" not in field_names

    # Custom run-stats facet — the cost-free instrumentation we shipped
    # in Phase 5 of Silver flows through to the lineage event.
    rs = event["run"]["facets"]["silverRunStats"]
    assert rs["rows_read"] == 2
    assert rs["rows_written"] == 2
    assert rs["rows_inserted"] == 2
    assert rs["strategy"] == "append"
    assert rs["silver_version_after"] is not None
