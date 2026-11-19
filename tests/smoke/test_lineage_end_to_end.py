"""End-to-end smoke: build a real Source → Bronze → Silver → Gold trail.

The most honest validation of the full lineage layer (Phases 7-A+B+C+D):

1. Synthesise a Source ingestion event (``emit_source_run_event``) —
   the orchestrator's job in production; we hand-craft for testing
2. Run the **real** ``SilverPipeline`` against a Bronze Delta table
3. Emit the Silver lineage event (``emit_silver_run_event``)
4. Run the **real** ``generate_dbt_project`` + ``dbt parse`` against
   a Gold project that references the same Silver table
5. Emit lineage events from the resulting manifest
   (``emit_from_dbt_manifest``)
6. Build a :class:`LineageGraph` from ALL JSONL files
7. Run real graph queries — assert the 4-layer trail (postgres → bronze
   → silver → gold) is connected end-to-end

Without this smoke, each emitter stands alone but their interop is
unverified. Specifically catches namespace-mismatch bugs (emitters
disagreeing on dataset names — the most likely cause of lineage events
that "look right" but produce a disconnected graph).

Pre-reqs: ``[gold_dev]`` (dbt) + ``[lineage]`` (OL) + ``[local]``
(deltalake/pyarrow). Skips when dbt isn't importable (3.14).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

pytestmark = pytest.mark.smoke


def _dbt_module_invocation() -> list[str] | None:
    try:
        import dbt
        import dbt.cli.main

        del dbt
    except Exception:
        return None
    return [sys.executable, "-m", "dbt.cli.main"]


@pytest.fixture(scope="module")
def dbt_invocation() -> list[str]:
    invocation = _dbt_module_invocation()
    if invocation is None:
        pytest.skip(
            "dbt-core not importable. Install with `uv sync --extra gold_dev` "
            "(needs Python 3.11-3.13)."
        )
    return invocation


def _bronze_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.table(
        {
            "customer_id": [r["customer_id"] for r in rows],
            "email": [r["email"] for r in rows],
            "segment": [r["segment"] for r in rows],
            "_run_id": ["bronze-run-1"] * len(rows),
            "_loaded_at": pa.array(["2026-04-01T00:00:00Z"] * len(rows), type=pa.string()),
            "_record_hash": [str(r["customer_id"]) for r in rows],
        }
    )


def test_bronze_to_silver_to_gold_lineage_round_trip(
    dbt_invocation: list[str], tmp_path: Path
) -> None:
    from deltalake import write_deltalake
    from lake_ingestion_framework.core.gold import (
        GoldModelConfig,
        GoldProjectConfig,
        GoldSource,
        Materialization,
        generate_dbt_project,
    )
    from lake_ingestion_framework.core.lineage import (
        JsonLinesSink,
        LineageEmitConfig,
        LineageGraph,
        SourceRunStats,
        emit_from_dbt_manifest,
        emit_silver_run_event,
        emit_source_run_event,
    )
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    # All four-layer emitters share the same warehouse-side OL namespace
    # so the graph stitches together. The Source emitter's *input* uses
    # the source-system namespace (postgres://) — that's how customers
    # know which DB a Bronze row came from.
    warehouse_namespace = "databricks://smoke"
    source_namespace = "postgres://prod-db.smoke:5432"

    # ── 1. Source → Bronze ────────────────────────────────────────
    # Hand-craft the SourceRunStats — in production the orchestrator
    # assembles this from the connector identity + LocalDeltaWriter's
    # WriteStats. The event is emitted before we run Silver so the
    # downstream layers can find a Bronze input.
    source_jsonl = tmp_path / "source.jsonl"
    with JsonLinesSink(source_jsonl, mode="a") as sink:
        emit_source_run_event(
            SourceRunStats(
                run_id="ingest-e2e-1",
                source_namespace=source_namespace,
                source_name="public.customers",
                bronze_namespace=warehouse_namespace,
                bronze_name="bronze.customers",
                rows_written=2,
                columns=("customer_id", "email", "segment"),
                source_columns=("customer_id", "email", "segment"),
                query_text="SELECT customer_id, email, segment FROM public.customers",
            ),
            sink,
        )

    # ── 2. Bronze → Silver ────────────────────────────────────────
    bronze_path = tmp_path / "bronze_customers"
    silver_path = tmp_path / "customers"  # name must match Gold's silver source
    write_deltalake(
        str(bronze_path),
        _bronze_table(
            [
                {"customer_id": 1, "email": "alice@x.com", "segment": "free"},
                {"customer_id": 2, "email": "bob@x.com", "segment": "premium"},
            ]
        ),
        mode="overwrite",
    )

    silver_stats = SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id="silver-e2e-1",
            strategy=SilverStrategy.APPEND,
        )
    )

    silver_jsonl = tmp_path / "silver.jsonl"
    # Pass dbt-compatible names so the graph stitches with the Gold
    # manifest (which names sources as 'silver.<table>').
    with JsonLinesSink(silver_jsonl, mode="a") as sink:
        emit_silver_run_event(
            silver_stats,
            sink,
            namespace=warehouse_namespace,
            silver_dataset_name="silver.customers",
            bronze_dataset_name="bronze.customers",
        )

    # ── 3. Silver → Gold (dbt) ─────────────────────────────────────
    gold_dir = tmp_path / "gold_dbt"
    generate_dbt_project(
        GoldProjectConfig(
            project_name="lineage_e2e",
            sources=(GoldSource(name="customers"),),
            models=(
                GoldModelConfig(
                    name="dim_customer",
                    domain="product",
                    silver_sources=("customers",),
                    business_keys=("customer_id",),
                    materialization=Materialization.TABLE,
                ),
            ),
        ),
        gold_dir,
    )

    parse_env = {
        **os.environ,
        "DATABRICKS_HOST": "fake.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/fake",
        "DATABRICKS_TOKEN": "fake-token",
        "GOLD_CATALOG": "main",
        "GOLD_SCHEMA": "gold",
    }
    deps = subprocess.run(  # noqa: S603 — sys.executable
        [*dbt_invocation, "deps", "--project-dir", str(gold_dir), "--profiles-dir", str(gold_dir)],
        capture_output=True,
        check=False,
        env=parse_env,
        text=True,
        timeout=120,
    )
    assert deps.returncode == 0, f"dbt deps failed:\n{deps.stdout}\n{deps.stderr}"

    parse = subprocess.run(  # noqa: S603 — sys.executable
        [
            *dbt_invocation,
            "parse",
            "--project-dir",
            str(gold_dir),
            "--profiles-dir",
            str(gold_dir),
            "--no-version-check",
            "--no-send-anonymous-usage-stats",
        ],
        capture_output=True,
        check=False,
        env=parse_env,
        text=True,
        timeout=120,
    )
    assert parse.returncode == 0, f"dbt parse failed:\n{parse.stdout}\n{parse.stderr}"

    gold_jsonl = tmp_path / "gold.jsonl"
    emit_from_dbt_manifest(
        LineageEmitConfig(
            manifest_path=gold_dir / "target" / "manifest.json",
            output_path=gold_jsonl,
            namespace=warehouse_namespace,
        )
    )

    # ── 4. Build graph + run queries across ALL FOUR LAYERS ───────
    graph = LineageGraph.from_jsonl([source_jsonl, silver_jsonl, gold_jsonl])

    source_q = f"{source_namespace}/public.customers"
    bronze_q = f"{warehouse_namespace}/bronze.customers"
    silver_q = f"{warehouse_namespace}/silver.customers"
    gold_q = f"{warehouse_namespace}/gold.dim_customer"

    # Every layer must be in the graph.
    assert source_q in graph.nodes
    assert bronze_q in graph.nodes
    assert silver_q in graph.nodes
    assert gold_q in graph.nodes

    # Source → Bronze edge from the Source emitter.
    assert bronze_q in graph.forward[source_q]
    # Bronze → Silver edge from the Silver emitter.
    assert silver_q in graph.forward[bronze_q]
    # Silver → Gold edge from the dbt manifest emitter.
    assert gold_q in graph.forward[silver_q]

    # End-to-end path query — full Source → Gold chain.
    full_path = graph.path(source_q, gold_q)
    assert full_path == [source_q, bronze_q, silver_q, gold_q], (
        f"Source→Gold path broken. Got: {full_path}. "
        f"Edges: {[(e.src, e.dst) for e in graph.edges]}"
    )

    # Upstream of gold should reach all the way back to the source DB.
    upstream_of_gold = graph.upstream(gold_q)
    assert silver_q in upstream_of_gold
    assert bronze_q in upstream_of_gold
    assert source_q in upstream_of_gold

    # Blast radius of source includes everything downstream.
    blast = graph.blast_radius(source_q)
    assert bronze_q in blast
    assert silver_q in blast
    assert gold_q in blast
