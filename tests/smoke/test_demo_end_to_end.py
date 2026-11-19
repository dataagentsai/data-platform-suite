"""End-to-end smoke gate for ``scripts/demo_end_to_end.py``.

Runs the demo as a subprocess and asserts:

- exit code 0
- the demo's workspace contains Bronze + Silver Delta tables with the
  right row counts and the framework's metadata cols
- the lineage JSONL files exist and produce a connected
  Source → Bronze → Silver (→ Gold) graph

This is the **acceptance gate** for the local-mode framework arc:
when this passes, a customer can run ``python scripts/demo_end_to_end.py``
and reproduce the same flow on their laptop.

Pre-reqs (skips with a clear message when missing):

- Docker reachable
- ``[postgres]`` + ``[local]`` + ``[lineage]`` extras
- ``[gold_dev]`` extra + Python 3.11-3.13 to validate the Gold leg;
  the smoke runs ``--no-gold`` automatically when dbt isn't importable
  so it still gates the Source → Silver path on Python 3.14

The smoke writes to a unique subdirectory under ``tmp_path`` so two
parallel runs don't collide.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


def _docker_ok() -> bool:
    if shutil.which("docker") is None:
        return False
    res = subprocess.run(  # noqa: S603 — docker probe; constant args
        ["docker", "info", "--format", "{{.ServerVersion}}"],  # noqa: S607
        capture_output=True,
        check=False,
        timeout=10,
    )
    return res.returncode == 0


def _extra_importable(module: str) -> bool:
    try:
        __import__(module)
    except Exception:
        return False
    return True


def _dbt_importable() -> bool:
    try:
        import dbt
        import dbt.cli.main

        del dbt
    except Exception:
        return False
    return True


@pytest.fixture(scope="module", autouse=True)
def _preflight() -> None:
    if not _docker_ok():
        pytest.skip("Docker daemon unreachable.")
    for module, extra in (
        ("psycopg", "postgres"),
        ("deltalake", "local"),
        ("openlineage.client", "lineage"),
    ):
        if not _extra_importable(module):
            pytest.skip(f"[{extra}] extra missing — `uv sync --extra {extra}` (module: {module}).")


def test_demo_end_to_end_runs(tmp_path: Path) -> None:
    """Run the demo script as a subprocess; assert artifacts + graph shape.

    Uses ``--workspace`` to keep each run isolated under tmp_path so
    parallel runs don't trip over each other's postgres ports / files.
    """
    workspace = tmp_path / "demo-workspace"
    args = [
        sys.executable,
        str(Path(__file__).parents[2] / "scripts" / "demo_end_to_end.py"),
        "--workspace",
        str(workspace),
    ]
    if not _dbt_importable():
        # On Python 3.14 (or any env without dbt), skip the Gold leg
        # so the smoke still validates the Source → Silver portion.
        args.append("--no-gold")

    result = subprocess.run(  # noqa: S603 — sys.executable + script path
        args,
        capture_output=True,
        check=False,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"demo failed (exit {result.returncode}):\n"
        f"--- stdout ---\n{result.stdout[-4000:]}\n"
        f"--- stderr ---\n{result.stderr[-2000:]}"
    )

    # ── Bronze tables exist with framework metadata cols ──────────
    from deltalake import DeltaTable

    bronze_customers = DeltaTable(str(workspace / "bronze" / "customers")).to_pyarrow_table()
    assert bronze_customers.num_rows == 5
    cust_cols = set(bronze_customers.column_names)
    assert {"customer_id", "email", "segment", "_run_id", "_loaded_at", "_record_hash"} <= cust_cols

    bronze_orders = DeltaTable(str(workspace / "bronze" / "orders")).to_pyarrow_table()
    assert bronze_orders.num_rows == 10
    assert "_record_hash" in bronze_orders.column_names

    # ── Silver tables exist with Silver metadata cols ─────────────
    silver_customers = DeltaTable(str(workspace / "silver" / "customers")).to_pyarrow_table()
    assert silver_customers.num_rows == 5
    silver_cols = set(silver_customers.column_names)
    assert {"customer_id", "email", "_silver_run_id", "_silver_loaded_at"} <= silver_cols
    # Bronze metadata cols must NOT appear (Silver pipeline drops them)
    assert "_run_id" not in silver_cols

    silver_orders = DeltaTable(str(workspace / "silver" / "orders")).to_pyarrow_table()
    assert silver_orders.num_rows == 10

    # ── Standard processors fired against the in-memory metadata ────
    # Row 5's seed is "  Eve@Example.COM  " — hygiene strips whitespace
    # and email_lowercase folds it to "eve@example.com". Customers
    # without explicit dedup retain all 5; segments uppercase via
    # column_role=code.
    cust_emails = set(silver_customers.column("email").to_pylist())
    assert (
        "eve@example.com" in cust_emails
    ), f"Expected processors to normalize Eve's email; got {cust_emails}"
    cust_segments = set(silver_customers.column("segment").to_pylist())
    assert {
        "PREMIUM",
        "FREE",
        "ENTERPRISE",
    } <= cust_segments, f"Expected code_uppercase to fire on segment; got {cust_segments}"

    # The demo persists the seeded metadata DB so customers can poke
    # at it with `lake-ingest processors describe` after the run.
    assert (workspace / "metadata-dev.sqlite").is_file()

    # ── Lineage JSONL files exist + are non-empty ─────────────────
    lineage_dir = workspace / "lineage"
    source_jsonl = lineage_dir / "source.jsonl"
    silver_jsonl = lineage_dir / "silver.jsonl"
    assert source_jsonl.is_file()
    assert source_jsonl.stat().st_size > 0
    assert silver_jsonl.is_file()
    assert silver_jsonl.stat().st_size > 0

    # ── Lineage graph: Source → Silver path connected ─────────────
    from lake_ingestion_framework.core.lineage import LineageGraph

    files = [source_jsonl, silver_jsonl]
    if _dbt_importable():
        gold_jsonl = lineage_dir / "gold.jsonl"
        assert gold_jsonl.is_file()
        assert gold_jsonl.stat().st_size > 0
        files.append(gold_jsonl)

    graph = LineageGraph.from_jsonl(files)

    namespace = "databricks://demo"
    source_ns_prefix = "postgres://localhost:"
    # Find source dataset (port may vary across runs)
    source_orders = next(
        (n for n in graph.nodes if n.startswith(source_ns_prefix) and "public.orders" in n),
        None,
    )
    assert source_orders is not None, f"missing source orders node in {sorted(graph.nodes)}"

    bronze_orders_q = f"{namespace}/bronze.orders"
    silver_orders_q = f"{namespace}/silver.orders"

    assert bronze_orders_q in graph.nodes
    assert silver_orders_q in graph.nodes

    # Source → Bronze edge
    assert bronze_orders_q in graph.forward[source_orders]
    # Bronze → Silver edge
    assert silver_orders_q in graph.forward[bronze_orders_q]

    # 3-hop or 4-hop path depending on Gold availability
    if _dbt_importable():
        gold_fct_q = f"{namespace}/gold.fct_orders"
        path = graph.path(source_orders, gold_fct_q)
        assert path == [
            source_orders,
            bronze_orders_q,
            silver_orders_q,
            gold_fct_q,
        ], f"4-layer path broken: {path}"
    else:
        path = graph.path(source_orders, silver_orders_q)
        assert path == [
            source_orders,
            bronze_orders_q,
            silver_orders_q,
        ], f"3-layer path broken: {path}"

    # ── Sanity: each emitted event is valid JSON ──────────────────
    for f in files:
        for line_no, line in enumerate(f.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError as exc:
                pytest.fail(f"{f}:{line_no} not valid JSON: {exc}")
