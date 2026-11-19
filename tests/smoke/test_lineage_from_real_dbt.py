"""End-to-end smoke: real dbt parse → real OL events.

Chains the Gold generator (Phase 1-6) → real ``dbt deps && dbt parse``
(Phase 11) → the lineage emitter (Phase 7-A) into one pipeline. This
is the acceptance gate for the lineage layer's first emitter: if the
generated dbt project parses, and the resulting manifest produces
OL-spec-compliant events, the round trip is honest end-to-end.

Why a separate smoke (not piggybacking on test_gold_dbt_parse): the
lineage emitter doesn't depend on dbt-core, only on ``manifest.json``
having been written. We could run lineage on Python 3.14 against a
manifest produced by a 3.11 dbt run. This smoke validates the
manifest→OL leg in isolation; ``test_gold_dbt_parse.py`` validates
the dbt leg.

**Pre-reqs**: ``[gold_dev]`` (dbt) + ``[lineage]`` (openlineage).
Skips when dbt isn't importable (Python 3.14 today).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


def _dbt_module_invocation() -> list[str] | None:
    try:
        import dbt
        import dbt.cli.main

        del dbt  # silence "imported but unused"
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


def test_lineage_from_real_dbt_manifest(dbt_invocation: list[str], tmp_path: Path) -> None:
    """Generate Gold → dbt parse → lineage emit → assert OL event shape."""
    from lake_ingestion_framework.core.gold import (
        GoldModelConfig,
        GoldProjectConfig,
        GoldSource,
        Materialization,
        generate_dbt_project,
    )
    from lake_ingestion_framework.core.lineage import (
        LineageEmitConfig,
        emit_from_dbt_manifest,
    )

    project_dir = tmp_path / "gold_dbt"
    generate_dbt_project(
        GoldProjectConfig(
            project_name="lineage_smoke",
            sources=(
                GoldSource(name="orders", description="Stripe orders"),
                GoldSource(name="customers", description="Salesforce customers"),
            ),
            models=(
                GoldModelConfig(
                    name="dim_customer",
                    domain="product",
                    silver_sources=("customers",),
                    business_keys=("customer_id",),
                    materialization=Materialization.TABLE,
                    description="Current-state customer dim",
                ),
                GoldModelConfig(
                    name="fct_orders",
                    domain="finance",
                    silver_sources=("orders",),
                    business_keys=("order_id",),
                    materialization=Materialization.INCREMENTAL,
                    incremental_watermark_column="updated_at",
                    description="Daily orders fact joined to current customer",
                    custom_sql=(
                        "SELECT o.order_id, c.segment, "
                        "{{ gold_meta_cols() }}\n"
                        "FROM {{ source('silver', 'orders') }} o\n"
                        "LEFT JOIN {{ ref('dim_customer') }} c USING (customer_id)"
                    ),
                ),
            ),
        ),
        project_dir,
    )

    parse_env = {
        **os.environ,
        "DATABRICKS_HOST": "fake.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/fake",
        "DATABRICKS_TOKEN": "fake-token",
        "GOLD_CATALOG": "main",
        "GOLD_SCHEMA": "gold",
    }

    deps = subprocess.run(  # noqa: S603 — dbt_invocation is sys.executable
        [
            *dbt_invocation,
            "deps",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
        ],
        capture_output=True,
        check=False,
        env=parse_env,
        text=True,
        timeout=120,
    )
    assert deps.returncode == 0, f"dbt deps failed:\n{deps.stdout}\n{deps.stderr}"

    parse = subprocess.run(  # noqa: S603 — dbt_invocation is sys.executable
        [
            *dbt_invocation,
            "parse",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
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

    manifest = project_dir / "target" / "manifest.json"
    assert manifest.is_file(), "dbt parse didn't write target/manifest.json"

    # Lineage leg ─ feed the real manifest to the emitter.
    lineage_out = tmp_path / "lineage.jsonl"
    stats = emit_from_dbt_manifest(
        LineageEmitConfig(
            manifest_path=manifest,
            output_path=lineage_out,
            namespace="databricks://smoke",
            job_namespace="dbt-smoke",
        )
    )

    assert (
        stats.events_written == 2
    ), f"Expected 2 events (dim_customer + fct_orders); got {stats.events_written}"

    events = [json.loads(line) for line in lineage_out.read_text().splitlines() if line]
    assert len(events) == 2

    # Both events have COMPLETE state, our producer URI, namespace overrides applied.
    for event in events:
        assert event["eventType"] == "COMPLETE"
        assert event["job"]["namespace"] == "dbt-smoke"
        assert event["producer"].endswith("/lake-ingest")
        # Every dataset uses our overridden namespace.
        for ds in [*event["inputs"], *event["outputs"]]:
            assert ds["namespace"] == "databricks://smoke"

    # The fact's event references the dim via the manifest's `depends_on`
    # (dbt resolves `{{ ref('dim_customer') }}`). Find the fct_orders event
    # and confirm dim_customer landed as an input.
    fct_event = next(e for e in events if "fct_orders" in e["job"]["name"])
    input_names = {i["name"] for i in fct_event["inputs"]}
    assert "silver.orders" in input_names
    assert "gold.dim_customer" in input_names
