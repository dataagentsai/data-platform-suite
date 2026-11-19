"""End-to-end smoke: real dbt parses our generated Gold project.

This is the *acceptance gate* for everything the Gold generator emits.
Each prior phase added more shapes (multi-source SQL, snapshots, custom
metadata cols) — without a real dbt parse, structural drift could land
silently. This test runs ``dbt deps`` then ``dbt parse`` against a
synthesized project that exercises every supported shape:

- Single-source ``table`` dim
- Multi-source ``incremental`` fact via ``custom_sql`` (joins a Silver
  source + a Gold dim via ``ref()``)
- Both snapshot strategies (``timestamp`` + ``check``)
- The framework's ``gold_meta_cols()`` macro

Exit code 0 from ``dbt parse`` means the entire project loads cleanly:
- Every YAML file valid
- Every ``{{ source(...) }}`` and ``{{ ref(...) }}`` resolves
- Every macro resolves
- No deprecation warnings (we treat ``--warn-error`` failures as fatal)

**Pre-reqs**: ``[gold_dev]`` extras (dbt-core + dbt-databricks). The
test skips automatically when ``dbt`` isn't importable — useful
because dbt-core doesn't yet support Python 3.14 (the framework's
default target). Run on Python 3.11-3.13 or in a separate venv until
upstream dbt support catches up.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


def _dbt_module_invocation() -> list[str] | None:
    """Return the ``[python, -m, dbt.cli.main]`` invocation, or ``None``.

    We invoke dbt via the *current* Python interpreter (``sys.executable``)
    rather than ``shutil.which('dbt')`` to avoid PATH confusion when the
    tester has multiple Python venvs around. ``None`` means dbt isn't
    usable from this interpreter — skip cleanly.

    Catch broadly: dbt-core raises ``mashumaro.UnserializableField`` (not
    ``ImportError``) on Python 3.14 because of an upstream serialisation
    bug in newer Pythons. Treat any failure during import as "skip" so
    the test stays green on environments without working dbt support.
    """
    try:
        import dbt  # only used to detect importability
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
            "dbt-core not importable from this interpreter. Install with "
            "`uv sync --extra gold_dev` (needs Python 3.11-3.13; dbt-core "
            "doesn't yet support 3.14)."
        )
    return invocation


def test_dbt_parse_succeeds_on_full_shape_project(
    dbt_invocation: list[str], tmp_path: Path
) -> None:
    """Generate a project covering every Gold capability; assert dbt parse exit 0."""
    from lake_ingestion_framework.core.gold import (
        GoldModelConfig,
        GoldProjectConfig,
        GoldSnapshotConfig,
        GoldSource,
        Materialization,
        SnapshotStrategy,
        generate_dbt_project,
    )

    project = GoldProjectConfig(
        project_name="smoke_gold",
        sources=(
            GoldSource(name="orders", description="Stripe orders fact source"),
            GoldSource(name="customers", description="Salesforce customers"),
        ),
        models=(
            # Single-source dim — exercises default templated SELECT.
            GoldModelConfig(
                name="dim_customer",
                domain="product",
                silver_sources=("customers",),
                business_keys=("customer_id",),
                materialization=Materialization.TABLE,
                description="Current-state customer dim",
            ),
            # Multi-source-via-ref fact — exercises custom_sql + ref()
            # + the gold_meta_cols() macro inside customer SQL.
            GoldModelConfig(
                name="fct_orders",
                domain="finance",
                silver_sources=("orders",),
                business_keys=("order_id",),
                materialization=Materialization.INCREMENTAL,
                incremental_watermark_column="updated_at",
                description="Orders enriched with current customer segment",
                custom_sql=(
                    "SELECT\n"
                    "    o.order_id,\n"
                    "    o.amount,\n"
                    "    o.updated_at,\n"
                    "    c.segment AS customer_segment,\n"
                    "    {{ gold_meta_cols() }}\n"
                    "FROM {{ source('silver', 'orders') }} o\n"
                    "LEFT JOIN {{ ref('dim_customer') }} c USING (customer_id)\n"
                    "{% if is_incremental() %}\n"
                    "  WHERE o.updated_at > "
                    "(SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL 3 DAYS\n"
                    "{% endif %}"
                ),
            ),
        ),
        snapshots=(
            # timestamp strategy
            GoldSnapshotConfig(
                name="snap_orders_daily",
                target_schema="gold_snapshots",
                source_model="fct_orders",
                unique_key="order_id",
                strategy=SnapshotStrategy.TIMESTAMP,
                updated_at_column="_gold_loaded_at",
                description="Daily snapshot of fct_orders",
            ),
            # check strategy on a different model
            GoldSnapshotConfig(
                name="snap_customer_segment",
                target_schema="gold_snapshots",
                source_model="dim_customer",
                unique_key="customer_id",
                strategy=SnapshotStrategy.CHECK,
                check_cols=("segment",),
            ),
        ),
    )

    project_dir = tmp_path / "gold_dbt"
    generate_dbt_project(project, project_dir)

    # Fake env vars satisfy the Databricks adapter's profile loader without
    # touching a real warehouse. ``dbt parse`` doesn't connect — it only
    # builds the manifest from local files.
    parse_env = {
        **os.environ,
        "DATABRICKS_HOST": "fake.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/fake",
        "DATABRICKS_TOKEN": "fake-token",
        "GOLD_CATALOG": "main",
        "GOLD_SCHEMA": "gold",
    }

    # ``dbt deps`` first — packages.yml lists dbt-utils; dbt parse refuses
    # to proceed without dbt_packages/ even if no SQL references the package.
    deps_result = subprocess.run(  # noqa: S603 — dbt_invocation is sys.executable
        [
            *dbt_invocation,
            "deps",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(project_dir),
        ],
        capture_output=True,
        check=False,  # asserted explicitly below for richer failure output
        env=parse_env,
        text=True,
        timeout=120,
    )
    assert deps_result.returncode == 0, (
        f"dbt deps failed:\n--- stdout ---\n{deps_result.stdout}\n"
        f"--- stderr ---\n{deps_result.stderr}"
    )

    parse_result = subprocess.run(  # noqa: S603 — dbt_invocation is sys.executable
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
        check=False,  # asserted explicitly below for richer failure output
        env=parse_env,
        text=True,
        timeout=120,
    )

    assert parse_result.returncode == 0, (
        f"dbt parse failed:\n--- stdout ---\n{parse_result.stdout}\n"
        f"--- stderr ---\n{parse_result.stderr}"
    )

    # Belt-and-braces: deprecation warnings are non-fatal but indicate
    # we're emitting an outdated shape. Treat them as test failures so
    # we catch dbt-version drift early.
    assert "DeprecationsSummary" not in parse_result.stdout, (
        "dbt parse emitted deprecation warnings — generator is producing an "
        f"outdated shape. Output:\n{parse_result.stdout}"
    )

    # Manifest is the proof — confirm dbt actually wrote target/.
    assert (project_dir / "target" / "manifest.json").is_file(), (
        "dbt parse claimed success but didn't write target/manifest.json. "
        f"Output:\n{parse_result.stdout}"
    )
