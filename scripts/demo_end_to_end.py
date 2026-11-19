#!/usr/bin/env python3
"""End-to-end local demo — Source → Bronze → Silver → Gold + lineage.

Boots a Docker postgres container, ingests two tables (``customers`` +
``orders``) through every layer the framework owns, emits OpenLineage
events at every hop, then runs lineage queries against the emitted
graph and prints the results. Intended to be eyeballed — verbose
output by design.

Usage::

    python scripts/demo_end_to_end.py [--keep-postgres] [--no-gold]

Pre-reqs:

- Docker Desktop running (postgres comes up in a throwaway container)
- ``uv sync --extra postgres --extra local --extra lineage [--extra gold_dev]``
- Python 3.11-3.13 for the Gold leg; on 3.14 the Gold step skips
  cleanly (dbt-core's mashumaro dep doesn't yet support 3.14)

What this is not:

- Not a smoke test (those live under ``tests/smoke/``); this script
  is for visual confirmation. ``tests/smoke/test_demo_end_to_end.py``
  ships the same flow as a CI gate.
- Not a production runner. Production drives the same components via
  the ADF Mimic / Workflows orchestrator.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────────
# Visual helpers
# ──────────────────────────────────────────────────────────────────────

_BLUE = "\033[34m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _heading(text: str) -> None:
    bar = "─" * 72
    print(f"\n{_BLUE}{bar}{_RESET}")
    print(f"{_BLUE}{_BOLD}  {text}{_RESET}")
    print(f"{_BLUE}{bar}{_RESET}")


def _step(text: str) -> None:
    print(f"{_BOLD}▶{_RESET} {text}")


def _ok(text: str) -> None:
    print(f"  {_GREEN}✓{_RESET} {text}")


def _warn(text: str) -> None:
    print(f"  {_YELLOW}⚠{_RESET} {text}")


def _fail(text: str) -> None:
    print(f"  {_RED}✗{_RESET} {text}", file=sys.stderr)


def _dim(text: str) -> None:
    print(f"  {_DIM}{text}{_RESET}")


# ──────────────────────────────────────────────────────────────────────
# Pre-flight
# ──────────────────────────────────────────────────────────────────────


@dataclass
class PreflightResult:
    docker_ok: bool
    docker_reason: str
    extras: dict[str, bool]
    dbt_ok: bool


def _docker_available() -> tuple[bool, str]:
    if shutil.which("docker") is None:
        return False, "docker CLI not on PATH (install Docker Desktop)"
    info = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    if info.returncode != 0:
        return False, "docker CLI installed but daemon unreachable (start Docker Desktop)"
    return True, ""


def _extras_available() -> dict[str, bool]:
    out: dict[str, bool] = {}
    for module, extra_name in (
        ("psycopg", "postgres"),
        ("deltalake", "local"),
        ("openlineage.client", "lineage"),
    ):
        try:
            __import__(module)
            out[extra_name] = True
        except Exception:
            out[extra_name] = False
    return out


def _dbt_available() -> bool:
    try:
        import dbt
        import dbt.cli.main

        del dbt
    except Exception:
        return False
    return True


def _preflight() -> PreflightResult:
    _heading("Pre-flight")
    docker_ok, docker_reason = _docker_available()
    if docker_ok:
        _ok("Docker reachable")
    else:
        _fail(f"Docker: {docker_reason}")

    extras = _extras_available()
    for name, ok in extras.items():
        if ok:
            _ok(f"[{name}] extra available")
        else:
            _fail(f"[{name}] extra missing — `uv sync --extra {name}`")

    dbt_ok = _dbt_available()
    if dbt_ok:
        _ok("dbt-core importable (Gold leg will run)")
    else:
        py = f"{sys.version_info.major}.{sys.version_info.minor}"
        _warn(
            f"dbt-core not importable on Python {py} — Gold leg will skip. "
            f"Use Python 3.11-3.13 + `uv sync --extra gold_dev`."
        )

    return PreflightResult(
        docker_ok=docker_ok,
        docker_reason=docker_reason,
        extras=extras,
        dbt_ok=dbt_ok,
    )


# ──────────────────────────────────────────────────────────────────────
# Postgres bring-up
# ──────────────────────────────────────────────────────────────────────


_PG_PORT = 55_434  # different port than the smoke test to avoid clash
_PG_PASSWORD = "demo_pw"  # noqa: S105 — local Docker container, not real cred
_PG_USER = "postgres"
_PG_DB = "demo"


@dataclass
class PostgresHandle:
    container_name: str
    host: str
    port: int
    user: str
    password: str
    database: str


def _boot_postgres() -> PostgresHandle:
    name = f"lake-ingest-demo-pg-{uuid.uuid4().hex[:8]}"
    _step(f"Starting postgres:16 container {name} on port {_PG_PORT}...")
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            f"{_PG_PORT}:5432",
            "-e",
            f"POSTGRES_PASSWORD={_PG_PASSWORD}",
            "-e",
            f"POSTGRES_USER={_PG_USER}",
            "-e",
            f"POSTGRES_DB={_PG_DB}",
            "postgres:16",
        ],
        capture_output=True,
        check=True,
    )

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "exec", name, "pg_isready", "-U", _PG_USER, "-d", _PG_DB],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            _ok(f"postgres ready on localhost:{_PG_PORT}")
            return PostgresHandle(
                container_name=name,
                host="localhost",
                port=_PG_PORT,
                user=_PG_USER,
                password=_PG_PASSWORD,
                database=_PG_DB,
            )
        time.sleep(1)
    raise RuntimeError(f"postgres in {name} didn't become ready within 60s")


def _teardown_postgres(handle: PostgresHandle) -> None:
    _step(f"Stopping container {handle.container_name}...")
    subprocess.run(
        ["docker", "stop", handle.container_name],
        capture_output=True,
        check=False,
        timeout=30,
    )
    _ok("teardown complete")


def _seed_postgres(pg: PostgresHandle) -> None:
    import psycopg

    dsn = (
        f"host={pg.host} port={pg.port} user={pg.user} "
        f"password={pg.password} dbname={pg.database}"
    )
    _step("Seeding public.customers (5 rows) + public.orders (10 rows)...")
    # ``pg_isready`` returns OK before postgres is fully accepting external
    # connections (it only checks the postmaster). Retry psycopg.connect()
    # for a few seconds rather than failing on the first race.
    deadline = time.monotonic() + 30
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn_handle = psycopg.connect(dsn, autocommit=True)
            break
        except psycopg.OperationalError as exc:
            last_exc = exc
            time.sleep(0.5)
    else:
        raise RuntimeError(f"postgres never accepted connections: {last_exc}")

    with conn_handle as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.customers (
                customer_id INTEGER PRIMARY KEY,
                email       TEXT NOT NULL,
                segment     TEXT NOT NULL,
                updated_at  TIMESTAMP NOT NULL
            )
            """
        )
        cur.execute("DELETE FROM public.customers")
        cur.executemany(
            "INSERT INTO public.customers VALUES (%s, %s, %s, %s)",
            [
                # The first 4 rows are "clean"; row 5 has whitespace + mixed
                # case in email + a lowercased segment code, so the standard
                # processors (hygiene / email_lowercase / code_uppercase)
                # have something to actually do during the Silver run.
                (1, "alice@example.com", "premium", "2026-04-01 09:00:00"),
                (2, "bob@example.com", "free", "2026-04-02 09:00:00"),
                (3, "carol@example.com", "premium", "2026-04-03 09:00:00"),
                (4, "dave@example.com", "free", "2026-04-04 09:00:00"),
                (5, "  Eve@Example.COM  ", "enterprise", "2026-04-05 09:00:00"),
            ],
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id    INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                amount      NUMERIC(10, 2) NOT NULL,
                status      TEXT NOT NULL,
                updated_at  TIMESTAMP NOT NULL
            )
            """
        )
        cur.execute("DELETE FROM public.orders")
        cur.executemany(
            "INSERT INTO public.orders VALUES (%s, %s, %s, %s, %s)",
            [
                (101, 1, 49.99, "shipped", "2026-04-10 10:00:00"),
                (102, 1, 19.99, "shipped", "2026-04-11 10:00:00"),
                (103, 2, 9.99, "pending", "2026-04-12 10:00:00"),
                (104, 3, 199.00, "shipped", "2026-04-13 10:00:00"),
                (105, 3, 75.50, "shipped", "2026-04-14 10:00:00"),
                (106, 4, 12.00, "cancelled", "2026-04-15 10:00:00"),
                (107, 5, 299.99, "shipped", "2026-04-16 10:00:00"),
                (108, 5, 599.00, "shipped", "2026-04-17 10:00:00"),
                (109, 1, 25.00, "pending", "2026-04-18 10:00:00"),
                (110, 2, 39.99, "shipped", "2026-04-19 10:00:00"),
            ],
        )
    _ok("seed complete")


# ──────────────────────────────────────────────────────────────────────
# Stage 1 — Source → Bronze
# ──────────────────────────────────────────────────────────────────────


def _ingest_table_to_bronze(
    pg: PostgresHandle,
    *,
    schema: str,
    table: str,
    bronze_path: Path,
    run_id: str,
    source_jsonl: Path,
    namespace: str,
    source_namespace: str,
) -> int:
    """Ingest one postgres table to a Bronze Delta path; emit Source lineage.

    Returns the number of rows written (used for visual sanity-checking).
    """
    from lake_ingestion_framework.core.lineage import (
        JsonLinesSink,
        SourceRunStats,
        emit_source_run_event,
    )
    from lake_ingestion_framework.core.source_connectors.postgres import (
        PostgresConnectionParams,
        PostgresConnector,
    )
    from lake_ingestion_framework.core.writers.local_delta import LocalDeltaWriter

    started = time.monotonic()
    connector = PostgresConnector(
        PostgresConnectionParams(
            host=pg.host,
            port=pg.port,
            database=pg.database,
            user=pg.user,
            password=pg.password,
            sslmode="disable",
        )
    )
    connector.connect()
    try:
        writer = LocalDeltaWriter(table_path=bronze_path)
        # Identifier is interpolated, but schema and table come from the
        # demo script's own constants (not user input); the postgres
        # connector's read_query is also bind-safe for parameter values.
        sql = f"SELECT * FROM {schema}.{table}"  # noqa: S608
        write_stats = writer.write_batched(
            connector.read_query(sql),
            run_id=run_id,
        )
    finally:
        connector.close()

    duration = time.monotonic() - started
    with JsonLinesSink(source_jsonl, mode="a") as sink:
        emit_source_run_event(
            SourceRunStats(
                run_id=run_id,
                source_namespace=source_namespace,
                source_name=f"{schema}.{table}",
                bronze_namespace=namespace,
                bronze_name=f"bronze.{table}",
                rows_written=write_stats.rows_written,
                columns=write_stats.columns,
                source_columns=tuple(c for c in write_stats.columns if not c.startswith("_")),
                batches_written=write_stats.batches_written,
                duration_seconds=duration,
                query_text=sql,
            ),
            sink,
        )
    return write_stats.rows_written


# ──────────────────────────────────────────────────────────────────────
# Stage 2 — Bronze → Silver
# ──────────────────────────────────────────────────────────────────────


def _silver_run(
    *,
    bronze_path: Path,
    silver_path: Path,
    run_id: str,
    strategy: str,
    business_keys: tuple[str, ...] | None,
    silver_jsonl: Path,
    namespace: str,
    silver_dataset_name: str,
    bronze_dataset_name: str,
    processor_context: Any | None = None,
) -> Any:
    from lake_ingestion_framework.core.lineage import (
        JsonLinesSink,
        emit_silver_run_event,
    )
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    stats = SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id=run_id,
            strategy=SilverStrategy(strategy),
            business_keys=business_keys,
            processor_context=processor_context,
        )
    )
    with JsonLinesSink(silver_jsonl, mode="a") as sink:
        emit_silver_run_event(
            stats,
            sink,
            namespace=namespace,
            silver_dataset_name=silver_dataset_name,
            bronze_dataset_name=bronze_dataset_name,
        )
    return stats


# ──────────────────────────────────────────────────────────────────────
# Stage 2b — Standard processors metadata
# ──────────────────────────────────────────────────────────────────────


def _setup_processor_metadata(workspace: Path) -> tuple[Any, Any]:
    """Seed an in-memory metadata DB with column transforms + per-env
    PII policy + processor_groups, then return per-table contexts.

    Mirrors what ``lake-ingest config sync`` produces from a customer's
    YAML — but built inline so the demo runs without touching the
    customer-repo authoring surface. Real customers do this via YAML;
    the demo wires it up programmatically to keep the script
    self-contained.
    """
    from lake_ingestion_framework.core.metadata.repository import (
        DatasetColumnRow,
        SqliteMetadataRepository,
    )
    from lake_ingestion_framework.core.silver import SilverProcessorContext
    from lake_ingestion_framework.core.types.enums import SourceType

    # Persist to disk so `lake-ingest processors describe --dataset
    # customers --env dev --metadata-db <path>` works after the demo
    # finishes. Picked up by the demo's "Try it" suggestions at the end.
    db_path = workspace / "metadata-dev.sqlite"
    repo = SqliteMetadataRepository(db_path)
    repo.bootstrap_schema()
    source_id = repo.upsert_source(
        source_name="postgres_demo",
        source_env="dev",
        source_type=SourceType.POSTGRES,
        is_enabled=True,
        config={"connection_pattern": "postgres_basic", "auth_method": "basic"},
    )

    # Customers — exercises hygiene + case_normalize (auto via
    # column_role / pii_class) + standardization (email_validate auto)
    cust_id = repo.upsert_dataset(
        source_id=source_id,
        dataset_name="customers",
        is_enabled=True,
        config={
            "realm": "adf-adls",
            "dataset_type": "TABLE",
            "load_type": "INCREMENTAL",
            "target_path_template": "raw/{source}/{object}",
            "schema_name": "public",
            "object_name": "customers",
            "watermark_column": "updated_at",
        },
        columns=[
            DatasetColumnRow(
                dataset_id=0,
                column_name="customer_id",
                data_type="INTEGER",
                is_nullable=False,
                ordinal_position=1,
                is_primary_key=True,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="email",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="email",
                source_path=None,
                # email_lowercase + email_validate fire automatically;
                # PII action comes from meta.pii_policy (pass_through in dev)
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="segment",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=3,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
                column_role="code",  # triggers code_uppercase
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="updated_at",
                data_type="TIMESTAMP",
                is_nullable=False,
                ordinal_position=4,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
        ],
    )

    # Orders — typed columns; type_safety + boolean_coercion handles them
    ord_id = repo.upsert_dataset(
        source_id=source_id,
        dataset_name="orders",
        is_enabled=True,
        config={
            "realm": "adf-adls",
            "dataset_type": "TABLE",
            "load_type": "INCREMENTAL",
            "target_path_template": "raw/{source}/{object}",
            "schema_name": "public",
            "object_name": "orders",
            "watermark_column": "updated_at",
        },
        columns=[
            DatasetColumnRow(
                dataset_id=0,
                column_name="order_id",
                data_type="INTEGER",
                is_nullable=False,
                ordinal_position=1,
                is_primary_key=True,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="customer_id",
                data_type="INTEGER",
                is_nullable=False,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="amount",
                data_type="NUMERIC",
                is_nullable=False,
                ordinal_position=3,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="status",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=4,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
                column_role="code",
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="updated_at",
                data_type="TIMESTAMP",
                is_nullable=False,
                ordinal_position=5,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
        ],
    )

    # Per-env PII policy — dev keeps clear PII so we can see processor
    # effects (case-normalised email). Prod would set action=hash.
    repo.upsert_pii_policy(
        pii_class="email",
        env="dev",
        action="pass_through",
    )

    platform_groups = {
        "hygiene": "enabled",
        "type_safety": "enabled",
        "case_normalize": "enabled",
        "timestamp_normalize": "enabled",
        "standardization": "opt_in",
        "pii": "enabled",
        "dedup": "dataset_decides",
        "provenance": "enabled",
    }

    cust_ctx = SilverProcessorContext(
        repo=repo,
        dataset_id=cust_id,
        platform_groups=platform_groups,
        env="dev",
    )
    ord_ctx = SilverProcessorContext(
        repo=repo,
        dataset_id=ord_id,
        platform_groups=platform_groups,
        env="dev",
    )
    return cust_ctx, ord_ctx


# ──────────────────────────────────────────────────────────────────────
# Stage 3 — Silver → Gold (dbt)
# ──────────────────────────────────────────────────────────────────────


def _generate_gold(project_dir: Path) -> None:
    from lake_ingestion_framework.core.gold import (
        GoldModelConfig,
        GoldProjectConfig,
        GoldSource,
        Materialization,
        generate_dbt_project,
    )

    generate_dbt_project(
        GoldProjectConfig(
            project_name="lake_ingest_demo",
            sources=(
                GoldSource(name="customers", description="Demo customers from postgres"),
                GoldSource(name="orders", description="Demo orders from postgres"),
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
                    description="Order fact joined to current customer segment",
                    custom_sql=(
                        "SELECT\n"
                        "    o.order_id,\n"
                        "    o.amount,\n"
                        "    o.status,\n"
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
        ),
        project_dir,
    )


def _dbt_invocation() -> list[str]:
    return [sys.executable, "-m", "dbt.cli.main"]


def _dbt_run(args: list[str], project_dir: Path) -> tuple[int, str]:
    parse_env = {
        **os.environ,
        "DATABRICKS_HOST": "fake.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/fake",
        "DATABRICKS_TOKEN": "fake-demo-token",
        "GOLD_CATALOG": "main",
        "GOLD_SCHEMA": "gold",
    }
    cmd = [
        *_dbt_invocation(),
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        check=False,
        env=parse_env,
        text=True,
        timeout=180,
    )
    return result.returncode, result.stdout + result.stderr


def _emit_gold_lineage(*, project_dir: Path, gold_jsonl: Path, namespace: str) -> int:
    from lake_ingestion_framework.core.lineage import (
        LineageEmitConfig,
        emit_from_dbt_manifest,
    )

    stats = emit_from_dbt_manifest(
        LineageEmitConfig(
            manifest_path=project_dir / "target" / "manifest.json",
            output_path=gold_jsonl,
            namespace=namespace,
        )
    )
    return stats.events_written


# ──────────────────────────────────────────────────────────────────────
# Stage 4 — Lineage queries
# ──────────────────────────────────────────────────────────────────────


def _print_query(label: str, nodes: set[str] | list[str] | None) -> None:
    print(f"  {_BOLD}{label}{_RESET}")
    if nodes is None:
        print(f"    {_DIM}(no path){_RESET}")
        return
    items = sorted(nodes) if isinstance(nodes, set) else nodes
    if not items:
        print(f"    {_DIM}(none){_RESET}")
        return
    for n in items:
        print(f"    • {n}")


def _print_path(label: str, path: list[str] | None) -> None:
    print(f"  {_BOLD}{label}{_RESET}")
    if not path:
        print(f"    {_DIM}(no path){_RESET}")
        return
    for i, node in enumerate(path):
        prefix = "    " if i == 0 else "      ↓\n    "
        print(f"{prefix}{node}")


# ──────────────────────────────────────────────────────────────────────
# Stage 5 — Inspection
# ──────────────────────────────────────────────────────────────────────


def _print_delta_summary(label: str, path: Path) -> None:
    from deltalake import DeltaTable

    if not (path / "_delta_log").is_dir():
        _warn(f"{label}: no _delta_log at {path}")
        return
    table = DeltaTable(str(path)).to_pyarrow_table()
    print(f"  {_BOLD}{label}{_RESET} ({path.name})")
    print(f"    rows={table.num_rows}, cols={len(table.column_names)}")
    print(f"    columns: {table.column_names}")


def _print_event_summary(label: str, jsonl: Path) -> None:
    if not jsonl.is_file():
        _warn(f"{label}: no events file at {jsonl}")
        return
    events = [json.loads(line) for line in jsonl.read_text().splitlines() if line]
    print(f"  {_BOLD}{label}{_RESET} ({jsonl.name})")
    for e in events:
        outputs = e.get("outputs") or []
        out_name = outputs[0]["name"] if outputs else "(none)"
        job = e.get("job", {})
        print(f"    • {job.get('namespace', '?')}/{job.get('name', '?')} → {out_name}")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="End-to-end local demo.")
    parser.add_argument(
        "--keep-postgres",
        action="store_true",
        help="Don't tear down the postgres container at the end (for poking around).",
    )
    parser.add_argument(
        "--no-gold",
        action="store_true",
        help="Skip the Gold/dbt leg even if dbt is available.",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("/tmp/lake-ingest-demo"),
        help="Where to write Bronze / Silver / Gold artifacts. Wiped at start.",
    )
    args = parser.parse_args(argv)

    pre = _preflight()
    if not pre.docker_ok or not all(pre.extras.values()):
        _fail("Pre-flight failed; fix the items above and re-run.")
        return 1

    workspace: Path = args.workspace
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    bronze_root = workspace / "bronze"
    silver_root = workspace / "silver"
    gold_dir = workspace / "gold_dbt"
    lineage_root = workspace / "lineage"
    lineage_root.mkdir()

    source_jsonl = lineage_root / "source.jsonl"
    silver_jsonl = lineage_root / "silver.jsonl"
    gold_jsonl = lineage_root / "gold.jsonl"

    warehouse_namespace = "databricks://demo"
    source_namespace = f"postgres://localhost:{_PG_PORT}/{_PG_DB}"

    pg = _boot_postgres()
    try:
        _seed_postgres(pg)

        # ── Source → Bronze ─────────────────────────────────────────
        _heading("1. Source → Bronze (postgres → Delta)")
        for table in ("customers", "orders"):
            _step(f"Ingesting public.{table} → {bronze_root}/{table}/...")
            rows = _ingest_table_to_bronze(
                pg,
                schema="public",
                table=table,
                bronze_path=bronze_root / table,
                run_id=f"ingest-{table}-1",
                source_jsonl=source_jsonl,
                namespace=warehouse_namespace,
                source_namespace=source_namespace,
            )
            _ok(f"wrote {rows} rows; emitted Source lineage event")

        # ── Bronze → Silver ─────────────────────────────────────────
        _heading("2. Bronze → Silver")
        _step(
            "Seeding processor metadata (in-memory) — column transforms + "
            "per-env PII policy + processor_groups..."
        )
        cust_ctx, ord_ctx = _setup_processor_metadata(workspace)
        _ok(
            f"metadata DB: {workspace / 'metadata-dev.sqlite'} "
            f"(try `lake-ingest processors describe --dataset customers "
            f"--env dev --metadata-db {workspace / 'metadata-dev.sqlite'}`)"
        )

        _step("Running SCD1 on customers (business_keys=customer_id)...")
        cust_stats = _silver_run(
            bronze_path=bronze_root / "customers",
            silver_path=silver_root / "customers",
            run_id="silver-customers-1",
            strategy="scd1",
            business_keys=("customer_id",),
            silver_jsonl=silver_jsonl,
            namespace=warehouse_namespace,
            silver_dataset_name="silver.customers",
            bronze_dataset_name="bronze.customers",
            processor_context=cust_ctx,
        )
        _ok(
            f"customers: rows_read={cust_stats.rows_read} "
            f"rows_inserted={cust_stats.rows_inserted} "
            f"version={cust_stats.silver_version_after} "
            f"({cust_stats.duration_seconds:.2f}s)"
        )
        _ok(
            f"  processors: {cust_stats.processors_fired} fired, "
            f"{cust_stats.processor_rows_affected} rows transformed "
            f"(hygiene + email_lowercase + email_validate + code_uppercase)"
        )

        _step("Running APPEND on orders...")
        ord_stats = _silver_run(
            bronze_path=bronze_root / "orders",
            silver_path=silver_root / "orders",
            run_id="silver-orders-1",
            strategy="append",
            business_keys=None,
            silver_jsonl=silver_jsonl,
            namespace=warehouse_namespace,
            silver_dataset_name="silver.orders",
            bronze_dataset_name="bronze.orders",
            processor_context=ord_ctx,
        )
        _ok(
            f"orders: rows_read={ord_stats.rows_read} "
            f"rows_written={ord_stats.rows_written} "
            f"version={ord_stats.silver_version_after} "
            f"({ord_stats.duration_seconds:.2f}s)"
        )
        _ok(
            f"  processors: {ord_stats.processors_fired} fired, "
            f"{ord_stats.processor_rows_affected} rows transformed "
            f"(int_promotion + code_uppercase on status)"
        )

        # ── Silver → Gold ───────────────────────────────────────────
        gold_ran = False
        if pre.dbt_ok and not args.no_gold:
            _heading("3. Silver → Gold (dbt project + parse)")
            _step("Generating dbt project (sources, models, snapshots, macros)...")
            _generate_gold(gold_dir)
            _ok(f"project written to {gold_dir}")

            _step("Running `dbt deps`...")
            rc, output = _dbt_run(["deps"], gold_dir)
            if rc != 0:
                _fail(f"dbt deps failed:\n{output[-1000:]}")
                return 2
            _ok("dbt-utils installed")

            _step("Running `dbt parse`...")
            rc, output = _dbt_run(
                [
                    "parse",
                    "--no-version-check",
                    "--no-send-anonymous-usage-stats",
                ],
                gold_dir,
            )
            if rc != 0:
                _fail(f"dbt parse failed:\n{output[-1000:]}")
                return 2
            _ok("manifest.json written; project parses cleanly")

            _step("Emitting OL events from dbt manifest...")
            n = _emit_gold_lineage(
                project_dir=gold_dir,
                gold_jsonl=gold_jsonl,
                namespace=warehouse_namespace,
            )
            _ok(f"emitted {n} Gold lineage events")
            gold_ran = True
        else:
            _heading("3. Silver → Gold (skipped)")
            if args.no_gold:
                _warn("--no-gold passed; skipping dbt leg")
            else:
                _warn("dbt-core not importable; skipping Gold leg")

        # ── Lineage queries ─────────────────────────────────────────
        _heading("4. Lineage queries (built from emitted JSONL)")
        from lake_ingestion_framework.core.lineage import LineageGraph

        files = [source_jsonl, silver_jsonl]
        if gold_ran:
            files.append(gold_jsonl)
        graph = LineageGraph.from_jsonl(files)
        _ok(f"loaded {len(graph.nodes)} datasets, {len(graph.edges)} edges")
        print()

        bronze_orders_q = f"{warehouse_namespace}/bronze.orders"
        silver_orders_q = f"{warehouse_namespace}/silver.orders"
        source_orders_q = f"{source_namespace}/public.orders"

        _print_query(
            f"Upstream of {silver_orders_q}",
            graph.upstream(silver_orders_q),
        )
        print()

        _print_query(
            f"Downstream of {source_orders_q}",
            graph.downstream(source_orders_q),
        )
        print()

        if gold_ran:
            gold_fct_q = f"{warehouse_namespace}/gold.fct_orders"
            _print_path(
                f"Path: {source_orders_q} → {gold_fct_q}",
                graph.path(source_orders_q, gold_fct_q),
            )
            print()
            _print_query(
                f"Blast radius of {bronze_orders_q}",
                graph.blast_radius(bronze_orders_q),
            )
            print()

        # ── Inspection ──────────────────────────────────────────────
        _heading("5. Inspection")
        _print_delta_summary("Bronze customers", bronze_root / "customers")
        _print_delta_summary("Bronze orders", bronze_root / "orders")
        _print_delta_summary("Silver customers", silver_root / "customers")
        _print_delta_summary("Silver orders", silver_root / "orders")
        print()
        _print_event_summary("Source lineage", source_jsonl)
        _print_event_summary("Silver lineage", silver_jsonl)
        if gold_ran:
            _print_event_summary("Gold lineage", gold_jsonl)

        # ── Summary ─────────────────────────────────────────────────
        _heading("Summary")
        _ok("Source → Bronze: real postgres → Delta (with framework metadata cols)")
        _ok("Bronze → Silver: SCD1 + APPEND strategies; OL events emitted")
        if gold_ran:
            _ok("Silver → Gold: real dbt parse; OL events emitted from manifest")
        else:
            _warn("Silver → Gold: skipped (dbt unavailable)")
        _ok(f"Lineage graph: {len(graph.nodes)} datasets, {len(graph.edges)} edges")
        _dim(f"Workspace: {workspace}")
        _dim(f"Lineage JSONL: {lineage_root}")
        if args.keep_postgres:
            _dim(
                f"Container kept up: {pg.container_name} (psql -h localhost "
                f"-p {pg.port} -U {pg.user} {pg.database})"
            )

    finally:
        if not args.keep_postgres:
            _teardown_postgres(pg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
