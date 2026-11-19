"""End-to-end smoke test: Bronze (LocalDeltaWriter output) → Silver Delta.

Reuses the same Docker postgres → ADF Mimic chain as
``test_postgres_to_delta.py`` to land Bronze, then runs ``SilverPipeline``
on top and verifies the Silver Delta has the expected shape:

- Bronze tracking columns (``_run_id``, ``_loaded_at``) are dropped
- Silver tracking columns (``_silver_run_id``, ``_silver_loaded_at``,
  ``_record_hash``) are present
- Same ``_silver_run_id`` on every row
- Distinct ``_record_hash`` per row (recomputed at Silver, not inherited)

Pre-reqs same as the postgres smoke test (Docker Desktop running +
``[postgres,local]`` extras). Skips with a clear message if Docker
isn't reachable.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


# ──────────────────────────────────────────────────────────────────────
# Docker bring-up — same shape as test_postgres_to_delta.py
# ──────────────────────────────────────────────────────────────────────


_PG_PORT = 55_433  # different port than the postgres test to avoid clash
_PG_PASSWORD = "smoketest_pw"
_PG_USER = "postgres"
_PG_DB = "smoketest"
_CONTAINER_NAME_PREFIX = "lake-ingest-smoke-silver-"


def _docker_available() -> tuple[bool, str]:
    if shutil.which("docker") is None:
        return False, "Docker CLI not on PATH. See tests/smoke/README.md."
    info = subprocess.run(  # noqa: S603
        ["docker", "info", "--format", "{{.ServerVersion}}"],  # noqa: S607
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if info.returncode != 0:
        return False, "Docker daemon unreachable. Start Docker Desktop."
    return True, ""


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[dict[str, str]]:
    ok, reason = _docker_available()
    if not ok:
        pytest.skip(reason)

    name = _CONTAINER_NAME_PREFIX + uuid.uuid4().hex[:8]
    subprocess.run(  # noqa: S603 — Docker bring-up; constants
        [  # noqa: S607 — Docker by name
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
        check=True,
        capture_output=True,
    )

    try:
        # Wait for ready
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            r = subprocess.run(  # noqa: S603 — pg_isready probe; constants
                ["docker", "exec", name, "pg_isready", "-U", _PG_USER, "-d", _PG_DB],  # noqa: S607
                check=False,
                capture_output=True,
                timeout=5,
            )
            if r.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres container {name} did not become ready in 60s")
        yield {
            "host": "localhost",
            "port": str(_PG_PORT),
            "user": _PG_USER,
            "password": _PG_PASSWORD,
            "database": _PG_DB,
        }
    finally:
        subprocess.run(  # noqa: S603 — Docker teardown; constants
            ["docker", "stop", name],  # noqa: S607 — Docker by name
            check=False,
            capture_output=True,
            timeout=30,
        )


# ──────────────────────────────────────────────────────────────────────
# Helpers (shared with test_postgres_to_delta.py — kept inline for
# readability since duplication is tiny)
# ──────────────────────────────────────────────────────────────────────


def _seed_postgres(params: dict[str, str]) -> None:
    import psycopg

    dsn = (
        f"host={params['host']} port={params['port']} user={params['user']} "
        f"password={params['password']} dbname={params['database']}"
    )
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS public.customers ("
            "id INTEGER PRIMARY KEY, email TEXT NOT NULL, "
            "first_name TEXT, last_name TEXT, modified_at TIMESTAMP NOT NULL)"
        )
        cur.execute("DELETE FROM public.customers")
        cur.executemany(
            "INSERT INTO public.customers "
            "(id, email, first_name, last_name, modified_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01 00:00:00"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01 00:00:00"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01 00:00:00"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01 00:00:00"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01 00:00:00"),
            ],
        )


def _build_artifact_tree(root: Path) -> None:
    """LinkedService + 2 Datasets + Pipeline JSON; matches test_postgres_to_delta."""
    (root / "linkedServices").mkdir(parents=True)
    (root / "datasets").mkdir(parents=True)
    (root / "pipelines").mkdir(parents=True)

    (root / "linkedServices" / "LS_POSTGRES.json").write_text(
        json.dumps(
            {
                "name": "LS_POSTGRES",
                "properties": {
                    "type": "AzurePostgreSql",
                    "typeProperties": {
                        "server": "@{linkedService().parameters.p_server}",
                        "port": "@{linkedService().parameters.p_port}",
                        "database": "@{linkedService().parameters.p_database}",
                        "username": "@{linkedService().parameters.p_user}",
                        "password": "@{linkedService().parameters.p_password}",
                        "sslmode": "disable",
                    },
                },
            }
        )
    )
    (root / "datasets" / "DS_POSTGRES_TABLE.json").write_text(
        json.dumps(
            {
                "name": "DS_POSTGRES_TABLE",
                "properties": {
                    "type": "PostgresqlTable",
                    "linkedServiceName": {
                        "referenceName": "LS_POSTGRES",
                        "type": "LinkedServiceReference",
                        "parameters": {
                            "p_server": "@pipeline().parameters.p_server",
                            "p_port": "@pipeline().parameters.p_port",
                            "p_database": "@pipeline().parameters.p_database",
                            "p_user": "@pipeline().parameters.p_user",
                            "p_password": "@pipeline().parameters.p_password",
                        },
                    },
                    "typeProperties": {
                        "schema": {"type": "Expression", "value": "@dataset().schema_name"},
                        "table": {"type": "Expression", "value": "@dataset().table_name"},
                    },
                },
            }
        )
    )
    (root / "datasets" / "DS_ADLS_PARQUET_SINK.json").write_text(
        json.dumps(
            {
                "name": "DS_ADLS_PARQUET_SINK",
                "properties": {
                    "type": "Parquet",
                    "typeProperties": {
                        "location": {
                            "type": "AzureBlobFSLocation",
                            "fileSystem": {"type": "Expression", "value": "@dataset().container"},
                            "folderPath": {
                                "type": "Expression",
                                "value": "@dataset().folder_path",
                            },
                        }
                    },
                },
            }
        )
    )
    (root / "pipelines" / "PL_INGEST_POSTGRES.json").write_text(
        json.dumps(
            {
                "name": "PL_INGEST_POSTGRES",
                "properties": {
                    "parameters": {
                        "p_server": {"type": "String"},
                        "p_port": {"type": "String"},
                        "p_database": {"type": "String"},
                        "p_user": {"type": "String"},
                        "p_password": {"type": "String"},
                        "schema_name": {"type": "String"},
                        "table_name": {"type": "String"},
                        "container": {"type": "String"},
                        "folder_path": {"type": "String"},
                    },
                    "activities": [
                        {
                            "name": "CopyFromPostgresToParquet",
                            "type": "Copy",
                            "inputs": [
                                {
                                    "referenceName": "DS_POSTGRES_TABLE",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "schema_name": "@pipeline().parameters.schema_name",
                                        "table_name": "@pipeline().parameters.table_name",
                                    },
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "DS_ADLS_PARQUET_SINK",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "container": "@pipeline().parameters.container",
                                        "folder_path": "@pipeline().parameters.folder_path",
                                    },
                                }
                            ],
                            "typeProperties": {
                                "source": {"type": "AzurePostgreSqlSource"},
                                "sink": {
                                    "type": "ParquetSink",
                                    "formatSettings": {"type": "ParquetWriteSettings"},
                                },
                            },
                        }
                    ],
                },
            }
        )
    )


def _seed_metadata(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE run_log (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_run_id TEXT, dataset_id TEXT, status TEXT,
            started_at TIMESTAMP, completed_at TIMESTAMP,
            run_type TEXT, pipeline_name TEXT, correlation_id TEXT,
            rows_processed TEXT, error_message TEXT
        );
        """
    )
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────────────────────────────
# The actual smoke test
# ──────────────────────────────────────────────────────────────────────


def test_bronze_to_silver_end_to_end(
    postgres_container: dict[str, str],
    tmp_path: Path,
) -> None:
    """Postgres → Bronze (mimic) → Silver (pipeline) → assert Silver shape."""
    from deltalake import DeltaTable
    from lake_ingestion_framework.core.metadata_cols import (
        BRONZE_LOADED_AT,
        BRONZE_RUN_ID,
        SILVER_LOADED_AT,
        SILVER_RECORD_HASH,
        SILVER_RUN_ID,
    )
    from lake_ingestion_framework.core.run import run_pipeline_local
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    # ── Stage 1: postgres → Bronze via ADF Mimic
    _seed_postgres(postgres_container)
    artifacts = tmp_path / "adf"
    _build_artifact_tree(artifacts)
    metadata_db = tmp_path / "meta.db"
    _seed_metadata(metadata_db)
    lakes_root = tmp_path / "lakes"

    bronze_report = run_pipeline_local(
        pipeline_name="PL_INGEST_POSTGRES",
        artifacts_root=artifacts,
        metadata_db=metadata_db,
        lakes_root=lakes_root,
        parameters={
            "p_server": postgres_container["host"],
            "p_port": postgres_container["port"],
            "p_database": postgres_container["database"],
            "p_user": postgres_container["user"],
            "p_password": postgres_container["password"],
            "schema_name": "public",
            "table_name": "customers",
            "container": "bronze",
            "folder_path": "postgres/customers",
        },
    )
    assert bronze_report.succeeded, f"Bronze failed: {bronze_report.pipeline.error}"
    bronze_path = lakes_root / "bronze" / "postgres" / "customers"

    # ── Stage 2: Bronze → Silver via SilverPipeline
    silver_path = lakes_root / "silver" / "postgres" / "customers"
    stats = SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze_path,
            silver_path=silver_path,
            run_id="silver-smoke-1",
            strategy=SilverStrategy.APPEND,
            hash_columns=("id", "email"),
        )
    )

    # ── Stage 3: assertions on the Silver Delta
    assert stats.rows_read == 5
    assert stats.rows_written == 5
    assert stats.strategy == SilverStrategy.APPEND

    silver_table = DeltaTable(str(silver_path)).to_pyarrow_table()
    assert silver_table.num_rows == 5

    cols = set(silver_table.column_names)
    # Silver metadata present
    assert {SILVER_RUN_ID, SILVER_LOADED_AT, SILVER_RECORD_HASH}.issubset(cols)
    # Bronze tracking metadata stripped
    assert BRONZE_RUN_ID not in cols
    assert BRONZE_LOADED_AT not in cols

    # All rows tagged with the run_id we passed in
    assert set(silver_table.column(SILVER_RUN_ID).to_pylist()) == {"silver-smoke-1"}
    # Single batch ⇒ one loaded_at value
    assert len(set(silver_table.column(SILVER_LOADED_AT).to_pylist())) == 1
    # Each row's hash is distinct
    assert len(set(silver_table.column(SILVER_RECORD_HASH).to_pylist())) == 5

    # Source business cols still there + intact
    assert set(silver_table.column("email").to_pylist()) == {
        "alice@example.com",
        "bob@example.com",
        "carol@example.com",
        "dave@example.com",
        "eve@example.com",
    }
