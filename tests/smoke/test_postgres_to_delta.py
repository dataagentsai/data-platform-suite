"""End-to-end smoke test: Docker postgres → ADF Mimic → local Delta.

This test proves the full local-mode loop works against a real source
database. It is the headline demo path described in
``docs/adf_mimic_design.md`` §12.3.

What it does:

1. Spin up a postgres:16 container on port 55432 (avoid clash with a
   developer's local postgres). Tears down on teardown.
2. Create a ``customers`` table and seed 5 rows.
3. Build a minimal artifact tree using the existing generator
   (LinkedService + Dataset + Pipeline JSON).
4. Seed a SQLite metadata DB with the run-log tables the mimic emulates.
5. Invoke ``run_pipeline_local()`` to execute the pipeline.
6. Read back the Delta table that landed and verify row counts +
   metadata columns.

Requirements (all in CLAUDE.md / README):

- Docker Desktop running
- ``pip install lake-ingest[postgres,local]``

Skipped by default. Run with::

    pytest -m smoke tests/smoke/

Or via the Makefile::

    make smoke-test

Skips with a clear message if Docker isn't reachable, so the suite
doesn't fail on machines without it.
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
# Docker bring-up
# ──────────────────────────────────────────────────────────────────────


_PG_PORT = 55_432
_PG_PASSWORD = "smoketest_pw"
_PG_USER = "postgres"
_PG_DB = "smoketest"
_CONTAINER_NAME_PREFIX = "lake-ingest-smoke-pg-"


def _docker_available() -> tuple[bool, str]:
    """Return (available, reason). False+reason when Docker can't be used."""
    if shutil.which("docker") is None:
        return False, (
            "Docker CLI not on PATH. Install Docker Desktop "
            "(https://www.docker.com/products/docker-desktop/) and re-run."
        )
    info = subprocess.run(  # noqa: S603
        ["docker", "info", "--format", "{{.ServerVersion}}"],  # noqa: S607 — Docker by name
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if info.returncode != 0:
        return False, (
            "Docker CLI is installed but the daemon isn't reachable. "
            "Start Docker Desktop and re-run.\n"
            f"docker info stderr: {info.stderr.strip()}"
        )
    return True, ""


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[dict[str, str]]:
    """Spin up a postgres:16 container; yield connection params."""
    ok, reason = _docker_available()
    if not ok:
        pytest.skip(reason)

    name = _CONTAINER_NAME_PREFIX + uuid.uuid4().hex[:8]
    subprocess.run(  # noqa: S603 — Docker bring-up; args are constants
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
        _wait_for_postgres(name)
        yield {
            "host": "localhost",
            "port": str(_PG_PORT),
            "user": _PG_USER,
            "password": _PG_PASSWORD,
            "database": _PG_DB,
        }
    finally:
        subprocess.run(  # noqa: S603 — Docker teardown; args are constants
            ["docker", "stop", name],  # noqa: S607 — Docker by name
            check=False,
            capture_output=True,
            timeout=30,
        )


def _wait_for_postgres(container: str, timeout_s: int = 60) -> None:
    """Poll ``pg_isready`` inside the container until it returns 0 or we time out."""
    deadline = time.monotonic() + timeout_s
    last_stderr = ""
    while time.monotonic() < deadline:
        result = subprocess.run(  # noqa: S603 — pg_isready probe; args are constants
            ["docker", "exec", container, "pg_isready", "-U", _PG_USER, "-d", _PG_DB],  # noqa: S607
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return
        last_stderr = result.stderr or result.stdout
        time.sleep(1)
    pytest.fail(
        f"postgres container {container} did not become ready within {timeout_s}s. "
        f"Last status: {last_stderr.strip()}"
    )


@pytest.fixture(scope="module")
def seeded_postgres(postgres_container: dict[str, str]) -> dict[str, str]:
    """Create the ``customers`` table + 5 rows. Returns same conn params."""
    import psycopg

    dsn = (
        f"host={postgres_container['host']} port={postgres_container['port']} "
        f"user={postgres_container['user']} password={postgres_container['password']} "
        f"dbname={postgres_container['database']}"
    )
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.customers (
                id          INTEGER PRIMARY KEY,
                email       TEXT NOT NULL,
                first_name  TEXT,
                last_name   TEXT,
                modified_at TIMESTAMP NOT NULL
            )
            """
        )
        cur.execute("DELETE FROM public.customers")
        cur.executemany(
            "INSERT INTO public.customers (id, email, first_name, last_name, modified_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01 00:00:00"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01 00:00:00"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01 00:00:00"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01 00:00:00"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01 00:00:00"),
            ],
        )
    return postgres_container


# ──────────────────────────────────────────────────────────────────────
# Artifact tree fixture
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def artifacts_root(tmp_path: Path) -> Path:
    """A minimal LinkedService + Dataset + Pipeline tree the mimic can run.

    Hand-rolled (rather than via the full generator) so the test is
    self-contained and obvious. Production paths use the generator.
    """
    root = tmp_path / "adf"
    (root / "linkedServices").mkdir(parents=True)
    (root / "datasets").mkdir(parents=True)
    (root / "pipelines").mkdir(parents=True)

    # ── LinkedService: postgres source — concrete properties (no Key Vault)
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

    # ── Dataset: source table — parameterised schema/table
    # Parameter references wrap in Expression dicts to match the shape the
    # generator emits (see realms/adf_adls/generator/datasets.py).
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
                        "schema": {
                            "type": "Expression",
                            "value": "@dataset().schema_name",
                        },
                        "table": {
                            "type": "Expression",
                            "value": "@dataset().table_name",
                        },
                    },
                },
            }
        )
    )

    # ── Dataset: ADLS sink — local file path comes via `container` + `folder_path`
    (root / "datasets" / "DS_ADLS_PARQUET_SINK.json").write_text(
        json.dumps(
            {
                "name": "DS_ADLS_PARQUET_SINK",
                "properties": {
                    "type": "Parquet",
                    "typeProperties": {
                        "location": {
                            "type": "AzureBlobFSLocation",
                            "fileSystem": {
                                "type": "Expression",
                                "value": "@dataset().container",
                            },
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

    # ── Pipeline: one Copy activity
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
    return root


# ──────────────────────────────────────────────────────────────────────
# Metadata DB fixture (SQLite stub of the run-log tables)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def metadata_db(tmp_path: Path) -> Path:
    db = tmp_path / "meta.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE run_log (
            run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            master_run_id TEXT,
            dataset_id    TEXT,
            status        TEXT,
            started_at    TIMESTAMP,
            completed_at  TIMESTAMP,
            run_type      TEXT,
            pipeline_name TEXT,
            correlation_id TEXT,
            rows_processed TEXT,
            error_message TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db


# ──────────────────────────────────────────────────────────────────────
# The actual smoke test
# ──────────────────────────────────────────────────────────────────────


def test_postgres_to_delta_end_to_end(
    seeded_postgres: dict[str, str],
    artifacts_root: Path,
    metadata_db: Path,
    tmp_path: Path,
) -> None:
    """The headline demo: postgres → mimic → local Delta, end-to-end."""
    from deltalake import DeltaTable
    from lake_ingestion_framework.core.run import run_pipeline_local

    lakes_root = tmp_path / "lakes"
    report = run_pipeline_local(
        pipeline_name="PL_INGEST_POSTGRES",
        artifacts_root=artifacts_root,
        metadata_db=metadata_db,
        lakes_root=lakes_root,
        parameters={
            "p_server": seeded_postgres["host"],
            "p_port": seeded_postgres["port"],
            "p_database": seeded_postgres["database"],
            "p_user": seeded_postgres["user"],
            "p_password": seeded_postgres["password"],
            "schema_name": "public",
            "table_name": "customers",
            "container": "bronze",
            "folder_path": "postgres/customers",
        },
    )

    # ── 1. Pipeline succeeded
    assert report.succeeded, (
        f"Pipeline failed: {report.pipeline.error}\n"
        f"Activity statuses: {[(r.status, r.error) for r in report.pipeline.activity_results]}"
    )

    # ── 2. Delta files exist at the expected sink path
    sink_path = lakes_root / "bronze" / "postgres/customers"
    assert sink_path.exists(), f"Sink path missing: {sink_path}"
    assert (sink_path / "_delta_log").exists(), "Delta _delta_log dir missing"

    # ── 3. Reading back the Delta table returns the 5 source rows + metadata cols.
    # Using pyarrow's Table directly to avoid pulling pandas in just for the
    # smoke verification.
    table = DeltaTable(str(sink_path)).to_pyarrow_table()
    assert table.num_rows == 5, f"Expected 5 rows, got {table.num_rows}"
    column_names = set(table.column_names)
    assert {"email", "_run_id", "_loaded_at", "_record_hash"}.issubset(column_names)

    emails = set(table.column("email").to_pylist())
    assert emails == {
        "alice@example.com",
        "bob@example.com",
        "carol@example.com",
        "dave@example.com",
        "eve@example.com",
    }
    # Same run_id on every row (one Copy run, one batch)
    assert len(set(table.column("_run_id").to_pylist())) == 1
    # Distinct hashes — every row's hash differs
    assert len(set(table.column("_record_hash").to_pylist())) == 5
