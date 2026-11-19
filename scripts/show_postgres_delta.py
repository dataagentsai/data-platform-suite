"""Demo inspection script: postgres → ADF Mimic → local Delta, with row dump.

Companion to ``tests/smoke/test_postgres_to_delta.py``. The pytest version
asserts and tears down (tmp_path is wiped); this one writes to a stable
path under ``/tmp/lake-ingest-demo/`` and dumps every column of every row
so a human can eyeball the result.

Run::

    python scripts/show_postgres_delta.py

Pre-reqs (same as the smoke test, see tests/smoke/README.md):

- Docker Desktop running
- ``pip install -e '.[postgres,local]'``

What it does (mirrors the pytest fixture chain):

1. Spin up postgres:16 on port 55432
2. Seed 5 rows into ``public.customers``
3. Hand-roll the minimal ADF artifact tree at /tmp/lake-ingest-demo/adf
4. Seed a SQLite metadata DB
5. Run ``PL_INGEST_POSTGRES`` via ``run_pipeline_local``
6. List the Delta files, print the schema, dump every row
7. Stop the postgres container; preserve the lake on disk for inspection
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

# ── Config (matches tests/smoke/test_postgres_to_delta.py)
_PG_PORT = 55_432
_PG_USER = "postgres"
_PG_PASSWORD = "smoketest_pw"  # noqa: S105 — local-only demo password
_PG_DB = "smoketest"
_WORK = Path("/tmp/lake-ingest-demo")  # noqa: S108 — demo path; user can override
_LAKES_ROOT = _WORK / "lakes"
_ARTIFACTS = _WORK / "adf"
_META_DB = _WORK / "meta.db"


def _build_artifact_tree(root: Path) -> None:
    """Hand-roll a minimal LinkedService + 2 Datasets + Pipeline tree."""
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


def _seed_metadata_db(db_path: Path) -> None:
    """Create the run_log table the mimic emulator writes to."""
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


def _wait_for_postgres(container: str, timeout_s: int = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = subprocess.run(  # noqa: S603 — pg_isready probe; constants
            ["docker", "exec", container, "pg_isready", "-U", _PG_USER, "-d", _PG_DB],  # noqa: S607
            check=False,
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"postgres container {container} did not become ready in {timeout_s}s")


def main() -> int:
    if shutil.which("docker") is None:
        sys.stderr.write(
            "Docker CLI not on PATH. Install Docker Desktop "
            "(https://www.docker.com/products/docker-desktop/).\n"
        )
        return 1

    # Reset working area
    if _WORK.exists():
        shutil.rmtree(_WORK)
    _WORK.mkdir(parents=True)

    # Start postgres
    print("→ starting postgres:16 container ...")
    container_name = "lake-ingest-demo-pg-" + uuid.uuid4().hex[:8]
    subprocess.run(  # noqa: S603 — Docker bring-up; constants
        [  # noqa: S607 — Docker by name
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
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
        print("→ waiting for postgres to accept connections ...")
        _wait_for_postgres(container_name)
        print("  ready")

        # Seed source rows
        import psycopg

        dsn = (
            f"host=localhost port={_PG_PORT} user={_PG_USER} "
            f"password={_PG_PASSWORD} dbname={_PG_DB}"
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
        print("→ seeded 5 rows in public.customers")

        # Build artifact tree + metadata
        _build_artifact_tree(_ARTIFACTS)
        print(f"→ wrote ADF artifacts to {_ARTIFACTS}")
        _seed_metadata_db(_META_DB)
        print(f"→ seeded SQLite metadata at {_META_DB}")

        # Run pipeline
        print()
        print("→ running pipeline PL_INGEST_POSTGRES via ADF Mimic ...")
        from lake_ingestion_framework.core.run import run_pipeline_local

        report = run_pipeline_local(
            pipeline_name="PL_INGEST_POSTGRES",
            artifacts_root=_ARTIFACTS,
            metadata_db=_META_DB,
            lakes_root=_LAKES_ROOT,
            parameters={
                "p_server": "localhost",
                "p_port": str(_PG_PORT),
                "p_database": _PG_DB,
                "p_user": _PG_USER,
                "p_password": _PG_PASSWORD,
                "schema_name": "public",
                "table_name": "customers",
                "container": "bronze",
                "folder_path": "postgres/customers",
            },
        )
        print(f"  status: {report.pipeline.status}  run_id: {report.pipeline.run_id}")

        # Inspect the Delta output
        sink = _LAKES_ROOT / "bronze" / "postgres" / "customers"
        print()
        print(f"→ Delta files at {sink}:")
        for p in sorted(sink.rglob("*")):
            if p.is_file():
                print(f"    {p.relative_to(sink)}  ({p.stat().st_size} bytes)")

        from deltalake import DeltaTable

        table = DeltaTable(str(sink)).to_pyarrow_table()
        print()
        print("→ Delta table contents:")
        print(f"  schema: {table.schema}")
        print()
        print("  rows:")
        for i, row in enumerate(table.to_pylist()):
            print(f"  [{i}]  {row}")

    finally:
        print()
        print(f"→ stopping postgres container {container_name}")
        subprocess.run(  # noqa: S603 — Docker teardown; constants
            ["docker", "stop", container_name],  # noqa: S607 — Docker by name
            check=False,
            capture_output=True,
            timeout=30,
        )
        print(f"→ artifacts + lake preserved at {_WORK}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
