"""Loader for Phase 6b-1 metadata DB log tables.

Two dialects ship:

- ``"azure_sql_db"`` — canonical T-SQL DDL applied at customer site.
  Each file is ``IF NOT EXISTS ... CREATE TABLE`` so re-running the
  deploy is safe (re-running won't re-create the table; running with
  a future schema migration will require a separate ALTER script).
- ``"sqlite"`` — ``CREATE TABLE IF NOT EXISTS`` for unit tests
  against the in-memory ``SqliteMetadataRepository``.

Tables that ship in 6b-1: ``master_run_log``, ``run_log``.
``watermark_audit`` ships alongside (the table exists from day 1 so
``v_watermark_status`` can join against it; CLI procs that write to
it land in Phase 6b-3). ``dataset_segments`` and ``segment_run_log``
land in 6b-2.
"""

from __future__ import annotations

import sqlite3
from importlib import resources
from typing import Literal

Dialect = Literal["azure_sql_db", "sqlite"]

# Deploy order matters: run_log has an FK to master_run_log, so the
# parent table must be created first. watermark_audit FKs only to
# dataset_master (which is in core schema), so it can be created any
# time after master_run_log; we deploy it last for tidiness.
TABLE_NAMES: tuple[str, ...] = (
    "master_run_log",
    "run_log",
    "watermark_audit",
)


def table_sql(table_name: str, *, dialect: Dialect) -> str:
    """Return the DDL for a single table in the chosen dialect."""
    if table_name not in TABLE_NAMES:
        raise ValueError(
            f"unknown table {table_name!r}; expected one of {TABLE_NAMES}",
        )
    return (
        resources.files(
            "lake_ingestion_framework.realms.adf_adls.runtime_sql.tables",
        )
        .joinpath(dialect)
        .joinpath(f"{table_name}.sql")
        .read_text()
    )


def all_tables(*, dialect: Dialect) -> list[tuple[str, str]]:
    """Return ``[(table_name, ddl), ...]`` in deploy (FK-safe) order."""
    return [(name, table_sql(name, dialect=dialect)) for name in TABLE_NAMES]


def bootstrap_sqlite_tables(conn: sqlite3.Connection) -> None:
    """Apply the SQLite table DDLs to an open connection.

    Used by tests that exercise the run-log / master-run-log code path
    against the in-memory metadata DB. Production deploys go through
    the T-SQL files via ``sqlcmd``.
    """
    for _, ddl in all_tables(dialect="sqlite"):
        conn.executescript(ddl)


__all__ = [
    "TABLE_NAMES",
    "Dialect",
    "all_tables",
    "bootstrap_sqlite_tables",
    "table_sql",
]
