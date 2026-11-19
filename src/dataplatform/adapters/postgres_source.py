"""SourceDatabase on PostgreSQL: the mock ERP and MES databases, one schema per system.

Locally this is the Postgres on the laptop (or in a container); on a cloud it is a managed Postgres. Only the
connection string changes. Needs psycopg 3 (`--with 'psycopg[binary]'`). The connection string never holds a
password in code: local sockets use peer auth; a cloud binding resolves the secret at run time.
"""

from __future__ import annotations

import os
from typing import Any

from dataplatform.adapters.sql_source import Dialect, SqlSource, ident

DEFAULT_DSN = "dbname=tyre_world"

DIALECT = Dialect(
    placeholder="%s",
    types={
        "string": "TEXT",
        "integer": "BIGINT",
        "number": "NUMERIC(18,2)",
        "date": "DATE",
        "timestamp": "TIMESTAMPTZ",
        "boolean": "BOOLEAN",
    },
    serial="BIGSERIAL PRIMARY KEY",
    qualify=lambda system, table: f"{ident(system)}.{ident(table)}",
    create_schema=lambda system: f"CREATE SCHEMA IF NOT EXISTS {ident(system)}",
)


def _create_database(dsn: str) -> None:
    import psycopg

    name = dict(p.split("=", 1) for p in dsn.split() if "=" in p).get("dbname", "")
    with psycopg.connect(dsn.replace(f"dbname={name}", "dbname=postgres"), autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            admin.execute(f"CREATE DATABASE {ident(name)}")  # nosec B608 - identifier checked by ident()


class PostgresSource(SqlSource):
    def __init__(self, conn: Any):
        super().__init__(conn, DIALECT)

    @classmethod
    def connect(cls, dsn: str | None = None) -> "PostgresSource":
        import psycopg

        dsn = dsn or os.environ.get("WORLD_PG_DSN", DEFAULT_DSN)
        _create_database(dsn)
        conn = psycopg.connect(dsn)
        conn.execute("SET TIME ZONE 'UTC'")
        conn.commit()
        return cls(conn)
