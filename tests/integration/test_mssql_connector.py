"""Integration tests for ``MssqlConnector``.

Skipped unless ``LAKE_INGEST_TEST_MSSQL_URL`` is set in the environment.
See ``tests/integration/README.md`` for setup.

The URL parses as::

    mssql://user:password@host:port/database?TrustServerCertificate=yes

Query-string overrides map to the matching ``MssqlConnectionParams``
fields. The ``TrustServerCertificate=yes`` and ``Encrypt=no`` flags are
typical for local Docker SQL Server.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import parse_qs, urlparse

import pytest
from lake_ingestion_framework.core.source_connectors.mssql import (
    MssqlConnectionParams,
    MssqlConnector,
)

pytestmark = pytest.mark.integration


MSSQL_URL_ENV = "LAKE_INGEST_TEST_MSSQL_URL"


def _params_from_env() -> MssqlConnectionParams:
    url = os.environ.get(MSSQL_URL_ENV)
    if not url:
        pytest.skip(f"{MSSQL_URL_ENV} not set; see tests/integration/README.md")

    parsed = urlparse(url)
    if parsed.scheme not in {"mssql", "sqlserver"}:
        pytest.skip(f"{MSSQL_URL_ENV} must be a mssql:// URL; got {parsed.scheme}")

    qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    return MssqlConnectionParams(
        server=parsed.hostname or "localhost",
        port=parsed.port or 1433,
        database=(parsed.path or "/").lstrip("/") or "master",
        user=parsed.username or "",
        password=parsed.password or "",
        encrypt=qs.get("Encrypt", "no"),
        trust_server_certificate=qs.get("TrustServerCertificate", "yes"),
        application_name="lake-ingest-integration-test",
        connect_timeout_seconds=15,
    )


@pytest.fixture()
def connector() -> Iterator[MssqlConnector]:
    """Connector connected for one test."""
    c = MssqlConnector(_params_from_env())
    c.connect()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def seeded_schema(connector: MssqlConnector) -> Iterator[str]:
    """Create a temporary schema with a customers table; drop on teardown.

    Uses bracket-quoted identifiers everywhere per CLAUDE.md security rules
    (no SQL by string interpolation of untrusted values; identifiers are
    test-controlled constants).
    """
    schema = "lake_ingest_test"
    conn = connector._conn  # test-only inspection
    cur = conn.cursor()
    try:
        # Drop if exists (cleans up after a previous failed run)
        cur.execute(
            "IF SCHEMA_ID('lake_ingest_test') IS NOT NULL "
            "BEGIN "
            "  IF OBJECT_ID('lake_ingest_test.customers', 'U') IS NOT NULL "
            "    DROP TABLE [lake_ingest_test].[customers]; "
            "  EXEC('DROP SCHEMA [lake_ingest_test]') "
            "END"
        )
        cur.execute("CREATE SCHEMA [lake_ingest_test]")
        cur.execute(
            """
            CREATE TABLE [lake_ingest_test].[customers] (
                [id]          INT NOT NULL PRIMARY KEY,
                [email]       NVARCHAR(255) NOT NULL,
                [first_name]  NVARCHAR(100) NULL,
                [last_name]   NVARCHAR(100) NULL,
                [modified_at] DATETIME2 NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        cur.close()

    try:
        yield schema
    finally:
        cur = conn.cursor()
        try:
            cur.execute(
                "IF OBJECT_ID('lake_ingest_test.customers', 'U') IS NOT NULL "
                "DROP TABLE [lake_ingest_test].[customers]"
            )
            cur.execute(
                "IF SCHEMA_ID('lake_ingest_test') IS NOT NULL "
                "EXEC('DROP SCHEMA [lake_ingest_test]')"
            )
            conn.commit()
        finally:
            cur.close()


def test_lists_user_schemas(connector: MssqlConnector, seeded_schema: str) -> None:
    schemas = connector.list_schemas()
    assert seeded_schema in schemas
    # Built-in role schemas excluded
    assert "sys" not in schemas
    assert "INFORMATION_SCHEMA" not in schemas
    assert "db_owner" not in schemas


def test_lists_tables_in_schema(connector: MssqlConnector, seeded_schema: str) -> None:
    tables = connector.list_tables(seeded_schema)
    assert [t.name for t in tables] == ["customers"]
    assert tables[0].table_type == "TABLE"


def test_describes_table_columns(connector: MssqlConnector, seeded_schema: str) -> None:
    cols = connector.describe_table(seeded_schema, "customers")
    by_name = {c.name: c for c in cols}

    assert by_name["id"].is_primary_key is True
    assert by_name["id"].is_nullable is False
    assert by_name["email"].is_nullable is False
    assert by_name["first_name"].is_nullable is True
    # SQL Server reports DATETIME2 columns as 'datetime2'
    assert "datetime" in by_name["modified_at"].data_type.lower()


def test_describe_unknown_table_returns_empty(
    connector: MssqlConnector, seeded_schema: str
) -> None:
    assert connector.describe_table(seeded_schema, "no_such_table") == []


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def seeded_with_rows(connector: MssqlConnector, seeded_schema: str) -> str:
    conn = connector._conn
    cur = conn.cursor()
    try:
        cur.executemany(
            "INSERT INTO [lake_ingest_test].[customers] "
            "([id], [email], [first_name], [last_name], [modified_at]) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01"),
            ],
        )
        conn.commit()
    finally:
        cur.close()
    return seeded_schema


def test_mssql_read_query_returns_all_rows(
    connector: MssqlConnector, seeded_with_rows: str
) -> None:
    batches = list(
        connector.read_query(
            "SELECT [id], [email] FROM [lake_ingest_test].[customers] ORDER BY [id]"
        )
    )
    rows = [r for b in batches for r in b.rows]
    assert len(rows) == 5
    assert rows[0] == (1, "alice@example.com")
    assert rows[-1] == (5, "eve@example.com")


def test_mssql_read_query_batch_size(connector: MssqlConnector, seeded_with_rows: str) -> None:
    batches = list(
        connector.read_query(
            "SELECT [id] FROM [lake_ingest_test].[customers] ORDER BY [id]",
            batch_size=2,
        )
    )
    assert [len(b.rows) for b in batches] == [2, 2, 1]


def test_mssql_read_query_qmark_params(connector: MssqlConnector, seeded_with_rows: str) -> None:
    batches = list(
        connector.read_query(
            "SELECT [id] FROM [lake_ingest_test].[customers] WHERE [id] > ? ORDER BY [id]",
            params=(3,),
        )
    )
    rows = [r for b in batches for r in b.rows]
    assert rows == [(4,), (5,)]


def test_mssql_read_query_empty_result(connector: MssqlConnector, seeded_with_rows: str) -> None:
    assert (
        list(
            connector.read_query("SELECT [id] FROM [lake_ingest_test].[customers] WHERE [id] > 999")
        )
        == []
    )


def test_mssql_read_query_propagates_errors(
    connector: MssqlConnector, seeded_with_rows: str
) -> None:
    from lake_ingestion_framework.core.exceptions import QueryFailedError

    with pytest.raises(QueryFailedError):
        list(connector.read_query("SELECT [no_such_col] FROM [lake_ingest_test].[customers]"))
