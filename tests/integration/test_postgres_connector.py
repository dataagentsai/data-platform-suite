"""Integration tests for ``PostgresConnector``.

Skipped unless ``LAKE_INGEST_TEST_PG_URL`` is set in the environment.
See ``tests/integration/README.md`` for setup.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest
from lake_ingestion_framework.core.source_connectors.postgres import (
    PostgresConnectionParams,
    PostgresConnector,
)

pytestmark = pytest.mark.integration


PG_URL_ENV = "LAKE_INGEST_TEST_PG_URL"


def _params_from_env() -> PostgresConnectionParams:
    """Parse a libpq-style URL into PostgresConnectionParams.

    Format: postgresql://user:password@host:port/database
    """
    url = os.environ.get(PG_URL_ENV)
    if not url:
        pytest.skip(f"{PG_URL_ENV} not set; see tests/integration/README.md")

    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        pytest.skip(f"{PG_URL_ENV} must be a postgres:// URL; got {parsed.scheme}")
    return PostgresConnectionParams(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=(parsed.path or "/").lstrip("/") or "postgres",
        user=parsed.username or "",
        password=parsed.password or "",
        sslmode="disable",
        application_name="lake-ingest-integration-test",
    )


@pytest.fixture()
def connector() -> Iterator[PostgresConnector]:
    """Connector connected to the test database for the duration of one test."""
    c = PostgresConnector(_params_from_env())
    c.connect()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def seeded_schema(connector: PostgresConnector) -> Iterator[str]:
    """Create a temporary schema with a customers table; drop on teardown."""
    schema = "lake_ingest_test"
    # Use the underlying connection directly — we're a test, not framework code
    conn = connector._conn
    assert conn is not None
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(
            f"""
            CREATE TABLE {schema}.customers (
                id          INTEGER PRIMARY KEY,
                email       TEXT NOT NULL,
                first_name  TEXT,
                last_name   TEXT,
                modified_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
            )
            """
        )
    try:
        yield schema
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_lists_user_schemas(connector: PostgresConnector, seeded_schema: str) -> None:
    schemas = connector.list_schemas()
    assert seeded_schema in schemas
    # System schemas excluded
    assert "pg_catalog" not in schemas
    assert "information_schema" not in schemas


def test_lists_tables_in_schema(connector: PostgresConnector, seeded_schema: str) -> None:
    tables = connector.list_tables(seeded_schema)
    assert [t.name for t in tables] == ["customers"]
    assert tables[0].table_type == "TABLE"


def test_describes_table_columns(connector: PostgresConnector, seeded_schema: str) -> None:
    cols = connector.describe_table(seeded_schema, "customers")
    by_name = {c.name: c for c in cols}

    assert by_name["id"].is_primary_key is True
    assert by_name["id"].is_nullable is False
    assert by_name["email"].is_nullable is False
    assert by_name["first_name"].is_nullable is True
    # Postgres reports timestamp columns as 'timestamp without time zone'
    assert "timestamp" in by_name["modified_at"].data_type.lower()


def test_describe_unknown_table_returns_empty(
    connector: PostgresConnector, seeded_schema: str
) -> None:
    assert connector.describe_table(seeded_schema, "no_such_table") == []


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def seeded_table(connector: PostgresConnector, seeded_schema: str) -> str:
    """Insert deterministic rows into the seeded customers table.

    No teardown needed — ``seeded_schema`` drops the schema with CASCADE
    on teardown.
    """
    conn = connector._conn
    assert conn is not None
    with conn.cursor() as cur:
        cur.executemany(
            # Schema name comes from a fixture-controlled constant, not user input.
            f"INSERT INTO {seeded_schema}.customers "  # noqa: S608 — schema is a fixture constant
            "(id, email, first_name, last_name, modified_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01 00:00:00"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01 00:00:00"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01 00:00:00"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01 00:00:00"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01 00:00:00"),
            ],
        )
    return f"{seeded_schema}.customers"


def test_read_query_returns_all_rows(connector: PostgresConnector, seeded_table: str) -> None:
    batches = list(
        connector.read_query(
            f"SELECT id, email FROM {seeded_table} ORDER BY id"  # noqa: S608 — table from fixture
        )
    )
    all_rows = [row for batch in batches for row in batch.rows]
    assert len(all_rows) == 5
    assert all_rows[0] == (1, "alice@example.com")
    assert all_rows[-1] == (5, "eve@example.com")


def test_read_query_columns_match_select_order(
    connector: PostgresConnector, seeded_table: str
) -> None:
    batches = list(
        connector.read_query(
            f"SELECT email, id, first_name FROM {seeded_table} WHERE id = 1"  # noqa: S608 — table from fixture
        )
    )
    assert batches[0].columns == ("email", "id", "first_name")


def test_read_query_batch_size_respected(connector: PostgresConnector, seeded_table: str) -> None:
    batches = list(
        connector.read_query(
            f"SELECT id FROM {seeded_table} ORDER BY id",  # noqa: S608 — table from fixture
            batch_size=2,
        )
    )
    # 5 rows, batch_size=2 → batches of [2, 2, 1]
    assert [len(b.rows) for b in batches] == [2, 2, 1]


def test_read_query_with_bind_params(connector: PostgresConnector, seeded_table: str) -> None:
    batches = list(
        connector.read_query(
            f"SELECT id FROM {seeded_table} WHERE id > %s ORDER BY id",  # noqa: S608 — table from fixture
            params=(3,),
        )
    )
    rows = [r for b in batches for r in b.rows]
    assert rows == [(4,), (5,)]


def test_read_query_empty_result(connector: PostgresConnector, seeded_table: str) -> None:
    batches = list(
        connector.read_query(
            f"SELECT id FROM {seeded_table} WHERE id > 999"  # noqa: S608 — table from fixture
        )
    )
    assert batches == []


def test_read_query_propagates_sql_errors(connector: PostgresConnector, seeded_table: str) -> None:
    from lake_ingestion_framework.core.exceptions import QueryFailedError

    with pytest.raises(QueryFailedError):
        list(
            connector.read_query(
                f"SELECT no_such_column FROM {seeded_table}"  # noqa: S608 — table from fixture
            )
        )


def test_read_query_yields_query_batches_with_columns_tuple(
    connector: PostgresConnector, seeded_table: str
) -> None:
    """Sanity: returned batches expose columns as a tuple (not list)."""
    from lake_ingestion_framework.core.source_connectors.protocol import QueryBatch

    batch = next(
        iter(
            connector.read_query(
                f"SELECT id FROM {seeded_table} LIMIT 1"  # noqa: S608 — table from fixture
            )
        )
    )
    assert isinstance(batch, QueryBatch)
    assert isinstance(batch.columns, tuple)
