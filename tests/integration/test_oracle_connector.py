"""Integration tests for ``OracleConnector``.

Skipped unless ``LAKE_INGEST_TEST_ORACLE_URL`` is set in the environment.
See ``tests/integration/README.md`` for setup.

URL shape::

    oracle://user:password@host:port/service_name

The ``oracledb`` thin-mode driver is used (pure Python, no Instant
Client install required).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest
from lake_ingestion_framework.core.source_connectors.oracle import (
    OracleConnectionParams,
    OracleConnector,
)

pytestmark = pytest.mark.integration


ORACLE_URL_ENV = "LAKE_INGEST_TEST_ORACLE_URL"


def _params_from_env() -> OracleConnectionParams:
    url = os.environ.get(ORACLE_URL_ENV)
    if not url:
        pytest.skip(f"{ORACLE_URL_ENV} not set; see tests/integration/README.md")

    parsed = urlparse(url)
    if parsed.scheme != "oracle":
        pytest.skip(f"{ORACLE_URL_ENV} must be an oracle:// URL; got {parsed.scheme}")

    service_name = (parsed.path or "/").lstrip("/")
    if not service_name:
        pytest.skip(f"{ORACLE_URL_ENV} must include /service_name in the path")

    return OracleConnectionParams(
        server=parsed.hostname or "localhost",
        port=parsed.port or 1521,
        service_name=service_name,
        user=parsed.username or "",
        password=parsed.password or "",
        application_name="lake-ingest-integration-test",
        connect_timeout_seconds=15,
    )


@pytest.fixture()
def connector() -> Iterator[OracleConnector]:
    c = OracleConnector(_params_from_env())
    c.connect()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def seeded_table(connector: OracleConnector) -> Iterator[str]:
    """Create a test table in the connecting user's schema; drop on teardown.

    Oracle uppercases unquoted identifiers, so the schema is the connecting
    user's name in upper-case (matching what list_schemas / describe_table
    will return).
    """
    user_upper = connector._params.user.upper()
    conn = connector._conn

    cur = conn.cursor()
    try:
        # Drop if exists — Oracle has no IF EXISTS, so handle the failure
        try:
            cur.execute("DROP TABLE lake_ingest_customers")
        except Exception:  # noqa: S110
            pass
        cur.execute(
            """
            CREATE TABLE lake_ingest_customers (
                id          NUMBER(10) NOT NULL PRIMARY KEY,
                email       VARCHAR2(255) NOT NULL,
                first_name  VARCHAR2(100) NULL,
                last_name   VARCHAR2(100) NULL,
                modified_at TIMESTAMP NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        cur.close()

    try:
        yield user_upper
    finally:
        cur = conn.cursor()
        try:
            try:
                cur.execute("DROP TABLE lake_ingest_customers")
                conn.commit()
            except Exception:  # noqa: S110
                pass
        finally:
            cur.close()


def test_lists_user_schemas(connector: OracleConnector, seeded_table: str) -> None:
    schemas = connector.list_schemas()
    # Connecting user's schema is reachable; system schemas are filtered
    assert seeded_table in schemas
    assert "SYS" not in schemas
    assert "SYSTEM" not in schemas


def test_lists_tables_in_user_schema(connector: OracleConnector, seeded_table: str) -> None:
    tables = connector.list_tables(seeded_table)
    table_names = [t.name for t in tables]
    # Oracle uppercases unquoted identifiers
    assert "LAKE_INGEST_CUSTOMERS" in table_names
    customers = next(t for t in tables if t.name == "LAKE_INGEST_CUSTOMERS")
    assert customers.table_type == "TABLE"


def test_describes_table_columns(connector: OracleConnector, seeded_table: str) -> None:
    cols = connector.describe_table(seeded_table, "LAKE_INGEST_CUSTOMERS")
    by_name = {c.name: c for c in cols}

    assert by_name["ID"].is_primary_key is True
    assert by_name["ID"].is_nullable is False
    assert by_name["EMAIL"].is_nullable is False
    assert by_name["FIRST_NAME"].is_nullable is True
    assert "TIMESTAMP" in by_name["MODIFIED_AT"].data_type.upper()


def test_describe_unknown_table_returns_empty(
    connector: OracleConnector, seeded_table: str
) -> None:
    assert connector.describe_table(seeded_table, "NO_SUCH_TABLE") == []


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def seeded_with_rows(connector: OracleConnector, seeded_table: str) -> str:
    conn = connector._conn
    cur = conn.cursor()
    try:
        cur.executemany(
            "INSERT INTO lake_ingest_customers "
            "(id, email, first_name, last_name, modified_at) "
            "VALUES (:1, :2, :3, :4, TO_TIMESTAMP(:5, 'YYYY-MM-DD HH24:MI:SS'))",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01 00:00:00"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01 00:00:00"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01 00:00:00"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01 00:00:00"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01 00:00:00"),
            ],
        )
        conn.commit()
    finally:
        cur.close()
    return seeded_table


def test_oracle_read_query_returns_all_rows(
    connector: OracleConnector, seeded_with_rows: str
) -> None:
    batches = list(connector.read_query("SELECT id, email FROM lake_ingest_customers ORDER BY id"))
    rows = [r for b in batches for r in b.rows]
    assert len(rows) == 5
    assert rows[0] == (1, "alice@example.com")
    assert rows[-1] == (5, "eve@example.com")


def test_oracle_read_query_batch_size(connector: OracleConnector, seeded_with_rows: str) -> None:
    batches = list(
        connector.read_query("SELECT id FROM lake_ingest_customers ORDER BY id", batch_size=2)
    )
    assert [len(b.rows) for b in batches] == [2, 2, 1]


def test_oracle_read_query_named_bind_params(
    connector: OracleConnector, seeded_with_rows: str
) -> None:
    batches = list(
        connector.read_query(
            "SELECT id FROM lake_ingest_customers WHERE id > :min_id ORDER BY id",
            params={"min_id": 3},
        )
    )
    rows = [r for b in batches for r in b.rows]
    assert rows == [(4,), (5,)]


def test_oracle_read_query_columns_match_select_order(
    connector: OracleConnector, seeded_with_rows: str
) -> None:
    batches = list(
        connector.read_query("SELECT email, id, first_name FROM lake_ingest_customers WHERE id = 1")
    )
    # Oracle uppercases unquoted column names in the description
    assert batches[0].columns == ("EMAIL", "ID", "FIRST_NAME")


def test_oracle_read_query_empty_result(connector: OracleConnector, seeded_with_rows: str) -> None:
    assert list(connector.read_query("SELECT id FROM lake_ingest_customers WHERE id > 999")) == []


def test_oracle_read_query_propagates_errors(
    connector: OracleConnector, seeded_with_rows: str
) -> None:
    from lake_ingestion_framework.core.exceptions import QueryFailedError

    with pytest.raises(QueryFailedError):
        list(connector.read_query("SELECT no_such_col FROM lake_ingest_customers"))
