"""Integration tests for ``MysqlConnector``.

Skipped unless ``LAKE_INGEST_TEST_MYSQL_URL`` is set in the environment.
See ``tests/integration/README.md`` for setup.

URL shape::

    mysql://user:password@host:port/database
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest
from lake_ingestion_framework.core.source_connectors.mysql import (
    MysqlConnectionParams,
    MysqlConnector,
)

pytestmark = pytest.mark.integration


MYSQL_URL_ENV = "LAKE_INGEST_TEST_MYSQL_URL"


def _params_from_env() -> MysqlConnectionParams:
    url = os.environ.get(MYSQL_URL_ENV)
    if not url:
        pytest.skip(f"{MYSQL_URL_ENV} not set; see tests/integration/README.md")

    parsed = urlparse(url)
    if parsed.scheme not in {"mysql", "mariadb"}:
        pytest.skip(f"{MYSQL_URL_ENV} must be a mysql:// URL; got {parsed.scheme}")

    return MysqlConnectionParams(
        server=parsed.hostname or "localhost",
        port=parsed.port or 3306,
        database=(parsed.path or "/").lstrip("/") or "test",
        user=parsed.username or "",
        password=parsed.password or "",
        application_name="lake-ingest-integration-test",
        connect_timeout_seconds=10,
    )


@pytest.fixture()
def connector() -> Iterator[MysqlConnector]:
    c = MysqlConnector(_params_from_env())
    c.connect()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def seeded_table(connector: MysqlConnector) -> Iterator[str]:
    """Create a customers table in the connection's database; drop on teardown."""
    db = connector._params.database
    conn = connector._conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS lake_ingest_customers")
        cur.execute(
            """
            CREATE TABLE lake_ingest_customers (
                id          INT NOT NULL PRIMARY KEY,
                email       VARCHAR(255) NOT NULL,
                first_name  VARCHAR(100) NULL,
                last_name   VARCHAR(100) NULL,
                modified_at DATETIME NOT NULL
            )
            """
        )
    try:
        yield db
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS lake_ingest_customers")


def test_lists_user_schemas(connector: MysqlConnector, seeded_table: str) -> None:
    schemas = connector.list_schemas()
    assert seeded_table in schemas
    assert "information_schema" not in schemas
    assert "mysql" not in schemas
    assert "performance_schema" not in schemas
    assert "sys" not in schemas


def test_lists_tables_in_schema(connector: MysqlConnector, seeded_table: str) -> None:
    tables = connector.list_tables(seeded_table)
    table_names = [t.name for t in tables]
    assert "lake_ingest_customers" in table_names
    customers = next(t for t in tables if t.name == "lake_ingest_customers")
    assert customers.table_type == "TABLE"


def test_describes_table_columns(connector: MysqlConnector, seeded_table: str) -> None:
    cols = connector.describe_table(seeded_table, "lake_ingest_customers")
    by_name = {c.name: c for c in cols}

    assert by_name["id"].is_primary_key is True
    assert by_name["id"].is_nullable is False
    assert by_name["email"].is_nullable is False
    assert by_name["first_name"].is_nullable is True
    assert "datetime" in by_name["modified_at"].data_type.lower()


def test_describe_unknown_table_returns_empty(connector: MysqlConnector, seeded_table: str) -> None:
    assert connector.describe_table(seeded_table, "no_such_table") == []


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def seeded_with_rows(connector: MysqlConnector, seeded_table: str) -> str:
    conn = connector._conn
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO lake_ingest_customers "
            "(id, email, first_name, last_name, modified_at) VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "alice@example.com", "Alice", "Anders", "2026-01-01 00:00:00"),
                (2, "bob@example.com", "Bob", "Brown", "2026-02-01 00:00:00"),
                (3, "carol@example.com", "Carol", "Cox", "2026-03-01 00:00:00"),
                (4, "dave@example.com", "Dave", "Davis", "2026-04-01 00:00:00"),
                (5, "eve@example.com", "Eve", "Evans", "2026-05-01 00:00:00"),
            ],
        )
    return seeded_table


def test_read_query_returns_all_rows(connector: MysqlConnector, seeded_with_rows: str) -> None:
    batches = list(connector.read_query("SELECT id, email FROM lake_ingest_customers ORDER BY id"))
    rows = [r for b in batches for r in b.rows]
    assert len(rows) == 5
    assert rows[0] == (1, "alice@example.com")
    assert rows[-1] == (5, "eve@example.com")


def test_read_query_batch_size_respected(connector: MysqlConnector, seeded_with_rows: str) -> None:
    batches = list(
        connector.read_query("SELECT id FROM lake_ingest_customers ORDER BY id", batch_size=2)
    )
    assert [len(b.rows) for b in batches] == [2, 2, 1]


def test_read_query_with_bind_params(connector: MysqlConnector, seeded_with_rows: str) -> None:
    batches = list(
        connector.read_query(
            "SELECT id FROM lake_ingest_customers WHERE id > %s ORDER BY id",
            params=(3,),
        )
    )
    rows = [r for b in batches for r in b.rows]
    assert rows == [(4,), (5,)]


def test_read_query_columns_match_select_order(
    connector: MysqlConnector, seeded_with_rows: str
) -> None:
    batches = list(
        connector.read_query("SELECT email, id, first_name FROM lake_ingest_customers WHERE id = 1")
    )
    assert batches[0].columns == ("email", "id", "first_name")


def test_read_query_empty_result(connector: MysqlConnector, seeded_with_rows: str) -> None:
    assert list(connector.read_query("SELECT id FROM lake_ingest_customers WHERE id > 999")) == []


def test_read_query_propagates_sql_errors(connector: MysqlConnector, seeded_with_rows: str) -> None:
    from lake_ingestion_framework.core.exceptions import QueryFailedError

    with pytest.raises(QueryFailedError):
        list(connector.read_query("SELECT no_such_col FROM lake_ingest_customers"))
