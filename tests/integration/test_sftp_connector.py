"""Integration tests for ``SftpConnector``.

Skipped unless ``LAKE_INGEST_TEST_SFTP_URL`` is set in the environment.
See ``tests/integration/README.md`` for setup.

URL shape::

    sftp://user:password@host:port/root_path

The ``root_path`` (URL path) is treated as the SFTP root_path config
value. Tests upload a small fixture tree at runtime, list it, and
describe a CSV + a JSON Lines file.
"""

from __future__ import annotations

import io
import os
import posixpath
from collections.abc import Iterator
from urllib.parse import unquote, urlparse

import pytest
from lake_ingestion_framework.core.source_connectors.sftp import (
    SftpConnectionParams,
    SftpConnector,
)

pytestmark = pytest.mark.integration


SFTP_URL_ENV = "LAKE_INGEST_TEST_SFTP_URL"


def _params_from_env() -> SftpConnectionParams:
    url = os.environ.get(SFTP_URL_ENV)
    if not url:
        pytest.skip(f"{SFTP_URL_ENV} not set; see tests/integration/README.md")

    parsed = urlparse(url)
    if parsed.scheme != "sftp":
        pytest.skip(f"{SFTP_URL_ENV} must be a sftp:// URL; got {parsed.scheme}")

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    if not user or not password:
        pytest.skip(f"{SFTP_URL_ENV} must include user:password")

    root_path = parsed.path or "."
    return SftpConnectionParams(
        server=parsed.hostname or "localhost",
        port=parsed.port or 22,
        auth_method="basic",
        user=user,
        password=password,
        root_path=root_path,
        connect_timeout_seconds=10,
    )


@pytest.fixture()
def connector() -> Iterator[SftpConnector]:
    c = SftpConnector(_params_from_env())
    c.connect()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture()
def seeded_tree(connector: SftpConnector) -> Iterator[str]:
    """Upload a small fixture tree and clean up afterward.

    Layout under ``root_path``::

        lake_ingest_test/
          orders/
            orders_2024-01-01.csv
          events/
            events.jsonl
    """
    sftp = connector._sftp
    root = connector._params.root_path

    base = posixpath.join(root, "lake_ingest_test")
    orders_dir = posixpath.join(base, "orders")
    events_dir = posixpath.join(base, "events")

    csv_body = io.BytesIO(
        b"id,email,modified_at\n"
        b"1,a@example.com,2024-01-01T00:00:00\n"
        b"2,b@example.com,2024-01-01T00:01:00\n"
        b"3,c@example.com,2024-01-01T00:02:00\n"
    )
    jsonl_body = io.BytesIO(
        b'{"event_id": 1, "kind": "click", "amount": 1.5}\n'
        b'{"event_id": 2, "kind": "view"}\n'
        b'{"event_id": 3, "kind": "purchase", "amount": 99.0}\n'
    )

    # Create tree (mkdir -p style — paramiko has no recursive mkdir)
    for d in (base, orders_dir, events_dir):
        try:
            sftp.mkdir(d)
        except OSError:
            # already exists from a previous failed run; ignore
            pass

    csv_path = posixpath.join(orders_dir, "orders_2024-01-01.csv")
    jsonl_path = posixpath.join(events_dir, "events.jsonl")
    sftp.putfo(csv_body, csv_path)
    sftp.putfo(jsonl_body, jsonl_path)

    # Re-point the connector at the test subtree so list_schemas only sees
    # our two subdirectories
    connector._params = SftpConnectionParams(**{**connector._params.__dict__, "root_path": base})

    try:
        yield base
    finally:
        # Best-effort cleanup
        for path in (csv_path, jsonl_path):
            try:
                sftp.remove(path)
            except OSError:
                pass
        for d in (orders_dir, events_dir, base):
            try:
                sftp.rmdir(d)
            except OSError:
                pass


def test_lists_subdirectories_as_schemas(connector: SftpConnector, seeded_tree: str) -> None:
    schemas = connector.list_schemas()
    assert sorted(schemas) == ["events", "orders"]


def test_lists_files_in_subdirectory(connector: SftpConnector, seeded_tree: str) -> None:
    tables = connector.list_tables("orders")
    assert [t.name for t in tables] == ["orders_2024-01-01.csv"]
    assert tables[0].table_type == "FILE"


def test_describes_csv_columns(connector: SftpConnector, seeded_tree: str) -> None:
    cols = connector.describe_table("orders", "orders_2024-01-01.csv")
    by_name = {c.name: c for c in cols}
    assert "id" in by_name
    assert by_name["id"].data_type == "integer"
    assert by_name["email"].data_type == "string"


def test_describes_jsonl_columns(connector: SftpConnector, seeded_tree: str) -> None:
    cols = connector.describe_table("events", "events.jsonl")
    by_name = {c.name: c for c in cols}
    assert by_name["event_id"].data_type == "integer"
    assert by_name["kind"].data_type == "string"
    # 'amount' is missing in the second row → should be nullable
    assert by_name["amount"].is_nullable is True
