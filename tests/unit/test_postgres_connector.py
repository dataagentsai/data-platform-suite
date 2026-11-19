"""Unit tests for ``PostgresConnector.read_query`` (Phase 0a).

These tests do NOT require psycopg or a real postgres. They mock the
psycopg module at the ``_lazy_import_psycopg`` boundary and exercise:

- Server-side cursor creation (named cursor, ``itersize`` set)
- Batch yielding behaviour
- Empty result handling
- Driver error mapping to ``QueryFailedError``
- ``batch_size`` validation
- ``connect()`` is called automatically if connector wasn't connected

Real-DB behaviour is covered in ``tests/integration/test_postgres_connector.py``.
"""

from __future__ import annotations

from typing import Any

import pytest
from lake_ingestion_framework.core.exceptions import (
    QueryFailedError,
    SourceUnavailableError,
)
from lake_ingestion_framework.core.source_connectors.postgres import (
    PostgresConnectionParams,
    PostgresConnector,
)

# ──────────────────────────────────────────────────────────────────────
# psycopg fakes
# ──────────────────────────────────────────────────────────────────────


class _FakeColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    """Minimal psycopg-cursor fake supporting the read_query path."""

    def __init__(
        self,
        *,
        name: str | None = None,
        description: list[_FakeColumn] | None = None,
        fetch_batches: list[list[tuple[Any, ...]]] | None = None,
        execute_raises: Exception | None = None,
    ) -> None:
        self.name = name
        self.itersize = 0  # production code sets this
        self.description = description
        self._fetch_batches = list(fetch_batches or [])
        self._execute_raises = execute_raises
        self.executed_sql: str | None = None
        self.executed_params: object = None
        self.closed = False

    def execute(self, sql: str, params: object = None) -> None:
        if self._execute_raises is not None:
            raise self._execute_raises
        self.executed_sql = sql
        self.executed_params = params

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if not self._fetch_batches:
            return []
        return self._fetch_batches.pop(0)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        self.closed = True


class _FakeConnection:
    """Tracks cursors created with ``cursor(name=...)`` for inspection."""

    def __init__(self, cursors_to_return: list[_FakeCursor]) -> None:
        self._queue = list(cursors_to_return)
        self.cursors_created: list[_FakeCursor] = []
        self.closed = False
        # read_query toggles autocommit off for the named-cursor lifetime
        # (server-side cursors require an open transaction).
        self.autocommit = True
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self, *, name: str | None = None) -> _FakeCursor:
        if not self._queue:
            raise AssertionError("Test asked for more cursors than configured.")
        cur = self._queue.pop(0)
        cur.name = name
        self.cursors_created.append(cur)
        return cur

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


class _FakePsycopgError(Exception):
    """Stand-in for ``psycopg.Error``."""


class _FakePsycopgModule:
    Error = _FakePsycopgError


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _params() -> PostgresConnectionParams:
    return PostgresConnectionParams(
        host="srv", port=5432, database="db", user="u", password="p", sslmode="disable"
    )


def _patch_psycopg(monkeypatch: pytest.MonkeyPatch, fake: _FakePsycopgModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.postgres._lazy_import_psycopg",
        lambda: fake,
    )


def _connector_with_conn(
    monkeypatch: pytest.MonkeyPatch,
    cursors: list[_FakeCursor],
) -> tuple[PostgresConnector, _FakeConnection]:
    fake_module = _FakePsycopgModule()
    _patch_psycopg(monkeypatch, fake_module)
    fake_conn = _FakeConnection(cursors)
    c = PostgresConnector(_params())
    c._conn = fake_conn  # bypass connect(); we own the fake
    return c, fake_conn


# ──────────────────────────────────────────────────────────────────────
# read_query — happy path
# ──────────────────────────────────────────────────────────────────────


def test_yields_one_batch_when_under_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(
        description=[_FakeColumn("id"), _FakeColumn("email")],
        fetch_batches=[
            [(1, "a@x.com"), (2, "b@x.com")],  # first fetchmany returns 2 rows
            [],  # second returns none → loop exits
        ],
    )
    c, _ = _connector_with_conn(monkeypatch, [cur])

    batches = list(c.read_query("SELECT id, email FROM customers", batch_size=10_000))

    assert len(batches) == 1
    assert batches[0].columns == ("id", "email")
    assert batches[0].rows == [(1, "a@x.com"), (2, "b@x.com")]


def test_yields_multiple_batches_for_large_results(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(
        description=[_FakeColumn("id")],
        fetch_batches=[
            [(1,), (2,)],
            [(3,), (4,)],
            [(5,)],
            [],
        ],
    )
    c, _ = _connector_with_conn(monkeypatch, [cur])

    batches = list(c.read_query("SELECT id FROM t", batch_size=2))

    assert [b.rows for b in batches] == [[(1,), (2,)], [(3,), (4,)], [(5,)]]
    # All batches share the same columns tuple (same object identity, since
    # the implementation reuses it across iterations)
    assert all(b.columns == ("id",) for b in batches)


def test_uses_server_side_named_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(description=[_FakeColumn("x")], fetch_batches=[[]])
    c, conn = _connector_with_conn(monkeypatch, [cur])

    list(c.read_query("SELECT x FROM t", batch_size=500))

    assert len(conn.cursors_created) == 1
    created = conn.cursors_created[0]
    # Server-side cursor must have a unique name
    assert created.name is not None
    assert created.name.startswith("lake_ingest_read_")
    # itersize aligned to batch_size for efficient wire usage
    assert created.itersize == 500


def test_passes_bind_params_through(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(description=[_FakeColumn("id")], fetch_batches=[[]])
    c, conn = _connector_with_conn(monkeypatch, [cur])

    list(c.read_query("SELECT id FROM t WHERE updated_at > %s", params=("2026-01-01",)))

    assert conn.cursors_created[0].executed_sql == "SELECT id FROM t WHERE updated_at > %s"
    assert conn.cursors_created[0].executed_params == ("2026-01-01",)


def test_empty_result_yields_no_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(description=[_FakeColumn("id")], fetch_batches=[[]])
    c, _ = _connector_with_conn(monkeypatch, [cur])

    batches = list(c.read_query("SELECT id FROM t WHERE 1=0"))

    assert batches == []


def test_cursor_is_closed_after_iteration(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(description=[_FakeColumn("id")], fetch_batches=[[(1,)], []])
    c, _ = _connector_with_conn(monkeypatch, [cur])

    list(c.read_query("SELECT id FROM t"))

    assert cur.closed is True


# ──────────────────────────────────────────────────────────────────────
# read_query — error & validation paths
# ──────────────────────────────────────────────────────────────────────


def test_invalid_batch_size_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = _FakePsycopgModule()
    _patch_psycopg(monkeypatch, fake_module)
    c = PostgresConnector(_params())
    c._conn = _FakeConnection([])

    for bad in (0, -1, -100):
        with pytest.raises(ValueError, match="batch_size must be positive"):
            # generator must be advanced for the body to run
            next(iter(c.read_query("SELECT 1", batch_size=bad)))


def test_no_result_set_raises_query_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """If someone passes a DDL statement, cursor.description is None — refuse silently."""
    cur = _FakeCursor(description=None, fetch_batches=[])
    c, _ = _connector_with_conn(monkeypatch, [cur])

    with pytest.raises(QueryFailedError, match="no result set"):
        list(c.read_query("CREATE TABLE x (a int)"))


def test_driver_error_mapped_to_query_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(
        description=None,
        fetch_batches=[],
        execute_raises=_FakePsycopgError("syntax error at or near 'FOO'"),
    )
    c, _ = _connector_with_conn(monkeypatch, [cur])

    with pytest.raises(QueryFailedError, match="syntax error"):
        list(c.read_query("FOO BAR"))


def test_connect_called_automatically_when_not_connected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If caller forgets to connect(), read_query should connect() lazily."""

    cur = _FakeCursor(description=[_FakeColumn("id")], fetch_batches=[[]])
    fake_conn = _FakeConnection([cur])
    fake_module = _FakePsycopgModule()
    # When PostgresConnector.connect() runs, it calls psycopg.connect(...).
    # We need to patch that to return our fake connection.
    fake_module.connect = lambda **_kwargs: fake_conn  # type: ignore[attr-defined]
    fake_module.OperationalError = _FakePsycopgError  # connect() catches OperationalError
    _patch_psycopg(monkeypatch, fake_module)

    c = PostgresConnector(_params())
    assert c._conn is None  # not yet connected

    list(c.read_query("SELECT id FROM t"))

    assert c._conn is fake_conn


def test_invariant_violation_if_connect_silently_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: if connect() returns without populating _conn, surface a real error."""
    fake_module = _FakePsycopgModule()
    _patch_psycopg(monkeypatch, fake_module)

    c = PostgresConnector(_params())
    # Sabotage connect() to be a no-op so the invariant is violated
    c.connect = lambda: None  # type: ignore[method-assign]

    with pytest.raises(SourceUnavailableError, match="invariant violated"):
        list(c.read_query("SELECT 1"))
