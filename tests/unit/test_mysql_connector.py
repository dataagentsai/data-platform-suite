"""Unit tests for the MySQL / MariaDB connector.

Tests run without pymysql installed — the driver is mocked. Real-DB
behavior is covered in ``tests/integration/test_mysql_connector.py``.
"""

from __future__ import annotations

from typing import Any

import pytest
from lake_ingestion_framework.core.exceptions import (
    AuthenticationError,
    QueryFailedError,
    SourceUnavailableError,
)
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    LocalEnvSecretResolver,
)
from lake_ingestion_framework.core.source_connectors.mysql import (
    MysqlConnectionParams,
    MysqlConnector,
    _is_auth_failure,
    _split_user_password,
)


def _resolver_with(
    monkeypatch: pytest.MonkeyPatch, secret: str = "user:pass"
) -> LocalEnvSecretResolver:
    monkeypatch.setenv("LAKE_INGEST_SECRET_TEST_VAULT_MYSQL_CREDS", secret)
    return LocalEnvSecretResolver()


# ──────────────────────────────────────────────────────────────────────
# from_config — parsing
# ──────────────────────────────────────────────────────────────────────


def test_from_config_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    connector = MysqlConnector.from_config(
        {
            "server": "mysql.example.com",
            "database": "orders",
            "connection_secret_name": "kv://test-vault/mysql-creds",
        },
        resolver,
    )
    params = connector._params
    assert params.server == "mysql.example.com"
    assert params.port == 3306
    assert params.database == "orders"
    assert params.user == "user"
    assert params.password == "pass"
    assert params.charset == "utf8mb4"
    assert params.connect_timeout_seconds == 30


def test_from_config_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "alice:s3cret")
    connector = MysqlConnector.from_config(
        {
            "server": "localhost",
            "database": "orders",
            "connection_secret_name": "kv://test-vault/mysql-creds",
            "port": "13306",
            "charset": "latin1",
            "connect_timeout_seconds": "5",
        },
        resolver,
    )
    params = connector._params
    assert params.port == 13306
    assert params.charset == "latin1"
    assert params.connect_timeout_seconds == 5


def test_from_config_missing_server(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="server"):
        MysqlConnector.from_config(
            {"database": "x", "connection_secret_name": "kv://test-vault/mysql-creds"},
            resolver,
        )


def test_from_config_missing_database(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="database"):
        MysqlConnector.from_config(
            {"server": "x", "connection_secret_name": "kv://test-vault/mysql-creds"},
            resolver,
        )


def test_from_config_invalid_secret_format(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "no-colon-here")
    with pytest.raises(AuthenticationError, match="user:password"):
        MysqlConnector.from_config(
            {
                "server": "s",
                "database": "d",
                "connection_secret_name": "kv://test-vault/mysql-creds",
            },
            resolver,
        )


# ──────────────────────────────────────────────────────────────────────
# connect() error mapping (mocked pymysql)
# ──────────────────────────────────────────────────────────────────────


class _FakeOperationalError(Exception):
    """Stand-in for pymysql.err.OperationalError."""

    def __init__(self, code: int, msg: str) -> None:
        super().__init__(code, msg)


class _FakeMySQLError(Exception):
    """Stand-in for pymysql.err.MySQLError."""


class _FakeErrModule:
    OperationalError = _FakeOperationalError
    MySQLError = _FakeMySQLError


class _FakeCursors:
    Cursor = object


class _FakePyMySQLModule:
    err = _FakeErrModule
    cursors = _FakeCursors

    def __init__(self, *, raise_on_connect: Exception | None = None) -> None:
        self._raise = raise_on_connect

    def connect(self, **_kwargs: Any) -> object:
        if self._raise is not None:
            raise self._raise
        return object()


def _patch_pymysql(monkeypatch: pytest.MonkeyPatch, fake: _FakePyMySQLModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.mysql._lazy_import_pymysql",
        lambda: fake,
    )


def _connector() -> MysqlConnector:
    return MysqlConnector(
        MysqlConnectionParams(
            server="srv",
            port=3306,
            database="db",
            user="u",
            password="p",
        )
    )


def test_connect_access_denied_mapped_to_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePyMySQLModule(
        raise_on_connect=_FakeOperationalError(1045, "Access denied for user 'u'")
    )
    _patch_pymysql(monkeypatch, fake)
    with pytest.raises(AuthenticationError, match="rejected credentials"):
        _connector().connect()


def test_connect_network_failure_mapped_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePyMySQLModule(
        raise_on_connect=_FakeOperationalError(2003, "Can't connect to MySQL server")
    )
    _patch_pymysql(monkeypatch, fake)
    with pytest.raises(SourceUnavailableError, match="Cannot reach MySQL"):
        _connector().connect()


def test_connect_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePyMySQLModule()
    _patch_pymysql(monkeypatch, fake)
    c = _connector()
    c.connect()
    first_conn = c._conn
    c.connect()
    assert c._conn is first_conn


def test_close_idempotent() -> None:
    c = _connector()
    c.close()
    c.close()


def test_pymysql_missing_raises_clean_error() -> None:
    import importlib.util

    if importlib.util.find_spec("pymysql") is not None:
        pytest.skip("pymysql is installed; this test only runs without the extra")
    with pytest.raises(SourceUnavailableError, match="pymysql not installed"):
        _connector().connect()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (_FakeOperationalError(1045, "Access denied for user 'reader'"), True),
        (_FakeOperationalError(2003, "Can't connect to MySQL server"), False),
        (_FakeOperationalError(2002, "Can't connect through socket"), False),
        (_FakeOperationalError(1043, "Bad handshake"), False),
        (Exception("Access denied for some reason"), True),  # falls back to msg check
        (Exception("Network unreachable"), False),
    ],
)
def test_is_auth_failure(exc: Exception, expected: bool) -> None:
    assert _is_auth_failure(exc) is expected


def test_split_user_password_valid() -> None:
    assert _split_user_password("alice:s3cret") == ("alice", "s3cret")


def test_split_user_password_password_with_colon() -> None:
    assert _split_user_password("alice:abc:def") == ("alice", "abc:def")


def test_split_user_password_no_colon_raises() -> None:
    with pytest.raises(AuthenticationError, match="user:password"):
        _split_user_password("nouser")


def test_split_user_password_empty_components() -> None:
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password(":pass")
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password("user:")


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


class _FakeMysqlError(Exception):
    """Stand-in for ``pymysql.err.MySQLError``."""


class _FakeSSCursor:
    """Server-side cursor stand-in supporting the read_query path."""

    def __init__(
        self,
        *,
        description: list[tuple[str, ...]] | None = None,
        fetch_batches: list[list[tuple[Any, ...]]] | None = None,
        execute_raises: Exception | None = None,
    ) -> None:
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

    def __enter__(self) -> _FakeSSCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        self.closed = True


class _FakeSSCursorClass:
    """Sentinel class identifying the SSCursor request."""


class _FakeMysqlCursorsModule:
    SSCursor = _FakeSSCursorClass


class _FakeMysqlErrModule:
    MySQLError = _FakeMysqlError


class _FakeMysqlConnection:
    """Returns the next configured cursor when SSCursor is requested."""

    def __init__(self, cursors: list[_FakeSSCursor]) -> None:
        self._queue = list(cursors)
        self.cursors_created: list[_FakeSSCursor] = []
        self.requested_cursor_classes: list[type] = []

    def cursor(self, cursor_class: type | None = None) -> _FakeSSCursor:
        self.requested_cursor_classes.append(cursor_class or type(None))
        if not self._queue:
            raise AssertionError("Test asked for more cursors than configured.")
        cur = self._queue.pop(0)
        self.cursors_created.append(cur)
        return cur

    def close(self) -> None:
        pass


class _FakePymysqlModule:
    cursors = _FakeMysqlCursorsModule
    err = _FakeMysqlErrModule


def _params() -> MysqlConnectionParams:
    return MysqlConnectionParams(server="srv", port=3306, database="db", user="u", password="p")


def _patch_pymysql(monkeypatch: pytest.MonkeyPatch, fake: _FakePymysqlModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.mysql._lazy_import_pymysql",
        lambda: fake,
    )


def _connector_with_conn(
    monkeypatch: pytest.MonkeyPatch,
    cursors: list[_FakeSSCursor],
) -> tuple[MysqlConnector, _FakeMysqlConnection]:
    fake_module = _FakePymysqlModule()
    _patch_pymysql(monkeypatch, fake_module)
    fake_conn = _FakeMysqlConnection(cursors)
    c = MysqlConnector(_params())
    c._conn = fake_conn
    return c, fake_conn


def test_read_query_yields_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(
        description=[("id",), ("email",)],
        fetch_batches=[[(1, "a@x.com")], [(2, "b@x.com")], []],
    )
    c, _ = _connector_with_conn(monkeypatch, [cur])

    batches = list(c.read_query("SELECT id, email FROM customers", batch_size=1))

    assert [b.rows for b in batches] == [[(1, "a@x.com")], [(2, "b@x.com")]]
    assert all(b.columns == ("id", "email") for b in batches)


def test_read_query_uses_server_side_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(description=[("x",)], fetch_batches=[[]])
    c, conn = _connector_with_conn(monkeypatch, [cur])

    list(c.read_query("SELECT x FROM t"))

    # Critical: SSCursor (not the buffered default) must be requested
    assert conn.requested_cursor_classes == [_FakeSSCursorClass]


def test_read_query_passes_bind_params(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(description=[("id",)], fetch_batches=[[]])
    c, _ = _connector_with_conn(monkeypatch, [cur])

    list(c.read_query("SELECT id FROM t WHERE updated_at > %s", params=("2026-01",)))

    assert cur.executed_sql == "SELECT id FROM t WHERE updated_at > %s"
    assert cur.executed_params == ("2026-01",)


def test_read_query_empty_result(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(description=[("id",)], fetch_batches=[[]])
    c, _ = _connector_with_conn(monkeypatch, [cur])
    assert list(c.read_query("SELECT id FROM t WHERE 1=0")) == []


def test_read_query_invalid_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = _FakePymysqlModule()
    _patch_pymysql(monkeypatch, fake_module)
    c = MysqlConnector(_params())
    c._conn = _FakeMysqlConnection([])

    with pytest.raises(ValueError, match="batch_size must be positive"):
        next(iter(c.read_query("SELECT 1", batch_size=0)))


def test_read_query_no_result_set_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(description=None, fetch_batches=[])
    c, _ = _connector_with_conn(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="no result set"):
        list(c.read_query("CREATE TABLE x (a int)"))


def test_read_query_driver_error_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeSSCursor(
        description=None,
        fetch_batches=[],
        execute_raises=_FakeMysqlError("(1064, 'syntax error')"),
    )
    c, _ = _connector_with_conn(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="syntax error"):
        list(c.read_query("FOO"))


def test_read_query_cursor_closed_after_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeSSCursor(description=[("id",)], fetch_batches=[[(1,)], []])
    c, _ = _connector_with_conn(monkeypatch, [cur])
    list(c.read_query("SELECT id FROM t"))
    assert cur.closed is True
