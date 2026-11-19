"""Unit tests for the SQL Server / Azure SQL DB connector.

These tests do NOT require pyodbc or a real SQL Server. They:

- Verify ``from_config`` parses the source_config KV map correctly.
- Verify the ODBC connection string is built as expected.
- Verify error mapping for auth vs. network failures.

Real-DB behavior (catalog walks against a live server) is covered in
``tests/integration/test_mssql_connector.py``.
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
from lake_ingestion_framework.core.source_connectors.mssql import (
    MssqlConnectionParams,
    MssqlConnector,
    _is_auth_failure,
    _split_user_password,
)

# ──────────────────────────────────────────────────────────────────────
# from_config — parsing
# ──────────────────────────────────────────────────────────────────────


def _resolver_with(
    monkeypatch: pytest.MonkeyPatch, *, secret: str = "user:pass"
) -> LocalEnvSecretResolver:
    monkeypatch.setenv("LAKE_INGEST_SECRET_TEST_VAULT_SQL_CREDS", secret)
    return LocalEnvSecretResolver()


def test_from_config_minimal_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    connector = MssqlConnector.from_config(
        {
            "server": "myserver.database.windows.net",
            "database": "orders",
            "connection_secret_name": "kv://test-vault/sql-creds",
        },
        resolver,
    )
    params = connector._params
    assert params.server == "myserver.database.windows.net"
    assert params.database == "orders"
    assert params.user == "user"
    assert params.password == "pass"
    # Defaults
    assert params.port == 1433
    assert params.encrypt == "yes"
    assert params.trust_server_certificate == "no"
    assert params.connect_timeout_seconds == 30
    assert params.driver == "ODBC Driver 18 for SQL Server"


def test_from_config_overrides_for_local_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    connector = MssqlConnector.from_config(
        {
            "server": "localhost",
            "database": "master",
            "connection_secret_name": "kv://test-vault/sql-creds",
            "encrypt": "no",
            "trust_server_certificate": "yes",
            "port": "11433",
            "connect_timeout_seconds": "5",
            "application_name": "lake-ingest-it",
        },
        resolver,
    )
    params = connector._params
    assert params.encrypt == "no"
    assert params.trust_server_certificate == "yes"
    assert params.port == 11433
    assert params.connect_timeout_seconds == 5
    assert params.application_name == "lake-ingest-it"


def test_from_config_missing_server_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="server"):
        MssqlConnector.from_config(
            {"database": "x", "connection_secret_name": "kv://test-vault/sql-creds"},
            resolver,
        )


def test_from_config_missing_database_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="database"):
        MssqlConnector.from_config(
            {"server": "x", "connection_secret_name": "kv://test-vault/sql-creds"},
            resolver,
        )


def test_from_config_invalid_secret_format(monkeypatch: pytest.MonkeyPatch) -> None:
    # Secret is missing the colon separator
    resolver = _resolver_with(monkeypatch, secret="userwithoutpassword")
    with pytest.raises(AuthenticationError, match="user:password"):
        MssqlConnector.from_config(
            {
                "server": "s",
                "database": "d",
                "connection_secret_name": "kv://test-vault/sql-creds",
            },
            resolver,
        )


# ──────────────────────────────────────────────────────────────────────
# Connection string
# ──────────────────────────────────────────────────────────────────────


def test_connection_string_for_azure_sql() -> None:
    params = MssqlConnectionParams(
        server="myserver.database.windows.net",
        port=1433,
        database="orders",
        user="reader",
        password="s3cret!",
    )
    connector = MssqlConnector(params)
    conn_str = connector._build_connection_string()
    assert "Driver={ODBC Driver 18 for SQL Server}" in conn_str
    assert "Server=tcp:myserver.database.windows.net,1433" in conn_str
    assert "Database=orders" in conn_str
    assert "UID=reader" in conn_str
    assert "PWD=s3cret!" in conn_str
    assert "Encrypt=yes" in conn_str
    assert "TrustServerCertificate=no" in conn_str


def test_connection_string_for_local_docker() -> None:
    params = MssqlConnectionParams(
        server="localhost",
        port=1433,
        database="master",
        user="sa",
        password="Test1234!Strong",
        encrypt="no",
        trust_server_certificate="yes",
    )
    connector = MssqlConnector(params)
    conn_str = connector._build_connection_string()
    assert "Encrypt=no" in conn_str
    assert "TrustServerCertificate=yes" in conn_str


# ──────────────────────────────────────────────────────────────────────
# connect() error mapping (mocked pyodbc)
# ──────────────────────────────────────────────────────────────────────


class _FakePyodbcError(Exception):
    """Stand-in for pyodbc.Error so we can simulate auth/network failures."""


class _FakePyodbcModule:
    Error = _FakePyodbcError

    def __init__(self, *, raise_on_connect: Exception | None = None) -> None:
        self._raise = raise_on_connect

    def connect(self, conn_str: str, *, timeout: int) -> object:
        if self._raise is not None:
            raise self._raise
        return object()


def _patch_pyodbc(monkeypatch: pytest.MonkeyPatch, fake: _FakePyodbcModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.mssql._lazy_import_pyodbc",
        lambda: fake,
    )


def _connector() -> MssqlConnector:
    return MssqlConnector(
        MssqlConnectionParams(
            server="srv",
            port=1433,
            database="db",
            user="u",
            password="p",
        )
    )


def test_connect_login_failure_mapped_to_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePyodbcModule(raise_on_connect=_FakePyodbcError("Login failed for user 'u'."))
    _patch_pyodbc(monkeypatch, fake)
    with pytest.raises(AuthenticationError, match="rejected credentials"):
        _connector().connect()


def test_connect_network_failure_mapped_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakePyodbcModule(raise_on_connect=_FakePyodbcError("Cannot connect: host unreachable."))
    _patch_pyodbc(monkeypatch, fake)
    with pytest.raises(SourceUnavailableError, match="Cannot reach"):
        _connector().connect()


def test_connect_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling connect() twice is a no-op the second time."""
    fake = _FakePyodbcModule()
    _patch_pyodbc(monkeypatch, fake)
    c = _connector()
    c.connect()
    first_conn = c._conn
    c.connect()
    assert c._conn is first_conn


def test_close_idempotent() -> None:
    """close() on an unconnected connector is a no-op."""
    c = _connector()
    c.close()  # No exception
    c.close()  # And again


def test_pyodbc_missing_raises_clean_error() -> None:
    """When pyodbc isn't installed, connect() surfaces a setup-friendly error."""
    c = _connector()
    # _lazy_import_pyodbc isn't mocked — real import fails because the extra
    # isn't installed in the dev venv. We intentionally don't install pyodbc
    # in [dev] to keep the gate-running venv lean.
    import importlib.util

    if importlib.util.find_spec("pyodbc") is not None:
        pytest.skip("pyodbc is installed; this test only runs without the extra")
    with pytest.raises(SourceUnavailableError, match="pyodbc not installed"):
        c.connect()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Login failed for user 'reader'", True),
        ("Password did not match for user 'x'", True),
        ("Cannot connect to server: host unreachable", False),
        ("Could not open a connection to SQL Server", False),
        ("Network access denied", False),
    ],
)
def test_is_auth_failure(message: str, expected: bool) -> None:
    assert _is_auth_failure(Exception(message)) is expected


def test_split_user_password_valid() -> None:
    assert _split_user_password("reader:s3cret!") == ("reader", "s3cret!")


def test_split_user_password_password_with_colon() -> None:
    """Passwords may contain colons; split on first only."""
    assert _split_user_password("reader:abc:def") == ("reader", "abc:def")


def test_split_user_password_no_colon_raises() -> None:
    with pytest.raises(AuthenticationError, match="user:password"):
        _split_user_password("nouser")


def test_split_user_password_empty_components_raises() -> None:
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password(":justpass")
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password("justuser:")


# ──────────────────────────────────────────────────────────────────────
# read_query (Phase 0a — IngestReader)
# ──────────────────────────────────────────────────────────────────────


class _FakePyodbcCursor:
    """pyodbc-cursor stand-in supporting the read_query path."""

    def __init__(
        self,
        *,
        description: list[tuple[str, ...]] | None = None,
        fetch_batches: list[list[tuple[Any, ...]]] | None = None,
        execute_raises: Exception | None = None,
        fetchmany_raises: Exception | None = None,
    ) -> None:
        self.description = description
        self._fetch_batches = list(fetch_batches or [])
        self._execute_raises = execute_raises
        self._fetchmany_raises = fetchmany_raises
        self.executed_sql: str | None = None
        self.executed_params: object = None
        self.closed = False

    def execute(self, sql: str, params: object = ()) -> None:
        if self._execute_raises is not None:
            raise self._execute_raises
        self.executed_sql = sql
        self.executed_params = params

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._fetchmany_raises is not None:
            raise self._fetchmany_raises
        if not self._fetch_batches:
            return []
        return self._fetch_batches.pop(0)

    def close(self) -> None:
        self.closed = True


class _FakePyodbcConnectionForRead:
    def __init__(self, cursors: list[_FakePyodbcCursor]) -> None:
        self._queue = list(cursors)
        self.cursors_created: list[_FakePyodbcCursor] = []

    def cursor(self) -> _FakePyodbcCursor:
        if not self._queue:
            raise AssertionError("Test asked for more cursors than configured.")
        cur = self._queue.pop(0)
        self.cursors_created.append(cur)
        return cur


class _FakePyodbcModuleWithError:
    Error = _FakePyodbcError


def _params_for_read() -> MssqlConnectionParams:
    return MssqlConnectionParams(server="srv", port=1433, database="db", user="u", password="p")


def _patch_pyodbc_module(monkeypatch: pytest.MonkeyPatch, fake: _FakePyodbcModuleWithError) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.mssql._lazy_import_pyodbc",
        lambda: fake,
    )


def _connector_for_read(
    monkeypatch: pytest.MonkeyPatch,
    cursors: list[_FakePyodbcCursor],
) -> tuple[MssqlConnector, _FakePyodbcConnectionForRead]:
    _patch_pyodbc_module(monkeypatch, _FakePyodbcModuleWithError())
    fake_conn = _FakePyodbcConnectionForRead(cursors)
    c = MssqlConnector(_params_for_read())
    c._conn = fake_conn
    return c, fake_conn


def test_mssql_read_query_yields_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(
        description=[("Id",), ("Name",)],
        fetch_batches=[[(1, "Alice"), (2, "Bob")], [(3, "Carol")], []],
    )
    c, _ = _connector_for_read(monkeypatch, [cur])
    batches = list(c.read_query("SELECT Id, Name FROM dbo.Customer", batch_size=2))
    assert [b.rows for b in batches] == [[(1, "Alice"), (2, "Bob")], [(3, "Carol")]]
    assert batches[0].columns == ("Id", "Name")


def test_mssql_read_query_passes_qmark_params(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(description=[("Id",)], fetch_batches=[[]])
    c, _ = _connector_for_read(monkeypatch, [cur])
    list(c.read_query("SELECT Id FROM t WHERE Id > ?", params=(10,)))
    assert cur.executed_sql == "SELECT Id FROM t WHERE Id > ?"
    assert cur.executed_params == (10,)


def test_mssql_read_query_empty_params_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """When no params given, pyodbc gets an empty tuple (not None)."""
    cur = _FakePyodbcCursor(description=[("Id",)], fetch_batches=[[]])
    c, _ = _connector_for_read(monkeypatch, [cur])
    list(c.read_query("SELECT Id FROM t"))
    assert cur.executed_params == ()


def test_mssql_read_query_empty_result(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(description=[("Id",)], fetch_batches=[[]])
    c, _ = _connector_for_read(monkeypatch, [cur])
    assert list(c.read_query("SELECT Id FROM t WHERE 1=0")) == []


def test_mssql_read_query_invalid_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyodbc_module(monkeypatch, _FakePyodbcModuleWithError())
    c = MssqlConnector(_params_for_read())
    c._conn = _FakePyodbcConnectionForRead([])
    with pytest.raises(ValueError, match="batch_size must be positive"):
        next(iter(c.read_query("SELECT 1", batch_size=-5)))


def test_mssql_read_query_no_result_set_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(description=None, fetch_batches=[])
    c, _ = _connector_for_read(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="no result set"):
        list(c.read_query("CREATE TABLE x (a int)"))


def test_mssql_read_query_execute_error_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(
        description=None,
        fetch_batches=[],
        execute_raises=_FakePyodbcError("Invalid object name 'foo'."),
    )
    c, _ = _connector_for_read(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="Invalid object name"):
        list(c.read_query("SELECT * FROM foo"))


def test_mssql_read_query_fetchmany_error_mapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakePyodbcCursor(
        description=[("Id",)],
        fetch_batches=[[(1,)]],
        fetchmany_raises=_FakePyodbcError("Connection reset by peer."),
    )
    c, _ = _connector_for_read(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="Connection reset"):
        list(c.read_query("SELECT Id FROM t"))


def test_mssql_read_query_cursor_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakePyodbcCursor(description=[("Id",)], fetch_batches=[[(1,)], []])
    c, _ = _connector_for_read(monkeypatch, [cur])
    list(c.read_query("SELECT Id FROM t"))
    assert cur.closed is True
