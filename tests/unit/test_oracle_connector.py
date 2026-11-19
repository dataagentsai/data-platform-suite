"""Unit tests for the Oracle Database connector.

Tests run without ``oracledb`` installed — the driver is mocked.
Real-DB behavior is covered in ``tests/integration/test_oracle_connector.py``.
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
from lake_ingestion_framework.core.source_connectors.oracle import (
    OracleConnectionParams,
    OracleConnector,
    _is_auth_failure,
    _split_user_password,
)


def _resolver_with(
    monkeypatch: pytest.MonkeyPatch, secret: str = "user:pass"
) -> LocalEnvSecretResolver:
    monkeypatch.setenv("LAKE_INGEST_SECRET_TEST_VAULT_ORACLE_CREDS", secret)
    return LocalEnvSecretResolver()


# ──────────────────────────────────────────────────────────────────────
# from_config — parsing
# ──────────────────────────────────────────────────────────────────────


def test_from_config_minimal(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    connector = OracleConnector.from_config(
        {
            "server": "oracle.example.com",
            "service_name": "ORCLCDB",
            "connection_secret_name": "kv://test-vault/oracle-creds",
        },
        resolver,
    )
    params = connector._params
    assert params.server == "oracle.example.com"
    assert params.port == 1521
    assert params.service_name == "ORCLCDB"
    assert params.user == "user"
    assert params.password == "pass"
    assert params.connect_timeout_seconds == 30


def test_from_config_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "alice:s3cret")
    connector = OracleConnector.from_config(
        {
            "server": "localhost",
            "service_name": "FREEPDB1",
            "connection_secret_name": "kv://test-vault/oracle-creds",
            "port": "11521",
            "connect_timeout_seconds": "10",
            "application_name": "lake-ingest-it",
        },
        resolver,
    )
    params = connector._params
    assert params.port == 11521
    assert params.connect_timeout_seconds == 10
    assert params.application_name == "lake-ingest-it"


def test_from_config_missing_server(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="server"):
        OracleConnector.from_config(
            {
                "service_name": "ORCLCDB",
                "connection_secret_name": "kv://test-vault/oracle-creds",
            },
            resolver,
        )


def test_from_config_missing_service_name(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch)
    with pytest.raises(QueryFailedError, match="service_name"):
        OracleConnector.from_config(
            {
                "server": "oracle.example.com",
                "connection_secret_name": "kv://test-vault/oracle-creds",
            },
            resolver,
        )


def test_from_config_invalid_secret_format(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "no-colon-here")
    with pytest.raises(AuthenticationError, match="user:password"):
        OracleConnector.from_config(
            {
                "server": "s",
                "service_name": "FREEPDB1",
                "connection_secret_name": "kv://test-vault/oracle-creds",
            },
            resolver,
        )


# ──────────────────────────────────────────────────────────────────────
# connect() error mapping (mocked oracledb)
# ──────────────────────────────────────────────────────────────────────


class _FakeDatabaseError(Exception):
    """Stand-in for oracledb.DatabaseError."""


class _FakeOracleDbModule:
    DatabaseError = _FakeDatabaseError

    def __init__(self, *, raise_on_connect: Exception | None = None) -> None:
        self._raise = raise_on_connect

    def connect(self, **_kwargs: Any) -> object:
        if self._raise is not None:
            raise self._raise
        return object()


def _patch_oracledb(monkeypatch: pytest.MonkeyPatch, fake: _FakeOracleDbModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.oracle._lazy_import_oracledb",
        lambda: fake,
    )


def _connector() -> OracleConnector:
    return OracleConnector(
        OracleConnectionParams(
            server="srv",
            port=1521,
            service_name="ORCLCDB",
            user="u",
            password="p",
        )
    )


def test_connect_invalid_credentials_mapped_to_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeOracleDbModule(
        raise_on_connect=_FakeDatabaseError("ORA-01017: invalid username/password; logon denied")
    )
    _patch_oracledb(monkeypatch, fake)
    with pytest.raises(AuthenticationError, match="rejected credentials"):
        _connector().connect()


def test_connect_listener_failure_mapped_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeOracleDbModule(raise_on_connect=_FakeDatabaseError("ORA-12541: TNS:no listener"))
    _patch_oracledb(monkeypatch, fake)
    with pytest.raises(SourceUnavailableError, match="Cannot reach Oracle"):
        _connector().connect()


def test_connect_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeOracleDbModule()
    _patch_oracledb(monkeypatch, fake)
    c = _connector()
    c.connect()
    first_conn = c._conn
    c.connect()
    assert c._conn is first_conn


def test_close_idempotent() -> None:
    c = _connector()
    c.close()
    c.close()


def test_oracledb_missing_raises_clean_error() -> None:
    import importlib.util

    if importlib.util.find_spec("oracledb") is not None:
        pytest.skip("oracledb is installed; this test only runs without the extra")
    with pytest.raises(SourceUnavailableError, match="oracledb not installed"):
        _connector().connect()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("ORA-01017: invalid username/password; logon denied", True),
        ("Some text including ORA-01017 anywhere", True),
        ("logon denied for user", True),
        ("ORA-12541: TNS:no listener", False),
        ("ORA-12170: TNS:Connect timeout occurred", False),
        ("ORA-12514: TNS:listener does not currently know of service", False),
        ("Generic network error", False),
    ],
)
def test_is_auth_failure(message: str, expected: bool) -> None:
    assert _is_auth_failure(Exception(message)) is expected


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


class _FakeOracleDatabaseError(Exception):
    """Stand-in for ``oracledb.DatabaseError``."""


class _FakeOracleCursor:
    def __init__(
        self,
        *,
        description: list[tuple[str, ...]] | None = None,
        fetch_batches: list[list[tuple[Any, ...]]] | None = None,
        execute_raises: Exception | None = None,
        fetchmany_raises: Exception | None = None,
    ) -> None:
        self.arraysize = 0
        self.description = description
        self._fetch_batches = list(fetch_batches or [])
        self._execute_raises = execute_raises
        self._fetchmany_raises = fetchmany_raises
        self.executed_sql: str | None = None
        self.executed_params: object = None
        self.closed = False

    def execute(self, sql: str, params: object = None) -> None:
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


class _FakeOracleConnection:
    def __init__(self, cursors: list[_FakeOracleCursor]) -> None:
        self._queue = list(cursors)
        self.cursors_created: list[_FakeOracleCursor] = []

    def cursor(self) -> _FakeOracleCursor:
        if not self._queue:
            raise AssertionError("Test asked for more cursors than configured.")
        cur = self._queue.pop(0)
        self.cursors_created.append(cur)
        return cur


class _FakeOracleModule:
    DatabaseError = _FakeOracleDatabaseError


def _read_params() -> OracleConnectionParams:
    return OracleConnectionParams(
        server="srv", port=1521, service_name="ORCL", user="u", password="p"
    )


def _patch_oracle(monkeypatch: pytest.MonkeyPatch, fake: _FakeOracleModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.oracle._lazy_import_oracledb",
        lambda: fake,
    )


def _read_connector(
    monkeypatch: pytest.MonkeyPatch, cursors: list[_FakeOracleCursor]
) -> tuple[OracleConnector, _FakeOracleConnection]:
    _patch_oracle(monkeypatch, _FakeOracleModule())
    fake_conn = _FakeOracleConnection(cursors)
    c = OracleConnector(_read_params())
    c._conn = fake_conn
    return c, fake_conn


def test_oracle_read_query_yields_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(
        description=[("ID",), ("NAME",)],
        fetch_batches=[[(1, "A"), (2, "B")], [(3, "C")], []],
    )
    c, _ = _read_connector(monkeypatch, [cur])
    batches = list(c.read_query("SELECT id, name FROM t", batch_size=2))
    assert [b.rows for b in batches] == [[(1, "A"), (2, "B")], [(3, "C")]]
    assert batches[0].columns == ("ID", "NAME")


def test_oracle_read_query_sets_arraysize(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(description=[("ID",)], fetch_batches=[[]])
    c, conn = _read_connector(monkeypatch, [cur])
    list(c.read_query("SELECT id FROM t", batch_size=2_500))
    # arraysize aligned to batch_size for efficient round-trips
    assert conn.cursors_created[0].arraysize == 2_500


def test_oracle_read_query_named_bind_params(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(description=[("ID",)], fetch_batches=[[]])
    c, _ = _read_connector(monkeypatch, [cur])
    list(c.read_query("SELECT id FROM t WHERE id > :min_id", params={"min_id": 100}))
    assert cur.executed_sql == "SELECT id FROM t WHERE id > :min_id"
    assert cur.executed_params == {"min_id": 100}


def test_oracle_read_query_default_params_empty_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeOracleCursor(description=[("ID",)], fetch_batches=[[]])
    c, _ = _read_connector(monkeypatch, [cur])
    list(c.read_query("SELECT id FROM t"))
    assert cur.executed_params == {}


def test_oracle_read_query_empty_result(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(description=[("ID",)], fetch_batches=[[]])
    c, _ = _read_connector(monkeypatch, [cur])
    assert list(c.read_query("SELECT id FROM t WHERE 1=0")) == []


def test_oracle_read_query_invalid_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_oracle(monkeypatch, _FakeOracleModule())
    c = OracleConnector(_read_params())
    c._conn = _FakeOracleConnection([])
    with pytest.raises(ValueError, match="batch_size must be positive"):
        next(iter(c.read_query("SELECT 1", batch_size=0)))


def test_oracle_read_query_no_result_set_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(description=None, fetch_batches=[])
    c, _ = _read_connector(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="no result set"):
        list(c.read_query("BEGIN NULL; END;"))


def test_oracle_read_query_execute_error_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(
        description=None,
        fetch_batches=[],
        execute_raises=_FakeOracleDatabaseError("ORA-00942: table or view does not exist"),
    )
    c, _ = _read_connector(monkeypatch, [cur])
    with pytest.raises(QueryFailedError, match="ORA-00942"):
        list(c.read_query("SELECT * FROM no_such_table"))


def test_oracle_read_query_cursor_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeOracleCursor(description=[("ID",)], fetch_batches=[[(1,)], []])
    c, _ = _read_connector(monkeypatch, [cur])
    list(c.read_query("SELECT id FROM t"))
    assert cur.closed is True
