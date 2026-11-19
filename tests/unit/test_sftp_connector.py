"""Unit tests for the SFTP connector.

Two layers covered:

1. **Format inference** (pure functions): CSV column inference and
   JSON Lines column inference. No paramiko involved.
2. **Connector logic**: ``from_config`` parsing, auth method
   switching, ``connect`` error mapping. paramiko is mocked.

Real-server behaviour (listing real files, end-to-end discover) is
covered in ``tests/integration/test_sftp_connector.py``.
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
from lake_ingestion_framework.core.source_connectors.sftp import (
    SftpConnectionParams,
    SftpConnector,
    _file_extension,
    _infer_csv_columns,
    _infer_jsonl_columns,
    _parse_extensions,
    _split_user_password,
)

# ──────────────────────────────────────────────────────────────────────
# CSV inference
# ──────────────────────────────────────────────────────────────────────


class TestCsvInference:
    def test_basic_csv_with_header(self) -> None:
        sample = b"id,email,age\n1,a@x,30\n2,b@x,25\n3,c@x,40\n"
        cols = _infer_csv_columns(sample)
        assert [c.name for c in cols] == ["id", "email", "age"]
        assert cols[0].data_type == "integer"
        assert cols[1].data_type == "string"
        assert cols[2].data_type == "integer"

    def test_csv_with_floats(self) -> None:
        sample = b"name,price\nWidget,9.99\nGadget,12.50\nThingamajig,3.14\n"
        cols = _infer_csv_columns(sample)
        assert cols[0].data_type == "string"
        assert cols[1].data_type == "numeric"

    def test_csv_with_mixed_int_and_float_promotes_to_numeric(self) -> None:
        sample = b"qty\n1\n2.5\n3\n"
        cols = _infer_csv_columns(sample)
        assert cols[0].data_type == "numeric"

    def test_csv_with_booleans(self) -> None:
        sample = b"id,active\n1,true\n2,false\n3,true\n"
        cols = _infer_csv_columns(sample)
        assert cols[1].data_type == "boolean"

    def test_csv_with_empty_values_marks_nullable(self) -> None:
        sample = b"id,nickname\n1,alice\n2,\n3,charlie\n"
        cols = _infer_csv_columns(sample)
        assert cols[0].is_nullable is False
        assert cols[1].is_nullable is True

    def test_csv_tab_delimited_via_default(self) -> None:
        sample = b"id\tname\n1\talice\n2\tbob\n"
        cols = _infer_csv_columns(sample, default_delimiter="\t")
        assert [c.name for c in cols] == ["id", "name"]
        assert cols[0].data_type == "integer"

    def test_csv_pipe_delimited_sniffed(self) -> None:
        sample = b"id|email|created\n1|a@x|2024-01-01\n2|b@x|2024-01-02\n3|c@x|2024-01-03\n"
        cols = _infer_csv_columns(sample)
        assert [c.name for c in cols] == ["id", "email", "created"]

    def test_empty_sample_returns_empty(self) -> None:
        assert _infer_csv_columns(b"") == []

    def test_whitespace_only_returns_empty(self) -> None:
        assert _infer_csv_columns(b"   \n\n") == []

    def test_header_only_returns_columns_default_string(self) -> None:
        sample = b"id,email,active\n"
        cols = _infer_csv_columns(sample)
        assert [c.name for c in cols] == ["id", "email", "active"]
        # No data rows -> all default to string
        assert all(c.data_type == "string" for c in cols)

    def test_unnamed_columns_get_synthetic_names(self) -> None:
        # Empty header cell
        sample = b"id,,age\n1,x,30\n2,y,40\n"
        cols = _infer_csv_columns(sample)
        names = [c.name for c in cols]
        assert names[0] == "id"
        assert names[1] == "col_2"
        assert names[2] == "age"


# ──────────────────────────────────────────────────────────────────────
# JSON Lines inference
# ──────────────────────────────────────────────────────────────────────


class TestJsonlInference:
    def test_basic_jsonl(self) -> None:
        sample = (
            b'{"id": 1, "email": "a@x", "active": true}\n'
            b'{"id": 2, "email": "b@x", "active": false}\n'
            b'{"id": 3, "email": "c@x", "active": true}\n'
        )
        cols = _infer_jsonl_columns(sample)
        by_name = {c.name: c for c in cols}
        assert by_name["id"].data_type == "integer"
        assert by_name["email"].data_type == "string"
        assert by_name["active"].data_type == "boolean"

    def test_jsonl_with_floats(self) -> None:
        sample = b'{"price": 9.99}\n{"price": 12.50}\n'
        cols = _infer_jsonl_columns(sample)
        assert cols[0].data_type == "numeric"

    def test_jsonl_missing_key_marks_nullable(self) -> None:
        sample = b'{"id": 1, "nickname": "alice"}\n{"id": 2}\n'
        cols = _infer_jsonl_columns(sample)
        by_name = {c.name: c for c in cols}
        assert by_name["id"].is_nullable is False
        assert by_name["nickname"].is_nullable is True

    def test_jsonl_null_value_marks_nullable(self) -> None:
        sample = b'{"id": 1, "nickname": null}\n{"id": 2, "nickname": "bob"}\n'
        cols = _infer_jsonl_columns(sample)
        by_name = {c.name: c for c in cols}
        assert by_name["nickname"].is_nullable is True

    def test_jsonl_array_and_object_types(self) -> None:
        sample = b'{"tags": ["a", "b"], "meta": {"k": 1}}\n{"tags": ["c"], "meta": {"k": 2}}\n'
        cols = _infer_jsonl_columns(sample)
        by_name = {c.name: c for c in cols}
        assert by_name["tags"].data_type == "array"
        assert by_name["meta"].data_type == "object"

    def test_jsonl_truncated_last_line_skipped(self) -> None:
        # The 32 KB sample window can cut a line in half — should not crash.
        sample = b'{"id": 1}\n{"id": 2}\n{"id":'
        cols = _infer_jsonl_columns(sample)
        assert [c.name for c in cols] == ["id"]
        assert cols[0].data_type == "integer"

    def test_jsonl_empty_sample(self) -> None:
        assert _infer_jsonl_columns(b"") == []

    def test_jsonl_non_object_lines_skipped(self) -> None:
        # Plain JSON arrays / scalars at the top level aren't records;
        # we only consider object lines.
        sample = b'[1, 2]\n42\n{"id": 1}\n'
        cols = _infer_jsonl_columns(sample)
        assert [c.name for c in cols] == ["id"]


# ──────────────────────────────────────────────────────────────────────
# from_config — basic auth
# ──────────────────────────────────────────────────────────────────────


def _resolver_with(monkeypatch: pytest.MonkeyPatch, secret: str) -> LocalEnvSecretResolver:
    monkeypatch.setenv("LAKE_INGEST_SECRET_TEST_VAULT_SFTP_CREDS", secret)
    return LocalEnvSecretResolver()


class TestFromConfigBasicAuth:
    def test_minimal_basic_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = _resolver_with(monkeypatch, "user:password")
        connector = SftpConnector.from_config(
            {
                "server": "sftp.example.com",
                "auth_method": "basic",
                "connection_secret_name": "kv://test-vault/sftp-creds",
            },
            resolver,
        )
        params = connector._params
        assert params.server == "sftp.example.com"
        assert params.port == 22
        assert params.auth_method == "basic"
        assert params.user == "user"
        assert params.password == "password"
        assert params.private_key_pem is None
        assert params.root_path == "."
        assert "csv" in params.file_extensions

    def test_basic_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = _resolver_with(monkeypatch, "alice:s3cret")
        connector = SftpConnector.from_config(
            {
                "server": "sftp.partner.com",
                "auth_method": "basic",
                "connection_secret_name": "kv://test-vault/sftp-creds",
                "port": "2222",
                "root_path": "/uploads",
                "file_extensions": "csv,jsonl",
                "connect_timeout_seconds": "10",
            },
            resolver,
        )
        params = connector._params
        assert params.port == 2222
        assert params.root_path == "/uploads"
        assert params.file_extensions == ("csv", "jsonl")
        assert params.connect_timeout_seconds == 10

    def test_basic_invalid_secret_format(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = _resolver_with(monkeypatch, "no-colon-here")
        with pytest.raises(AuthenticationError, match="user:password"):
            SftpConnector.from_config(
                {
                    "server": "sftp.example.com",
                    "auth_method": "basic",
                    "connection_secret_name": "kv://test-vault/sftp-creds",
                },
                resolver,
            )


# ──────────────────────────────────────────────────────────────────────
# from_config — ssh_key auth
# ──────────────────────────────────────────────────────────────────────


# Test fixture: a plausibly-shaped string that the connector will store
# verbatim. We intentionally do NOT mimic the real PEM BEGIN/END markers
# so gitleaks / detect-private-key don't flag this file.
_FAKE_PEM = "FAKE-NOT-A-REAL-KEY-FOR-UNIT-TESTS-ONLY"


class TestFromConfigSshKey:
    def test_ssh_key_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = _resolver_with(monkeypatch, _FAKE_PEM)
        connector = SftpConnector.from_config(
            {
                "server": "sftp.example.com",
                "auth_method": "ssh_key",
                "connection_secret_name": "kv://test-vault/sftp-creds",
                "user": "alice",
            },
            resolver,
        )
        params = connector._params
        assert params.auth_method == "ssh_key"
        assert params.user == "alice"
        assert params.private_key_pem == _FAKE_PEM
        assert params.password is None

    def test_ssh_key_missing_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = _resolver_with(monkeypatch, _FAKE_PEM)
        with pytest.raises(QueryFailedError, match="user"):
            SftpConnector.from_config(
                {
                    "server": "sftp.example.com",
                    "auth_method": "ssh_key",
                    "connection_secret_name": "kv://test-vault/sftp-creds",
                },
                resolver,
            )


# ──────────────────────────────────────────────────────────────────────
# from_config — common errors
# ──────────────────────────────────────────────────────────────────────


def test_unknown_auth_method(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "x:y")
    with pytest.raises(QueryFailedError, match="auth_method"):
        SftpConnector.from_config(
            {
                "server": "x",
                "auth_method": "magic",
                "connection_secret_name": "kv://test-vault/sftp-creds",
            },
            resolver,
        )


def test_missing_server(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "x:y")
    with pytest.raises(QueryFailedError, match="server"):
        SftpConnector.from_config(
            {
                "auth_method": "basic",
                "connection_secret_name": "kv://test-vault/sftp-creds",
            },
            resolver,
        )


# ──────────────────────────────────────────────────────────────────────
# connect() error mapping (mocked paramiko)
# ──────────────────────────────────────────────────────────────────────


class _FakeParamikoSSHException(Exception):
    pass


class _FakeAuthException(Exception):
    pass


class _FakeTransport:
    def __init__(
        self,
        *,
        raise_on_init: Exception | None = None,
        raise_on_connect: Exception | None = None,
    ) -> None:
        self.banner_timeout: int | None = None
        self._raise_on_connect = raise_on_connect
        self.closed = False
        if raise_on_init is not None:
            raise raise_on_init

    def connect(self, **_kwargs: Any) -> None:
        if self._raise_on_connect is not None:
            raise self._raise_on_connect

    def close(self) -> None:
        self.closed = True


class _FakeSFTPClient:
    def __init__(self) -> None:
        self.closed = False

    @classmethod
    def from_transport(cls, _transport: Any) -> _FakeSFTPClient:
        return cls()

    def close(self) -> None:
        self.closed = True


class _FakeParamikoModule:
    SSHException = _FakeParamikoSSHException
    AuthenticationException = _FakeAuthException

    def __init__(
        self,
        *,
        transport_init_error: Exception | None = None,
        transport_connect_error: Exception | None = None,
    ) -> None:
        self._transport_init_error = transport_init_error
        self._transport_connect_error = transport_connect_error

    def Transport(self, _addr: tuple[str, int]) -> _FakeTransport:
        return _FakeTransport(
            raise_on_init=self._transport_init_error,
            raise_on_connect=self._transport_connect_error,
        )

    SFTPClient = _FakeSFTPClient


def _patch_paramiko(monkeypatch: pytest.MonkeyPatch, fake: _FakeParamikoModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.sftp._lazy_import_paramiko",
        lambda: fake,
    )


def _connector() -> SftpConnector:
    return SftpConnector(
        SftpConnectionParams(
            server="sftp.example.com",
            port=22,
            auth_method="basic",
            user="u",
            password="p",
        )
    )


class TestConnectErrorMapping:
    def test_transport_init_failure_mapped_to_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _FakeParamikoModule(
            transport_init_error=_FakeParamikoSSHException("Banner read failed")
        )
        _patch_paramiko(monkeypatch, fake)
        with pytest.raises(SourceUnavailableError, match="Cannot reach SFTP"):
            _connector().connect()

    def test_transport_init_oserror_mapped_to_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = _FakeParamikoModule(transport_init_error=OSError("Connection refused"))
        _patch_paramiko(monkeypatch, fake)
        with pytest.raises(SourceUnavailableError, match="Cannot reach SFTP"):
            _connector().connect()

    def test_auth_failure_mapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeParamikoModule(
            transport_connect_error=_FakeAuthException("Authentication failed")
        )
        _patch_paramiko(monkeypatch, fake)
        with pytest.raises(AuthenticationError, match="rejected credentials"):
            _connector().connect()

    def test_handshake_failure_mapped_to_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeParamikoModule(
            transport_connect_error=_FakeParamikoSSHException("Protocol mismatch")
        )
        _patch_paramiko(monkeypatch, fake)
        with pytest.raises(SourceUnavailableError, match="handshake failed"):
            _connector().connect()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("orders.csv", "csv"),
        ("orders.CSV", "csv"),
        ("data.tsv", "tsv"),
        ("events.jsonl", "jsonl"),
        ("events.ndjson", "ndjson"),
        ("README", ""),
        ("archive.tar.gz", "gz"),
    ],
)
def test_file_extension(filename: str, expected: str) -> None:
    assert _file_extension(filename) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ("csv", "tsv", "jsonl", "ndjson")),
        ("", ("csv", "tsv", "jsonl", "ndjson")),
        ("csv", ("csv",)),
        ("csv,jsonl", ("csv", "jsonl")),
        (".csv, .Tsv, ndjson", ("csv", "tsv", "ndjson")),
    ],
)
def test_parse_extensions(raw: str | None, expected: tuple[str, ...]) -> None:
    assert _parse_extensions(raw) == expected


def test_split_user_password_basic() -> None:
    assert _split_user_password("alice:s3cret") == ("alice", "s3cret")


def test_split_user_password_password_with_colon() -> None:
    assert _split_user_password("alice:abc:def") == ("alice", "abc:def")


def test_split_user_password_no_colon() -> None:
    with pytest.raises(AuthenticationError, match="user:password"):
        _split_user_password("nouser")


def test_split_user_password_empty_components() -> None:
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password(":pass")
    with pytest.raises(AuthenticationError, match="empty"):
        _split_user_password("user:")


def test_close_idempotent() -> None:
    """close() on a never-connected connector is a no-op."""
    c = _connector()
    c.close()  # No exception
    c.close()  # And again


def test_paramiko_missing_raises_clean_error() -> None:
    """When paramiko isn't installed, connect() surfaces a setup-friendly error."""
    import importlib.util

    if importlib.util.find_spec("paramiko") is not None:
        pytest.skip("paramiko is installed; this test only runs without the extra")
    with pytest.raises(SourceUnavailableError, match="paramiko not installed"):
        _connector().connect()


# ──────────────────────────────────────────────────────────────────────
# read_file (Phase 0a — FileIngestReader)
# ──────────────────────────────────────────────────────────────────────


import io as _io  # noqa: E402


class _FakeSftpFile(_io.RawIOBase):
    """Stand-in for paramiko's SFTPFile — readable RawIOBase backed by BytesIO.

    Inheriting from ``RawIOBase`` gives us ``readable``/``writable``/
    ``seekable``/``__iter__`` etc. for free, which ``io.TextIOWrapper``
    needs.
    """

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._buffer = _io.BytesIO(data)
        self.prefetch_called = False

    def prefetch(self) -> None:
        self.prefetch_called = True

    def readable(self) -> bool:  # type: ignore[override]
        return True

    def readinto(self, b: Any) -> int | None:  # type: ignore[override]
        return self._buffer.readinto(b)

    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        return self._buffer.read(n)


class _FakeSftpClient:
    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.opened: list[str] = []

    def open(self, path: str, mode: str = "rb") -> _FakeSftpFile:
        self.opened.append(path)
        if path not in self._files:
            raise OSError(f"No such file: {path}")
        return _FakeSftpFile(self._files[path])


def _connector_with_files(files: dict[str, bytes]) -> SftpConnector:
    c = SftpConnector(
        SftpConnectionParams(
            server="srv",
            port=22,
            auth_method="basic",
            user="u",
            password="p",
        )
    )
    c._sftp = _FakeSftpClient(files)
    return c


def test_read_file_csv_yields_typed_rows() -> None:
    csv_bytes = b"id,email\n1,a@x.com\n2,b@x.com\n3,c@x.com\n"
    c = _connector_with_files({"/data/customers.csv": csv_bytes})

    batches = list(c.read_file("/data/customers.csv", "csv", batch_size=10))

    assert len(batches) == 1
    assert batches[0].columns == ("id", "email")
    assert batches[0].rows == [("1", "a@x.com"), ("2", "b@x.com"), ("3", "c@x.com")]


def test_read_file_csv_respects_batch_size() -> None:
    csv_bytes = b"id\n" + b"".join(f"{i}\n".encode() for i in range(1, 6))
    c = _connector_with_files({"/d/n.csv": csv_bytes})

    batches = list(c.read_file("/d/n.csv", "csv", batch_size=2))

    # 5 rows, batch_size=2 → batches of [2, 2, 1]
    assert [len(b.rows) for b in batches] == [2, 2, 1]


def test_read_file_tsv_uses_tab_default() -> None:
    tsv_bytes = b"id\temail\n1\ta@x.com\n2\tb@x.com\n"
    c = _connector_with_files({"/d/n.tsv": tsv_bytes})

    batches = list(c.read_file("/d/n.tsv", "tsv", batch_size=100))

    assert batches[0].columns == ("id", "email")
    assert batches[0].rows[0] == ("1", "a@x.com")


def test_read_file_csv_custom_delimiter() -> None:
    pipe_bytes = b"id|name\n1|alice\n2|bob\n"
    c = _connector_with_files({"/d/n.csv": pipe_bytes})

    batches = list(
        c.read_file("/d/n.csv", "csv", batch_size=100, format_options={"delimiter": "|"})
    )

    assert batches[0].columns == ("id", "name")
    assert batches[0].rows == [("1", "alice"), ("2", "bob")]


def test_read_file_csv_no_header() -> None:
    raw_bytes = b"1,alice\n2,bob\n"
    c = _connector_with_files({"/d/n.csv": raw_bytes})

    batches = list(
        c.read_file("/d/n.csv", "csv", batch_size=100, format_options={"has_header": False})
    )

    assert batches[0].columns == ("col_1", "col_2")
    assert batches[0].rows == [("1", "alice"), ("2", "bob")]


def test_read_file_csv_pads_short_rows() -> None:
    bytes_with_short_row = b"a,b,c\n1,2,3\n4,5\n"
    c = _connector_with_files({"/d/n.csv": bytes_with_short_row})

    batches = list(c.read_file("/d/n.csv", "csv", batch_size=100))

    assert batches[0].rows == [("1", "2", "3"), ("4", "5", "")]


def test_read_file_jsonl_yields_dict_rows() -> None:
    jsonl = b'{"id":1,"email":"a@x.com"}\n{"id":2,"email":"b@x.com"}\n'
    c = _connector_with_files({"/d/n.jsonl": jsonl})

    batches = list(c.read_file("/d/n.jsonl", "jsonl", batch_size=100))

    assert batches[0].columns == ("id", "email")
    assert batches[0].rows == [(1, "a@x.com"), (2, "b@x.com")]


def test_read_file_jsonl_handles_missing_keys() -> None:
    """Rows missing a key emit None in that position."""
    jsonl = b'{"id":1,"email":"a@x.com"}\n{"id":2}\n{"id":3,"email":"c@x.com"}\n'
    c = _connector_with_files({"/d/n.jsonl": jsonl})

    batches = list(c.read_file("/d/n.jsonl", "jsonl", batch_size=100))

    assert batches[0].rows == [(1, "a@x.com"), (2, None), (3, "c@x.com")]


def test_read_file_jsonl_skips_malformed_lines() -> None:
    jsonl = b'{"id":1}\nNOT_JSON\n{"id":2}\n'
    c = _connector_with_files({"/d/n.jsonl": jsonl})

    batches = list(c.read_file("/d/n.jsonl", "jsonl", batch_size=100))

    rows = [r for b in batches for r in b.rows]
    assert rows == [(1,), (2,)]


def test_read_file_unsupported_format_raises() -> None:
    c = _connector_with_files({"/d/n.parquet": b"PAR1"})
    with pytest.raises(QueryFailedError, match="Unsupported file_format"):
        list(c.read_file("/d/n.parquet", "parquet"))


def test_read_file_invalid_batch_size() -> None:
    c = _connector_with_files({"/d/n.csv": b"a\n1\n"})
    with pytest.raises(ValueError, match="batch_size must be positive"):
        list(c.read_file("/d/n.csv", "csv", batch_size=0))


def test_read_file_open_error_mapped() -> None:
    c = _connector_with_files({})
    with pytest.raises(QueryFailedError, match="Cannot open"):
        list(c.read_file("/d/missing.csv", "csv"))
