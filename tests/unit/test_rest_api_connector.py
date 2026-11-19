"""Unit tests for the generic REST API connector.

httpx is mocked — no real HTTP, no internet. Covers:

- ``from_config`` parsing (bearer / basic auth, missing keys, bad
  endpoints JSON)
- Header construction (bearer / basic encoding)
- ``_navigate_record_path`` for nested response shapes
- ``describe_table`` end-to-end with a mocked httpx client
- Error mapping (auth-status codes, network errors, non-JSON, missing
  record path)
"""

from __future__ import annotations

import base64
import json
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
from lake_ingestion_framework.core.source_connectors.rest_api import (
    RestApiConnectionParams,
    RestApiConnector,
    RestEndpoint,
    _navigate_record_path,
    _parse_endpoints,
)


def _resolver_with(monkeypatch: pytest.MonkeyPatch, secret: str) -> LocalEnvSecretResolver:
    monkeypatch.setenv("LAKE_INGEST_SECRET_TEST_VAULT_API_TOKEN", secret)
    return LocalEnvSecretResolver()


_ENDPOINTS_JSON = json.dumps(
    [
        {"name": "customers", "path": "/v1/customers"},
        {"name": "orders", "path": "/v1/orders"},
    ]
)


# ──────────────────────────────────────────────────────────────────────
# from_config — parsing
# ──────────────────────────────────────────────────────────────────────


def test_from_config_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "tok-abc")
    connector = RestApiConnector.from_config(
        {
            "base_url": "https://api.example.com",
            "auth_method": "bearer",
            "connection_secret_name": "kv://test-vault/api-token",
            "endpoints": _ENDPOINTS_JSON,
        },
        resolver,
    )
    params = connector._params
    assert params.base_url == "https://api.example.com"
    assert params.auth_method == "bearer"
    assert params.auth_value == "tok-abc"
    assert [ep.name for ep in params.endpoints] == ["customers", "orders"]
    assert [ep.path for ep in params.endpoints] == ["/v1/customers", "/v1/orders"]
    assert params.default_record_path == ""
    assert params.connect_timeout_seconds == 30


def test_from_config_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "alice:s3cret")
    connector = RestApiConnector.from_config(
        {
            "base_url": "https://api.example.com/",  # trailing slash stripped
            "auth_method": "basic",
            "connection_secret_name": "kv://test-vault/api-token",
            "endpoints": _ENDPOINTS_JSON,
            "default_record_path": "data.items",
            "connect_timeout_seconds": "5",
            "user_agent": "test-agent/1.0",
        },
        resolver,
    )
    params = connector._params
    assert params.base_url == "https://api.example.com"
    assert params.auth_method == "basic"
    assert params.auth_value == "alice:s3cret"
    assert params.default_record_path == "data.items"
    assert params.connect_timeout_seconds == 5
    assert params.user_agent == "test-agent/1.0"


def test_from_config_missing_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "tok")
    with pytest.raises(QueryFailedError, match="base_url"):
        RestApiConnector.from_config(
            {
                "auth_method": "bearer",
                "connection_secret_name": "kv://test-vault/api-token",
                "endpoints": _ENDPOINTS_JSON,
            },
            resolver,
        )


def test_from_config_missing_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "tok")
    with pytest.raises(QueryFailedError, match="endpoints"):
        RestApiConnector.from_config(
            {
                "base_url": "https://api.example.com",
                "auth_method": "bearer",
                "connection_secret_name": "kv://test-vault/api-token",
            },
            resolver,
        )


def test_from_config_unknown_auth_method(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "tok")
    with pytest.raises(QueryFailedError, match="auth_method"):
        RestApiConnector.from_config(
            {
                "base_url": "https://api.example.com",
                "auth_method": "magic",
                "connection_secret_name": "kv://test-vault/api-token",
                "endpoints": _ENDPOINTS_JSON,
            },
            resolver,
        )


def test_from_config_basic_secret_without_colon(monkeypatch: pytest.MonkeyPatch) -> None:
    resolver = _resolver_with(monkeypatch, "no-colon-here")
    with pytest.raises(AuthenticationError, match="user:password"):
        RestApiConnector.from_config(
            {
                "base_url": "https://api.example.com",
                "auth_method": "basic",
                "connection_secret_name": "kv://test-vault/api-token",
                "endpoints": _ENDPOINTS_JSON,
            },
            resolver,
        )


# ──────────────────────────────────────────────────────────────────────
# _parse_endpoints
# ──────────────────────────────────────────────────────────────────────


def test_parse_endpoints_basic() -> None:
    endpoints = _parse_endpoints(_ENDPOINTS_JSON)
    assert endpoints == (
        RestEndpoint(name="customers", path="/v1/customers"),
        RestEndpoint(name="orders", path="/v1/orders"),
    )


def test_parse_endpoints_invalid_json() -> None:
    with pytest.raises(QueryFailedError, match="not valid JSON"):
        _parse_endpoints("not-json")


def test_parse_endpoints_not_a_list() -> None:
    with pytest.raises(QueryFailedError, match="non-empty JSON list"):
        _parse_endpoints('{"name": "x"}')


def test_parse_endpoints_empty_list() -> None:
    with pytest.raises(QueryFailedError, match="non-empty JSON list"):
        _parse_endpoints("[]")


def test_parse_endpoints_missing_name() -> None:
    with pytest.raises(QueryFailedError, match="missing 'name'"):
        _parse_endpoints('[{"path": "/x"}]')


def test_parse_endpoints_missing_path() -> None:
    with pytest.raises(QueryFailedError, match="missing 'path'"):
        _parse_endpoints('[{"name": "x"}]')


def test_parse_endpoints_duplicate_name() -> None:
    with pytest.raises(QueryFailedError, match="Duplicate"):
        _parse_endpoints('[{"name": "x", "path": "/a"}, {"name": "x", "path": "/b"}]')


# ──────────────────────────────────────────────────────────────────────
# _navigate_record_path
# ──────────────────────────────────────────────────────────────────────


def test_navigate_root_array() -> None:
    payload = [{"id": 1}, {"id": 2}]
    assert _navigate_record_path(payload, "") == payload


def test_navigate_single_segment() -> None:
    payload = {"data": [{"id": 1}, {"id": 2}]}
    assert _navigate_record_path(payload, "data") == [{"id": 1}, {"id": 2}]


def test_navigate_nested_segments() -> None:
    payload = {"response": {"items": [{"id": 1}]}}
    assert _navigate_record_path(payload, "response.items") == [{"id": 1}]


def test_navigate_missing_segment() -> None:
    payload = {"data": [{"id": 1}]}
    with pytest.raises(QueryFailedError, match="missing record path segment"):
        _navigate_record_path(payload, "items")


def test_navigate_non_list_result() -> None:
    payload = {"data": {"id": 1}}
    with pytest.raises(QueryFailedError, match="not a JSON list"):
        _navigate_record_path(payload, "data")


def test_navigate_skips_non_dict_items() -> None:
    payload = {"data": [{"id": 1}, "scalar", 42, {"id": 2}]}
    assert _navigate_record_path(payload, "data") == [{"id": 1}, {"id": 2}]


# ──────────────────────────────────────────────────────────────────────
# Header construction
# ──────────────────────────────────────────────────────────────────────


def _connector(*, auth_method: str = "bearer", auth_value: str = "tok") -> RestApiConnector:
    return RestApiConnector(
        RestApiConnectionParams(
            base_url="https://api.example.com",
            auth_method=auth_method,
            auth_value=auth_value,
            endpoints=(RestEndpoint(name="customers", path="/v1/customers"),),
            default_record_path="data",
        )
    )


def test_bearer_header() -> None:
    headers = _connector(auth_method="bearer", auth_value="tok-xyz")._build_headers()
    assert headers["Authorization"] == "Bearer tok-xyz"
    assert headers["Accept"] == "application/json"


def test_basic_header() -> None:
    headers = _connector(auth_method="basic", auth_value="alice:s3cret")._build_headers()
    expected = base64.b64encode(b"alice:s3cret").decode("ascii")
    assert headers["Authorization"] == f"Basic {expected}"


# ──────────────────────────────────────────────────────────────────────
# describe_table — mocked httpx Client
# ──────────────────────────────────────────────────────────────────────


class _FakeHttpError(Exception):
    """Stand-in for httpx.HTTPError."""


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        body: Any = None,
        text: str = "",
        reason_phrase: str = "OK",
        json_decode_error: bool = False,
    ) -> None:
        self.status_code = status_code
        self.reason_phrase = reason_phrase
        self.text = text
        self._body = body
        self._json_decode_error = json_decode_error

    def json(self) -> Any:
        if self._json_decode_error:
            raise json.JSONDecodeError("Expecting value", "doc", 0)
        return self._body


class _FakeClient:
    def __init__(
        self, *, response: _FakeResponse | None = None, raise_on_get: Exception | None = None
    ) -> None:
        self._response = response
        self._raise = raise_on_get
        self.closed = False
        self.calls: list[str] = []

    def get(self, path: str) -> _FakeResponse:
        self.calls.append(path)
        if self._raise is not None:
            raise self._raise
        assert self._response is not None
        return self._response

    def close(self) -> None:
        self.closed = True


class _FakeHttpxModule:
    HTTPError = _FakeHttpError

    def __init__(self, *, client: _FakeClient | None = None) -> None:
        self._client = client or _FakeClient(response=_FakeResponse(body=[]))

    def Client(self, **_kwargs: Any) -> _FakeClient:
        return self._client


def _patch_httpx(monkeypatch: pytest.MonkeyPatch, fake: _FakeHttpxModule) -> None:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.rest_api._lazy_import_httpx",
        lambda: fake,
    )


def test_describe_returns_inferred_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    body = {
        "data": [
            {"id": 1, "email": "a@x", "active": True},
            {"id": 2, "email": "b@x", "active": False},
        ]
    }
    fake_client = _FakeClient(response=_FakeResponse(body=body))
    fake_module = _FakeHttpxModule(client=fake_client)
    _patch_httpx(monkeypatch, fake_module)

    c = _connector()
    cols = c.describe_table("api", "customers")
    by_name = {col.name: col for col in cols}
    assert by_name["id"].data_type == "integer"
    assert by_name["email"].data_type == "string"
    assert by_name["active"].data_type == "boolean"
    assert fake_client.calls == ["/v1/customers"]


def test_describe_unknown_endpoint_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpxModule()
    _patch_httpx(monkeypatch, fake)
    c = _connector()
    assert c.describe_table("api", "no_such_endpoint") == []


def test_describe_auth_failure_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(
        response=_FakeResponse(status_code=401, reason_phrase="Unauthorized", text="bad token")
    )
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(AuthenticationError, match="rejected credentials"):
        _connector().describe_table("api", "customers")


def test_describe_auth_failure_403(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(
        response=_FakeResponse(status_code=403, reason_phrase="Forbidden", text="nope")
    )
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(AuthenticationError):
        _connector().describe_table("api", "customers")


def test_describe_500_mapped_to_query_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(
        response=_FakeResponse(status_code=500, reason_phrase="Internal Server Error", text="boom")
    )
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(QueryFailedError, match="500"):
        _connector().describe_table("api", "customers")


def test_describe_network_error_mapped_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(raise_on_get=_FakeHttpError("connection reset"))
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(SourceUnavailableError, match="Cannot reach"):
        _connector().describe_table("api", "customers")


def test_describe_non_json_response(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = _FakeClient(
        response=_FakeResponse(status_code=200, body=None, json_decode_error=True)
    )
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(QueryFailedError, match="non-JSON"):
        _connector().describe_table("api", "customers")


def test_describe_missing_record_path(monkeypatch: pytest.MonkeyPatch) -> None:
    body = {"different_key": []}
    fake_client = _FakeClient(response=_FakeResponse(body=body))
    _patch_httpx(monkeypatch, _FakeHttpxModule(client=fake_client))
    with pytest.raises(QueryFailedError, match="missing record path"):
        _connector().describe_table("api", "customers")


def test_close_idempotent() -> None:
    c = _connector()
    c.close()
    c.close()


def test_httpx_missing_raises_clean_error() -> None:
    import importlib.util

    if importlib.util.find_spec("httpx") is not None:
        pytest.skip("httpx is installed; this test only runs without the extra")
    with pytest.raises(SourceUnavailableError, match="httpx not installed"):
        _connector().connect()


# ──────────────────────────────────────────────────────────────────────
# list_schemas / list_tables
# ──────────────────────────────────────────────────────────────────────


def test_list_schemas_returns_single_virtual_schema() -> None:
    c = _connector()
    assert c.list_schemas() == ["api"]


def test_list_tables_returns_endpoints_with_extra_config() -> None:
    c = _connector()
    tables = c.list_tables("api")
    assert [t.name for t in tables] == ["customers"]
    assert tables[0].table_type == "ENDPOINT"
    assert tables[0].extra_config == {"rest_endpoint_path": "/v1/customers"}


def test_list_tables_unknown_schema_returns_empty() -> None:
    c = _connector()
    assert c.list_tables("not-the-virtual-schema") == []


# ──────────────────────────────────────────────────────────────────────
# read_endpoint (Phase 0a — RestIngestReader)
# ──────────────────────────────────────────────────────────────────────


class _ReadFakeResponse:
    """Stand-in for httpx.Response for read_endpoint tests."""

    def __init__(
        self,
        status_code: int,
        json_payload: Any = None,
        text: str = "",
        reason: str = "OK",
    ) -> None:
        self.status_code = status_code
        self._json = json_payload
        self.text = text
        self.reason_phrase = reason

    def json(self) -> Any:
        if isinstance(self._json, Exception):
            raise self._json
        return self._json


class _ReadFakeClient:
    """Captures GET calls; returns queued responses in order."""

    def __init__(self, responses: list[_ReadFakeResponse]) -> None:
        self._queue = list(responses)
        self.gets: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> _ReadFakeResponse:
        self.gets.append((path, dict(params or {})))
        if not self._queue:
            raise AssertionError("Test asked for more responses than configured.")
        return self._queue.pop(0)

    def close(self) -> None:
        pass


class _ReadFakeHttpxModule:
    HTTPError = Exception


def _read_connector(
    monkeypatch: pytest.MonkeyPatch, responses: list[_ReadFakeResponse]
) -> tuple[RestApiConnector, _ReadFakeClient]:
    fake_module = _ReadFakeHttpxModule()
    monkeypatch.setattr(
        "lake_ingestion_framework.core.source_connectors.rest_api._lazy_import_httpx",
        lambda: fake_module,
    )
    fake_client = _ReadFakeClient(responses)
    c = RestApiConnector(
        RestApiConnectionParams(
            base_url="https://api.example.com",
            auth_method="bearer",
            auth_value="token",
            endpoints=(RestEndpoint(name="orders", path="/v1/orders"),),
        )
    )
    c._client = fake_client
    return c, fake_client


def test_read_endpoint_no_pagination_yields_one_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    c, client = _read_connector(monkeypatch, [_ReadFakeResponse(200, payload)])

    batches = list(c.read_endpoint("/v1/orders", batch_size=100))

    assert client.gets == [("/v1/orders", {})]
    assert len(batches) == 1
    assert batches[0].columns == ("id", "name")
    assert batches[0].rows == [(1, "alice"), (2, "bob")]


def test_read_endpoint_record_path_navigates(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    c, _ = _read_connector(monkeypatch, [_ReadFakeResponse(200, payload)])

    batches = list(c.read_endpoint("/v1/x", record_path="data.items", batch_size=100))

    assert batches[0].rows == [(1,), (2,)]


def test_read_endpoint_offset_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """Offset pagination loops until a page returns fewer than ``limit``."""
    page1 = [{"id": i} for i in range(1, 4)]
    page2 = [{"id": i} for i in range(4, 7)]
    page3 = [{"id": 7}]  # < limit → stop
    c, client = _read_connector(
        monkeypatch,
        [
            _ReadFakeResponse(200, page1),
            _ReadFakeResponse(200, page2),
            _ReadFakeResponse(200, page3),
        ],
    )

    batches = list(
        c.read_endpoint(
            "/v1/orders",
            pagination={"type": "offset", "limit": 3},
            batch_size=100,
        )
    )

    rows = [r for b in batches for r in b.rows]
    assert rows == [(i,) for i in range(1, 8)]
    assert [params for _, params in client.gets] == [
        {"limit": 3, "offset": 0},
        {"limit": 3, "offset": 3},
        {"limit": 3, "offset": 6},
    ]


def test_read_endpoint_offset_stops_on_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty page also stops the loop."""
    page1 = [{"id": i} for i in range(1, 4)]
    page2: list[dict[str, Any]] = []
    c, client = _read_connector(
        monkeypatch, [_ReadFakeResponse(200, page1), _ReadFakeResponse(200, page2)]
    )

    batches = list(
        c.read_endpoint("/v1/orders", pagination={"type": "offset", "limit": 3}, batch_size=100)
    )

    assert [r for b in batches for r in b.rows] == [(1,), (2,), (3,)]
    assert len(client.gets) == 2


def test_read_endpoint_offset_custom_param_names(monkeypatch: pytest.MonkeyPatch) -> None:
    page1 = [{"id": 1}]
    c, client = _read_connector(monkeypatch, [_ReadFakeResponse(200, page1)])

    list(
        c.read_endpoint(
            "/v1/x",
            pagination={
                "type": "offset",
                "limit": 50,
                "limit_param": "page_size",
                "offset_param": "skip",
            },
        )
    )

    assert client.gets[0][1] == {"page_size": 50, "skip": 0}


def test_read_endpoint_batch_size_chunks_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    page = [{"id": i} for i in range(1, 11)]
    c, _ = _read_connector(monkeypatch, [_ReadFakeResponse(200, page)])

    batches = list(c.read_endpoint("/v1/x", batch_size=3))

    assert [len(b.rows) for b in batches] == [3, 3, 3, 1]


def test_read_endpoint_query_params_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    c, client = _read_connector(monkeypatch, [_ReadFakeResponse(200, [])])

    list(c.read_endpoint("/v1/x", query_params={"status": "active"}))

    assert client.gets[0][1] == {"status": "active"}


def test_read_endpoint_auth_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    c, _ = _read_connector(
        monkeypatch,
        [_ReadFakeResponse(401, text="Unauthorized", reason="Unauthorized")],
    )

    with pytest.raises(AuthenticationError, match="rejected credentials"):
        list(c.read_endpoint("/v1/x"))


def test_read_endpoint_http_error_mapped_to_query_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, _ = _read_connector(
        monkeypatch,
        [_ReadFakeResponse(500, text="server boom", reason="Internal Server Error")],
    )

    with pytest.raises(QueryFailedError, match="HTTP 500"):
        list(c.read_endpoint("/v1/x"))


def test_read_endpoint_non_json_response_mapped(monkeypatch: pytest.MonkeyPatch) -> None:
    c, _ = _read_connector(
        monkeypatch,
        [_ReadFakeResponse(200, json_payload=json.JSONDecodeError("nope", "doc", 0))],
    )

    with pytest.raises(QueryFailedError, match="non-JSON"):
        list(c.read_endpoint("/v1/x"))


def test_read_endpoint_unsupported_pagination_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, _ = _read_connector(monkeypatch, [_ReadFakeResponse(200, [{"id": 1}])])

    with pytest.raises(QueryFailedError, match="Unsupported pagination type"):
        list(c.read_endpoint("/v1/x", pagination={"type": "cursor"}))


def test_read_endpoint_invalid_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    c, _ = _read_connector(monkeypatch, [])

    with pytest.raises(ValueError, match="batch_size must be positive"):
        list(c.read_endpoint("/v1/x", batch_size=0))


def test_read_endpoint_handles_missing_keys_across_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Columns are union of keys ordered by first-appearance across the batch."""
    page = [{"id": 1, "email": "a@x.com"}, {"id": 2}, {"id": 3, "name": "n3"}]
    c, _ = _read_connector(monkeypatch, [_ReadFakeResponse(200, page)])

    batches = list(c.read_endpoint("/v1/x", batch_size=100))

    assert batches[0].columns == ("id", "email", "name")
    assert batches[0].rows == [
        (1, "a@x.com", None),
        (2, None, None),
        (3, None, "n3"),
    ]
