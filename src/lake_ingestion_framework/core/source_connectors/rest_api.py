"""Generic REST API source connector — read-only sample-based discover.

Different shape from RDBMS / file connectors: REST APIs have no
catalog the connector can walk. The operator declares the list of
endpoints in ``source_config.endpoints`` (a JSON list); ``discover``
calls each endpoint once, samples the response, and infers columns.

For a REST source, the natural mapping into the ``SourceConnector``
protocol is:

- ``list_schemas()`` → returns one virtual schema (``"api"``).
- ``list_tables(schema)`` → returns one ``TableMeta`` per declared
  endpoint, with ``table_type="ENDPOINT"`` and the URL path stashed
  in ``extra_config["rest_endpoint_path"]``.
- ``describe_table(schema, name)`` → fetches the endpoint's first
  page, navigates to ``record_path``, returns inferred columns.

**Pagination is intentionally out of scope here.** Discover-time
sampling only fetches one page; pagination is a runtime concern that
the realm's ingest engine (ADF Copy / Glue / Spark) handles via
``pagination_*`` keys in dataset_config.

Auth methods (Phase 4 MVP):

- ``bearer`` — ``Authorization: Bearer <token>`` header. Secret = the
  raw token (no ``user:`` prefix).
- ``basic`` — ``Authorization: Basic <base64(user:password)>`` header.
  Secret = ``user:password`` (same shape as RDBMS connectors).

API-key (header / query) and OAuth2 client-credentials are deferred
to follow-up turns when a customer pulls.

Driver: ``httpx>=0.27`` via ``pip install lake-ingest[rest_api]``.
Imported lazily so the framework stays importable without the extra.

Per ``docs/discovery.md`` → "Discovery is read-only". This module
issues ``GET`` requests only.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from lake_ingestion_framework.core.exceptions import (
    AuthenticationError,
    QueryFailedError,
    SourceUnavailableError,
)
from lake_ingestion_framework.core.source_connectors._jsonl_inference import (
    infer_columns_from_records,
)
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    SecretResolver,
)
from lake_ingestion_framework.core.source_connectors.protocol import (
    ColumnInfo,
    QueryBatch,
    TableMeta,
)
from lake_ingestion_framework.core.types.value_objects import KvRef

_AUTH_METHODS = frozenset({"bearer", "basic"})

# Number of records to feed into column inference.
_SAMPLE_RECORDS = 50

# HTTP status codes treated as auth failures.
_HTTP_AUTH_STATUSES = frozenset({401, 403})

# HTTP error-status threshold (anything ≥ 400 except auth statuses → QueryFailedError).
_HTTP_ERROR_STATUS_FLOOR = 400


@dataclass(frozen=True)
class RestEndpoint:
    """One declared endpoint under a REST source."""

    name: str
    path: str


@dataclass(frozen=True)
class RestApiConnectionParams:
    """Resolved connection params for a REST API source.

    Built from ``source_config`` after secret resolution. Held only
    for the lifetime of one ``RestApiConnector`` — never persisted.
    """

    base_url: str
    auth_method: str  # "bearer" | "basic"
    auth_value: str  # bearer token, or "user:password" for basic
    endpoints: tuple[RestEndpoint, ...]
    default_record_path: str = ""
    user_agent: str = "lake-ingest-discover/0.1"
    connect_timeout_seconds: int = 30


class RestApiConnector:
    """Generic REST API ``SourceConnector`` implementation."""

    SCHEMA_NAME = "api"

    def __init__(self, params: RestApiConnectionParams) -> None:
        self._params = params
        # httpx Client. Held as Any so the framework imports without the
        # [rest_api] extra installed.
        self._client: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> RestApiConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys:
        - ``base_url`` — e.g. ``https://api.example.com``
        - ``auth_method`` — ``bearer`` or ``basic``
        - ``connection_secret_name`` — kv://<vault>/<secret>
        - ``endpoints`` — JSON list of ``{"name", "path"}`` objects

        Optional:
        - ``default_record_path`` (e.g. ``data``, ``items``)
        - ``connect_timeout_seconds`` (30)
        - ``user_agent``
        """
        base_url = _required(config, "base_url").rstrip("/")
        auth_method = _required(config, "auth_method").lower()
        if auth_method not in _AUTH_METHODS:
            raise QueryFailedError(
                query_hash="config",
                message=(
                    f"REST auth_method must be one of {sorted(_AUTH_METHODS)}; got {auth_method!r}."
                ),
            )

        secret_ref_raw = _required(config, "connection_secret_name")
        endpoints_raw = _required(config, "endpoints")
        default_record_path = config.get("default_record_path", "")
        timeout = int(config.get("connect_timeout_seconds", "30"))
        user_agent = config.get("user_agent", "lake-ingest-discover/0.1")

        ref = KvRef.parse(secret_ref_raw)
        auth_value = secret_resolver.resolve(ref)

        if auth_method == "basic" and ":" not in auth_value:
            raise AuthenticationError("REST basic-auth secret must be in 'user:password' form.")

        endpoints = _parse_endpoints(endpoints_raw)

        return cls(
            RestApiConnectionParams(
                base_url=base_url,
                auth_method=auth_method,
                auth_value=auth_value,
                endpoints=endpoints,
                default_record_path=default_record_path,
                user_agent=user_agent,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._client is not None:
            return
        httpx = _lazy_import_httpx()
        try:
            self._client = httpx.Client(
                base_url=self._params.base_url,
                timeout=self._params.connect_timeout_seconds,
                headers=self._build_headers(),
                follow_redirects=True,
            )
        except httpx.HTTPError as exc:
            raise SourceUnavailableError(
                f"Cannot create HTTP client for {self._params.base_url}: {exc}"
            ) from exc

    def close(self) -> None:
        if self._client is None:
            return
        self._client.close()
        self._client = None

    def list_schemas(self) -> list[str]:
        # REST APIs don't have a discoverable schema namespace; the
        # connector exposes a single virtual schema for protocol parity
        # with RDBMS connectors.
        return [self.SCHEMA_NAME]

    def list_tables(self, schema: str) -> list[TableMeta]:
        if schema != self.SCHEMA_NAME:
            return []
        return [
            TableMeta(
                schema=self.SCHEMA_NAME,
                name=ep.name,
                table_type="ENDPOINT",
                row_count_estimate=None,
                # Plumbed through DatasetProposal.extra_config → dataset_config
                extra_config={"rest_endpoint_path": ep.path},
            )
            for ep in self._params.endpoints
        ]

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        ep = self._endpoint_for(table)
        if ep is None:
            return []

        records = self._fetch_records(ep)
        return infer_columns_from_records(records[:_SAMPLE_RECORDS])

    # ── RestIngestReader protocol

    def read_endpoint(
        self,
        endpoint_path: str,
        record_path: str = "",
        query_params: dict[str, Any] | None = None,
        pagination: dict[str, Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Stream records from a REST endpoint.

        Pagination shapes (v1):
        - ``{"type": "none"}`` (default) — single GET
        - ``{"type": "offset", "limit": <int>, "limit_param": <str>,
           "offset_param": <str>}`` — increment offset by limit until a
           page returns < ``limit`` records.

        Records are flattened to ``QueryBatch`` rows with columns from
        the first batch (later batches reuse the same column tuple;
        missing keys become ``None``, extra keys are dropped).
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")

        if self._client is None:
            self.connect()
        if self._client is None:
            raise SourceUnavailableError(
                "REST client not initialised after connect() — internal invariant violated."
            )

        pagination_cfg = pagination or {"type": "none"}
        pagination_type = str(pagination_cfg.get("type", "none")).lower()
        base_params = dict(query_params or {})

        columns: tuple[str, ...] | None = None
        buffer: list[dict[str, Any]] = []

        for page_records in self._iter_pages(
            endpoint_path, record_path, base_params, pagination_type, pagination_cfg
        ):
            buffer.extend(page_records)
            while len(buffer) >= batch_size:
                chunk = buffer[:batch_size]
                buffer = buffer[batch_size:]
                if columns is None:
                    columns = _columns_from_records(chunk)
                yield QueryBatch(
                    columns=columns,
                    rows=[tuple(d.get(c) for c in columns) for d in chunk],
                )
        if buffer:
            if columns is None:
                columns = _columns_from_records(buffer)
            yield QueryBatch(
                columns=columns,
                rows=[tuple(d.get(c) for c in columns) for d in buffer],
            )

    def _iter_pages(
        self,
        endpoint_path: str,
        record_path: str,
        base_params: dict[str, Any],
        pagination_type: str,
        pagination_cfg: dict[str, Any],
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield page-record-lists per the configured pagination type."""
        if pagination_type == "none":
            yield self._fetch_page(endpoint_path, base_params, record_path)
            return

        if pagination_type == "offset":
            limit = int(pagination_cfg.get("limit", 100))
            limit_param = str(pagination_cfg.get("limit_param", "limit"))
            offset_param = str(pagination_cfg.get("offset_param", "offset"))
            offset = int(pagination_cfg.get("start_offset", 0))
            while True:
                params = {**base_params, limit_param: limit, offset_param: offset}
                records = self._fetch_page(endpoint_path, params, record_path)
                if not records:
                    return
                yield records
                if len(records) < limit:
                    return
                offset += limit

        raise QueryFailedError(
            query_hash="read_endpoint",
            message=(
                f"Unsupported pagination type {pagination_type!r}. "
                "Supported in v1: none, offset."
            ),
        )

    def _fetch_page(
        self,
        endpoint_path: str,
        query_params: dict[str, Any],
        record_path: str,
    ) -> list[dict[str, Any]]:
        """Fetch a single page and navigate to the records list."""
        if self._client is None:
            raise SourceUnavailableError("REST client not initialised.")
        httpx = _lazy_import_httpx()
        try:
            response = self._client.get(endpoint_path, params=query_params)
        except httpx.HTTPError as exc:
            raise SourceUnavailableError(
                f"Cannot reach REST endpoint {endpoint_path!r}: {exc}"
            ) from exc

        if response.status_code in _HTTP_AUTH_STATUSES:
            raise AuthenticationError(
                f"REST API rejected credentials at {endpoint_path!r}: "
                f"HTTP {response.status_code} {response.reason_phrase}"
            )
        if response.status_code >= _HTTP_ERROR_STATUS_FLOOR:
            raise QueryFailedError(
                query_hash="read_endpoint",
                message=(
                    f"REST endpoint {endpoint_path!r} returned HTTP "
                    f"{response.status_code} {response.reason_phrase}: "
                    f"{response.text[:512]}"
                ),
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise QueryFailedError(
                query_hash="read_endpoint",
                message=f"REST endpoint {endpoint_path!r} returned non-JSON: {exc}",
            ) from exc
        return _navigate_record_path(payload, record_path)

    # ── Internal helpers

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": self._params.user_agent,
        }
        if self._params.auth_method == "bearer":
            headers["Authorization"] = f"Bearer {self._params.auth_value}"
        elif self._params.auth_method == "basic":
            import base64

            encoded = base64.b64encode(self._params.auth_value.encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {encoded}"
        return headers

    def _endpoint_for(self, name: str) -> RestEndpoint | None:
        for ep in self._params.endpoints:
            if ep.name == name:
                return ep
        return None

    def _fetch_records(self, endpoint: RestEndpoint) -> list[dict[str, Any]]:
        """Fetch one page from the endpoint and return the records array."""
        if self._client is None:
            self.connect()
        if self._client is None:
            raise SourceUnavailableError(
                "REST client not initialised after connect() — internal invariant violated."
            )
        httpx = _lazy_import_httpx()
        try:
            response = self._client.get(endpoint.path)
        except httpx.HTTPError as exc:
            raise SourceUnavailableError(
                f"Cannot reach REST endpoint {endpoint.path!r}: {exc}"
            ) from exc

        if response.status_code in _HTTP_AUTH_STATUSES:
            raise AuthenticationError(
                f"REST API rejected credentials at {endpoint.path!r}: "
                f"HTTP {response.status_code} {response.reason_phrase}"
            )
        if response.status_code >= _HTTP_ERROR_STATUS_FLOOR:
            raise QueryFailedError(
                query_hash="discover",
                message=(
                    f"REST endpoint {endpoint.path!r} returned HTTP "
                    f"{response.status_code} {response.reason_phrase}: "
                    f"{response.text[:512]}"
                ),
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise QueryFailedError(
                query_hash="discover",
                message=f"REST endpoint {endpoint.path!r} returned non-JSON: {exc}",
            ) from exc

        return _navigate_record_path(payload, self._params.default_record_path)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"REST API source config missing required key {key!r}.",
        )
    return value


def _parse_endpoints(raw: str) -> tuple[RestEndpoint, ...]:
    """Parse the ``endpoints`` JSON-encoded source_config value.

    Expected shape: ``[{"name": "<dataset_name>", "path": "/v1/x"}, ...]``.
    Empty list rejected; duplicate names rejected.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise QueryFailedError(
            query_hash="config",
            message=f"REST source config 'endpoints' is not valid JSON: {exc}",
        ) from exc

    if not isinstance(data, list) or not data:
        raise QueryFailedError(
            query_hash="config",
            message="REST source config 'endpoints' must be a non-empty JSON list.",
        )

    seen_names: set[str] = set()
    endpoints: list[RestEndpoint] = []
    for entry in data:
        if not isinstance(entry, dict):
            raise QueryFailedError(
                query_hash="config",
                message=f"REST endpoint entry must be an object; got {type(entry).__name__}.",
            )
        name = entry.get("name")
        path = entry.get("path")
        if not isinstance(name, str) or not name:
            raise QueryFailedError(
                query_hash="config",
                message="REST endpoint entry missing 'name' (non-empty string).",
            )
        if not isinstance(path, str) or not path:
            raise QueryFailedError(
                query_hash="config",
                message=f"REST endpoint {name!r} missing 'path' (non-empty string).",
            )
        if name in seen_names:
            raise QueryFailedError(
                query_hash="config",
                message=f"Duplicate REST endpoint name: {name!r}.",
            )
        seen_names.add(name)
        endpoints.append(RestEndpoint(name=name, path=path))
    return tuple(endpoints)


def _navigate_record_path(payload: Any, record_path: str) -> list[dict[str, Any]]:
    """Walk a dotted ``record_path`` to find the records array.

    Empty path = the payload itself must be a list. Otherwise
    descend through dotted keys. Non-list result raises.
    """
    cursor: Any = payload
    if record_path:
        for segment in record_path.split("."):
            if not isinstance(cursor, dict) or segment not in cursor:
                raise QueryFailedError(
                    query_hash="discover",
                    message=(
                        f"REST response missing record path segment {segment!r} "
                        f"(full path: {record_path!r})."
                    ),
                )
            cursor = cursor[segment]

    if not isinstance(cursor, list):
        raise QueryFailedError(
            query_hash="discover",
            message=(
                f"REST response at record_path={record_path!r} is not a JSON list "
                f"(got {type(cursor).__name__})."
            ),
        )

    # Skip non-object entries (numbers, strings, nulls); tolerate them
    # the same way SFTP's JSON Lines inference does.
    return [item for item in cursor if isinstance(item, dict)]


def _columns_from_records(records: list[dict[str, Any]]) -> tuple[str, ...]:
    """Stable column ordering: keys ordered by first-appearance across records."""
    seen: dict[str, None] = {}
    for rec in records:
        for k in rec:
            if k not in seen:
                seen[k] = None
    return tuple(seen)


def _lazy_import_httpx() -> Any:
    """Import httpx on first use; surface a clean error if missing."""
    try:
        import httpx
    except ImportError as exc:
        raise SourceUnavailableError(
            "httpx not installed. Install with: pip install lake-ingest[rest_api]"
        ) from exc
    return httpx


__all__ = [
    "RestApiConnectionParams",
    "RestApiConnector",
    "RestEndpoint",
]
