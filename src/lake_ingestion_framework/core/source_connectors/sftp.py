"""SFTP source connector — read-only file enumeration + format inference.

Different shape from RDBMS connectors:

- No INFORMATION_SCHEMA. We list directories + files via the SFTP
  protocol.
- "Schema" = an immediate subdirectory of ``root_path`` (or
  ``root_path`` itself when no subdirs exist).
- "Table" = one file matching a known data-file extension.
- ``describe_table`` samples the first few KB of the file and infers
  columns from a CSV header / JSON Lines keys + values.

MVP supports two formats:

- **CSV** (extensions: ``.csv``, ``.tsv``) — auto-detect delimiter
  via ``csv.Sniffer``; header row + sample rows for type inference.
- **JSON Lines** (``.jsonl``, ``.ndjson``) — line-by-line JSON
  objects; columns = union of keys across sampled lines.

Parquet, Excel, fixed-width, gzip-compressed-CSV are deferred to
follow-up turns when a customer pulls.

Auth:

- ``auth_method=basic`` — secret is ``user:password``
  (same shape as Postgres / SQL Server connectors).
- ``auth_method=ssh_key`` — secret is the PEM private key string;
  ``user`` is read from the source_config. RSA + Ed25519 + ECDSA
  keys are tried in order. Encrypted keys (with passphrase) are
  not yet supported — strip the passphrase or use unencrypted keys
  for now.

Driver: ``paramiko>=3.0`` via ``pip install lake-ingest[sftp]``.
Imported lazily so the framework stays importable without the extra.

Per ``docs/discovery.md`` → "Discovery is read-only". This module
issues no writes / deletes / renames against the source.
"""

from __future__ import annotations

import csv
import io
import json
import posixpath
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
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

# Default file extensions we recognise as ingestible. Operators can override
# via the ``file_extensions`` source_config key (comma-separated).
_DEFAULT_FILE_EXTENSIONS = ("csv", "tsv", "jsonl", "ndjson")
_AUTH_METHODS = frozenset({"basic", "ssh_key"})

# Bytes to read from each file when sampling for format inference.
# 32 KB is enough to fit a header + ~100 rows in most real-world files.
_SAMPLE_BYTES = 32 * 1024
# Max sample rows to use for type inference per file.
_SAMPLE_ROWS = 50

# Type-inference regexes — tight, ASCII-only.
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+([eE][+-]?\d+)?$")
_BOOL_VALUES = frozenset({"true", "false", "yes", "no", "1", "0", "t", "f"})


@dataclass(frozen=True)
class SftpConnectionParams:
    """Resolved connection params for an SFTP source.

    Built from ``source_config`` after secret resolution. Held only
    for the lifetime of one ``SftpConnector`` — never persisted.
    """

    server: str
    port: int
    auth_method: str  # "basic" or "ssh_key"
    user: str
    password: str | None = None  # for basic auth
    private_key_pem: str | None = None  # for ssh_key auth
    root_path: str = "."
    file_extensions: tuple[str, ...] = field(default_factory=lambda: _DEFAULT_FILE_EXTENSIONS)
    connect_timeout_seconds: int = 30


class SftpConnector:
    """SFTP ``SourceConnector`` implementation."""

    def __init__(self, params: SftpConnectionParams) -> None:
        self._params = params
        # paramiko Transport + SFTPClient. Held as Any to keep the framework
        # importable without the [sftp] extra.
        self._transport: Any = None
        self._sftp: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> SftpConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys:
        - ``server``
        - ``connection_secret_name`` (kv://<vault>/<secret>)
        - ``auth_method`` (must be "basic" or "ssh_key")
        - ``user`` (required when ``auth_method=ssh_key``)

        Optional:
        - ``port`` (22)
        - ``root_path`` (".")
        - ``file_extensions`` (CSV-shaped list of extensions)
        - ``connect_timeout_seconds`` (30)
        """
        server = _required(config, "server")
        auth_method = _required(config, "auth_method").lower()
        if auth_method not in _AUTH_METHODS:
            raise QueryFailedError(
                query_hash="config",
                message=(
                    f"SFTP auth_method must be one of {sorted(_AUTH_METHODS)}; got {auth_method!r}."
                ),
            )

        secret_ref_raw = _required(config, "connection_secret_name")
        port = int(config.get("port", "22"))
        root_path = config.get("root_path", ".")
        timeout = int(config.get("connect_timeout_seconds", "30"))
        file_extensions = _parse_extensions(config.get("file_extensions"))

        ref = KvRef.parse(secret_ref_raw)
        secret = secret_resolver.resolve(ref)

        if auth_method == "basic":
            user, password = _split_user_password(secret)
            return cls(
                SftpConnectionParams(
                    server=server,
                    port=port,
                    auth_method="basic",
                    user=user,
                    password=password,
                    root_path=root_path,
                    file_extensions=file_extensions,
                    connect_timeout_seconds=timeout,
                )
            )

        # ssh_key
        user = _required(config, "user")
        return cls(
            SftpConnectionParams(
                server=server,
                port=port,
                auth_method="ssh_key",
                user=user,
                private_key_pem=secret,
                root_path=root_path,
                file_extensions=file_extensions,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._sftp is not None:
            return
        paramiko = _lazy_import_paramiko()
        try:
            transport = paramiko.Transport((self._params.server, self._params.port))
            transport.banner_timeout = self._params.connect_timeout_seconds
        except paramiko.SSHException as exc:
            raise SourceUnavailableError(
                f"Cannot reach SFTP at {self._params.server}:{self._params.port}: {exc}"
            ) from exc
        except OSError as exc:
            raise SourceUnavailableError(
                f"Cannot reach SFTP at {self._params.server}:{self._params.port}: {exc}"
            ) from exc

        try:
            self._authenticate(transport, paramiko)
        except paramiko.AuthenticationException as exc:
            transport.close()
            raise AuthenticationError(
                f"SFTP rejected credentials for {self._params.user}@{self._params.server}: {exc}"
            ) from exc
        except paramiko.SSHException as exc:
            transport.close()
            raise SourceUnavailableError(
                f"SFTP handshake failed for {self._params.server}:{self._params.port}: {exc}"
            ) from exc

        self._transport = transport
        self._sftp = paramiko.SFTPClient.from_transport(transport)
        if self._sftp is None:
            transport.close()
            raise SourceUnavailableError(f"SFTP channel open failed for {self._params.server}.")

    def close(self) -> None:
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def list_schemas(self) -> list[str]:
        """Return immediate subdirectories of ``root_path``.

        If ``root_path`` has no subdirectories (only files), return a
        single virtual schema named after the basename of ``root_path``.
        """
        sftp = self._sftp_or_connect()
        try:
            attrs = sftp.listdir_attr(self._params.root_path)
        except OSError as exc:
            raise QueryFailedError(
                query_hash="discover",
                message=f"Cannot list root_path {self._params.root_path!r}: {exc}",
            ) from exc

        subdirs = sorted(a.filename for a in attrs if _is_dir(a) and not a.filename.startswith("."))
        if subdirs:
            return subdirs
        # No subdirs: if there's at least one file, expose root as one schema
        files = [a for a in attrs if not _is_dir(a) and self._has_known_extension(a.filename)]
        if files:
            base = posixpath.basename(self._params.root_path.rstrip("/")) or "_root"
            return [base]
        return []

    def list_tables(self, schema: str) -> list[TableMeta]:
        sftp = self._sftp_or_connect()
        target = self._dir_for_schema(schema)
        try:
            attrs = sftp.listdir_attr(target)
        except OSError as exc:
            raise QueryFailedError(
                query_hash="discover",
                message=f"Cannot list directory {target!r}: {exc}",
            ) from exc

        tables: list[TableMeta] = []
        for a in attrs:
            if _is_dir(a):
                continue
            if a.filename.startswith("."):
                continue
            if not self._has_known_extension(a.filename):
                continue
            tables.append(
                TableMeta(
                    schema=schema,
                    name=a.filename,
                    table_type="FILE",
                    row_count_estimate=None,  # Would require full scan
                )
            )
        tables.sort(key=lambda t: t.name)
        return tables

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        """Sample the file and infer columns by format."""
        sftp = self._sftp_or_connect()
        remote_path = posixpath.join(self._dir_for_schema(schema), table)
        try:
            with sftp.open(remote_path, "rb") as fh:
                fh.prefetch()
                sample = fh.read(_SAMPLE_BYTES)
        except OSError as exc:
            raise QueryFailedError(
                query_hash="discover",
                message=f"Cannot read {remote_path!r}: {exc}",
            ) from exc

        if not sample:
            return []

        ext = _file_extension(table)
        if ext in {"csv", "tsv"}:
            return _infer_csv_columns(sample, default_delimiter="\t" if ext == "tsv" else ",")
        if ext in {"jsonl", "ndjson"}:
            return _infer_jsonl_columns(sample)

        # Unknown format — return empty list rather than guess. The
        # operator should set ``file_format`` manually in the proposed
        # YAML.
        return []

    # ── FileIngestReader protocol

    def read_file(
        self,
        remote_path: str,
        file_format: str,
        batch_size: int = 10_000,
        format_options: dict[str, Any] | None = None,
    ) -> Iterator[QueryBatch]:
        """Stream rows from a remote file in ``batch_size`` chunks.

        Supported formats: ``csv``, ``tsv``, ``jsonl``, ``ndjson``.

        ``format_options`` keys honoured (all optional):

        - CSV/TSV: ``delimiter`` (default per extension), ``encoding``
          (default ``utf-8``), ``has_header`` (default ``True``)
        - JSONL/NDJSON: ``encoding`` (default ``utf-8``)
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")

        sftp = self._sftp_or_connect()
        opts = format_options or {}
        fmt = file_format.lower()

        try:
            file_handle = sftp.open(remote_path, "rb")
            file_handle.prefetch()
        except OSError as exc:
            raise QueryFailedError(
                query_hash="read_file",
                message=f"Cannot open remote file {remote_path!r}: {exc}",
            ) from exc

        try:
            if fmt in {"csv", "tsv"}:
                yield from _stream_csv(
                    file_handle,
                    batch_size=batch_size,
                    default_delimiter="\t" if fmt == "tsv" else ",",
                    options=opts,
                )
            elif fmt in {"jsonl", "ndjson"}:
                yield from _stream_jsonl(file_handle, batch_size=batch_size, options=opts)
            else:
                raise QueryFailedError(
                    query_hash="read_file",
                    message=(
                        f"Unsupported file_format {file_format!r}. "
                        "Supported: csv, tsv, jsonl, ndjson."
                    ),
                )
        finally:
            file_handle.close()

    # ── Internal helpers

    def _authenticate(self, transport: Any, paramiko: Any) -> None:
        if self._params.auth_method == "basic":
            transport.connect(username=self._params.user, password=self._params.password)
            return

        # ssh_key
        pem = self._params.private_key_pem
        if pem is None:
            raise AuthenticationError("SSH key auth requires a PEM private key.")
        key = _load_private_key(pem, paramiko)
        transport.connect(username=self._params.user, pkey=key)

    def _sftp_or_connect(self) -> Any:
        if self._sftp is None:
            self.connect()
        if self._sftp is None:
            raise SourceUnavailableError(
                "SFTP connection not established after connect() — internal invariant violated."
            )
        return self._sftp

    def _dir_for_schema(self, schema: str) -> str:
        """Resolve a schema name back to a remote directory path.

        - If a subdirectory of ``root_path`` matches ``schema``, return it.
        - Else fall back to ``root_path`` itself (the "single virtual schema"
          case from ``list_schemas``).
        """
        candidate = posixpath.join(self._params.root_path, schema)
        sftp = self._sftp
        try:
            attr = sftp.stat(candidate)
            if _is_dir(attr):
                return candidate
        except OSError:
            pass
        return self._params.root_path

    def _has_known_extension(self, filename: str) -> bool:
        return _file_extension(filename) in self._params.file_extensions


# ──────────────────────────────────────────────────────────────────────
# Format inference (pure functions, easily unit-testable)
# ──────────────────────────────────────────────────────────────────────


def _infer_csv_columns(sample: bytes, *, default_delimiter: str = ",") -> list[ColumnInfo]:
    """Infer columns from a CSV sample.

    Algorithm:
    1. Decode sample as UTF-8 (replace errors).
    2. Sniff delimiter via ``csv.Sniffer`` — fall back to ``default_delimiter``.
    3. Read the first row as a header.
    4. Read the next up to ``_SAMPLE_ROWS`` data rows.
    5. Per column: collect string values; infer type from the set.
    """
    text = sample.decode("utf-8", errors="replace")
    if not text.strip():
        return []

    delimiter = _sniff_delimiter(text, default_delimiter)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    try:
        header = next(reader)
    except StopIteration:
        return []

    # Strip BOM and whitespace
    header = [h.lstrip("﻿").strip() for h in header]

    rows: list[list[str]] = []
    for i, row in enumerate(reader):
        if i >= _SAMPLE_ROWS:
            break
        rows.append(row)

    return [
        ColumnInfo(
            name=name if name else f"col_{idx + 1}",
            data_type=_infer_column_type(rows, idx),
            is_nullable=_column_has_nulls(rows, idx),
            ordinal_position=idx + 1,
        )
        for idx, name in enumerate(header)
    ]


def _infer_jsonl_columns(sample: bytes) -> list[ColumnInfo]:
    """Infer columns from a JSON Lines sample.

    - Decode + split by newline.
    - Parse each line as JSON object (skip non-object / malformed lines).
    - Delegate column inference to the shared helper used by REST too.
    """
    text = sample.decode("utf-8", errors="replace")
    if not text.strip():
        return []

    rows: list[dict[str, Any]] = []
    for raw_line in text.splitlines()[:_SAMPLE_ROWS]:
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # Tolerate the last line being a truncated record from our
            # 32 KB sample. Skip silently.
            continue
        if isinstance(obj, dict):
            rows.append(obj)

    return infer_columns_from_records(rows)


def _infer_column_type(rows: list[list[str]], col_idx: int) -> str:
    """CSV column type inference. Conservative: defaults to ``string``."""
    values = [
        row[col_idx].strip() for row in rows if col_idx < len(row) and row[col_idx].strip() != ""
    ]
    if not values:
        return "string"
    if all(_BOOL_VALUES.__contains__(v.lower()) for v in values):
        return "boolean"
    if all(_INT_RE.match(v) for v in values):
        return "integer"
    if all(_INT_RE.match(v) or _FLOAT_RE.match(v) for v in values):
        return "numeric"
    return "string"


def _column_has_nulls(rows: list[list[str]], col_idx: int) -> bool:
    """A column is nullable if any sampled row has an empty value or is missing the column."""
    for row in rows:
        if col_idx >= len(row):
            return True
        if row[col_idx].strip() == "":
            return True
    return False


def _sniff_delimiter(text: str, default: str) -> str:
    """Guess the CSV delimiter from a sample. Fall back to ``default``."""
    sniff_sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=",\t|;")
        return dialect.delimiter
    except csv.Error:
        return default


def _stream_csv(
    file_handle: Any,
    *,
    batch_size: int,
    default_delimiter: str,
    options: dict[str, Any],
) -> Iterator[QueryBatch]:
    """Stream rows from a CSV/TSV file in batches.

    Wraps the binary remote file in a TextIOWrapper so ``csv.reader`` can
    consume it lazily — never loads the full file into memory.
    """
    encoding = str(options.get("encoding", "utf-8"))
    delimiter = str(options.get("delimiter") or default_delimiter)
    has_header = bool(options.get("has_header", True))

    text_stream = io.TextIOWrapper(file_handle, encoding=encoding, errors="replace", newline="")
    reader = csv.reader(text_stream, delimiter=delimiter)

    columns: tuple[str, ...] | None = None
    if has_header:
        try:
            header = next(reader)
        except StopIteration:
            return
        columns = tuple(h.lstrip("﻿").strip() or f"col_{i + 1}" for i, h in enumerate(header))

    batch: list[tuple[Any, ...]] = []
    for raw_row in reader:
        if columns is None:
            # No header: synthesize column names from the first row's width
            columns = tuple(f"col_{i + 1}" for i in range(len(raw_row)))
        # Pad short rows / truncate long rows so all rows match column count
        if len(raw_row) < len(columns):
            normalized = raw_row + [""] * (len(columns) - len(raw_row))
        elif len(raw_row) > len(columns):
            normalized = raw_row[: len(columns)]
        else:
            normalized = raw_row
        batch.append(tuple(normalized))
        if len(batch) >= batch_size:
            yield QueryBatch(columns=columns, rows=batch)
            batch = []
    if batch and columns is not None:
        yield QueryBatch(columns=columns, rows=batch)


def _stream_jsonl(
    file_handle: Any,
    *,
    batch_size: int,
    options: dict[str, Any],
) -> Iterator[QueryBatch]:
    """Stream JSON Lines records.

    Columns are the union of keys across rows in the *first* batch —
    later batches use the same column list so downstream writers see a
    stable schema. Rows missing a column emit ``None`` in that position;
    extra keys in later rows are dropped (logged via the connector if
    needed in future).
    """
    encoding = str(options.get("encoding", "utf-8"))
    text_stream = io.TextIOWrapper(file_handle, encoding=encoding, errors="replace", newline="")

    columns: tuple[str, ...] | None = None
    batch_dicts: list[dict[str, Any]] = []
    for raw_line in text_stream:
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # Skip malformed lines silently — common in real-world JSONL
            continue
        if not isinstance(obj, dict):
            continue
        batch_dicts.append(obj)
        if len(batch_dicts) >= batch_size:
            if columns is None:
                columns = _columns_from_dicts(batch_dicts)
            yield QueryBatch(
                columns=columns,
                rows=[tuple(d.get(c) for c in columns) for d in batch_dicts],
            )
            batch_dicts = []
    if batch_dicts:
        if columns is None:
            columns = _columns_from_dicts(batch_dicts)
        yield QueryBatch(
            columns=columns,
            rows=[tuple(d.get(c) for c in columns) for d in batch_dicts],
        )


def _columns_from_dicts(records: list[dict[str, Any]]) -> tuple[str, ...]:
    """Stable column ordering: keys ordered by first-appearance across the batch."""
    seen: dict[str, None] = {}  # dict preserves insertion order
    for rec in records:
        for k in rec:
            if k not in seen:
                seen[k] = None
    return tuple(seen)


def _file_extension(filename: str) -> str:
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""


def _is_dir(attr: Any) -> bool:
    """Return True if a paramiko ``SFTPAttributes`` represents a directory.

    paramiko exposes the POSIX mode in ``st_mode``; ``S_IFDIR`` is 0o40000.
    """
    import stat

    mode = getattr(attr, "st_mode", None)
    return bool(mode and stat.S_ISDIR(mode))


def _parse_extensions(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return _DEFAULT_FILE_EXTENSIONS
    return tuple(ext.strip().lstrip(".").lower() for ext in raw.split(",") if ext.strip())


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"SFTP source config missing required key {key!r}.",
        )
    return value


def _split_user_password(secret: str) -> tuple[str, str]:
    if ":" not in secret:
        raise AuthenticationError("SFTP basic-auth secret must be in 'user:password' form.")
    user, password = secret.split(":", 1)
    if not user or not password:
        raise AuthenticationError("SFTP basic-auth secret has empty user or password.")
    return user, password


def _load_private_key(pem: str, paramiko: Any) -> Any:
    """Try RSA, Ed25519, ECDSA in order. Encrypted keys not supported."""
    last_error: Exception | None = None
    for key_class_name in ("RSAKey", "Ed25519Key", "ECDSAKey"):
        key_class = getattr(paramiko, key_class_name, None)
        if key_class is None:
            continue
        try:
            return key_class.from_private_key(io.StringIO(pem))
        except paramiko.SSHException as exc:
            last_error = exc
            continue
    raise AuthenticationError(
        f"SSH private key could not be parsed as RSA / Ed25519 / ECDSA "
        f"(or key is passphrase-encrypted, which isn't supported yet): {last_error}"
    )


def _lazy_import_paramiko() -> Any:
    try:
        import paramiko
    except ImportError as exc:
        raise SourceUnavailableError(
            "paramiko not installed. Install with: pip install lake-ingest[sftp]"
        ) from exc
    return paramiko


__all__ = [
    "SftpConnectionParams",
    "SftpConnector",
]
