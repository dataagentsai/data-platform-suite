"""``SourceConnector`` Protocol — what every source connector implements.

Implementations (PostgresConnector, AzureSqlConnector, ...) are
**read-only** — `discover` walks catalogs, samples rows, and proposes
metadata. No INSERT / UPDATE / DELETE on the source, ever.

Per ``docs/discovery.md`` → "Discovery is read-only".

Two sibling protocols are defined here:

- ``SourceConnector`` — catalog introspection (used by ``lake-ingest discover``)
- ``IngestReader``    — bulk row reads (used by the ADF Mimic Copy executor and
  any future local executor; see ``docs/adf_mimic_design.md`` §3.1)

A connector implementation is free to satisfy either or both. The two
protocols are kept separate because they have different lifecycles and
testing surfaces — a connector that only knows how to discover can ship
without learning to ingest, and vice versa.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class TableMeta:
    """Catalog entry for one table-shaped object in the source.

    ``table_type`` is "TABLE" / "VIEW" for RDBMS, "FILE" for SFTP-shaped
    sources, "ENDPOINT" for REST-API-shaped sources. The ``extra_config``
    dict carries connector-specific metadata that the discover use case
    merges into the proposed ``dataset_config`` row (e.g.
    ``rest_endpoint_path`` for REST sources).
    """

    schema: str
    name: str
    table_type: str
    # Rough estimate via pg_class.reltuples / sys.dm_db_partition_stats — not a tight count.
    row_count_estimate: int | None = None
    extra_config: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnInfo:
    """Column metadata for one column in a source table.

    The shape mirrors what we eventually write to
    ``meta.dataset_config_columns`` plus a few extras (max_length,
    numeric_precision) that drive flattening / type-mapping decisions
    downstream.
    """

    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    is_primary_key: bool = False
    max_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


class SourceConnector(Protocol):
    """Read-only connector to a source system.

    Lifecycle: ``connect()`` → use → ``close()``. Implementations should
    be safe to call ``close()`` multiple times. ``connect()`` is
    expected to be idempotent (no-op on already-connected).
    """

    def connect(self) -> None: ...

    def close(self) -> None: ...

    def list_schemas(self) -> list[str]:
        """Return user-visible schema names. Excludes catalog / system schemas."""

    def list_tables(self, schema: str) -> list[TableMeta]:
        """Return tables + views in ``schema``. Empty list if the schema is empty."""

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        """Return columns for ``schema.table`` ordered by ``ordinal_position``."""


@dataclass(frozen=True)
class QueryBatch:
    """One batch of rows from a source query.

    ``columns`` is the column list for the entire query result; the
    same tuple object is reused across batches in one iteration. Treat
    it as read-only. ``rows`` are tuples in column order — chosen over
    per-row dicts because dict construction adds non-trivial overhead
    on million-row pulls.
    """

    columns: tuple[str, ...]
    rows: list[tuple[Any, ...]]


class IngestReader(Protocol):
    """Connector role: bulk read of query results in batches.

    Lifecycle: caller opens the connection (``connect()``), then calls
    ``read_query`` which yields ``QueryBatch`` instances until exhausted,
    then closes the connection (``close()``). One ``read_query`` call
    consumes one cursor; subsequent calls open a fresh cursor.

    Implementations should use a server-side / streaming cursor where
    the driver supports it, so memory stays bounded by ``batch_size``.
    """

    def read_query(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Execute ``sql`` and yield batches.

        ``params`` are positional bind values (per DB-API ``%s`` /
        ``?`` style). Each yielded ``QueryBatch.rows`` has at most
        ``batch_size`` entries; the final batch may be shorter. If the
        query returns no rows, no batches are yielded (callers should
        treat the empty stream the same as an empty batch).
        """


class FileIngestReader(Protocol):
    """Connector role: bulk read of file contents in batches.

    Used by file-shaped sources (SFTP, S3-blob, local filesystem). The
    "spec" is a remote path plus a format hint, not a SQL query.
    Lifecycle mirrors ``IngestReader``: caller connects, calls
    ``read_file``, closes.

    Implementations should stream the file and parse per ``file_format``,
    yielding ``QueryBatch`` instances of at most ``batch_size`` rows.
    """

    def read_file(
        self,
        remote_path: str,
        file_format: str,
        batch_size: int = 10_000,
        format_options: dict[str, Any] | None = None,
    ) -> Iterator[QueryBatch]:
        """Stream rows from ``remote_path`` parsed as ``file_format``.

        Supported formats (v1): ``csv``, ``tsv``, ``jsonl``, ``ndjson``.
        ``format_options`` may carry per-format knobs (CSV delimiter,
        has-header, encoding) — implementations document which keys they
        honour.
        """


class RestIngestReader(Protocol):
    """Connector role: bulk read of records from a REST endpoint.

    Used by REST-shaped sources. The "spec" is an endpoint path plus
    optional pagination/query-param config, not a SQL query.

    Pagination shapes supported in v1:

    - ``{"type": "none"}`` — single GET returns all records (default)
    - ``{"type": "offset", "limit": 100, "limit_param": "limit",
       "offset_param": "offset"}`` — increment ``offset`` by ``limit``
       until a page returns < ``limit`` records.

    More pagination shapes (cursor, link-header, page-number) are added
    when a customer pulls.
    """

    def read_endpoint(
        self,
        endpoint_path: str,
        record_path: str = "",
        query_params: dict[str, Any] | None = None,
        pagination: dict[str, Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Stream records from ``endpoint_path``.

        ``record_path`` walks dotted keys to find the records array in
        the JSON response (empty = response is the array directly).
        ``batch_size`` is the maximum rows yielded per ``QueryBatch``;
        successive batches may come from the same page or span pages.
        """


__all__ = [
    "ColumnInfo",
    "FileIngestReader",
    "IngestReader",
    "QueryBatch",
    "RestIngestReader",
    "SourceConnector",
    "TableMeta",
]
