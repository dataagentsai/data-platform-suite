"""The four mock-source ports (goal G4, data-platform-catalog dpc/ports.yaml: source systems).

The world writes into source systems only through these shapes, so the same simulated company feeds any cloud:
a database on the laptop today, a managed one later; a local folder today, object storage later; a local log of
shards today, a managed stream later. Each is a separate adapter (M2) and each passes the same contract tests (M5).

Names here say what a system is (a database, a file drop, an interface, a stream), never which product it is (R1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

Row = dict[str, Any]


@dataclass(frozen=True)
class TableSpec:
    """A source table: its columns (name, logical type from the contract) and its primary key."""

    name: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]
    change_feed: bool = False  # inserts, updates and deletes are also written to <name>__changes
    full_copy: bool = False  # the source publishes a nightly copy into <name>__copy


@dataclass(frozen=True)
class SourceChange:
    """One change at a source database. `in_feed=False` is a change the feed silently skipped (the table still has it)."""

    op: str  # I | U | D
    row: Row
    change_seq: int
    committed_at: str  # ISO timestamp, UTC
    in_feed: bool = True


@dataclass(frozen=True)
class DropFile:
    name: str
    arrived_at: str  # ISO timestamp, UTC
    size: int
    checksum: str


@dataclass(frozen=True)
class StreamRecord:
    shard: str
    seq: int  # position in the shard, assigned by the stream on append
    arrived_at: str
    data: Row


class SourceDatabase(Protocol):
    """A database with a change feed (a change-capture source) and a nightly full copy for master tables."""

    def ensure(self, system: str, tables: list[TableSpec], reset: bool = False) -> None: ...

    def apply(self, system: str, table: TableSpec, changes: list[SourceChange]) -> None:
        """Apply changes to the live table (newest change sequence wins) and append the in-feed ones to the feed."""

    def publish_copy(self, system: str, table: TableSpec, extracted_at: str, rows: list[Row]) -> None: ...

    def rows(self, system: str, table: TableSpec) -> list[Row]:
        """The live table now, sorted by key."""

    def feed(self, system: str, table: TableSpec, after_pos: int = 0) -> list[Row]:
        """Feed rows in commit order after a feed position, each with feed_pos, op and committed_at beside the row's
        own columns. Readers keep a feed position, not a change sequence: an old change can commit late."""

    def copy(self, system: str, table: TableSpec, extracted_at: str) -> list[Row]: ...

    def copies(self, system: str, table: TableSpec) -> list[str]:
        """The extraction times of the full copies published so far, oldest first (a puller takes the newest)."""


class FileDrop(Protocol):
    """A folder partners and lab systems drop files into. Arrival time is part of the file (object storage: LastModified)."""

    def put(self, system: str, dataset: str, name: str, data: bytes, arrived_at: str) -> DropFile: ...

    def list(self, system: str, dataset: str) -> list[DropFile]:
        """Files in arrival order."""

    def get(self, system: str, dataset: str, name: str) -> bytes: ...


class RecordInterface(Protocol):
    """A request-response interface over a set of records, read in pages since a cursor (the dealer system)."""

    def upsert(self, system: str, resource: str, key: str, record: Row, updated_at: str) -> None: ...

    def page(self, system: str, resource: str, cursor: str, limit: int) -> tuple[list[Row], str]:
        """Records changed after `cursor` ('' = from the start), oldest first; returns them and the next cursor."""


class EventStream(Protocol):
    """An append-only stream split into shards by a partition key; readers keep their own position per shard."""

    def append(self, stream: str, records: list[tuple[str, str, Row]]) -> None:
        """(partition key, arrived_at, record) in arrival order."""

    def shards(self, stream: str) -> list[str]: ...

    def read(self, stream: str, shard: str, after_seq: int, limit: int) -> list[StreamRecord]: ...
