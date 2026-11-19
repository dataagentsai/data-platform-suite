"""The ports ingestion talks to (data-platform-catalog dpc/ports.yaml: landing_store, table_writer, tokenizer).

Columns are (name, contract logical type): string, integer, number, timestamp, date, boolean. Each adapter maps the
logical types to its own table types; the kernel never names a table format (R1).
"""

from __future__ import annotations

import re
from typing import Any, Protocol

from dataplatform.model import Delivery

Row = dict[str, Any]
Columns = list[tuple[str, str]]
SAFE_BATCH_KEY = re.compile(r"^[A-Za-z0-9_:.-]{1,128}$")  # batch keys reach a write predicate: every writer checks


def check_batch_key(key: str) -> str:
    if not SAFE_BATCH_KEY.match(key):
        raise ValueError(f"not a safe batch key: {key!r}")
    return key


class DeliveryExists(RuntimeError):
    """Landing refuses a second write of the same delivery: a delivery is immutable once its manifest exists
    (landing_store invariant, DUP-02). The mover treats it as already landed."""


class Landing(Protocol):
    """landing_store for push deliveries: the bytes exactly as delivered, with a manifest beside them."""

    def put(
        self,
        source: str,
        dataset: str,
        window: str,
        name: str,
        data: bytes,
        rows: int,
        extra: dict[str, Any] | None = None,
    ) -> Delivery:
        """Store a delivery and then its manifest. Raises DeliveryExists if (window, name) was landed before."""

    def deliveries(self, source: str, dataset: str) -> list[Delivery]:
        """Deliveries that have a manifest, in window then name order."""

    def read_bytes(self, delivery: Delivery) -> bytes: ...

    def manifest(self, delivery: Delivery) -> dict[str, Any]:
        """The manifest the mover wrote, including any extra fields (feed positions, arrival time)."""


class BronzeWriter(Protocol):
    """table_writer for bronze: append-only as delivered, idempotent by batch key."""

    def write(self, table: str, batch_key: str, rows: list[Row], columns: Columns, replace: bool = False) -> int:
        """Write one batch in one transaction, replacing whatever the table holds for this batch key: the same batch
        written twice leaves the table unchanged (DUP-01, DPC-0024) and a failed write leaves no trace (ORCH-03).
        `replace` is the lander saying "this batch may be in the table already" (a reload): a writer that appends new
        batches cheaply must then replace. Returns the rows written."""

    def write_many(self, table: str, batches: list[tuple[str, list[Row]]], columns: Columns) -> int:
        """Write several new batches of one table in one transaction (one commit instead of one per batch); all or
        nothing. The lander logs them as loaded only after this returns. Returns the rows written."""

    def fingerprint(
        self, table: str, where: dict[str, str] | None = None, sums: tuple[str, ...] = ()
    ) -> dict[str, float]:
        """{"row_count": n, <column>: total, ...} for the table, or for the rows where every column equals its value.
        A table never written has row_count 0."""

    def keys(self, table: str, columns: tuple[str, ...], where: dict[str, str]) -> set[tuple[Any, ...]]:
        """The distinct values of `columns` in the rows matching `where` (the mass-delete guard's comparison)."""


class CurrentKeys(Protocol):
    """Silver's current keys, for the mass-delete guard (DPC-0054: compare a full copy with silver's current keys)."""

    def current_keys(self, source: str, dataset: str, keys: tuple[str, ...]) -> set[tuple[Any, ...]] | None:
        """The keys of the current, not deleted silver rows, or None when silver was never written."""


class Tokenizer(Protocol):
    """tokenizer: a personal value in, a token out, before anything reaches bronze (DPC-0070)."""

    def token(self, column_class: str, value: str) -> str: ...
