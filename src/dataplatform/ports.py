"""The ports slice 1 uses (data-platform-catalog dpc/ports.yaml). The kernel imports only these shapes.

An adapter is any object with these methods: a local folder, a Databricks volume, a Delta table, a list in a test.
Swapping a product means a new adapter and one line in the binding, never a change here.
"""
from __future__ import annotations

from typing import Any, Iterable, Protocol

from dataplatform.model import CheckResult, Delivery, RunRecord


class LandingStore(Protocol):
    """landing_store: files exactly as delivered, kept so a bad read can be redone."""

    def deliveries(self, source: str, dataset: str) -> list[Delivery]: ...

    def read(self, delivery: Delivery) -> list[dict[str, Any]]: ...

    def write(self, source: str, dataset: str, window: str, name: str, rows: list[dict[str, Any]]) -> Delivery: ...


class MetadataStore(Protocol):
    """metadata_service: config is read from here at run time, never from YAML (agreed 27 Oct)."""

    def dataset_config(self, source: str, dataset: str) -> dict[str, Any]: ...

    def batch_status(self, batch_key: str) -> str | None: ...

    def record_batch(self, batch_key: str, delivery: Delivery, status: str, run_id: str, reason: str = "") -> None: ...


class TableWriter(Protocol):
    """table_writer: append rows to a bronze table. Silver is written by the pipeline product, behind the same port."""

    def append(self, table: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> None: ...


class RunLog(Protocol):
    """run_log: one row per step per run, with the config fingerprint (DPC-0020, CFG-02)."""

    def write(self, record: RunRecord) -> None: ...


class CheckSink(Protocol):
    """Where check verdicts go. Keyed by run id, so the evidence view can join everything for one load."""

    def write(self, results: Iterable[CheckResult]) -> None: ...
