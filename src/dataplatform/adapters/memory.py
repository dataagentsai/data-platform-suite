"""In-memory adapters for tests. Same ports as the Spark/Delta ones."""
from __future__ import annotations

from typing import Any, Iterable

from dataplatform.model import CheckResult, Delivery, RunRecord


class MemoryMeta:
    def __init__(self, configs: dict[tuple[str, str], dict[str, Any]] | None = None):
        self.configs = configs or {}
        self.batches: dict[str, dict[str, Any]] = {}

    def dataset_config(self, source: str, dataset: str) -> dict[str, Any]:
        return self.configs[(source, dataset)]

    def batch_status(self, batch_key: str) -> str | None:
        return self.batches.get(batch_key, {}).get("status")

    def record_batch(self, batch_key: str, delivery: Delivery, status: str, run_id: str, reason: str = "") -> None:
        self.batches[batch_key] = {"status": status, "file": delivery.name, "run_id": run_id, "reason": reason}


class MemoryTables:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {}

    def append(self, table: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> None:
        names = [c for c, _ in columns]
        self.tables.setdefault(table, []).extend({c: r.get(c) for c in names} for r in rows)


class MemoryLog:
    def __init__(self) -> None:
        self.records: list[Any] = []

    def write(self, record: RunRecord | Iterable[CheckResult]) -> None:
        if isinstance(record, RunRecord):
            self.records.append(record)
        else:
            self.records.extend(record)
