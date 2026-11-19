"""Slice 1's batch registry (dataplatform.ports.MetadataStore: batch_status(key), record_batch) on the W1a metadata
store, so slice 1's lander (dataplatform.lander) records into the same file log as W1b's ingestion, through the
store's procedures: batch_status(source, dataset, key) and log_file. Bound to one dataset and one dataset run, which
the caller starts and finishes. Slice 1's own Spark tables (spark_delta.SparkMeta) stay for the Databricks realm
until it moves to the new engine.
"""

from __future__ import annotations

from typing import Any

from dataplatform.meta.port import FileRecord, MetadataStore
from dataplatform.model import Delivery


class StoreBatchRegistry:
    def __init__(self, store: MetadataStore, source: str, dataset: str):
        self.store, self.source, self.dataset = store, source, dataset

    def dataset_config(self, source: str, dataset: str) -> dict[str, Any]:
        return self.store.dataset_view(source, dataset)

    def batch_status(self, batch_key: str) -> str | None:
        return self.store.batch_status(self.source, self.dataset, batch_key)

    def record_batch(self, batch_key: str, delivery: Delivery, status: str, run_id: str, reason: str = "") -> None:
        rows = delivery.declared_rows if status == "loaded" else 0
        self.store.log_file(
            FileRecord(
                run_id,
                self.source,
                self.dataset,
                batch_key,
                delivery.name,
                delivery.window,
                delivery.checksum,
                delivery.declared_rows,
                rows,
                status,
                reason,
            )
        )
