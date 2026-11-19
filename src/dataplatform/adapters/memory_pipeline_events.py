"""The pipeline event-log port (dataplatform.obs.pipeline_events) in memory: rows added by tests."""

from __future__ import annotations

from datetime import datetime

from dataplatform.obs.pipeline_events import TableUpdateRow


class MemoryPipelineEvents:
    def __init__(self, rows: list[TableUpdateRow] | None = None) -> None:
        self.rows = list(rows or [])

    def table_updates(self, since: datetime, until: datetime) -> list[TableUpdateRow]:
        return sorted((r for r in self.rows if since <= r.started_at < until),
                      key=lambda r: (r.started_at, r.pipeline, r.table))
