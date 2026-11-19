"""Declarative pipelines report themselves (ADR-024 rule 5): the pipeline event log, behind one read-only port.

Where silver or gold runs as a declarative pipeline, the engine keeps an event log per pipeline update: which tables
it refreshed, how many rows each flow wrote, and whether it completed. Per-table rows and status come from there,
not from a second record in our run log. The ops builder turns each row into a dataset-run row with origin
`pipeline_events` (obs/ops.py), joined to our run by the run id the orchestrator passed to the update (RUN_TAG).

Adapters:
    adapters/memory_pipeline_events.py      rows held in a list (tests)
    adapters/databricks_pipeline_events.py  the pipeline event log through a SQL runner (a stub until silver or gold
                                            runs as a declarative pipeline; its contract row runs on a fake runner)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

STATUSES = ("succeeded", "failed", "skipped", "running")


@dataclass(frozen=True)
class TableUpdateRow:
    pipeline: str
    update_id: str
    table: str  # the table the flow wrote (catalog.schema.table)
    status: str  # one of STATUSES
    rows_written: int | None  # None when the engine did not report a count (a view, a skipped flow)
    started_at: datetime
    ended_at: datetime | None
    run_id: str  # the platform run id the update carried, "" when it carried none


class PipelineEvents(Protocol):
    def table_updates(self, since: datetime, until: datetime) -> list[TableUpdateRow]:
        """Every table a pipeline update refreshed that started in [since, until), oldest first."""
        ...
