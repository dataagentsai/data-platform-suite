"""The system-records port (dataplatform.obs.system) in memory: rows added by tests or the fixture runner."""

from __future__ import annotations

from datetime import datetime

from dataplatform.obs.system import AccessRow, JobRunRow, LineageRow


class MemorySystemTables:
    def __init__(self) -> None:
        self.job_rows: list[JobRunRow] = []
        self.lineage_rows: list[LineageRow] = []
        self.access_rows: list[AccessRow] = []

    def jobs(self, since: datetime, until: datetime) -> list[JobRunRow]:
        return sorted((j for j in self.job_rows if since <= j.started_at < until),
                      key=lambda j: (j.started_at, j.job_run_id))

    def lineage(self, since: datetime, until: datetime) -> list[LineageRow]:
        return sorted((x for x in self.lineage_rows if since <= x.at < until),
                      key=lambda x: (x.at, x.source_table, x.target_table))

    def access(self, since: datetime, until: datetime) -> list[AccessRow]:
        return sorted((a for a in self.access_rows if since <= a.at < until),
                      key=lambda a: (a.at, a.user, a.object))
