"""The platform's own records, behind one port: job runs, lineage, and access (ADR-024: platform telemetry adopted).

Every engine keeps these somewhere (system tables, an audit log). The ops builder joins them to the run log by run id,
so each adapter resolves its product's job run to the platform's run id (the trace id the recorder put on the job as a
parameter or a tag, obs/recorder.py RUN_TAG). Rows here name no product (R1). Spend and billing are not read: they
belong to the separate cost product (ADR-024 rule 8), which attributes spend through the same job tags.

Push, not poll (ADR-024 rule 4): `DailyCopy` copies the telemetry once a business day from a watermark into the
compact rows the ops tables are built from; anything newer is read on demand (`DailyCopy.on_demand`), by a tool or a
screen that asks, never by a job on a short interval.

Adapters:
    adapters/memory_system.py       rows held in lists (tests and the fixture runner)
    adapters/databricks_system.py   the engine's system tables through a SQL runner (statement API or session)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class JobRunRow:
    job_run_id: str
    job_name: str
    run_id: str  # the platform's run id, "" when the job carried none
    started_at: datetime
    ended_at: datetime | None
    result: str  # succeeded | failed | cancelled | running
    tags: dict[str, str] = field(default_factory=dict)  # the job's tags: dp_domain, dp_data_product, dp_run_id


@dataclass(frozen=True)
class LineageRow:
    at: datetime
    source_table: str
    target_table: str
    run_id: str
    job_run_id: str


@dataclass(frozen=True)
class AccessRow:
    at: datetime
    user: str
    action: str  # read | write | ...
    object: str  # a table's full name


@dataclass(frozen=True)
class SystemSnapshot:
    jobs: list[JobRunRow] = field(default_factory=list)
    lineage: list[LineageRow] = field(default_factory=list)
    access: list[AccessRow] = field(default_factory=list)


class SystemTables(Protocol):
    def jobs(self, since: datetime, until: datetime) -> list[JobRunRow]: ...

    def lineage(self, since: datetime, until: datetime) -> list[LineageRow]: ...

    def access(self, since: datetime, until: datetime) -> list[AccessRow]: ...


def read_all(port: SystemTables, since: datetime, until: datetime) -> SystemSnapshot:
    return SystemSnapshot(port.jobs(since, until), port.lineage(since, until), port.access(since, until))


def _business_day(at: datetime, tz: str) -> str:
    return at.astimezone(ZoneInfo(tz)).date().isoformat()


class DailyCopy:
    """The telemetry, copied once per business day from a watermark (ADR-024 rule 4).

    `copy(now)` reads [watermark, now) and appends it, once per business day; a second call the same day reads
    nothing. The watermark is plain data, so a deployment keeps it in a table between days.
    """

    def __init__(self, port: SystemTables, watermark: datetime, tz: str = "UTC") -> None:
        self.port, self.watermark, self.tz = port, watermark, tz
        self.copied_on: str | None = None
        self.rows = SystemSnapshot()

    def copy(self, now: datetime) -> bool:
        day = _business_day(now, self.tz)
        if self.copied_on == day or now <= self.watermark:
            return False
        new = read_all(self.port, self.watermark, now)
        self.rows = SystemSnapshot([*self.rows.jobs, *new.jobs], [*self.rows.lineage, *new.lineage],
                                   [*self.rows.access, *new.access])
        self.watermark, self.copied_on = now, day
        return True

    def on_demand(self, now: datetime) -> SystemSnapshot:
        """The copy plus what is newer than the watermark, read now for the one who asked (costs a query)."""
        new = read_all(self.port, self.watermark, now) if now > self.watermark else SystemSnapshot()
        return SystemSnapshot([*self.rows.jobs, *new.jobs], [*self.rows.lineage, *new.lineage],
                              [*self.rows.access, *new.access])
