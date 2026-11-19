"""The run log, read back for operations (DPC-0057): every run in a time window with its timestamps.

W1a's store port answers one run at a time (run_report). Operations needs the window: which runs started between two
instants, with start and end times, so it can count, time and join them. This read-only port says that, and nothing
else; it never writes. Rows are plain frozen records keyed by run id (= the trace id, see obs/trace.py).

Adapters:
    adapters/memory_runlog.py     StampingRunLog: wraps any metadata store, stamps each write with the clock port's
                                  time, and answers this port (tests, the fixture runner, simulated days)
    adapters/postgres_runlog.py   one read-only procedure over the store's run tables (sql/postgres_obs.sql)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class RunRow:
    run_id: str
    domain: str
    env: str
    trigger: str
    run_type: str
    status: str
    started_at: datetime
    ended_at: datetime | None


@dataclass(frozen=True)
class DatasetRunRow:
    run_id: str
    source: str
    dataset: str
    status: str
    config_fingerprint: str
    watermark_before: str | None
    watermark_after: str | None
    counts: dict[str, int]
    message: str
    started_at: datetime
    ended_at: datetime | None


@dataclass(frozen=True)
class StepRow:
    run_id: str
    source: str
    dataset: str
    step: str
    status: str
    counts: dict[str, int]
    message: str
    at: datetime


@dataclass(frozen=True)
class FileRow:
    run_id: str
    source: str
    dataset: str
    batch_key: str
    file_name: str
    window: str
    rows: int
    status: str
    reason: str
    at: datetime


@dataclass(frozen=True)
class ErrorRow:
    run_id: str
    source: str | None
    dataset: str | None
    step: str
    error_class: str
    message: str
    at: datetime


@dataclass(frozen=True)
class RunLogSnapshot:
    runs: list[RunRow] = field(default_factory=list)
    datasets: list[DatasetRunRow] = field(default_factory=list)
    steps: list[StepRow] = field(default_factory=list)
    files: list[FileRow] = field(default_factory=list)
    errors: list[ErrorRow] = field(default_factory=list)


class RunLogReader(Protocol):
    def snapshot(self, domain: str, since: datetime, until: datetime) -> RunLogSnapshot:
        """Runs of the domain that started in [since, until), with everything recorded for them, in record order."""
        ...


def merge(*parts: RunLogSnapshot) -> RunLogSnapshot:
    return RunLogSnapshot(
        runs=[r for p in parts for r in p.runs],
        datasets=[r for p in parts for r in p.datasets],
        steps=[r for p in parts for r in p.steps],
        files=[r for p in parts for r in p.files],
        errors=[r for p in parts for r in p.errors],
    )
