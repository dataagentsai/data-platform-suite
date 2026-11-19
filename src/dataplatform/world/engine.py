"""The world engine: runs the clock a day at a time, asks the domain model what its source systems did that day,
writes it into the mock sources through their ports, and saves the oracle's totals for the day.

Domain-free (M3): everything about tyres lives in the domain model the DWD names. The engine checks that the model
writes only to systems the DWD declares, through the port that matches each system's kind, and that the model
handles every incident kind on the calendar (an unhandled kind is a spec gap, not something to skip).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Protocol

from dataplatform.meta.port import BackfillPlan, Correction
from dataplatform.source_ports import EventStream, FileDrop, RecordInterface, Row, SourceChange, SourceDatabase
from dataplatform.world.dwd import Dwd

Chunk = list[tuple[str, str, Row]]


@dataclass
class Emission:
    """Everything the sources did on one world day, in commit or arrival order within each list."""

    changes: list[tuple[str, str, list[SourceChange]]] = field(default_factory=list)  # system, dataset, changes
    copies: list[tuple[str, str, str, list[Row]]] = field(default_factory=list)  # system, dataset, extracted_at, rows
    files: list[tuple[str, str, str, bytes, str]] = field(
        default_factory=list
    )  # system, dataset, name, bytes, arrived_at
    records: list[tuple[str, str, str, Row, str]] = field(
        default_factory=list
    )  # system, dataset, key, record, updated_at
    events: dict[tuple[str, str], Iterator[Chunk]] = field(default_factory=dict)  # (system, dataset) -> chunks
    corrections: list[tuple[BackfillPlan, list[Correction]]] = field(default_factory=list)  # people, not sources
    notes: dict[str, Any] = field(default_factory=dict)  # records the world keeps beside the sources


class DomainModel(Protocol):
    handles: frozenset[str]

    def day(self, day: int) -> Emission: ...

    def oracle(self, day: int) -> dict[str, Any]: ...

    def incidents(self) -> list[dict[str, Any]]: ...


class CorrectionSink(Protocol):
    """Where people record corrections: the metadata store's corrections table (ADR-017), never a source system."""

    def record_corrections(self, plan: BackfillPlan, corrections: list[Correction]) -> None: ...


@dataclass
class Sinks:
    db: SourceDatabase
    drop: FileDrop
    api: RecordInterface
    stream: EventStream
    corrections: CorrectionSink | None = None


PORT_FOR_KIND = {"database": "changes/copies", "file_drop": "files", "rest_api": "records", "stream": "events"}


class WorldError(RuntimeError):
    pass


class World:
    def __init__(self, dwd: Dwd, model: DomainModel, sinks: Sinks, out_dir: str | Path):
        self.dwd, self.model, self.sinks, self.out = dwd, model, sinks, Path(out_dir)
        unhandled = sorted({e["kind"] for e in dwd.world.get("calendar", [])} - set(model.handles))
        if unhandled:
            raise WorldError(f"the domain model cannot plant incident kind(s) {unhandled}; add them or fix the DWD")

    def prepare(self) -> None:
        for system in self.dwd.systems:
            if self.dwd.kind_of(system) == "database":
                self.sinks.db.ensure(system, [d.table for d in self.dwd.in_system(system)], reset=True)

    def run(self, days: int) -> list[dict[str, Any]]:
        self.prepare()
        reports = [self.step(day) for day in range(1, days + 1)]
        self.finish()
        return reports

    def step(self, day: int) -> dict[str, Any]:
        """One world day: the sources' output written through the ports, then the oracle's totals saved. Days run in
        order from 1 after prepare(), so a caller can run the platform between two days."""
        (self.out / "oracle").mkdir(parents=True, exist_ok=True)
        emission = self.model.day(day)
        written = self.write(emission)
        oracle = self.model.oracle(day)
        oracle["written"] = written
        (self.out / "oracle" / f"day-{day:02d}.json").write_text(json.dumps(oracle, indent=1, sort_keys=True))
        for name, note in emission.notes.items():
            (self.out / "world").mkdir(parents=True, exist_ok=True)
            (self.out / "world" / name).write_text(json.dumps(note, indent=1, sort_keys=True))
        return oracle

    def finish(self) -> None:
        (self.out / "oracle").mkdir(parents=True, exist_ok=True)
        (self.out / "oracle" / "incidents.json").write_text(
            json.dumps(self.model.incidents(), indent=1, sort_keys=True)
        )

    def _check(self, system: str, dataset: str, kind: str) -> None:
        if system not in self.dwd.systems or self.dwd.kind_of(system) != kind:
            raise WorldError(f"{system} is not a {kind} system in the DWD")
        if dataset not in self.dwd.datasets or self.dwd.datasets[dataset].system != system:
            raise WorldError(f"{dataset} is not held by {system} in the DWD")

    def write(self, e: Emission) -> dict[str, int]:
        """Route each output to its port. Returns rows written per dataset (what the sources hold new today)."""
        counts: dict[str, int] = {}

        def add(ds: str, n: int) -> None:
            counts[ds] = counts.get(ds, 0) + n

        for system, ds, changes in e.changes:
            self._check(system, ds, "database")
            self.sinks.db.apply(system, self.dwd.datasets[ds].table, changes)
            add(ds, len(changes))
        for system, ds, at, rows in e.copies:
            self._check(system, ds, "database")
            self.sinks.db.publish_copy(system, self.dwd.datasets[ds].table, at, rows)
            add(ds, len(rows))
        for system, ds, name, data, at in e.files:
            self._check(system, ds, "file_drop")
            self.sinks.drop.put(system, ds, name, data, at)
            add(ds, max(0, data.count(b"\n") - 1))
        for system, ds, key, record, at in e.records:
            self._check(system, ds, "rest_api")
            self.sinks.api.upsert(system, self.dwd.datasets[ds].table_name, key, record, at)
            add(ds, 1)
        for plan, fixes in e.corrections:
            ds = f"{plan.source}.{plan.dataset}"
            if ds not in self.dwd.datasets:
                raise WorldError(f"a correction names {ds}, which no system holds")
            if self.sinks.corrections is None:
                raise WorldError("the world records corrections but no corrections table is bound")
            self.sinks.corrections.record_corrections(plan, fixes)
        for (system, ds), chunks in e.events.items():
            self._check(system, ds, "stream")
            for chunk in chunks:
                self.sinks.stream.append(self.dwd.datasets[ds].table_name, chunk)
                add(ds, len(chunk))
        return counts
