"""Monitoring, pushed by runs (ADR-024 rule 4): nothing here runs on a fixed interval.

    monitor = Monitor(reader, system, store, "tyre", config, notifier, since=start)
    recorder = RunRecorder(store, tracer, on_finish=(lambda run_id: monitor.run_finished(run_id, clock.now()),))
    ...                                    # a run ends -> its datasets, detectors and checks raise or resolve alerts
    monitor.wake(now)                      # a lateness check, only once `monitor.due` has come (no event for lateness)
    monitor.copy_system(now)               # platform telemetry, copied once a business day from a watermark
    tables = monitor.tables(now)           # the compact ops tables the ops metric views and dashboards read

Lateness is checked against the run log through the run-log port and the store (no engine compute). Everything it
touches is a port; the logic is in the pure modules (freshness, alerts, ops). The alert state and history live on
the Monitor; a deployment keeps them in a table. Delivery goes through the notifier port: the platform's own
notification channels (adapters/databricks_notify.py), with routing and noise rules ours (obs/alerts.py).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from dataplatform.meta.port import MetadataStore
from dataplatform.obs import freshness as fresh
from dataplatform.obs import ops
from dataplatform.obs.alerts import Alert, AlertConfig, Event, Scope, Signal, evaluate
from dataplatform.obs.pipeline_events import PipelineEvents
from dataplatform.obs.runlog import RunLogReader, RunLogSnapshot
from dataplatform.obs.system import DailyCopy, SystemTables

RUN_SIGNALS = frozenset({"run_failed", "detector", "check_failed"})
LATENESS = frozenset({"freshness"})


class Notifier(Protocol):
    """Where an alert goes: a pager, a ticket queue, a digest. The route says which; the adapter delivers."""

    def send(self, alert: Alert) -> None: ...


class MemoryNotifier:
    def __init__(self) -> None:
        self.sent: list[Alert] = []

    def send(self, alert: Alert) -> None:
        self.sent.append(alert)


@dataclass
class CheckOutput:
    sent: list[Alert]
    events: list[Event]
    freshness: list[fresh.FreshnessResult] = field(default_factory=list)


def _only(log: RunLogSnapshot, run_id: str) -> RunLogSnapshot:
    return RunLogSnapshot([r for r in log.runs if r.run_id == run_id],
                          [d for d in log.datasets if d.run_id == run_id],
                          [s for s in log.steps if s.run_id == run_id],
                          [f for f in log.files if f.run_id == run_id],
                          [e for e in log.errors if e.run_id == run_id])


class Monitor:
    def __init__(self, reader: RunLogReader, system: SystemTables, store: MetadataStore, domain: str,
                 config: AlertConfig, notifier: Notifier, since: datetime,
                 pipeline: PipelineEvents | None = None) -> None:
        self.reader, self.store, self.domain = reader, store, domain
        self.config, self.notifier, self.since, self.pipeline = config, notifier, since, pipeline
        self.copy = DailyCopy(system, since, config.business_timezone)
        self.state: dict[str, Any] = {}
        self.freshness_history: list[fresh.FreshnessResult] = []
        self.events: list[Event] = []
        self.sent: list[Alert] = []
        self.due: datetime | None = None
        self.lateness_checks = 0

    def _evaluate(self, signals: list[Signal], now: datetime, scope: Scope) -> CheckOutput:
        evaluation = evaluate(self.config, signals, self.state, now, scope)
        self.state = evaluation.state
        for alert in evaluation.sent:
            self.notifier.send(alert)
        self.events.extend(evaluation.events)
        self.sent.extend(evaluation.sent)
        return CheckOutput(evaluation.sent, evaluation.events)

    def _lateness(self, log: RunLogSnapshot, now: datetime) -> CheckOutput:
        targets = fresh.targets_from_store(self.store, self.domain, self.config.tiers)
        results = fresh.check(targets, log.datasets, fresh.read_bookmarks(self.store, targets), now)
        self.freshness_history.extend(results)
        self.due = fresh.next_due(targets, log.datasets, now)
        self.lateness_checks += 1
        out = self._evaluate(ops.freshness_signals(results, targets), now, Scope(LATENESS))
        out.freshness = results
        return out

    def run_finished(self, run_id: str, now: datetime) -> CheckOutput:
        """Pushed by the recorder as a run ends: the run's own datasets, detectors and checks, then lateness (a run
        that loads can end a lateness)."""
        log = self.reader.snapshot(self.domain, self.since, now)
        run = _only(log, run_id)
        targets = fresh.targets_from_store(self.store, self.domain, self.config.tiers)
        names = frozenset(f"{d.source}.{d.dataset}" for d in run.datasets)
        out = self._evaluate(ops.run_signals(run, targets), now, Scope(RUN_SIGNALS, names))
        late = self._lateness(log, now)
        return CheckOutput(out.sent + late.sent, out.events + late.events, late.freshness)

    def check_lateness(self, now: datetime) -> CheckOutput:
        return self._lateness(self.reader.snapshot(self.domain, self.since, now), now)

    def wake(self, now: datetime) -> CheckOutput | None:
        """The scheduler's one wake-up: a lateness check only when one is due (`self.due`), else nothing runs."""
        if self.due is None or now < self.due:
            return None
        return self.check_lateness(now)

    def copy_system(self, now: datetime) -> bool:
        return self.copy.copy(now)

    def tables(self, now: datetime) -> ops.Tables:
        log = self.reader.snapshot(self.domain, self.since, now)
        pipeline = self.pipeline.table_updates(self.since, now) if self.pipeline else None
        return ops.build(log, self.copy.rows, self.freshness_history, self.events, self.config.business_timezone,
                         pipeline)
