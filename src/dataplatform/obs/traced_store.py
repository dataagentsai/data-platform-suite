"""Live wiring of observability (B10, ADR-024): a metadata store that traces every run as it is recorded and pushes
"run finished" to its hooks. The engine runners already record through the store port; wrapping the store wires them
to the tracer and the monitor without changing a runner (push, not poll).

    store = TracedStore(PostgresMetaStore(...), LocalTracer(), on_finish=(lambda r: monitor.run_finished(r, now()),))

Spans: one per run (named by the run's trigger), a child per dataset run, a child per step; the run id is set on every
span as dp_run_id, so a trace viewer and the run log name the same run. Every call is forwarded unchanged: a store
behind this one cannot tell it is there.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from dataplatform.meta.port import MetadataStore, StepRecord
from dataplatform.obs.trace import Span, Tracer

RUN_TAG = "dp_run_id"


class TracedStore:
    def __init__(self, store: MetadataStore, tracer: Tracer, on_finish: Iterable[Callable[[str], Any]] = ()):
        self._store, self._tracer, self._on_finish = store, tracer, tuple(on_finish)
        self._runs: dict[str, Span] = {}
        self._datasets: dict[tuple[str, str, str], Span] = {}

    def __getattr__(self, name: str) -> Any:  # every other operation goes straight through
        return getattr(self._store, name)

    def start_run(self, run_id: str, domain: str, env: str, trigger: str = "manual", run_type: str = "normal") -> None:
        self._store.start_run(run_id, domain, env, trigger, run_type)
        self._runs[run_id] = self._tracer.start(f"run.{trigger}", {RUN_TAG: run_id, "domain": domain, "env": env,
                                                                   "run_type": run_type})

    def start_dataset_run(self, run_id: str, source: str, dataset: str, config_fingerprint: str,
                          config: dict[str, Any]) -> None:
        self._store.start_dataset_run(run_id, source, dataset, config_fingerprint, config)
        parent = self._runs.get(run_id)
        if parent is not None:
            self._datasets[(run_id, source, dataset)] = parent.child(
                f"dataset.{source}.{dataset}", {RUN_TAG: run_id, "fingerprint": config_fingerprint})

    def log_step(self, step: StepRecord) -> None:
        self._store.log_step(step)
        parent = self._datasets.get((step.run_id, step.source, step.dataset))
        if parent is not None:
            span = parent.child(f"step.{step.step}", {RUN_TAG: step.run_id, **{f"count.{k}": v for k, v in
                                                                              step.counts.items()}})
            span.end("error" if step.status == "failed" else "ok", step.message[:200])

    def finish_dataset_run(self, run_id: str, source: str, dataset: str, status: str, *args: Any, **kw: Any) -> None:
        self._store.finish_dataset_run(run_id, source, dataset, status, *args, **kw)
        span = self._datasets.pop((run_id, source, dataset), None)
        if span is not None:
            span.end("error" if status == "failed" else "ok", status)

    def finish_run(self, run_id: str) -> str:
        status = self._store.finish_run(run_id)
        span = self._runs.pop(run_id, None)
        if span is not None:
            span.set({"status": status})
            span.end("error" if status == "failed" else "ok", status)
        for hook in self._on_finish:
            hook(run_id)
        return status
