"""The run recorder: one id from start to finish (G8, OPS-03, DPC-0057).

It wraps the metadata store port (W1a) and the telemetry port. Opening a run opens a root span, and the span's trace
id becomes the run id the store keys every record by. Each dataset and each step is a child span of that trace, so a
trace viewer, the run log, the bill and the lineage graph all name the same run the same way:

    with recorder.run("tyre", "dev", trigger="schedule") as run:          # run.run_id == the trace id
        with run.dataset("erp", "sales_order_line", fingerprint, config) as ds:
            with ds.step("land") as step:
                step.counts["rows"] = 120
            ds.check("quality", "not_null_order_id", passed=True)
            ds.detect("DEL-01", {"keys": 5})                              # a detector fired (warned)
            ds.succeed({"rows_written": 120}, watermark="1234", bookmarks={"feed": "..."})
        job_tags = run.job_tags("sales")                                  # put these on the job (domain, product, id)

Step names follow the one step format (obs/steps.py, ADR-024):
    quality.<rule>   recon.<entry>   profile.<name>   detect.<detector id>   anything else is a pipeline step.

Push, not poll (ADR-024 rule 4): when a run finishes, the recorder calls every `on_finish` hook with the run id, so
alerts are raised as runs end (obs/monitor.py Monitor.run_finished), not by a job that polls the run log.

Nothing here writes a table: every write is one call through the store's port. Hooks only, no engine code (W3 rule).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from dataplatform.meta.port import FileRecord, MetadataStore, StepRecord
from dataplatform.obs.steps import CHECK_KINDS, DETECT
from dataplatform.obs.trace import Span, Tracer, as_uuid

RUN_TAG = "dp_run_id"
OnFinish = Callable[[str], None]


class RecorderError(RuntimeError):
    """The recorder was used out of order (a dataset finished twice, a step after the dataset closed)."""


@dataclass
class StepHandle:
    status: str = "succeeded"
    counts: dict[str, int] = field(default_factory=dict)
    message: str = ""

    def warn(self, message: str) -> None:
        self.status, self.message = "warned", message

    def skip(self, message: str) -> None:
        self.status, self.message = "skipped", message


class DatasetHandle:
    def __init__(self, run: RunHandle, source: str, dataset: str, span: Span, watermark_before: str | None):
        self.run, self.source, self.dataset, self.span = run, source, dataset, span
        self.watermark_before = watermark_before
        self.status: str | None = None

    @property
    def store(self) -> MetadataStore:
        return self.run.recorder.store

    def _open(self) -> None:
        if self.status is not None:
            raise RecorderError(f"{self.source}.{self.dataset} is already {self.status}")

    def _log(self, step: str, status: str, counts: dict[str, int] | None, message: str) -> None:
        self._open()
        self.store.log_step(StepRecord(self.run.run_id, self.source, self.dataset, step, status,
                                       dict(counts or {}), message))

    @contextmanager
    def step(self, name: str) -> Iterator[StepHandle]:
        handle, span = StepHandle(), self.span.child(f"step {name}", {"dp.step": name})
        try:
            yield handle
        except Exception as exc:
            self._log(name, "failed", handle.counts, f"{type(exc).__name__}: {exc}")
            span.end("error", type(exc).__name__)
            raise
        self._log(name, handle.status, handle.counts, handle.message)
        span.set({f"dp.count.{k}": v for k, v in handle.counts.items()})
        span.end()

    def check(self, kind: str, name: str, passed: bool, counts: dict[str, int] | None = None,
              message: str = "", warn_only: bool = False) -> None:
        """A quality rule, a reconciliation entry or a profile: its verdict as a step of this dataset run."""
        if kind not in CHECK_KINDS:
            raise ValueError(f"check kind {kind!r} is not one of {CHECK_KINDS}")
        status = "succeeded" if passed else ("warned" if warn_only else "failed")
        self._log(f"{kind}.{name}", status, counts, message or ("" if passed else f"{kind}.{name} did not pass"))

    def detect(self, detector: str, counts: dict[str, int] | None = None, message: str = "",
               blocked: bool = False) -> None:
        """A detector fired (DAC id, e.g. DEL-01). Handled is warned; blocked (the load stopped) is failed."""
        self._log(f"{DETECT}.{detector}", "failed" if blocked else "warned", counts,
                  message or f"{detector} {'blocked the load' if blocked else 'handled'}")
        self.span.set({f"dp.detect.{detector}": "blocked" if blocked else "handled"})

    def error(self, step: str, error_class: str, message: str) -> None:
        """An error the step handled (a contract mismatch held back, a retry that later passed): logged, not raised."""
        self._open()
        self.store.log_error(self.run.run_id, self.source, self.dataset, step, error_class, message)

    def file(self, batch_key: str, file_name: str, window: str, checksum: str, rows: int, status: str,
             declared_rows: int | None = None, reason: str = "") -> None:
        self._open()
        self.store.log_file(FileRecord(self.run.run_id, self.source, self.dataset, batch_key, file_name, window,
                                       checksum, declared_rows, rows, status, reason))

    def _finish(self, status: str, counts: dict[str, int] | None, watermark: str | None,
                bookmarks: dict[str, str] | None, message: str) -> None:
        self._open()
        self.store.finish_dataset_run(self.run.run_id, self.source, self.dataset, status, counts, watermark,
                                      bookmarks, message)
        self.status = status
        self.span.set({"dp.status": status, **{f"dp.count.{k}": v for k, v in (counts or {}).items()}})
        self.span.end("error" if status == "failed" else "ok", message)

    def succeed(self, counts: dict[str, int] | None = None, watermark: str | None = None,
                bookmarks: dict[str, str] | None = None) -> None:
        self._finish("succeeded", counts, watermark, bookmarks, "")

    def fail(self, message: str, counts: dict[str, int] | None = None) -> None:
        self._finish("failed", counts, None, None, message)

    def skip(self, message: str, counts: dict[str, int] | None = None) -> None:
        self._finish("skipped", counts, None, None, message)


class RunHandle:
    def __init__(self, recorder: RunRecorder, span: Span, domain: str, env: str):
        self.recorder, self.span, self.domain, self.env = recorder, span, domain, env
        self.run_id = span.trace_id
        self.status: str | None = None
        self.open: list[DatasetHandle] = []
        self.logged: set[int] = set()  # errors already logged by a dataset, not logged again by the run

    @contextmanager
    def dataset(self, source: str, dataset: str, config_fingerprint: str,
                config: dict[str, Any]) -> Iterator[DatasetHandle]:
        span = self.span.child(f"dataset {source}.{dataset}", {"dp.source": source, "dp.dataset": dataset,
                                                               "dp.config_fingerprint": config_fingerprint})
        before = self.recorder.store.start_dataset_run(self.run_id, source, dataset, config_fingerprint, config)
        handle = DatasetHandle(self, source, dataset, span, before)
        self.open.append(handle)
        try:
            yield handle
        except Exception as exc:
            self.recorder.store.log_error(self.run_id, source, dataset, "dataset", type(exc).__name__, str(exc))
            self.logged.add(id(exc))
            if handle.status is None:
                handle.fail(f"{type(exc).__name__}: {exc}")
            raise
        finally:
            self.open.remove(handle)
        if handle.status is None:
            handle.succeed()

    def job_tags(self, data_product: str = "") -> dict[str, str]:
        """Tags for the job and compute this run uses: domain, data product and run id. Spend itself is the separate
        cost product's (ADR-024 rule 8); these tags are what lets it attribute spend to a run and a product."""
        tags = {RUN_TAG: self.run_id, "dp_domain": self.domain, "dp_env": self.env}
        return {**tags, "dp_data_product": data_product} if data_product else tags

    def lineage_run_id(self) -> str:
        """The run id as lineage events carry it (UUID form of the same 128 bits)."""
        return as_uuid(self.run_id)

    def log_fields(self) -> dict[str, str]:
        """Fields for every structured log line written during the run."""
        return {"run_id": self.run_id, "trace_id": self.run_id, "span_id": self.span.span_id}


class RunRecorder:
    def __init__(self, store: MetadataStore, tracer: Tracer, on_finish: tuple[OnFinish, ...] = ()):
        self.store, self.tracer, self.on_finish = store, tracer, on_finish

    def _finished(self, run_id: str) -> None:
        for hook in self.on_finish:
            hook(run_id)

    @contextmanager
    def run(self, domain: str, env: str, trigger: str = "manual", run_type: str = "normal") -> Iterator[RunHandle]:
        span = self.tracer.start(f"run {domain}", {"dp.domain": domain, "dp.env": env, "dp.trigger": trigger,
                                                   "dp.run_type": run_type})
        self.store.start_run(span.trace_id, domain, env, trigger, run_type)
        handle = RunHandle(self, span, domain, env)
        try:
            yield handle
        except Exception as exc:
            if id(exc) not in handle.logged:
                self.store.log_error(handle.run_id, None, None, "run", type(exc).__name__, str(exc))
            for ds in list(handle.open):
                if ds.status is None:
                    ds.fail(f"run stopped: {type(exc).__name__}")
            handle.status = self.store.finish_run(handle.run_id)
            span.set({"dp.status": handle.status})
            span.end("error", type(exc).__name__)
            self._finished(handle.run_id)
            raise
        handle.status = self.store.finish_run(handle.run_id)
        span.set({"dp.status": handle.status})
        span.end("error" if handle.status in ("failed", "partial") else "ok")
        self._finished(handle.run_id)
