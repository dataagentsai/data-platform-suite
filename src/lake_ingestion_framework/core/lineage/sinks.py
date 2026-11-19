"""Lineage event sinks — where emitted OL events go.

A ``LineageSink`` accepts pre-serialized OL events (as dicts) and
persists them somewhere. The same protocol fits per-event streaming
(Silver/Source emitters, one event per pipeline run) and bulk emission
(dbt manifest emitter, N events per call).

Phase 7-A/B ships :class:`JsonLinesSink` only — JSONL is the canonical
OpenLineage wire format and every downstream consumer (Marquez,
OpenMetadata, UC dbt-import, Atlan ingestion) reads it natively.

Future sinks (HTTP transport to Marquez / UC native push / `meta.lineage_*`
SQL) layer on the same protocol — the emitters that produce events
don't need to change.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol


class LineageSink(Protocol):
    """Streaming sink: accepts events one at a time.

    ``emit`` is called once per event by the producer. ``close`` is
    called when the producer is done — sinks that buffer (e.g., HTTP
    batchers, future SQL writers) flush here.
    """

    def emit(self, event: dict[str, Any]) -> None: ...

    def close(self) -> None: ...


class JsonLinesSink:
    """Append events to a JSONL file (one OL event per line).

    Opens the file lazily on first ``emit`` so a sink that's never used
    (e.g., when the customer didn't pass ``--lineage-output``) creates
    no file at all. Subsequent calls append.

    Idempotent on writes: each ``emit`` produces a deterministic JSON
    line (sorted keys); re-emitting the same event is a no-op modulo
    duplicate lines. Caller is responsible for de-duping if needed.
    """

    def __init__(self, path: Path, *, mode: str = "a") -> None:
        """``mode='w'`` truncates on first write (one-shot dbt-manifest
        runs); ``mode='a'`` appends (per-run Silver/Source emissions).
        Default ``a`` favours the per-run case.
        """
        if mode not in ("w", "a"):
            raise ValueError(f"JsonLinesSink mode must be 'w' or 'a', got {mode!r}")
        self._path = path
        self._mode = mode
        self._file: Any = None  # opened lazily

    def emit(self, event: dict[str, Any]) -> None:
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open(self._mode, encoding="utf-8")
        self._file.write(json.dumps(event, sort_keys=True))
        self._file.write("\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    @property
    def path(self) -> Path:
        return self._path

    def __enter__(self) -> JsonLinesSink:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
