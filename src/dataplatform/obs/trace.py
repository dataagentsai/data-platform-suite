"""The telemetry port: one id for a run, everywhere (G8). The run id IS the trace id.

A run opens a root span; its trace id (32 lowercase hex characters, the W3C trace-context form) becomes the run id
the run log is keyed by, the tag on the compute it bills, the run id on its lineage events and the field on every
log line. Join any two of those on the run id and you have the whole story of one load.

    Span, Tracer    the port. The kernel only ever calls these.
    LocalTracer     a product-free tracer: makes W3C ids and keeps finished spans in a list (tests, local runs).
                    The standard tracing API adapter is adapters/otel_tracer.py; its exporter is optional.
"""

from __future__ import annotations

import re
import secrets
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

TRACE_ID = re.compile(r"^[0-9a-f]{32}$")
SPAN_ID = re.compile(r"^[0-9a-f]{16}$")
SPAN_END = ("ok", "error")


class Span(Protocol):
    @property
    def trace_id(self) -> str: ...

    @property
    def span_id(self) -> str: ...

    def child(self, name: str, attrs: dict[str, Any] | None = None) -> Span: ...

    def set(self, attrs: dict[str, Any]) -> None: ...

    def end(self, status: str = "ok", message: str = "") -> None: ...


class Tracer(Protocol):
    def start(self, name: str, attrs: dict[str, Any] | None = None) -> Span:
        """A new root span: a new trace, so a new run id."""
        ...


def valid_trace_id(value: str) -> bool:
    return bool(TRACE_ID.match(value)) and value != "0" * 32


def new_trace_id() -> str:
    while True:
        value = secrets.token_hex(16)
        if valid_trace_id(value):
            return value


def new_span_id() -> str:
    while True:
        value = secrets.token_hex(8)
        if value != "0" * 16:
            return value


def as_uuid(trace_id: str) -> str:
    """The run id in the UUID form lineage events want for their run id (same 128 bits)."""
    if not valid_trace_id(trace_id):
        raise ValueError(f"not a trace id: {trace_id!r}")
    return str(uuid.UUID(hex=trace_id))


@dataclass
class LocalSpan:
    tracer: LocalTracer
    name: str
    trace_id: str
    span_id: str
    parent_id: str | None
    attrs: dict[str, Any] = field(default_factory=dict)
    status: str | None = None
    message: str = ""

    def child(self, name: str, attrs: dict[str, Any] | None = None) -> LocalSpan:
        return LocalSpan(self.tracer, name, self.trace_id, new_span_id(), self.span_id, dict(attrs or {}))

    def set(self, attrs: dict[str, Any]) -> None:
        self.attrs.update(attrs)

    def end(self, status: str = "ok", message: str = "") -> None:
        if status not in SPAN_END:
            raise ValueError(f"span status {status!r} is not one of {SPAN_END}")
        if self.status is not None:
            return
        self.status, self.message = status, message
        self.tracer.finished.append(self)


class LocalTracer:
    def __init__(self) -> None:
        self.finished: list[LocalSpan] = []

    def start(self, name: str, attrs: dict[str, Any] | None = None) -> LocalSpan:
        return LocalSpan(self, name, new_trace_id(), new_span_id(), None, dict(attrs or {}))
