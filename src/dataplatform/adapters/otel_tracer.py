"""The telemetry port (dataplatform.obs.trace) on the OpenTelemetry API.

Needs only `opentelemetry-api`. With the SDK installed and a tracer provider configured (an exporter of your choice:
OTLP, console, in-memory), spans are recorded and exported; the run id is the trace id the SDK assigned. With the API
alone (no provider), nothing is exported, but ids still work: the adapter makes a W3C trace id itself and carries
it in a non-recording span, so the run log, tags and lineage still share one id. The exporter is optional by design.
"""

from __future__ import annotations

import importlib
from typing import Any

from dataplatform.obs.trace import SPAN_END, new_span_id, new_trace_id


def _api() -> Any:
    return importlib.import_module("opentelemetry.trace")


def _attr(value: Any) -> Any:
    return value if isinstance(value, str | bool | int | float) else str(value)


class OtelSpan:
    def __init__(self, tracer: OtelTracer, span: Any) -> None:
        self._tracer, self._span = tracer, span
        self._ended = False

    @property
    def trace_id(self) -> str:
        return f"{self._span.get_span_context().trace_id:032x}"

    @property
    def span_id(self) -> str:
        return f"{self._span.get_span_context().span_id:016x}"

    def child(self, name: str, attrs: dict[str, Any] | None = None) -> OtelSpan:
        api = self._tracer.api
        if not self._span.is_recording():  # API only: a child of our own, under the parent's trace id
            parent = self._span.get_span_context()
            return OtelSpan(self._tracer, api.NonRecordingSpan(api.SpanContext(parent.trace_id,
                                                                              int(new_span_id(), 16), False)))
        ctx = api.set_span_in_context(self._span)
        return OtelSpan(self._tracer, self._tracer.tracer.start_span(name, context=ctx, attributes=_attrs(attrs)))

    def set(self, attrs: dict[str, Any]) -> None:
        self._span.set_attributes(_attrs(attrs))

    def end(self, status: str = "ok", message: str = "") -> None:
        if status not in SPAN_END:
            raise ValueError(f"span status {status!r} is not one of {SPAN_END}")
        if self._ended:
            return
        api = self._tracer.api
        code = api.StatusCode.ERROR if status == "error" else api.StatusCode.OK
        self._span.set_status(api.Status(code, message or None) if status == "error" else api.Status(code))
        self._span.end()
        self._ended = True


def _attrs(attrs: dict[str, Any] | None) -> dict[str, Any]:
    return {k: _attr(v) for k, v in (attrs or {}).items()}


class OtelTracer:
    def __init__(self, tracer_provider: Any = None, name: str = "dataplatform.obs") -> None:
        self.api = _api()
        self.tracer = self.api.get_tracer(name, tracer_provider=tracer_provider)

    def start(self, name: str, attrs: dict[str, Any] | None = None) -> OtelSpan:
        root = importlib.import_module("opentelemetry.context").Context()
        span = self.tracer.start_span(name, context=root, attributes=_attrs(attrs))
        if not span.get_span_context().is_valid:  # API only, no SDK: make the ids ourselves
            ctx = self.api.SpanContext(int(new_trace_id(), 16), int(new_span_id(), 16), False)
            span = self.api.NonRecordingSpan(ctx)
        return OtelSpan(self, span)
