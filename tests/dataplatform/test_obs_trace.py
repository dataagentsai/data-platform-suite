"""Contract table for the telemetry port (ADR-020, M5): every tracer adapter passes the same rows.

Adapters: LocalTracer (kernel, product-free), OtelTracer with the API alone (no exporter: ids still made), and
OtelTracer on the SDK with an in-memory exporter (spans recorded and exported). The OpenTelemetry rows are skipped
when the packages are not installed (`make world-test` installs them).
"""

import importlib.util
import re

import pytest

from dataplatform.obs.trace import LocalTracer, as_uuid, new_trace_id, valid_trace_id

HAS_API = importlib.util.find_spec("opentelemetry") is not None
HAS_SDK = HAS_API and importlib.util.find_spec("opentelemetry.sdk") is not None


def local():
    return LocalTracer()


def otel_api_only():
    from dataplatform.adapters.otel_tracer import OtelTracer

    return OtelTracer()  # no provider passed and none configured: the API's no-op tracer


def otel_sdk():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from dataplatform.adapters.otel_tracer import OtelTracer

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = OtelTracer(provider)
    tracer.exporter = exporter  # for the export rows
    return tracer


ADAPTERS = [
    pytest.param(local, id="local"),
    pytest.param(otel_api_only, id="otel-api-only", marks=pytest.mark.skipif(not HAS_API, reason="no otel api")),
    pytest.param(otel_sdk, id="otel-sdk", marks=pytest.mark.skipif(not HAS_SDK, reason="no otel sdk")),
]


def two_roots(t):
    return t.start("a").trace_id != t.start("b").trace_id


def child_shares_trace(t):
    root = t.start("run")
    kid = root.child("dataset").child("step")
    return kid.trace_id == root.trace_id and kid.span_id != root.span_id


CASES = [
    ("a root's trace id is W3C form", lambda t: valid_trace_id(t.start("run").trace_id), True),
    ("a span id is 16 hex", lambda t: bool(re.match(r"^[0-9a-f]{16}$", t.start("run").span_id)), True),
    ("every root is a new trace", two_roots, True),
    ("children share the root's trace id", child_shares_trace, True),
    ("attributes and end are accepted", lambda t: (s := t.start("r", {"n": 1, "x": [1]}), s.set({"k": "v"}),
                                                    s.end(), s.end())[0].trace_id != "", True),
    ("an unknown end status is refused", lambda t: t.start("r").end("maybe"), ValueError),
]


@pytest.mark.parametrize("make", ADAPTERS)
@pytest.mark.parametrize(("case", "act", "expect"), CASES, ids=[c[0] for c in CASES])
def test_tracer_contract(make, case, act, expect):
    tracer = make()
    if isinstance(expect, type) and issubclass(expect, Exception):
        with pytest.raises(expect):
            act(tracer)
    else:
        assert act(tracer) == expect


@pytest.mark.skipif(not HAS_SDK, reason="no otel sdk")
def test_sdk_exports_spans_under_the_run_id():
    tracer = otel_sdk()
    root = tracer.start("run tyre")
    root.child("dataset erp.sales_order_line").end()
    root.end("error", "boom")
    spans = tracer.exporter.get_finished_spans()
    assert [s.name for s in spans] == ["dataset erp.sales_order_line", "run tyre"]
    assert {f"{s.context.trace_id:032x}" for s in spans} == {root.trace_id}


@pytest.mark.parametrize(
    ("value", "ok"),
    [("0" * 32, False), ("A" * 32, False), ("abc", False), ("0123456789abcdef0123456789abcdef", True),
     (new_trace_id(), True)],
)
def test_trace_id_rule(value, ok):
    assert valid_trace_id(value) is ok


@pytest.mark.parametrize(
    ("trace_id", "expect"),
    [("0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef"), ("nope", ValueError)],
)
def test_lineage_uuid_form(trace_id, expect):
    if expect is ValueError:
        with pytest.raises(ValueError, match="not a trace id"):
            as_uuid(trace_id)
    else:
        assert as_uuid(trace_id) == expect
