"""Retry, circuit breaker and connection cap (X18, B7): table-driven, with a fake clock and a recorded sleep.

Rules proven here (DAC): ORCH-02 (retries for passing failures only; a bug is never retried).
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from dataplatform.resilience import (
    CircuitOpen,
    RetryPolicy,
    SourceGuard,
    TransientError,
    call_with_retry,
)


class Flaky:
    """Fails with the given errors in order, then returns 'ok'."""

    def __init__(self, *errors: BaseException):
        self.errors, self.calls = list(errors), 0

    def __call__(self) -> str:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "ok"


RETRY_CASES = [
    ("works first time: no wait", [], RetryPolicy(2, 10), "ok", 1, []),
    ("a timeout then success: one wait", [TimeoutError("slow")], RetryPolicy(2, 10), "ok", 2, [10]),
    ("two temporary errors: backoff doubles", [TransientError("x"), ConnectionError("y")], RetryPolicy(2, 10), "ok", 3,
     [10, 20]),
    ("out of retries: the last error is raised", [TimeoutError("a"), TimeoutError("b"), TimeoutError("c")],
     RetryPolicy(2, 10), TimeoutError, 3, [10, 20]),
    ("a bug is never retried", [ValueError("bad column")], RetryPolicy(5, 10), ValueError, 1, []),
    ("waits are capped", [TimeoutError()] * 3, RetryPolicy(3, 100, factor=10, max_wait_s=300), "ok", 4,
     [100, 300, 300]),
    ("no retries configured", [TimeoutError("a")], RetryPolicy(0, 10), TimeoutError, 1, []),
]


@pytest.mark.parametrize(("case", "errors", "policy", "want", "calls", "waits"), RETRY_CASES,
                         ids=[c[0] for c in RETRY_CASES])
def test_retry(case: str, errors: list[BaseException], policy: RetryPolicy, want: Any, calls: int,  # noqa: PLR0917 -- one parameter per table column
               waits: list[float]) -> None:
    fn, slept = Flaky(*errors), []
    if isinstance(want, type):
        with pytest.raises(want):
            call_with_retry(fn, policy, slept.append)
    else:
        assert call_with_retry(fn, policy, slept.append) == want
    assert fn.calls == calls
    assert slept == waits


def test_policy_from_settings() -> None:
    p = RetryPolicy.from_settings({"retry_count": "3", "retry_interval_seconds": "5"})
    assert (p.retries, p.waits()) == (3, [5.0, 10.0, 20.0])


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


NO_RETRY = RetryPolicy(0, 0)


def _fail() -> str:
    raise TimeoutError("down")


BREAKER_CASES = [
    # (case, steps, want states); a step is ("fail" | "ok", seconds to advance before it)
    ("closed while failures stay below the threshold", [("fail", 0), ("fail", 0)], ["closed", "closed"]),
    ("opens at the threshold", [("fail", 0), ("fail", 0), ("fail", 0)], ["closed", "closed", "open"]),
    ("a success resets the count", [("fail", 0), ("fail", 0), ("ok", 0), ("fail", 0)],
     ["closed", "closed", "closed", "closed"]),
    ("half-open after the reset time; a success closes it",
     [("fail", 0), ("fail", 0), ("fail", 0), ("ok", 61)], ["closed", "closed", "open", "closed"]),
    ("a failed trial opens it again", [("fail", 0), ("fail", 0), ("fail", 0), ("fail", 61)],
     ["closed", "closed", "open", "open"]),
]


@pytest.mark.parametrize(("case", "steps", "states"), BREAKER_CASES, ids=[c[0] for c in BREAKER_CASES])
def test_circuit_breaker(case: str, steps: list[tuple[str, float]], states: list[str]) -> None:
    clock = Clock()
    guard = SourceGuard(clock, lambda s: None, threshold=3, reset_s=60)
    got = []
    for what, advance in steps:
        clock.now += advance
        try:
            guard.call("erp", (lambda: "ok") if what == "ok" else _fail, NO_RETRY)
        except TimeoutError:
            pass
        got.append(guard.breaker("erp").state(clock.now))
    assert got == states


def test_open_circuit_refuses_without_calling() -> None:
    guard = SourceGuard(Clock(), lambda s: None, threshold=1, reset_s=60)
    with pytest.raises(TimeoutError):
        guard.call("erp", _fail, NO_RETRY)
    fn = Flaky()
    with pytest.raises(CircuitOpen, match="erp"):
        guard.call("erp", fn, NO_RETRY)
    assert fn.calls == 0


def test_breakers_are_per_source() -> None:
    guard = SourceGuard(Clock(), lambda s: None, threshold=1, reset_s=60)
    with pytest.raises(TimeoutError):
        guard.call("erp", _fail, NO_RETRY)
    assert guard.call("mes", lambda: "ok", NO_RETRY) == "ok"


def test_retries_are_recorded_for_the_run_log() -> None:
    guard = SourceGuard(Clock(), lambda s: None)
    assert guard.call("erp", Flaky(TimeoutError("x")), RetryPolicy(2, 1)) == "ok"
    assert guard.retries_seen == [("erp", 1, "TimeoutError")]


def test_connection_cap_limits_concurrent_calls() -> None:
    guard = SourceGuard(Clock(), lambda s: None, caps={"erp": 2})
    active, peak, lock, gate = [0], [0], threading.Lock(), threading.Event()

    def call() -> str:
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        gate.wait(0.2)
        with lock:
            active[0] -= 1
        return "ok"

    threads = [threading.Thread(target=guard.call, args=("erp", call, NO_RETRY)) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak[0] == 2


def test_pipeline_reads_go_through_the_guard() -> None:
    from types import SimpleNamespace

    from dataplatform.ingest.pipelines import _read

    ctx = SimpleNamespace(spec=SimpleNamespace(source="erp", settings={"retry_count": "2",
                                                                       "retry_interval_seconds": "1"}))
    slept: list[float] = []
    src = SimpleNamespace(guard=SourceGuard(Clock(), slept.append))
    fn = Flaky(TimeoutError("reset by peer"))
    assert _read(ctx, src, fn) == "ok"  # type: ignore[arg-type]
    assert (fn.calls, slept) == (2, [1.0])
    direct = SimpleNamespace(guard=None)
    with pytest.raises(TimeoutError):
        _read(ctx, direct, Flaky(TimeoutError("x")))  # type: ignore[arg-type]
