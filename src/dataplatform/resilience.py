"""Resilience for calls to sources (X18, B7): retry with backoff, a circuit breaker per source, a connection cap per
source. Standard library only; time comes in as functions (DPC-0032: time only from the clock port), so tests drive it.

    RetryPolicy     how often and how long to wait; only errors declared temporary are retried (DPC-0005)
    CircuitBreaker  after N failures in a row a source is left alone for a while, then one trial call is let through
    SourceGuard     the three together: guard.call(source, fn) is what an ingestion pipeline uses to read a source

Settings (registry): retry_count, retry_interval_seconds (dataset); max_concurrent_datasets (source, the cap);
circuit_failure_threshold, circuit_reset_seconds (proposed, B7).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")


class TransientError(RuntimeError):
    """A failure that may pass if tried again (timeout, throttled, connection reset). Adapters raise it, or a subclass,
    for the failures they know are temporary; anything else is not retried."""


class CircuitOpen(RuntimeError):
    """The source failed too often in a row; it is not called again until its reset time has passed."""


RETRY_ON: tuple[type[BaseException], ...] = (TransientError, TimeoutError, ConnectionError)


@dataclass(frozen=True)
class RetryPolicy:
    retries: int = 2  # tries after the first
    interval_s: float = 60.0  # wait before the first retry
    factor: float = 2.0  # each wait is this times the one before
    max_wait_s: float = 600.0
    retry_on: tuple[type[BaseException], ...] = RETRY_ON

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> RetryPolicy:
        return cls(retries=int(settings.get("retry_count", 2) or 0),
                   interval_s=float(settings.get("retry_interval_seconds", 60) or 0))

    def waits(self) -> list[float]:
        return [min(self.interval_s * self.factor**n, self.max_wait_s) for n in range(max(self.retries, 0))]


def call_with_retry(fn: Callable[[], T], policy: RetryPolicy, sleep: Callable[[float], None],
                    on_retry: Callable[[int, BaseException, float], None] | None = None) -> T:
    """Call fn; on a declared temporary error wait and try again, up to policy.retries times. The last error is raised
    when every try failed; any other error is raised at once."""
    waits = policy.waits()
    for attempt in range(len(waits) + 1):
        try:
            return fn()
        except policy.retry_on as exc:
            if attempt == len(waits):
                raise
            if on_retry is not None:
                on_retry(attempt + 1, exc, waits[attempt])
            sleep(waits[attempt])
    raise AssertionError("unreachable")  # pragma: no cover


@dataclass
class CircuitBreaker:
    """closed: calls go through. open: calls are refused until reset_s has passed since it opened. half-open: one trial
    call; success closes it, failure opens it again."""

    threshold: int = 3
    reset_s: float = 1800.0
    failures: int = 0
    opened_at: float | None = None

    def state(self, now: float) -> str:
        if self.opened_at is None:
            return "closed"
        return "half-open" if now - self.opened_at >= self.reset_s else "open"

    def before(self, now: float, source: str) -> None:
        if self.state(now) == "open":
            raise CircuitOpen(f"{source}: {self.failures} failures in a row; not called again until "
                              f"{self.reset_s:.0f}s after the last one")

    def success(self) -> None:
        self.failures, self.opened_at = 0, None

    def failure(self, now: float) -> None:
        self.failures += 1
        if self.failures >= self.threshold or self.opened_at is not None:
            self.opened_at = now


@dataclass
class SourceGuard:
    """Retry, circuit breaker and connection cap for every source a run reads. One guard per run (or per process);
    the breaker's state lives here for the run: a breaker that must outlive runs is kept in the metadata store."""

    clock: Callable[[], float]
    sleep: Callable[[float], None]
    caps: dict[str, int] = field(default_factory=dict)  # source -> max concurrent calls
    threshold: int = 3
    reset_s: float = 1800.0
    breakers: dict[str, CircuitBreaker] = field(default_factory=dict)
    _sems: dict[str, threading.BoundedSemaphore] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    retries_seen: list[tuple[str, int, str]] = field(default_factory=list)  # (source, attempt, error) for the run log

    def breaker(self, source: str) -> CircuitBreaker:
        with self._lock:
            return self.breakers.setdefault(source, CircuitBreaker(self.threshold, self.reset_s))

    def _sem(self, source: str) -> threading.BoundedSemaphore | None:
        cap = self.caps.get(source)
        if not cap:
            return None
        with self._lock:
            return self._sems.setdefault(source, threading.BoundedSemaphore(int(cap)))

    def call(self, source: str, fn: Callable[[], T], policy: RetryPolicy | None = None) -> T:
        breaker = self.breaker(source)
        breaker.before(self.clock(), source)
        sem = self._sem(source)

        def once() -> T:
            if sem is None:
                return fn()
            with sem:
                return fn()

        def noted(attempt: int, exc: BaseException, wait: float) -> None:
            self.retries_seen.append((source, attempt, type(exc).__name__))

        try:  # the breaker counts calls that failed after their retries, not each failed try
            out = call_with_retry(once, policy or RetryPolicy(), self.sleep, noted)
        except BaseException:
            breaker.failure(self.clock())
            raise
        breaker.success()
        return out
