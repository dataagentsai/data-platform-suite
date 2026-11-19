"""The clock port (DPC-0032): observability reads time only through this, so a simulated week runs in a second.

    SystemClock     the wall clock, in UTC.
    ManualClock     a clock the caller sets or moves (tests, the fixture runner, the scenario runner).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """An aware datetime."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("a clock needs an aware datetime")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("a clock needs an aware datetime")
        self._now = instant

    def advance(self, seconds: float) -> datetime:
        self._now = self._now + timedelta(seconds=seconds)
        return self._now
