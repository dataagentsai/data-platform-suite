"""Windows of a fact (ADR-006), when one is complete (ADR-018) and when a rebuild is a restatement (ADR-017). Pure.

A window is named by text, so the kernel never needs the engine's types:
    day     YYYY-MM-DD (a business date)
    month   YYYY-MM-01 (the month's first day, as the window column holds it)
    hour    YYYY-MM-DDTHH:MM:SSZ (the hour's start, UTC)

Day and month windows start at midnight in the business time zone; an hour is an instant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

HOUR_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _month_add(d: date, months: int) -> date:
    m = d.year * 12 + d.month - 1 + months
    return date(m // 12, m % 12 + 1, 1)


def start(window: str, unit: str, tz: str) -> datetime:
    if unit == "hour":
        return datetime.strptime(window, HOUR_FORMAT).replace(tzinfo=UTC)
    d = date.fromisoformat(window[:10])
    return datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(tz))


def end(window: str, unit: str, tz: str) -> datetime:
    """The first instant after the window."""
    if unit == "hour":
        return start(window, unit, tz) + timedelta(hours=1)
    d = date.fromisoformat(window[:10])
    nxt = _month_add(d, 1) if unit == "month" else d + timedelta(days=1)
    return datetime(nxt.year, nxt.month, nxt.day, tzinfo=ZoneInfo(tz))


def of(t: datetime, unit: str, tz: str) -> str:
    """The window an instant falls in."""
    if unit == "hour":
        return t.astimezone(UTC).replace(minute=0, second=0, microsecond=0).strftime(HOUR_FORMAT)
    local = t.astimezone(ZoneInfo(tz)).date()
    return (local.replace(day=1) if unit == "month" else local).isoformat()


def range_start(as_of: str, unit: str, lookback: int, tz: str) -> str:
    """The first window the recompute rebuilds on business date `as_of`: `lookback` units back (ADR-006)."""
    d = date.fromisoformat(as_of)
    if unit == "month":
        return _month_add(d.replace(day=1), -lookback).isoformat()
    if unit == "day":
        return (d - timedelta(days=lookback)).isoformat()
    day_end = datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(tz)) + timedelta(days=1)
    return of(day_end - timedelta(hours=lookback), "hour", tz)


def day_end(business_date: str, tz: str) -> datetime:
    """How far an input has loaded in source time once a business day's delivery window is in silver."""
    return end(business_date, "day", tz)


def caught_up(window: str, unit: str, tz: str, watermark: datetime | None) -> bool:
    """One input has loaded past the window's end (ADR-018). No watermark: it has loaded nothing yet."""
    return watermark is not None and end(window, unit, tz) <= watermark


def feeds_caught_up(window: str, unit: str, tz: str, feeds: dict[str, datetime | None],
                    expected: set[str]) -> tuple[bool, list[str]]:
    """A per-feed input (one file per dealer): each expected feed's newest data time falls in this window or a
    later one. Returns (complete, feeds still missing)."""
    missing = sorted(f for f in expected if (t := feeds.get(f)) is None or of(t, unit, tz) < window)
    return not missing, missing


@dataclass(frozen=True)
class Numbers:
    """What readers of a window see: its rows and the sums of its measures, and whether it was marked complete."""

    rows: int
    sums: tuple[tuple[str, float], ...]
    complete: bool

    def same_numbers(self, other: Numbers, tolerance: float = 1e-6) -> bool:
        a, b = dict(self.sums), dict(other.sums)
        return self.rows == other.rows and all(abs(a.get(k, 0.0) - b.get(k, 0.0)) <= tolerance for k in set(a) | set(b))


def is_restatement(before: Numbers | None, after: Numbers | None, window_end: datetime,
                   last_published: datetime | None, waited_for_feed: bool = False) -> bool:
    """A rebuild restates a window when numbers readers were entitled to treat as final change:
        - the window was published complete and its numbers change (a correction, a late change), or
        - the window, already over when it was published, was flagged incomplete because a member's feed had not
          delivered (a dealer's file, LATE-01), and the rebuild that completes it changes its numbers.
    A provisional number still waiting for the ordinary lag of an input (the next hour's lab file), a period still
    open, or a window seen for the first time is not restated. A complete window whose rows all left it is."""
    if before is None:
        return False
    ended = last_published is not None and window_end <= last_published
    seen_final = before.complete or (ended and waited_for_feed)
    if after is None:  # every row left the window (a correction moved them): what readers saw is gone
        return before.complete
    return after.complete and not before.same_numbers(after) and seen_final
