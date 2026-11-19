"""The simulated clock (DPC-0032: jobs read time only through the clock port). A world day is one business day in
the business time zone; day 1 starts at the DWD's clock.start."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc


class SimClock:
    def __init__(self, start: str, business_tz: str):
        self.tz = ZoneInfo(business_tz)
        self.start = datetime.fromisoformat(start).astimezone(self.tz)

    def day_start(self, day: int) -> datetime:
        """00:00 business time on the given world day, as an aware datetime."""
        d = self.start.date() + timedelta(days=day - 1)
        return datetime(d.year, d.month, d.day, tzinfo=self.tz)

    def day_end(self, day: int) -> datetime:
        return self.day_start(day + 1)

    def date(self, day: int) -> date:
        return self.day_start(day).date()

    def at(self, day: int, hour: int, minute: int = 0, second: float = 0) -> datetime:
        return self.day_start(day) + timedelta(hours=hour, minutes=minute, seconds=second)

    def day_of(self, instant: datetime) -> int:
        """Which world day an instant falls on, in business time."""
        return (instant.astimezone(self.tz).date() - self.start.date()).days + 1


def iso(dt: datetime) -> str:
    """Every timestamp the world writes: UTC, ISO 8601, second precision."""
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()
