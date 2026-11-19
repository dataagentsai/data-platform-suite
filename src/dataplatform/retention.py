"""Retention (ADR-029, B8): how long each copy is kept, so a fault found late still has a correct copy to reload from.
Pure functions over dates and settings; the landing adapter or the cloud's lifecycle rules carry the actions out.

    Retention.from_settings   the dataset's retention settings, with ADR-029's defaults
    landing_actions           keep, archive or delete each delivery in landing
    bronze_cutoff             the oldest window bronze must keep; older windows go once a baseline batch exists

Defaults (ADR-029): landing hot 30 days; landing archive as long as bronze, but 90 days when the dataset holds personal
data (landing holds it in clear); bronze 1095 days; a baseline batch before bronze ages out; 7 days of table versions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

PERSONAL_ARCHIVE_DAYS = 90
ISO_DAY = len("2026-11-09")


@dataclass(frozen=True)
class Retention:
    landing_hot_days: int = 30
    landing_archive_days: int = 1095
    bronze_days: int = 1095
    bronze_baseline: bool = True
    version_history_days: int = 7

    @classmethod
    def from_settings(cls, settings: dict[str, Any], personal: bool = False) -> Retention:
        def num(key: str, default: int) -> int:
            value = settings.get(key)
            return int(value) if value not in (None, "") else default

        bronze = num("bronze_retention_days", cls.bronze_days)
        archive_default = PERSONAL_ARCHIVE_DAYS if personal else bronze
        archive = num("landing_archive_days", archive_default)
        if personal:  # never longer than the personal-data limit, whatever the setting says
            archive = min(archive, PERSONAL_ARCHIVE_DAYS)
        return cls(
            landing_hot_days=num("landing_hot_days", cls.landing_hot_days),
            landing_archive_days=archive,
            bronze_days=bronze,
            bronze_baseline=str(settings.get("bronze_baseline", "true")).lower() != "false",
            version_history_days=num("version_history_days", cls.version_history_days),
        )


def window_day(window: str) -> date:
    """A delivery window's first day: 2026-11-09, 2026-11-09T10, 2026-11 (the month's first day)."""
    text = window[:10]
    return date.fromisoformat(text if len(text) == ISO_DAY else f"{text[:7]}-01")


def landing_actions(deliveries: list[tuple[str, str, str]], taken: set[tuple[str, str]], today: date,
                    r: Retention) -> list[tuple[str, str, str]]:
    """For each (window, name, place) with place 'hot' or 'archive': (window, name, action) with action keep, archive
    or delete. A delivery the lander never took (not in `taken`) is always kept: it may be the only copy."""
    out = []
    for window, name, place in deliveries:
        age = (today - window_day(window)).days
        if (window, name) not in taken:
            action = "keep"
        elif place == "hot" and age > r.landing_hot_days + r.landing_archive_days:
            action = "delete"
        elif place == "hot" and age > r.landing_hot_days:
            action = "archive"
        elif place == "archive" and age > r.landing_hot_days + r.landing_archive_days:
            action = "delete"
        else:
            action = "keep"
        out.append((window, name, action))
    return out


def bronze_cutoff(today: date, r: Retention) -> date:
    """Windows before this day may leave bronze, and only after a baseline batch holds silver's state as of it
    (ADR-029), unless the dataset keeps bronze forever (bronze_baseline false and a very long retention)."""
    return today - timedelta(days=r.bronze_days)


PERSONAL_CLASSES = ("restricted", "personal", "pii", "sensitive")
TAKEN = ("loaded", "skipped_duplicate", "void")  # the lander is done with these; held or rejected ones wait for a fix


def apply_landing_retention(archive: Any, store: Any, source: str, dataset: str, today: date) -> dict[str, int]:
    """Carry out landing_actions for one dataset through a landing archive adapter (LandingArchive locally, lifecycle
    rules on a cloud). Personal data is decided from the contract's column classes. Returns counts per action."""
    view = store.dataset_view(source, dataset)
    personal = any(str(c.get("classification", "")).lower() in PERSONAL_CLASSES for c in view["columns"])
    r = Retention.from_settings(view.get("settings") or {}, personal)
    taken = {(f.window, f.file_name) for f in store.dataset_files(source, dataset) if f.status in TAKEN}
    listing = archive.listing(source, dataset)
    places = {(w, n): p for w, n, p in listing}
    counts = {"keep": 0, "archive": 0, "delete": 0}
    for window, name, action in landing_actions(listing, taken, today, r):
        counts[action] += 1
        if action == "archive":
            archive.archive(source, dataset, window, name)
        elif action == "delete":
            archive.delete(source, dataset, window, name, places[(window, name)])
    return counts
