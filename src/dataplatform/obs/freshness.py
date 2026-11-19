"""Freshness against the manifest's targets (FRESH-01, OPS-01, G10). Pure functions, plus one reader of the store.

Two questions, two measures:

    dataset     how long since the dataset last loaded successfully? A run that found nothing new still counts:
                the platform looked and is current with its source. Basis: the newest succeeded dataset run's end.
    bookmark    is one feed of a dataset behind the others? Each bookmark (a dealer's file, a shard, a cursor) whose
                value is a time is compared with the newest bookmark of the same dataset. One dealer's October file
                missing while every other dealer's arrived is a lagging bookmark (LATE-01), even though the dataset
                as a whole loaded today.

Both use the dataset's own targets: sla_freshness_hours is the warning, sla_freshness_error_hours the error.
Targets come from the metadata store's merged view (so from the manifest), never from code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from dataplatform.meta.port import MetadataStore
from dataplatform.obs.runlog import DatasetRunRow

STATES = ("ok", "warn", "error", "never")
FAILING = ("error",)
TIER_BY_ERROR_HOURS = ((4.0, 1), (48.0, 2))  # error target <= 4 h: tier 1; <= 48 h: tier 2; else tier 3


@dataclass(frozen=True)
class FreshnessTarget:
    source: str
    dataset: str
    warn_hours: float
    error_hours: float
    tier: int
    alert_to: str  # the rota alerts go to (manifest owners.alerts)
    owner: str  # the dataset owner (manifest owners.dataset)

    @property
    def name(self) -> str:
        return f"{self.source}.{self.dataset}"


@dataclass(frozen=True)
class FreshnessResult:
    source: str
    dataset: str
    key: str  # "" for the dataset itself, else the bookmark key
    basis: str  # last_success | bookmark_lag | none
    age_hours: float | None
    warn_hours: float
    error_hours: float
    state: str
    tier: int
    checked_at: datetime

    @property
    def subject(self) -> str:
        name = f"{self.source}.{self.dataset}"
        return f"{name}/{self.key}" if self.key else name


def tier_for(error_hours: float, override: int | None = None) -> int:
    if override is not None:
        return override
    for limit, tier in TIER_BY_ERROR_HOURS:
        if error_hours <= limit:
            return tier
    return 3


def target_from_view(view: dict[str, Any], tiers: dict[str, int] | None = None) -> FreshnessTarget | None:
    """A dataset's targets from the store's merged view; None when the dataset declares no freshness target."""
    settings = view.get("settings") or {}
    if "sla_freshness_hours" not in settings:
        return None
    warn = float(settings["sla_freshness_hours"])
    error = float(settings.get("sla_freshness_error_hours", warn * 2))
    owners = (view.get("manifest") or {}).get("owners") or {}
    name = f"{view['source']}.{view['dataset']}"
    return FreshnessTarget(view["source"], view["dataset"], warn, error, tier_for(error, (tiers or {}).get(name)),
                           str(owners.get("alerts") or owners.get("dataset") or ""),
                           str(owners.get("dataset") or ""))


def targets_from_store(store: MetadataStore, domain: str,
                       tiers: dict[str, int] | None = None) -> list[FreshnessTarget]:
    out = []
    for ref in store.datasets(domain):
        if ref.is_enabled:
            target = target_from_view(store.dataset_view(ref.source, ref.dataset), tiers)
            if target:
                out.append(target)
    return out


def state_of(age_hours: float | None, warn: float, error: float) -> str:
    if age_hours is None:
        return "never"
    if age_hours > error:
        return "error"
    return "warn" if age_hours > warn else "ok"


def _hours(a: datetime, b: datetime) -> float:
    return round((a - b).total_seconds() / 3600.0, 3)


def last_success(datasets: list[DatasetRunRow], now: datetime) -> dict[tuple[str, str], datetime]:
    out: dict[tuple[str, str], datetime] = {}
    for d in datasets:
        if d.status == "succeeded" and d.ended_at is not None and d.ended_at <= now:
            key = (d.source, d.dataset)
            out[key] = max(out.get(key, d.ended_at), d.ended_at)
    return out


def _as_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else None


def check(targets: list[FreshnessTarget], datasets: list[DatasetRunRow],
          bookmarks: dict[tuple[str, str], dict[str, str]], now: datetime) -> list[FreshnessResult]:
    """Every target's dataset freshness, then each time-valued bookmark's lag behind its newest sibling."""
    seen = last_success(datasets, now)
    out = []
    for t in targets:
        at = seen.get((t.source, t.dataset))
        age = _hours(now, at) if at else None
        out.append(FreshnessResult(t.source, t.dataset, "", "last_success" if at else "none", age,
                                   t.warn_hours, t.error_hours, state_of(age, t.warn_hours, t.error_hours),
                                   t.tier, now))
        times = {k: v for k, raw in sorted(bookmarks.get((t.source, t.dataset), {}).items())
                 if (v := _as_time(raw)) is not None}
        if len(times) < 2:  # noqa: PLR2004 - a lag needs a sibling to lag behind
            continue
        newest = max(times.values())
        for key, value in times.items():
            lag = _hours(newest, value)
            out.append(FreshnessResult(t.source, t.dataset, key, "bookmark_lag", lag, t.warn_hours,
                                       t.error_hours, state_of(lag, t.warn_hours, t.error_hours), t.tier, now))
    return out


def read_bookmarks(store: MetadataStore, targets: list[FreshnessTarget]) -> dict[tuple[str, str], dict[str, str]]:
    return {(t.source, t.dataset): store.bookmarks(t.source, t.dataset) for t in targets}


def next_due(targets: list[FreshnessTarget], datasets: list[DatasetRunRow], now: datetime) -> datetime | None:
    """When the next dataset would pass its error target if nothing loads before then: the one moment a lateness
    check is worth running (ADR-024 rule 4, no fixed interval). None when nothing can become late (never loaded
    datasets are already reported as `never`; a bookmark lag does not grow with the clock)."""
    seen = last_success(datasets, now)
    due = [at + timedelta(hours=t.error_hours) for t in targets if (at := seen.get((t.source, t.dataset)))]
    later = [d for d in due if d > now]
    return min(later) if later else None
