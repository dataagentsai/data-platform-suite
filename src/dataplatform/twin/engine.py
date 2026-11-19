"""An engine layer's run of the world, attached to the DataTwin harness so the scenarios' `engine` checks run.

The engine runs the same seeded world day by day (ingestion: dataplatform.ingest.daybyday; silver:
dataplatform.silver.daybyday), with whatever adapters the caller chose; the twin only keeps what the checks read:

    verdicts    per incident, the verdict(s) the layer gave it (handled | flagged | deferred | missed)
    totals      per day, whether the layer equalled the oracle's totals that day (bronze, or silver)
    bookmarks   per day, each dataset's bookmarks after the day's run (ingestion)

Domain-free and product-free: it knows the day-by-day reports, not tyres, not engines.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataplatform.ingest.daybyday import Day, Verdict
from dataplatform.silver.daybyday import SilverDay

if TYPE_CHECKING:
    from dataplatform.gold.daybyday import GoldDay

EXPECTS = ("verdict", "totals", "bookmark_moves")


@dataclass
class LayerRun:
    layer: str
    verdicts: dict[str, list[Verdict]] = field(default_factory=dict)
    totals: dict[int, bool] = field(default_factory=dict)
    bookmarks: dict[int, dict[str, dict[str, str]]] = field(default_factory=dict)


def _by_incident(verdicts: Iterable[Verdict]) -> dict[str, list[Verdict]]:
    out: dict[str, list[Verdict]] = {}
    for v in verdicts:
        out.setdefault(v.incident, []).append(v)
    return out


def from_ingestion(days: list[Day], bookmarks: Mapping[int, dict[str, dict[str, str]]] | None = None) -> LayerRun:
    return LayerRun("ingestion", _by_incident(v for d in days for v in d.verdicts),
                    {d.day: d.bronze_ok for d in days}, dict(bookmarks or {}))


def from_silver(days: list[SilverDay]) -> LayerRun:
    return LayerRun("silver", _by_incident(v for d in days for v in d.verdicts), {d.day: d.ok for d in days})


def from_gold(days: list[GoldDay]) -> LayerRun:
    return LayerRun("gold", _by_incident(v for d in days for v in d.verdicts), {d.day: d.ok for d in days})


def check(run: LayerRun, incident: str | None, args: Mapping[str, Any]) -> tuple[bool, str]:
    """One `engine` check on a layer's run: the incident's verdict, a day's totals, or a bookmark's movement."""
    expect = str(args.get("expect", ""))
    if expect == "verdict":
        wanted = args["verdict"] if isinstance(args["verdict"], list) else [args["verdict"]]
        got = run.verdicts.get(incident or "", [])
        ok = bool(got) and all(v.verdict in wanted for v in got)
        return ok, f"{run.layer}: {[(v.detector, v.verdict) for v in got]}, expected {wanted}; " + \
            "; ".join(v.evidence for v in got)
    if expect == "totals":
        day = int(args["day"])
        return run.totals.get(day) is True, f"{run.layer} equals the oracle on day {day}: {run.totals.get(day)}"
    if expect == "bookmark_moves":
        day, ds, key = int(args["day"]), str(args["dataset"]), str(args["key"])
        values = [run.bookmarks.get(d, {}).get(ds, {}).get(key) for d in range(1, day + 1)]
        before, on = values[:-1], values[-1]
        ok = len(before) > 0 and len(set(before)) == 1 and on != before[-1]
        return ok, f"{ds}/{key} bookmark by day: {values}; must hold until day {day} and move on it"
    return False, f"an engine check needs expect: one of {EXPECTS}"
