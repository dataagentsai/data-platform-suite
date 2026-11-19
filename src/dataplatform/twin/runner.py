"""The DataTwin scenario runner and its oracle checks (G10, TEST-05, SIM-01).

    harness = Harness.build(slice_dir, days=14)          # the world for 14 days + the platform's run log for them
    results = [run(s, harness) for s in load_all()]      # passed | pending (with the layers it waits for) | failed

The harness plays the world once (seeded, so every run is the same) and keeps each day's oracle. The platform's run
log for the `obs` checks comes from the fixture runner (obs/fixture.py), and every check on that layer says so
(`fixture: true`). An engine layer's own run of the same world (twin/engine.py) is attached with `attach`; its
`engine` checks then run (ingestion, silver and gold). DES (W1e) checks stay pending.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_runlog import StampingRunLog
from dataplatform.adapters.memory_sources import MemoryFileDrop, MemoryRecords, MemorySourceDb, MemoryStream
from dataplatform.meta import loader
from dataplatform.obs import dashboards
from dataplatform.obs.alerts import DEFAULT_RULES, Event, business_day, load_config
from dataplatform.obs.clock import ManualClock
from dataplatform.obs.fixture import FixtureRunner
from dataplatform.obs.trace import LocalTracer
from dataplatform.twin.engine import LayerRun
from dataplatform.twin.engine import check as engine_check
from dataplatform.twin.scenario import AVAILABLE, LAYERS, Check, Scenario
from dataplatform.world import dwd as dwd_loader
from dataplatform.world.domains import model_for
from dataplatform.world.engine import Sinks, World

STATUSES = ("passed", "failed", "pending")


@dataclass(frozen=True)
class CheckResult:
    id: str
    layer: str
    status: str
    detail: str = ""
    fixture: bool = False


@dataclass(frozen=True)
class ScenarioResult:
    scenario: str
    status: str  # passed | pending | failed
    checks: tuple[CheckResult, ...]
    waits_for: tuple[str, ...] = ()


@dataclass
class Harness:
    calendar: dict[str, dict[str, Any]]
    reports: dict[int, dict[str, Any]]
    start: datetime
    tz: str
    fixture: FixtureRunner
    tables: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    engine: dict[str, LayerRun] = field(default_factory=dict)

    @property
    def available(self) -> frozenset[str]:
        return AVAILABLE | frozenset(self.engine)

    def attach(self, run: LayerRun) -> Harness:
        self.engine[run.layer] = run
        return self

    @property
    def events(self) -> list[Event]:
        return self.fixture.monitor.events

    def date(self, day: int) -> str:
        """The business date of a world day."""
        return business_day(self.start + timedelta(days=day - 1, hours=12), self.tz)

    @classmethod
    def build(cls, slice_dir: str | Path, days: int = 14, scale: float = 0.02,
              out_dir: str | Path | None = None) -> Harness:
        root = Path(slice_dir)
        dwd = dwd_loader.load(root)
        model = model_for(dwd, scale)
        start = model.clock.day_start(1)
        clock = ManualClock(start)
        store = StampingRunLog(MemoryMetaStore(), clock)
        loader.load(store, root / "manifest")
        # the world's people record corrections in the metadata store (ADR-017, W1b), so the store is bound as a sink
        sinks = Sinks(MemorySourceDb(), MemoryFileDrop(), MemoryRecords(), MemoryStream(), store)
        out = Path(out_dir) if out_dir else Path(tempfile.mkdtemp(prefix="twin-"))
        reports = {r["day"]: r for r in World(dwd, model, sinks, out).run(days)}
        config = load_config(DEFAULT_RULES, start)
        fixture = FixtureRunner(store, store, LocalTracer(), clock, dwd.platform["domain"], "dev", start,
                                dwd.world.get("calendar", []), config)
        tables: dict[str, list[dict[str, Any]]] = {}
        for day in sorted(reports):
            tables = fixture.run_day(day, reports[day])
        calendar = {c["id"]: c for c in dwd.world.get("calendar", [])}
        return cls(calendar, reports, start, config.business_timezone, fixture, tables)


# ------------------------------------------------------------------------------------------------ check kinds


def _dig(doc: Any, path: list[Any]) -> Any:
    for key in path:
        doc = doc[key]
    return doc


def check_planted(s: Scenario, c: Check, h: Harness) -> tuple[bool, str]:
    entry = h.calendar.get(s.incident or "")
    if not entry:
        return False, f"no calendar entry {s.incident!r} in the world"
    day, detector = int(entry["day"]), entry["expect"]["detector"]
    found = [f for f in h.reports.get(day, {}).get("findings", []) if f["incident"] == s.incident]
    ok = bool(found) and all(f["detector"] == detector for f in found)
    return ok, f"day {day}: {len(found)} finding(s) for {detector}"


def check_oracle(s: Scenario, c: Check, h: Harness) -> tuple[bool, str]:
    a = c.args
    value = _dig(h.reports[int(a["day"])], list(a["path"]))
    if "equals" in a:
        return value == a["equals"], f"{a['path']} = {value!r}, expected {a['equals']!r}"
    if "min" in a:
        return value >= a["min"], f"{a['path']} = {value!r}, expected at least {a['min']!r}"
    if "length" in a:
        return len(value) == a["length"], f"{a['path']} has {len(value)}, expected {a['length']}"
    return False, "an oracle check needs equals, min or length"


def check_on_dashboard(s: Scenario, c: Check, h: Harness) -> tuple[bool, str]:
    a = c.args
    board = dashboards.by_id(str(a["dashboard"]))
    widget = next((w for w in board.widgets if w.id == a["widget"]), None)
    if widget is None:
        return False, f"no widget {a['widget']} on {board.id}"
    rows = dashboards.evaluate(board.query(widget.query), h.tables)
    want = dict(a.get("match") or {})
    if "day" in a:
        want["day"] = h.date(int(a["day"]))
    hits = [r for r in rows if all(r.get(k) == v for k, v in want.items())]
    return bool(hits), f"{board.id}/{widget.id}: {len(hits)} row(s) match {want}"


def check_one_alert(s: Scenario, c: Check, h: Harness) -> tuple[bool, str]:
    a = c.args
    mine = [e for e in h.events if e.rule == a["rule"] and e.subject == a["subject"]]
    fired = [e for e in mine if e.kind == "fired"]
    resolved = [e for e in mine if e.kind == "resolved"]
    days = (sorted(business_day(e.at, h.tz) for e in fired), sorted(business_day(e.at, h.tz) for e in resolved))
    want = ([h.date(int(a["fired_day"]))], [h.date(int(a["resolved_day"]))] if "resolved_day" in a else days[1])
    return days == want, f"fired on {days[0]}, resolved on {days[1]}; expected {want[0]}, {want[1]}"


def check_engine(s: Scenario, c: Check, h: Harness) -> tuple[bool, str]:
    return engine_check(h.engine[c.layer], s.incident, c.args)


CHECKS = {"planted": check_planted, "oracle": check_oracle, "on_dashboard": check_on_dashboard,
          "one_alert": check_one_alert, "engine": check_engine}


def run(s: Scenario, h: Harness, available: frozenset[str] | None = None) -> ScenarioResult:
    """Run every check whose layer is available (default: what the harness has); the rest are pending."""
    available = h.available if available is None else available
    results = []
    for c in s.checks:
        no_run = c.kind == "engine" and c.layer not in h.engine  # a layer said available but never attached
        if "pending" in c.args:  # the layer exists but owes what the check needs: said, not failed
            results.append(CheckResult(c.id, c.layer, "pending", f"waits: {c.args['pending']}"))
            continue
        if c.layer not in available or c.kind not in CHECKS or no_run:
            results.append(CheckResult(c.id, c.layer, "pending", f"waits for {c.waits_for}: {c.args.get('says')}"))
            continue
        try:
            ok, detail = CHECKS[c.kind](s, c, h)
        except (KeyError, IndexError, TypeError) as exc:
            ok, detail = False, f"{type(exc).__name__}: {exc}"
        results.append(CheckResult(c.id, c.layer, "passed" if ok else "failed", detail, c.layer == "obs"))
    if any(r.status == "failed" for r in results):
        status = "failed"
    else:
        status = "pending" if any(r.status == "pending" for r in results) else "passed"
    waits = sorted({r.layer for r in results if r.status == "pending"}, key=list(LAYERS).index)
    return ScenarioResult(s.id, status, tuple(results), tuple(waits))


def unexplained_alerts(scenarios: list[Scenario], h: Harness) -> list[Event]:
    """SIM-01 for alerts: every alert that fired belongs to a scenario's expected alert (none on a clean day)."""
    expected = {(c.args["rule"], c.args["subject"]) for s in scenarios for c in s.checks if c.kind == "one_alert"}
    return [e for e in h.events if e.kind == "fired" and (e.rule, e.subject) not in expected]
