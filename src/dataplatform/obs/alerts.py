"""Alerts that people still read (OPS-01, OPS-06, DPC-0019, DPC-0056). Rules are configuration; evaluation is pure.

The noise controls, each one a test row in tests/dataplatform/test_obs_alerts.py:

    change of state   a subject (a dataset, a dataset's bookmark, a dataset's detector) alerts when it starts failing
                      and resolves when it recovers. Still failing on the next check: nothing new is sent.
    one per day       a rule sends at most one alert per route per business day. A second subject failing the same
                      day, or the same one flapping, joins that day's alert (its `subjects`); nothing new is sent.
    routed by tier    the data product's tier picks the channel (page, ticket, digest); the dataset's rota or owner
                      is who it goes to. Tier comes from the alert config, else from the freshness error target.
    snoozes expire    a snooze needs a reason, an owner and an expiry no further than `snooze_max_days` away. While
                      it holds, the subject's alert is held back (counted as snoozed). After it expires, a subject
                      still failing alerts again.

    evaluate(config, signals, state, now, scope) -> Evaluation(state, sent, events)

One alert engine (ADR-024 rule 3): owners record severities in their step rows and never send alerts. Signals are
pushed (rule 4): a finished run evaluates its own datasets (`Scope` names them, so subjects of datasets that did not
run are left as they are), and a lateness check evaluates freshness alone.

`state` is plain data (dicts), so any store can keep it between checks (the memory adapter today; a table later).
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

SIGNALS = ("run_failed", "freshness", "detector", "check_failed", "heartbeat")
CHANNELS = ("page", "ticket", "digest")


class AlertConfigError(ValueError):
    """An alert rule, route or snooze the config may not hold."""


@dataclass(frozen=True)
class Rule:
    id: str
    signal: str
    title: str
    enabled: bool = True


@dataclass(frozen=True)
class Route:
    channel: str
    to: str  # "owners.alerts" | "owners.dataset" | a literal address


@dataclass(frozen=True)
class Snooze:
    rule: str
    subject: str  # a glob over subjects, e.g. "erp.material" or "partner.dealer_sellout/*"
    reason: str
    owner: str
    until: datetime


@dataclass(frozen=True)
class AlertConfig:
    rules: tuple[Rule, ...]
    routes: dict[int, Route]
    tiers: dict[str, int] = field(default_factory=dict)  # dataset (source.dataset) -> tier override
    snoozes: tuple[Snooze, ...] = ()
    snooze_max_days: int = 30
    business_timezone: str = "UTC"

    def rule(self, rule_id: str) -> Rule:
        for r in self.rules:
            if r.id == rule_id:
                return r
        raise AlertConfigError(f"no alert rule {rule_id!r}")


@dataclass(frozen=True)
class Signal:
    """One subject's verdict for one rule's signal at one check."""

    signal: str
    subject: str
    failing: bool
    tier: int
    owners: dict[str, str]  # {"alerts": rota, "dataset": owner}
    run_id: str = ""
    detail: str = ""


@dataclass
class Alert:
    """One message to one route: a rule's alert for a business day, with every subject that joined it."""

    rule: str
    day: str
    channel: str
    to: str
    first_at: datetime
    subjects: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class Event:
    at: datetime
    rule: str
    subject: str
    kind: str  # fired | grouped | snoozed | resolved
    channel: str
    to: str
    run_id: str = ""


@dataclass(frozen=True)
class Scope:
    """What one evaluation speaks for: these signal kinds, and (if given) only subjects of these datasets. A subject
    outside the scope keeps its state; inside it, a subject absent from the signals has recovered."""

    signals: frozenset[str] = frozenset(SIGNALS)
    datasets: frozenset[str] | None = None

    def covers(self, signal: str, subject: str) -> bool:
        return signal in self.signals and (self.datasets is None or subject.split("/", 1)[0] in self.datasets)


@dataclass
class Evaluation:
    state: dict[str, Any]
    sent: list[Alert]  # new messages this check (fired alerts and resolutions)
    events: list[Event]


# ------------------------------------------------------------------------------------------------ config


def _need(doc: dict[str, Any], key: str, where: str) -> Any:
    if key not in doc or doc[key] in (None, ""):
        raise AlertConfigError(f"{where}: '{key}' is required")
    return doc[key]


def _aware(value: Any, where: str) -> datetime:
    at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if at.tzinfo is None:
        raise AlertConfigError(f"{where}: 'until' needs a time zone")
    return at


def parse_snooze(doc: dict[str, Any], now: datetime, max_days: int) -> Snooze:
    where = f"snooze {doc.get('rule')}/{doc.get('subject')}"
    reason = str(_need(doc, "reason", where))
    if len(reason.strip()) < 10:  # noqa: PLR2004 - the same bar as a watermark reset (meta.port.MIN_REASON)
        raise AlertConfigError(f"{where}: a snooze needs a real reason")
    until = _aware(_need(doc, "until", where), where)
    if until - now > timedelta(days=max_days):
        raise AlertConfigError(f"{where}: expires more than {max_days} days ahead")
    return Snooze(str(_need(doc, "rule", where)), str(_need(doc, "subject", where)), reason,
                  str(_need(doc, "owner", where)), until)


def parse_config(doc: dict[str, Any], now: datetime) -> AlertConfig:
    rules = []
    for r in _need(doc, "rules", "alert config"):
        rule = Rule(str(_need(r, "id", "rule")), str(_need(r, "signal", "rule")), str(r.get("title", r["id"])),
                    bool(r.get("enabled", True)))
        if rule.signal not in SIGNALS:
            raise AlertConfigError(f"rule {rule.id}: signal {rule.signal!r} is not one of {SIGNALS}")
        rules.append(rule)
    if len({r.id for r in rules}) != len(rules):
        raise AlertConfigError("rule ids must be unique")
    routes = {}
    for tier, route in _need(doc, "routes", "alert config").items():
        channel = str(_need(route, "channel", f"route {tier}"))
        if channel not in CHANNELS:
            raise AlertConfigError(f"route {tier}: channel {channel!r} is not one of {CHANNELS}")
        routes[int(tier)] = Route(channel, str(_need(route, "to", f"route {tier}")))
    for tier in (1, 2, 3):
        if tier not in routes:
            raise AlertConfigError(f"no route for tier {tier}")
    max_days = int(doc.get("snooze_max_days", 30))
    known = {r.id for r in rules}
    snoozes = tuple(parse_snooze(s, now, max_days) for s in doc.get("snoozes") or [])
    for s in snoozes:
        if s.rule not in known:
            raise AlertConfigError(f"snooze names rule {s.rule!r}, which the config does not have")
    return AlertConfig(tuple(rules), routes, {str(k): int(v) for k, v in (doc.get("tiers") or {}).items()},
                       snoozes, max_days, str(doc.get("business_timezone", "UTC")))


def load_config(path: str | Path, now: datetime) -> AlertConfig:
    import yaml  # noqa: PLC0415 - local only, as in manifest.py: the platform reads config from the store

    return parse_config(yaml.safe_load(Path(path).read_text()), now)


DEFAULT_RULES = Path(__file__).with_name("rules") / "alerts.yaml"

# ------------------------------------------------------------------------------------------------ evaluation


def _zone(name: str) -> tzinfo:
    return ZoneInfo(name)


def business_day(at: datetime, tz: str) -> str:
    return at.astimezone(_zone(tz)).date().isoformat()


def route_for(config: AlertConfig, signal: Signal) -> Route:
    route = config.routes[signal.tier]
    if route.to.startswith("owners."):
        who = signal.owners.get(route.to.split(".", 1)[1]) or signal.owners.get("dataset") or "unrouted"
        return replace(route, to=who)
    return route


def snoozed(config: AlertConfig, rule: str, subject: str, now: datetime) -> Snooze | None:
    for s in config.snoozes:
        if s.rule == rule and now < s.until and fnmatch.fnmatchcase(subject, s.subject):
            return s
    return None


def _key(rule: str, subject: str) -> str:
    return f"{rule}|{subject}"


def evaluate(config: AlertConfig, signals: list[Signal], state: dict[str, Any], now: datetime,
             scope: Scope | None = None) -> Evaluation:
    """One check. Subjects a rule knew as failing that are absent from this check's signals have recovered (within
    the scope: by default every signal and every subject)."""
    scope = scope or Scope()
    subjects: dict[str, Any] = {k: dict(v) for k, v in (state.get("subjects") or {}).items()}
    day = business_day(now, config.business_timezone)
    daily: dict[str, Any] = {k: v for k, v in (state.get("daily") or {}).items() if k.split("|")[1] == day}
    by_rule: dict[str, dict[str, Signal]] = {r.id: {} for r in config.rules if r.enabled and r.signal in scope.signals}
    signal_of = {r.id: r.signal for r in config.rules}
    for sig in signals:
        for r in config.rules:
            if r.id in by_rule and r.signal == sig.signal:
                by_rule[r.id][sig.subject] = sig
    sent: list[Alert] = []
    events: list[Event] = []
    for rule_id, current in by_rule.items():
        failing = {s: sig for s, sig in current.items() if sig.failing}
        for _, sig in sorted(failing.items()):
            _fail(config, rule_id, sig, subjects, daily, day, now, sent, events)
        for key in sorted(k for k in subjects if k.startswith(f"{rule_id}|")):
            subject = key.split("|", 1)[1]
            if subject not in failing and scope.covers(signal_of[rule_id], subject):
                _recover(rule_id, subject, subjects.pop(key), now, sent, events, day)
    return Evaluation({"subjects": subjects, "daily": daily}, sent, events)


def _fail(config: AlertConfig, rule_id: str, sig: Signal, subjects: dict[str, Any], daily: dict[str, Any],
          day: str, now: datetime, sent: list[Alert], events: list[Event]) -> None:
    key = _key(rule_id, sig.subject)
    entry = subjects.setdefault(key, {"since": now.isoformat(), "notified": False})
    if entry["notified"]:
        return  # still failing: change of state only
    route = route_for(config, sig)
    entry.update(channel=route.channel, to=route.to)
    if snoozed(config, rule_id, sig.subject, now):
        events.append(Event(now, rule_id, sig.subject, "snoozed", route.channel, route.to, sig.run_id))
        return
    entry["notified"] = True
    group = f"{rule_id}|{day}|{route.channel}|{route.to}"
    item = {"subject": sig.subject, "run_id": sig.run_id, "detail": sig.detail}
    if group in daily:
        daily[group]["subjects"].append(item)
        events.append(Event(now, rule_id, sig.subject, "grouped", route.channel, route.to, sig.run_id))
        return
    daily[group] = {"first_at": now.isoformat(), "subjects": [item]}
    sent.append(Alert(rule_id, day, route.channel, route.to, now, [item]))
    events.append(Event(now, rule_id, sig.subject, "fired", route.channel, route.to, sig.run_id))


def _recover(rule_id: str, subject: str, entry: dict[str, Any], now: datetime, sent: list[Alert],
             events: list[Event], day: str) -> None:
    if not entry.get("notified"):
        return  # never alerted (snoozed throughout): nothing to resolve
    channel, to = str(entry.get("channel", "")), str(entry.get("to", ""))
    events.append(Event(now, rule_id, subject, "resolved", channel, to))
    sent.append(Alert(f"{rule_id}:resolved", day, channel, to, now, [{"subject": subject, "run_id": "",
                                                                      "detail": "recovered"}]))


def counts_per_rule_per_day(events: list[Event], tz: str = "UTC") -> dict[tuple[str, str], int]:
    """OPS-06 evidence: alerts sent per rule per business day (fired only; grouped and snoozed are not sent)."""
    out: dict[tuple[str, str], int] = {}
    for e in events:
        if e.kind == "fired":
            key = (e.rule, business_day(e.at, tz))
            out[key] = out.get(key, 0) + 1
    return out
