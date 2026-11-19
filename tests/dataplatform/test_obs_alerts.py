"""Alert noise controls (OPS-06, DPC-0056) and routing (OPS-01, DPC-0019), table-driven over the pure evaluator.

Each row is a timeline: a list of checks, each (hours after T0, the subjects failing for the rule). The row's expect
is what was sent, as (hour, rule, subject(s), channel, to). OPS-06's own test is the first rows: fail one check for a
day, exactly one alert fires; it recovers, exactly one resolves; a snooze expires and the alert returns.
"""

from datetime import UTC, datetime, timedelta

import pytest

from dataplatform.obs.alerts import (
    DEFAULT_RULES,
    AlertConfigError,
    Scope,
    Signal,
    counts_per_rule_per_day,
    evaluate,
    load_config,
    parse_config,
)

T0 = datetime(2026, 11, 9, 0, 30, tzinfo=UTC)  # 06:00 business time (Asia/Kolkata)
OWNERS = {"alerts": "erp-rota", "dataset": "sales@x"}
BASE = {"business_timezone": "Asia/Kolkata",
        "rules": [{"id": "fresh", "signal": "freshness"}, {"id": "det", "signal": "detector"}],
        "routes": {1: {"channel": "page", "to": "owners.alerts"}, 2: {"channel": "ticket", "to": "owners.alerts"},
                   3: {"channel": "digest", "to": "owners.dataset"}}}


def cfg(**over):
    return parse_config({**BASE, **over}, T0)


def sig(subject, failing, tier=1, signal="freshness"):
    return Signal(signal, subject, failing, tier, OWNERS, "run-1")


def play(config, timeline, known=("a", "b")):
    """timeline: [(hour, {failing subjects})]; every known subject is signalled each check."""
    state, sent = {}, []
    for hour, failing in timeline:
        now = T0 + timedelta(hours=hour)
        ev = evaluate(config, [sig(s, s in failing) for s in known], state, now)
        state = ev.state
        sent += [(hour, a.rule, tuple(x["subject"] for x in a.subjects), a.channel, a.to) for a in ev.sent]
    return sent


SNOOZE = {"rule": "fresh", "subject": "a", "reason": "source down for planned upgrade", "owner": "ops@x",
          "until": (T0 + timedelta(hours=10)).isoformat()}

TIMELINES = [
    ("OPS-06: failing for a day fires once", cfg(), [(h, {"a"}) for h in range(0, 24, 2)],
     [(0, "fresh", ("a",), "page", "erp-rota")]),
    ("OPS-06: recovery resolves once", cfg(), [(0, {"a"}), (2, {"a"}), (4, set()), (6, set())],
     [(0, "fresh", ("a",), "page", "erp-rota"), (4, "fresh:resolved", ("a",), "page", "erp-rota")]),
    ("a second subject the same day joins the day's alert", cfg(), [(0, {"a"}), (2, {"a", "b"})],
     [(0, "fresh", ("a",), "page", "erp-rota")]),
    ("flapping the same day sends no second alert", cfg(), [(0, {"a"}), (1, set()), (2, {"a"})],
     [(0, "fresh", ("a",), "page", "erp-rota"), (1, "fresh:resolved", ("a",), "page", "erp-rota")]),
    ("failing again on a new business day alerts again", cfg(), [(0, {"a"}), (1, set()), (24, {"a"})],
     [(0, "fresh", ("a",), "page", "erp-rota"), (1, "fresh:resolved", ("a",), "page", "erp-rota"),
      (24, "fresh", ("a",), "page", "erp-rota")]),
    ("business day, not UTC day: 23:00 and 01:00 IST are two days", cfg(),
     [(17, {"a"}), (18, set()), (19, {"a"})],  # 23:30 IST day 1, then 01:30 IST day 2
     [(17, "fresh", ("a",), "page", "erp-rota"), (18, "fresh:resolved", ("a",), "page", "erp-rota"),
      (19, "fresh", ("a",), "page", "erp-rota")]),
    ("OPS-06: a snooze holds the alert", cfg(snoozes=[SNOOZE]), [(0, {"a"}), (4, {"a"})], []),
    ("OPS-06: an expired snooze lets a still-failing alert return", cfg(snoozes=[SNOOZE]),
     [(0, {"a"}), (4, {"a"}), (11, {"a"}), (12, {"a"})], [(11, "fresh", ("a",), "page", "erp-rota")]),
    ("a snoozed subject that recovers sends no resolution", cfg(snoozes=[SNOOZE]), [(0, {"a"}), (4, set())], []),
    ("a snooze is per subject: others still alert", cfg(snoozes=[SNOOZE]), [(0, {"a", "b"})],
     [(0, "fresh", ("b",), "page", "erp-rota")]),
    ("a disabled rule sends nothing", cfg(rules=[{"id": "fresh", "signal": "freshness", "enabled": False}]),
     [(0, {"a"})], []),
]


@pytest.mark.parametrize(("case", "config", "timeline", "expect"), TIMELINES, ids=[c[0] for c in TIMELINES])
def test_noise_controls(case, config, timeline, expect):
    assert play(config, timeline) == expect


@pytest.mark.parametrize(
    ("tier", "owners", "expect"),
    [(1, OWNERS, ("page", "erp-rota")), (2, OWNERS, ("ticket", "erp-rota")), (3, OWNERS, ("digest", "sales@x")),
     (1, {"dataset": "sales@x"}, ("page", "sales@x")), (1, {}, ("page", "unrouted"))],
)
def test_routed_by_tier(tier, owners, expect):
    ev = evaluate(cfg(), [Signal("freshness", "a", True, tier, owners)], {}, T0)
    assert (ev.sent[0].channel, ev.sent[0].to) == expect


def test_routes_split_a_rules_day_by_recipient():
    ev = evaluate(cfg(), [sig("a", True, 1), sig("b", True, 3)], {}, T0)
    assert sorted((a.channel, a.to) for a in ev.sent) == [("digest", "sales@x"), ("page", "erp-rota")]


def test_alert_carries_run_id_and_rules_stay_apart():
    ev = evaluate(cfg(), [sig("a", True), sig("x/DEL-01", True, signal="detector")], {}, T0)
    assert sorted(a.rule for a in ev.sent) == ["det", "fresh"]
    assert {a.subjects[0]["run_id"] for a in ev.sent} == {"run-1"}


def test_counts_per_rule_per_day_is_the_evidence():
    state, events = {}, []
    for hour, failing in [(0, {"a"}), (2, {"a", "b"}), (3, set()), (25, {"a"})]:
        ev = evaluate(cfg(), [sig(s, s in failing) for s in ("a", "b")], state, T0 + timedelta(hours=hour))
        state, events = ev.state, events + ev.events
    assert counts_per_rule_per_day(events, "Asia/Kolkata") == {("fresh", "2026-11-09"): 1, ("fresh", "2026-11-10"): 1}
    assert [e.kind for e in events][:2] == ["fired", "grouped"]


REFUSALS = [
    ("snooze without a reason", {"snoozes": [{**SNOOZE, "reason": ""}]}),
    ("snooze with a token reason", {"snoozes": [{**SNOOZE, "reason": "because"}]}),
    ("snooze without an owner", {"snoozes": [{k: v for k, v in SNOOZE.items() if k != "owner"}]}),
    ("snooze without an expiry", {"snoozes": [{k: v for k, v in SNOOZE.items() if k != "until"}]}),
    ("snooze expiry without a time zone", {"snoozes": [{**SNOOZE, "until": "2026-11-10T00:00:00"}]}),
    ("snooze too far ahead", {"snoozes": [{**SNOOZE, "until": (T0 + timedelta(days=31)).isoformat()}]}),
    ("snooze names an unknown rule", {"snoozes": [{**SNOOZE, "rule": "nope"}]}),
    ("unknown signal", {"rules": [{"id": "x", "signal": "vibes"}]}),
    ("duplicate rule ids", {"rules": [{"id": "x", "signal": "freshness"}, {"id": "x", "signal": "detector"}]}),
    ("a tier without a route", {"routes": {1: {"channel": "page", "to": "owners.alerts"}}}),
    ("unknown channel", {"routes": {**BASE["routes"], 1: {"channel": "carrier pigeon", "to": "x"}}}),
]


@pytest.mark.parametrize(("case", "over"), REFUSALS, ids=[c[0] for c in REFUSALS])
def test_config_refusals(case, over):
    with pytest.raises(AlertConfigError):
        cfg(**over)


def test_default_rules_load():
    config = load_config(DEFAULT_RULES, T0)
    assert {r.signal for r in config.rules} == {"run_failed", "freshness", "detector", "check_failed", "heartbeat"}
    assert config.business_timezone == "Asia/Kolkata"


@pytest.mark.parametrize(
    ("case", "scope", "expect"),
    [
        ("no scope: an absent subject has recovered", None, ["det:resolved", "fresh:resolved"]),
        ("a run's scope leaves datasets that did not run alone", Scope(frozenset({"detector"}), frozenset({"b"})),
         []),
        ("a run's scope resolves its own dataset's detector", Scope(frozenset({"detector"}), frozenset({"a"})),
         ["det:resolved"]),
        ("a lateness check speaks for freshness only", Scope(frozenset({"freshness"})), ["fresh:resolved"]),
    ],
)
def test_pushed_evaluations_keep_to_their_scope(case, scope, expect):
    config = cfg()
    first = evaluate(config, [sig("a", True), sig("a/DEL-01", True, signal="detector")], {}, T0)
    later = evaluate(config, [], first.state, T0 + timedelta(hours=1), scope)
    assert sorted(a.rule for a in later.sent) == expect
