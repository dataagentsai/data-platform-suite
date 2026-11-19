"""The platform's success criteria and the scorecard a run computes against them (B20; CCA-P D4 4.1: define what
good means first, one metric per dimension with a target, no blended score). Pure.

CRITERIA is the single list (STATUS.html#criteria shows the same 20). `scorecard` takes what a run can see
(Evidence) and gives each criterion a state: met, not_met, or not_measured when this run cannot see it (cost,
privacy scans, coverage: measured elsewhere). A criterion is never met by default.

Timeliness from a simulated run uses the world's clock (the hop stamps the platform writes from the clock port),
so its numbers say how the schedule behaves, not how fast a real network is; the evidence says so.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

STATES = ("met", "not_met", "not_measured")
# C8 judges the hops the platform owns. commit->bronze also holds the source's and the delivery's delay, and in a
# simulated run the first load carries history committed before day 1 (found on the cloud proof run 16 Nov: p50
# 4.6 days in a 3-day run), so it is shown as context, not judged, until measured on rows committed during the run.
HOP_TARGETS = {"bronze->silver": 30 * 60}  # seconds, p95
CONTEXT_HOPS = ("commit->bronze",)
DAY_SECONDS_TARGET = 6 * 60  # one day's load, all layers, at scale 0.1 (C9)


@dataclass(frozen=True)
class Criterion:
    id: str
    dimension: str
    metric: str
    target: str


CRITERIA = (
    Criterion("C1", "Correctness", "Days on which every layer equals the oracle", "100% of days, every layer"),
    Criterion("C2", "Correctness", "Certified metric answers equal to gold and the world", "100% of known answers"),
    Criterion("C3", "Correctness", "Live source agrees with silver, or the missing keys and hop are named", "100%"),
    Criterion("C4", "Correctness", "Gold fact runs whose row ledger balances", "100%"),
    Criterion("C5", "Fault handling", "Planted faults handled at their layer, none silent", "every planted fault"),
    Criterion("C6", "Fault handling", "False alarms on clean days", "0"),
    Criterion("C7", "Timeliness", "Days whose gold is complete by 06:00 business time", "95% of days"),
    Criterion("C8", "Timeliness", "Hop latency p95 the platform owns (bronze to silver)", "30 minutes"),
    Criterion("C9", "Performance", "One day's load, all layers, scale 0.1", "6 minutes or less"),
    Criterion("C10", "Cost", "Cost per daily load per data product", "within budget (after a 14-day baseline)"),
    Criterion("C11", "Reliability", "Rerunning a day changes nothing", "0 duplicate rows, 0 changed windows"),
    Criterion("C12", "Reliability", "Two runs writing one table at once", "never"),
    Criterion("C13", "Recovery", "A bad batch withdrawn from every layer, changed windows logged", "one run cycle"),
    Criterion("C14", "Explainability", "A past window rebuilt from its run log equals what it published", "100%"),
    Criterion("C15", "Privacy", "Restricted values in clear in bronze, silver or gold", "0"),
    Criterion("C16", "Privacy", "Right-to-be-forgotten requests completed", "within 30 days"),
    Criterion("C17", "Security", "Drift between the access model and catalog grants", "0"),
    Criterion("C18", "Security", "Secrets in code, logs or the run log", "0"),
    Criterion("C19", "Change safety", "Unexplained data differences that reach production", "0"),
    Criterion("C20", "Coverage", "DAC rules with a passing test or a written waiver", "89 of 89"),
)


@dataclass
class Evidence:
    """What one run saw. Empty fields mean the run did not look, never that all was well."""

    layer_days: dict[str, list[bool]] = field(default_factory=dict)  # bronze|silver|gold -> equals oracle per day
    verdicts: list[tuple[str, str]] = field(default_factory=list)  # (incident, verdict) at any layer
    recon: list[tuple[str, bool, bool]] = field(default_factory=list)  # (dataset, agrees, difference named)
    ledger: list[str] = field(default_factory=list)  # status of every gold.ledger step
    hops: dict[str, float] = field(default_factory=dict)  # hop -> worst p95 seconds over datasets
    seconds_per_day: float | None = None
    known_answers: dict[str, bool] = field(default_factory=dict)  # answer -> equals gold and the world
    privacy: list[tuple[str, str, int]] = field(default_factory=list)  # (layer, dataset.column, values in clear)


@dataclass(frozen=True)
class Score:
    id: str
    dimension: str
    metric: str
    target: str
    state: str
    value: str
    evidence: str


def _pct(good: int, total: int) -> str:
    return f"{good} of {total}" + (f" ({100 * good / total:.0f}%)" if total else "")


def _all(n_good: int, n: int, what: str) -> tuple[str, str, str]:
    if not n:
        return "not_measured", "", f"this run saw no {what}"
    return ("met" if n_good == n else "not_met"), _pct(n_good, n), what


def _c1(e: Evidence) -> tuple[str, str, str]:
    days = [all(day) for day in zip(*e.layer_days.values(), strict=False)] if e.layer_days else []
    layers = ", ".join(sorted(e.layer_days))
    return _all(sum(days), len(days), f"days checked on {layers or 'no layer'}")


def _c2(e: Evidence) -> tuple[str, str, str]:
    return _all(sum(e.known_answers.values()), len(e.known_answers), "certified answers checked")


def _c3(e: Evidence) -> tuple[str, str, str]:
    return _all(sum(1 for _, ok, named in e.recon if ok or named), len(e.recon), "dataset-days reconciled")


def _c4(e: Evidence) -> tuple[str, str, str]:
    return _all(sum(1 for s in e.ledger if s == "succeeded"), len(e.ledger), "gold fact runs with a ledger")


def _c5(e: Evidence) -> tuple[str, str, str]:
    incidents: dict[str, set[str]] = {}
    for incident, verdict in e.verdicts:
        incidents.setdefault(incident, set()).add(verdict)
    handled = sorted(i for i, v in incidents.items() if "handled" in v)
    missed = sorted(set(incidents) - set(handled))
    state, value, what = _all(len(handled), len(incidents), "planted faults")
    return state, value, f"{what}; not handled: {', '.join(missed)}" if missed else what


def _c8(e: Evidence) -> tuple[str, str, str]:
    seen = {h: s for h, s in e.hops.items() if h in HOP_TARGETS}
    if not seen:
        return "not_measured", "", "no hop stamps in this run"
    over = {h: s for h, s in seen.items() if s > HOP_TARGETS[h]}
    value = ", ".join(f"{h} {s / 60:.1f} min" for h, s in sorted(seen.items()))
    context = "; ".join(f"{h} {e.hops[h] / 60:.0f} min (context, not judged)" for h in CONTEXT_HOPS if h in e.hops)
    why = "worst dataset p95, from the world's clock (simulated schedule)" + (f"; {context}" if context else "")
    return ("not_met" if over else "met"), value, why


def _c9(e: Evidence) -> tuple[str, str, str]:
    if e.seconds_per_day is None:
        return "not_measured", "", "no run time"
    s = e.seconds_per_day
    return ("met" if s <= DAY_SECONDS_TARGET else "not_met"), f"{s / 60:.1f} min a day", "wall time / days"


def _c15(e: Evidence) -> tuple[str, str, str]:
    if not e.privacy:
        return "not_measured", "", "no restricted column scanned"
    bad = [f"{layer} {col}: {n}" for layer, col, n in e.privacy if n]
    value = f"{sum(n for *_, n in e.privacy)} in clear over {len(e.privacy)} restricted columns"
    return ("not_met" if bad else "met"), value, "; ".join(bad) or "every value a token or empty (bronze, silver)"


MEASURES: dict[str, Callable[[Evidence], tuple[str, str, str]]] = {
    "C1": _c1, "C2": _c2, "C3": _c3, "C4": _c4, "C5": _c5, "C8": _c8, "C9": _c9, "C15": _c15}


def scorecard(e: Evidence) -> list[Score]:
    out = []
    for c in CRITERIA:
        state, value, why = MEASURES[c.id](e) if c.id in MEASURES else (
            "not_measured", "", "measured outside the run (tests, CI, cost or privacy scans)")
        out.append(Score(c.id, c.dimension, c.metric, c.target, state, value, why))
    return out


COLUMNS = (("run_id", "text"), ("id", "text"), ("dimension", "text"), ("metric", "text"), ("target", "text"),
           ("state", "text"), ("value", "text"), ("evidence", "text"))


def rows(run_id: str, scores: list[Score]) -> list[dict[str, Any]]:
    """The scorecard as ops table rows (ops_success), for the dashboards."""
    return [{"run_id": run_id, **{k: getattr(s, k) for k in ("id", "dimension", "metric", "target", "state",
                                                                "value", "evidence")}} for s in scores]
