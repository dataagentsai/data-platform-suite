"""DAC coverage (B20 C20): every platform rule has a passing test or a written waiver. Pure.

A rule counts as tested when a test file cites its id (tests name the rule they prove, e.g. "LEDG-01"). Cited only
in code is `built`: the rule is kept somewhere but no test says so. A waiver needs a reason, who approved it and an
end date; an expired or incomplete waiver does not count. The report lists what is left, MUST rules first.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

STATES = ("tested", "waived", "built", "none")  # best first
LEVEL_ORDER = {"MUST": 0, "SHOULD": 1, "MAY": 2}
MIN_REASON = 10  # characters: the same floor as a voided batch's reason


@dataclass(frozen=True)
class Rule:
    id: str
    title: str
    concern: str
    level: str


@dataclass(frozen=True)
class Covered:
    rule: Rule
    state: str
    where: tuple[str, ...]  # test files (tested) or code files (built), or the waiver reason


def rules_from(docs: Iterable[list[dict[str, Any]]]) -> list[Rule]:
    """Rules from the catalog's obligation files (each a YAML list)."""
    return [Rule(str(r["id"]), str(r.get("title", "")), str(r.get("concern", "")), str(r.get("level", "")))
            for doc in docs for r in doc or []]


def _cited(rule_id: str, files: Mapping[str, str]) -> tuple[str, ...]:
    pat = re.compile(rf"(?<![A-Z0-9-]){re.escape(rule_id)}(?![0-9])")
    return tuple(sorted(name for name, text in files.items() if pat.search(text)))


def valid_waiver(w: Mapping[str, Any], today: date) -> bool:
    if not all(str(w.get(k, "")).strip() for k in ("id", "reason", "approved_by", "until")):
        return False
    return date.fromisoformat(str(w["until"])) >= today and len(str(w["reason"]).strip()) >= MIN_REASON


def coverage(rules: list[Rule], tests: Mapping[str, str], code: Mapping[str, str],
             waivers: list[Mapping[str, Any]], today: date) -> list[Covered]:
    waived = {str(w["id"]): str(w["reason"]) for w in waivers if valid_waiver(w, today)}
    out = []
    for r in rules:
        t = _cited(r.id, tests)
        if t:
            out.append(Covered(r, "tested", t))
        elif r.id in waived:
            out.append(Covered(r, "waived", (waived[r.id],)))
        else:
            c = _cited(r.id, code)
            out.append(Covered(r, "built" if c else "none", c))
    return out


def summary(covered: list[Covered]) -> dict[str, int]:
    return {s: sum(1 for c in covered if c.state == s) for s in STATES}


def left(covered: list[Covered]) -> list[Covered]:
    """Rules with neither a test nor a waiver: MUST first, then by id."""
    todo = [c for c in covered if c.state in ("built", "none")]
    return sorted(todo, key=lambda c: (LEVEL_ORDER.get(c.rule.level, 9), c.rule.id))
