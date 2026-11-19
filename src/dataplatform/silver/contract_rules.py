"""The contract's row rules as quality-stage rules (DAC DQ-01: rules written in the contract run; ADR-009: severity sets
the action). Pure and product-free; the predicates are the engine-neutral SQL subset both engines take.

Translated (a row must satisfy the predicate; nulls pass here, the required-column rules catch them):
    library invalidValues on a column, arguments.validValues  ->  col IS NULL OR col IN ('a', 'b')
    sql "SELECT COUNT(*) FROM ${object} WHERE <cond>", mustBe 0 ->  coalesce(NOT (<cond>), true)
Not row rules, left to their own stages and reported as such: table rules (duplicateValues -> the duplicates stage,
rowCount -> the lander's volume check). Anything else is returned as not translated, never silently dropped.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import replace

from dataplatform.meta.port import DqRule
from dataplatform.silver.plan import Rule

_COUNT_WHERE = re.compile(r"^\s*SELECT\s+COUNT\(\*\)\s+FROM\s+\$\{object\}\s+WHERE\s+(?P<cond>.+?)\s*;?\s*$",
                          re.IGNORECASE | re.DOTALL)
_UNSAFE = re.compile(r";|--|/\*|\bselect\b|\bfrom\b|\bjoin\b|\bdrop\b|\binsert\b|\bupdate\b|\bdelete\b", re.IGNORECASE)
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
HANDLED_ELSEWHERE = {"duplicateValues": "the duplicates stage", "rowCount": "the lander's volume check"}
# The store's actions are per rule (ADR-009: critical quarantines the rows and blocks the dataset, major quarantines,
# minor keeps and flags); at row level `stop` means the rows are quarantined, the dataset block is the run's job.
ROW_ACTION = {"quarantine": "quarantine", "stop": "quarantine", "warn": "flag"}


def _quote(v: object) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def translate(rule: DqRule) -> Rule | str:
    """The quality-stage rule, or the reason it is not one."""
    p = rule.params
    action = ROW_ACTION.get(rule.action)
    if action is None:
        return f"{rule.rule_id}: action '{rule.action}' has no row meaning, not translated"
    if rule.check in HANDLED_ELSEWHERE and not rule.column:
        return f"{rule.rule_id}: a table rule, kept by {HANDLED_ELSEWHERE[rule.check]}"
    if rule.check == "invalidValues" and rule.column and _IDENT.match(rule.column):
        valid = (p.get("arguments") or {}).get("validValues")
        if isinstance(valid, list) and valid and int(p.get("mustBe", 0)) == 0:
            allowed = ", ".join(_quote(v) for v in valid)
            return Rule(rule.rule_id, f"{rule.column} IS NULL OR {rule.column} IN ({allowed})", rule.severity,
                        action)
    if rule.check == "sql":
        m = _COUNT_WHERE.match(str(p.get("query") or ""))
        if m and int(p.get("mustBe", -1)) == 0:
            cond = m.group("cond")
            if _UNSAFE.search(cond):
                return f"{rule.rule_id}: the condition is not a plain row condition, not translated"
            return Rule(rule.rule_id, f"coalesce(NOT ({cond}), true)", rule.severity, action)
    return f"{rule.rule_id}: check '{rule.check}' is not a row rule this stage can run, not translated"


def contract_rules(rules: Iterable[DqRule]) -> tuple[tuple[Rule, ...], tuple[str, ...]]:
    """(row rules for the quality stage, reasons for every rule that is not one)."""
    done = [translate(r) for r in rules]
    return tuple(r for r in done if isinstance(r, Rule)), tuple(r for r in done if isinstance(r, str))


def with_overrides(rules: Iterable[Rule], overrides: Mapping[str, str]) -> tuple[Rule, ...]:
    """The dataset's own severity -> action choices (manifest `quality.actions`, ADR-009) laid over the rules the store
    gave: a dataset may flag what the contract would quarantine, or the other way round."""
    return tuple(replace(r, action=overrides.get(r.severity, r.action)) for r in rules)
