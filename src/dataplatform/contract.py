"""Reading an ODCS v3.2.0 contract: the columns a delivery must carry, and the quality rules as row predicates.

The contract is the open standard's file, adopted as is. This module only reads the parts slice 1 needs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# ODCS logical types to the column types our writers use.
TYPES = {"string": "string", "number": "double", "integer": "bigint", "timestamp": "timestamp", "date": "date",
         "boolean": "boolean"}


@dataclass(frozen=True)
class ColumnVerdict:
    ok: bool
    missing: tuple[str, ...]
    extra: tuple[str, ...]

    @property
    def reason(self) -> str:
        if self.ok:
            return ""
        return "missing required column(s): " + ", ".join(self.missing) + (
            f"; new column(s) seen: {', '.join(self.extra)}" if self.extra else "")


def properties(contract: dict[str, Any]) -> list[dict[str, Any]]:
    return contract["schema"][0]["properties"]


def columns(contract: dict[str, Any]) -> list[tuple[str, str]]:
    return [(p["name"], TYPES[p["logicalType"]]) for p in properties(contract)]


def primary_key(contract: dict[str, Any]) -> list[str]:
    keyed = [p for p in properties(contract) if p.get("primaryKey")]
    return [p["name"] for p in sorted(keyed, key=lambda p: p.get("primaryKeyPosition", 0))]


def check_columns(contract: dict[str, Any], observed: set[str], allowed_extra: set[str] = frozenset()) -> ColumnVerdict:
    """A missing required column stops the load (SCH-02). New columns are allowed through to bronze only
    (manifest load.new_columns = hold_in_bronze), so they are reported, not fatal."""
    required = {p["name"] for p in properties(contract) if p.get("required")}
    missing = tuple(sorted(required - observed))
    extra = tuple(sorted(observed - {p["name"] for p in properties(contract)} - set(allowed_extra)))
    return ColumnVerdict(ok=not missing, missing=missing, extra=extra)


_WHERE = re.compile(r"\bWHERE\b(.+)$", re.IGNORECASE | re.DOTALL)


def row_rules(contract: dict[str, Any]) -> dict[str, str]:
    """Column-level ODCS rules as SQL predicates a good row satisfies: {rule_name: predicate}.

    required            -> col IS NOT NULL
    invalidValues       -> col IN (...)
    sql 'COUNT(*) ... WHERE cond' with mustBe 0  -> NOT (cond)
    Table-level rules (duplicateValues, rowCount) are not row rules; the check step runs them.
    """
    rules: dict[str, str] = {}
    for p in properties(contract):
        name = p["name"]
        if p.get("required"):
            rules[f"{name}_present"] = f"{name} IS NOT NULL"
        for q in p.get("quality", []):
            if q.get("metric") == "invalidValues":
                vals = ", ".join(f"'{v}'" for v in q["arguments"]["validValues"])
                rules[f"{name}_valid"] = f"{name} IN ({vals})"
            elif q.get("type") == "sql" and q.get("mustBe") == 0:
                m = _WHERE.search(q["query"])
                if not m:
                    raise ValueError(f"cannot turn the sql rule on {name} into a row predicate: {q['query']}")
                rules[f"{name}_rule"] = f"NOT ({m.group(1).strip()})"
    return rules
