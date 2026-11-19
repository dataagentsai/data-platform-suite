"""Load + validate DQ rules from YAML.

Customer authors rules in ``<customer>-ingestion/config/dq/<layer>_<dataset>.yaml``;
this module parses + validates them into :class:`DqRule` instances.
The output is what ``lake-ingest config sync`` upserts into
``meta.dq_rules``.

Validation happens at this **single boundary** — once a rule passes
``load_rules_from_yaml`` and lands in the DB, downstream code trusts
it. Same posture as the rest of the framework's metadata loaders.

Predicate safety surface (v1):

- **Allowed**: arbitrary SQL expression that evaluates to a boolean
  (``email IS NULL``, ``amount < 0``, ``status NOT IN ('a','b')``,
  ``order_id IS NULL OR ship_date < order_date``)
- **Forbidden**: DDL / DML keywords (``DROP``, ``INSERT``, ``UPDATE``,
  ``DELETE``, ``CREATE``, ``ALTER``, ``TRUNCATE``, ``GRANT``, ``REVOKE``)
- **Forbidden**: statement-separators (``;``)
- **Forbidden**: comment markers (``--``, ``/*``) — silent SQL
  injection vector
- **Cheap-eval lint** (warning, not error): ``COUNT(*)``, ``GROUP BY``,
  ``ORDER BY``, ``WINDOW``, ``OVER`` — these break cheap-eval
  semantics; the linter warns and the runtime will refuse expensive
  forms in a future hardening pass

We deliberately don't parse the SQL with sqlglot in v1 — adds a
hefty dep and the regex-based forbidden-keyword check covers the
real risks. Customer wants to opt into stricter parsing? Easy
follow-up.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

import yaml

from lake_ingestion_framework.core.dq.types import (
    Action,
    Container,
    DqRule,
    DqRuleSet,
    DqValidationError,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    RuleSource,
    RuleType,
    Severity,
    Tech,
)

_E = TypeVar("_E", bound=StrEnum)

# ──────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CheapEvalWarning:
    """Non-fatal lint finding from the cheap-eval scanner.

    The loader returns these alongside the parsed :class:`DqRuleSet`
    so callers (CLI, ``config sync``) can surface them. They never
    block sync — customer might intentionally use ``COUNT(*)`` in a
    rare case — but they're loud enough to nudge cheap-eval
    discipline.
    """

    rule_name: str
    pattern: str
    suggestion: str
    line: int | None = None


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def load_rules_from_yaml(
    path: Path,
) -> tuple[DqRuleSet, tuple[CheapEvalWarning, ...]]:
    """Load + validate one ``config/dq/<...>.yaml`` file.

    Returns ``(rule_set, warnings)``. Raises :class:`DqValidationError`
    on any malformed input. Warnings are non-fatal (cheap-eval lints).

    The file's filename has no semantic meaning to the loader — the
    YAML body declares ``dataset`` + ``layer``. Customer convention is
    ``<layer>_<dataset>.yaml`` for clarity in PR diffs.
    """
    if not path.is_file():
        raise DqValidationError(f"DQ rules file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        raise DqValidationError(f"DQ rules file {path} is empty.")
    if not isinstance(raw, dict):
        raise DqValidationError(
            f"DQ rules file {path} must be a YAML mapping; got {type(raw).__name__}."
        )

    _reject_unknown_keys(raw, _FILE_KEYS, f"{path} top-level")

    dataset_name = _required_str(raw, "dataset", path)
    layer_raw = _required_str(raw, "layer", path)
    layer = _enum_or_raise(Layer, layer_raw, f"{path}: layer")

    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, list):
        raise DqValidationError(f"{path}: 'rules' must be a list.")
    if not rules_raw:
        raise DqValidationError(f"{path}: 'rules' must be non-empty.")

    parsed: list[DqRule] = []
    warnings: list[CheapEvalWarning] = []
    for idx, entry in enumerate(rules_raw):
        if not isinstance(entry, dict):
            raise DqValidationError(
                f"{path}: rules[{idx}] must be a mapping; got {type(entry).__name__}."
            )
        rule, rule_warnings = _hydrate_rule(entry, dataset_name, layer, path)
        parsed.append(rule)
        warnings.extend(rule_warnings)

    rule_set = DqRuleSet(
        dataset_name=dataset_name,
        layer=layer,
        rules=tuple(parsed),
        source_path=str(path),
    )
    return rule_set, tuple(warnings)


def lint_predicate_for_cheap_eval(rule_name: str, predicate: str) -> tuple[CheapEvalWarning, ...]:
    """Warn about predicate patterns that defeat cheap-eval semantics.

    Run by the loader on every rule, but exposed publicly so CLI and
    future linters can call it directly (e.g., ``lake-ingest dq lint``).
    """
    findings: list[CheapEvalWarning] = []
    upper = predicate.upper()
    for pattern, suggestion in _CHEAP_EVAL_PATTERNS:
        if pattern.search(upper):
            findings.append(
                CheapEvalWarning(
                    rule_name=rule_name,
                    pattern=pattern.pattern,
                    suggestion=suggestion,
                )
            )
    return tuple(findings)


# ──────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────


_FILE_KEYS: frozenset[str] = frozenset({"dataset", "layer", "rules"})

_RULE_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "predicate",
        "injection_point",
        "severity",
        "action",
        "quarantine_strategy",
        "tech",
        "container",
        "rule_type",
        "sample_cap",
        "is_enabled",
        "authored_by",
        "description",
        # source intentionally NOT in YAML — always user_authored
        # for YAML-loaded rules; framework-invariant + auto_recommended
        # rules write directly to the DB.
    }
)

# Forbidden keywords — anything that could rewrite data or cross
# statement boundaries. Word-boundary anchored so e.g. "alterations"
# in a description doesn't false-positive (descriptions go through a
# different validator anyway, but predicates also avoid this trap).
_FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    "DROP",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "EXEC",
    "EXECUTE",
    "CALL",
)
_FORBIDDEN_KEYWORD_RE = re.compile(
    r"\b(" + "|".join(_FORBIDDEN_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Comment markers — `--` is a SQL line comment; `/* */` block. Both
# can hide injection. Reject before they ever reach the engine.
_COMMENT_RE = re.compile(r"--|/\*")

# Cheap-eval lint patterns — non-fatal; warn and let it through.
# Customer can suppress by adding `description: noqa: cheap-eval` if
# they have a justified case (future hardening might honor this).
_CHEAP_EVAL_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bCOUNT\s*\(\s*\*\s*\)"),
        "COUNT(*) forces a full scan; cheap form is `EXISTS (SELECT 1 FROM t WHERE <bad>)` "
        "or `SELECT 1 FROM t WHERE <bad> LIMIT 1`.",
    ),
    (
        re.compile(r"\bGROUP\s+BY\b"),
        "GROUP BY in a rule predicate forces aggregation; consider expressing the rule "
        "as a per-row predicate. If aggregation is essential, this rule belongs in the "
        "TrendCheck sub-component (deferred to v2).",
    ),
    (
        re.compile(r"\bORDER\s+BY\b"),
        "ORDER BY adds a sort with no benefit to a per-row predicate; remove it.",
    ),
    (
        re.compile(r"\bOVER\s*\("),
        "Window functions force a full scan; consider a per-row predicate. If windowing is "
        "essential, this rule belongs in the TrendCheck sub-component (deferred to v2).",
    ),
)


def _hydrate_rule(
    raw: dict[str, Any],
    dataset_name: str,
    layer: Layer,
    path: Path,
) -> tuple[DqRule, tuple[CheapEvalWarning, ...]]:
    _reject_unknown_keys(raw, _RULE_KEYS, f"{path}: rule {raw.get('name', '<unnamed>')!r}")

    name = _required_str(raw, "name", path)
    predicate = _required_str(raw, "predicate", path)

    _validate_predicate_safety(name, predicate)
    warnings = lint_predicate_for_cheap_eval(name, predicate)

    injection_point = _enum_or_raise(
        InjectionPoint,
        _required_str(raw, "injection_point", path),
        f"{path}: rule {name!r} injection_point",
    )
    severity = _enum_or_raise(
        Severity, _required_str(raw, "severity", path), f"{path}: rule {name!r} severity"
    )
    action = _enum_or_raise(
        Action, _required_str(raw, "action", path), f"{path}: rule {name!r} action"
    )

    quarantine_strategy: QuarantineStrategy | None = None
    raw_qs = raw.get("quarantine_strategy")
    if raw_qs is not None:
        quarantine_strategy = _enum_or_raise(
            QuarantineStrategy, raw_qs, f"{path}: rule {name!r} quarantine_strategy"
        )

    tech = _enum_or_raise(Tech, raw.get("tech", "sql"), f"{path}: rule {name!r} tech")
    container = _enum_or_raise(
        Container, raw.get("container", "duckdb"), f"{path}: rule {name!r} container"
    )
    rule_type = _enum_or_raise(
        RuleType, raw.get("rule_type", "predicate"), f"{path}: rule {name!r} rule_type"
    )

    sample_cap = int(raw.get("sample_cap", 100))
    is_enabled = bool(raw.get("is_enabled", True))
    authored_by = raw.get("authored_by")
    description = str(raw.get("description", ""))

    rule = DqRule(
        name=name,
        dataset_name=dataset_name,
        layer=layer,
        predicate=predicate,
        injection_point=injection_point,
        severity=severity,
        action=action,
        quarantine_strategy=quarantine_strategy,
        rule_type=rule_type,
        tech=tech,
        container=container,
        sample_cap=sample_cap,
        is_enabled=is_enabled,
        source=RuleSource.USER_AUTHORED,
        authored_by=str(authored_by) if authored_by else None,
        description=description,
    )
    return rule, warnings


def _validate_predicate_safety(rule_name: str, predicate: str) -> None:
    """Reject predicates with statement separators, comments, or DDL/DML
    keywords. These would let a malicious or careless YAML edit bypass
    the framework's read-only posture."""
    if ";" in predicate:
        raise DqValidationError(
            f"Rule {rule_name!r}: predicate must not contain ';' "
            "(only single SQL expressions are allowed)."
        )
    if _COMMENT_RE.search(predicate):
        raise DqValidationError(
            f"Rule {rule_name!r}: predicate must not contain SQL comments "
            "('--' or '/*') — they can hide injection."
        )
    bad = _FORBIDDEN_KEYWORD_RE.search(predicate)
    if bad:
        raise DqValidationError(
            f"Rule {rule_name!r}: predicate contains forbidden keyword "
            f"{bad.group(1).upper()!r}. Predicates must be read-only "
            "boolean expressions; DDL/DML keywords are rejected."
        )


def _enum_or_raise(enum_cls: type[_E], value: Any, role: str) -> _E:
    if isinstance(value, enum_cls):
        return value
    if not isinstance(value, str):
        raise DqValidationError(f"{role}: must be a string, got {type(value).__name__}.")
    try:
        return enum_cls(value)
    except ValueError:
        valid = sorted(m.value for m in enum_cls)
        raise DqValidationError(
            f"{role}: {value!r} is not a valid {enum_cls.__name__}. Valid: {valid}."
        ) from None


def _required_str(raw: dict[str, Any], key: str, path: Path) -> str:
    if key not in raw:
        raise DqValidationError(f"{path}: missing required key {key!r}.")
    value = raw[key]
    if not isinstance(value, str):
        raise DqValidationError(f"{path}: {key!r} must be a string, got {type(value).__name__}.")
    if not value.strip():
        raise DqValidationError(f"{path}: {key!r} must be non-empty.")
    return value


def _reject_unknown_keys(raw: dict[str, Any], allowed: frozenset[str], role: str) -> None:
    unknown = set(raw) - allowed
    if unknown:
        raise DqValidationError(
            f"{role}: unknown key(s) {sorted(unknown)}. Allowed: {sorted(allowed)}."
        )
