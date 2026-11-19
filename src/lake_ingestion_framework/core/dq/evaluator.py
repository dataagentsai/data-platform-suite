"""SQL evaluator for DQ rules — runs a rule's predicate against a
relation and reports violations.

v1 ships the duckdb container only; spark lands in Phase 4 (wired to
``SilverPipeline``). Both implementations satisfy :class:`DqEvaluator`
so the action engine (Phase 3) stays container-agnostic.

Cheap-eval semantics
====================
The evaluator never runs ``COUNT(*)`` over a relation. For each rule:

* ``sample_cap == 0`` — fast bool path: ``SELECT 1 ... WHERE pred LIMIT 1``;
  no rows captured, ``violations_seen = -1`` when fired.
* ``sample_cap > 0`` — ``SELECT * ... WHERE pred LIMIT cap+1``;
  the ``+1`` is a bounded overflow probe so the consumer knows
  whether more violators exist (``violations_capped=True``).
* ``sample_cap == -1`` — uncapped: ``SELECT * ... WHERE pred``; rare,
  reserved for tiny dimension tables.

``violations_seen`` is therefore a *lower bound* (or exact, when
``violations_capped`` is False). Computing the true count requires
a separate query and is intentionally not the evaluator's job — that
decision belongs to the action engine, where context is richer.

Predicates are trusted at this layer; safety / cheap-eval lints ran
at sync time in :mod:`yaml_loader`. Relation names are validated
against ``_RELATION_RE`` and bracket-quoted at SQL emission.

The framework intentionally never ``import duckdb`` at module top —
the caller hands in a connection. That keeps ``lake_ingestion_framework``
importable without the ``[dq]`` extra. Phase 4's ``SparkEvaluator``
will follow the same posture for ``pyspark``.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol

from lake_ingestion_framework.core.dq.types import DqRule

if TYPE_CHECKING:
    import duckdb

# Allow simple identifiers and dotted ones (catalog.schema.table). v1
# duckdb usage is single-name registered relations, but the regex
# leaves room for future spark sessions where 3-part names are common.
_RELATION_PART = r"[A-Za-z_][A-Za-z0-9_]{0,127}"
_RELATION_RE = re.compile(rf"^{_RELATION_PART}(?:\.{_RELATION_PART}){{0,2}}$")

EvaluationStatus = Literal["passed", "failed", "errored"]


@dataclass(frozen=True)
class RuleEvaluation:
    """Outcome of running one rule against one relation.

    Pure facts about what the predicate matched. The action engine
    (Phase 3) is what decides — based on ``rule.action`` — whether to
    halt, log, or quarantine.
    """

    rule: DqRule
    status: EvaluationStatus
    """``passed`` = predicate matched no rows; ``failed`` = ≥ 1 match;
    ``errored`` = engine raised at evaluation time (bad column,
    syntax, missing relation)."""

    violations_seen: int
    """Rows actually returned by the violator query.

    * ``0`` when ``status == "passed"``
    * ``-1`` when ``sample_cap == 0`` and ``status == "failed"``
      (boolean-only path; we know the rule fired but not the count)
    * positive integer otherwise — capped at ``sample_cap``
    """

    violations_capped: bool
    """True when ``violations_seen == sample_cap`` and the underlying
    query may have had more matching rows (the ``+1`` overflow probe
    fired). Treat ``violations_seen`` as a lower bound."""

    sample: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    """Captured violator rows (≤ ``sample_cap``). Empty when
    ``sample_cap == 0`` or ``status != "failed"``. Each dict is
    ``{column_name: value}``."""

    duration_ms: int = 0
    """Wall-clock cost of the evaluation, including SQL build + execute
    + sample materialisation. Surfaced into ``meta.dq_results`` by the
    action engine."""

    error: str | None = None
    """Set only when ``status == "errored"``. Engine error message,
    truncated to 512 chars to fit comfortably in run-log columns and
    keep accidental PII out of audit tables."""


class DqEvaluator(Protocol):
    """Container-agnostic eval contract.

    Phase 4 will add ``SparkEvaluator`` for prod realms wired to
    Databricks Workflows; Phase 3 (action engine) and Phase 5 (CLI)
    program against this Protocol so neither knows or cares which
    container is running.
    """

    def evaluate(self, rule: DqRule, *, relation_name: str) -> RuleEvaluation: ...


class DuckdbEvaluator:
    """Evaluates DQ rules against a duckdb connection.

    The caller is responsible for registering data into the connection
    (``conn.register(name, table)`` or ``CREATE TABLE name AS ...``)
    before calling :meth:`evaluate`. Owning the connection here would
    force per-rule setup — both wasteful and surprising when the
    action engine wants to run many rules against one cached relation.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def evaluate(self, rule: DqRule, *, relation_name: str) -> RuleEvaluation:
        if not _RELATION_RE.match(relation_name):
            return RuleEvaluation(
                rule=rule,
                status="errored",
                violations_seen=0,
                violations_capped=False,
                error=f"invalid relation name: {relation_name!r}",
            )

        start = time.perf_counter()
        try:
            if rule.sample_cap == 0:
                fired = self._exists(rule, relation_name)
                return RuleEvaluation(
                    rule=rule,
                    status="failed" if fired else "passed",
                    violations_seen=-1 if fired else 0,
                    violations_capped=False,
                    duration_ms=_elapsed_ms(start),
                )

            samples, capped = self._sample(rule, relation_name)
            if not samples:
                return RuleEvaluation(
                    rule=rule,
                    status="passed",
                    violations_seen=0,
                    violations_capped=False,
                    duration_ms=_elapsed_ms(start),
                )
            return RuleEvaluation(
                rule=rule,
                status="failed",
                violations_seen=len(samples),
                violations_capped=capped,
                sample=tuple(samples),
                duration_ms=_elapsed_ms(start),
            )
        except Exception as exc:
            return RuleEvaluation(
                rule=rule,
                status="errored",
                violations_seen=0,
                violations_capped=False,
                duration_ms=_elapsed_ms(start),
                error=str(exc)[:512],
            )

    # ── helpers ──────────────────────────────────────────────────────

    def _exists(self, rule: DqRule, relation: str) -> bool:
        """Boolean-only path for ``sample_cap == 0``."""
        sql = (
            f"SELECT 1 FROM {_quote_relation(relation)} "  # noqa: S608 — ident validated, predicate sync-validated
            f"WHERE ({rule.predicate}) LIMIT 1"
        )
        row = self._conn.execute(sql).fetchone()
        return row is not None

    def _sample(self, rule: DqRule, relation: str) -> tuple[list[dict[str, Any]], bool]:
        """Run the violator query and return rows + overflow flag.

        For ``sample_cap == -1`` (uncapped), no LIMIT is applied and
        ``violations_capped`` is always False (we got everything).
        Otherwise we ``LIMIT cap+1``; if the result has ``cap+1`` rows
        we know there are more, drop the extra row, and flag capped.
        """
        if rule.sample_cap == -1:
            sql = (
                f"SELECT * FROM {_quote_relation(relation)} "  # noqa: S608
                f"WHERE ({rule.predicate})"
            )
            return self._fetch_dicts(sql), False

        cap = rule.sample_cap
        sql = (
            f"SELECT * FROM {_quote_relation(relation)} "  # noqa: S608
            f"WHERE ({rule.predicate}) LIMIT {cap + 1}"
        )
        rows = self._fetch_dicts(sql)
        if len(rows) > cap:
            return rows[:cap], True
        return rows, False

    def _fetch_dicts(self, sql: str) -> list[dict[str, Any]]:
        """Run a SELECT and return rows as ``[{col: value}, ...]``.

        Avoids the pandas dependency that ``fetchdf`` would bring in;
        we read the result's ``description`` for column names and zip
        with each fetched tuple.
        """
        result = self._conn.execute(sql)
        columns = [d[0] for d in (result.description or [])]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _quote_relation(name: str) -> str:
    """Bracket-quote each part of a (possibly dotted) relation name.

    Caller must have validated the shape against ``_RELATION_RE``.
    """
    return ".".join(f'"{part}"' for part in name.split("."))


def _elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


__all__ = [
    "DqEvaluator",
    "DuckdbEvaluator",
    "EvaluationStatus",
    "RuleEvaluation",
]
