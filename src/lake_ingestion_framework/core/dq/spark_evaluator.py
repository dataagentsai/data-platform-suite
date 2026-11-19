"""Spark container for the DQ evaluator.

Mirrors :class:`DuckdbEvaluator` for production realms wired to
Databricks Workflows. Both implementations satisfy the
:class:`DqEvaluator` Protocol so the action engine (Phase 3) and the
pipeline (Phase 4) program against one shape and never know which
container is running.

Cheap-eval semantics — same contract as the duckdb evaluator:

* ``sample_cap > 0`` — ``df.filter(predicate).limit(cap+1).collect()``;
  the ``+1`` is the overflow probe → ``violations_capped=True`` when
  the underlying DataFrame had more matches than the cap.
* ``sample_cap == 0`` — ``df.filter(predicate).limit(1).collect()``,
  treat as boolean (``violations_seen=-1`` when fired, ``0`` when not).
* ``sample_cap == -1`` — ``df.filter(predicate).collect()``; uncapped.
  Spark materialises everything to the driver — use only for small
  dimension tables.

Why no module-top ``import pyspark``: the framework remains importable
without PySpark on the classpath. The customer creates the
:class:`SparkSession` in their realm runner (Databricks Workflow / job
DAG / spark-submit) and hands the evaluator any object satisfying the
:class:`SparkSessionLike` shape — real ``SparkSession`` in prod, a
fake in unit tests.

Testing posture: this module's unit tests use a duck-typed fake
DataFrame to exercise the eval / cap / error branches without
spinning up a JVM. Phase 7 smoke tests will exercise the real
PySpark binding end-to-end against a Databricks cluster.
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any, Protocol

from lake_ingestion_framework.core.dq.evaluator import RuleEvaluation
from lake_ingestion_framework.core.dq.types import DqRule

if TYPE_CHECKING:
    pass  # pyspark types are duck-typed via SparkSessionLike below.


# Same shape as the duckdb evaluator — single + dotted (≤ 3 parts).
_RELATION_PART = r"[A-Za-z_][A-Za-z0-9_]{0,127}"
_RELATION_RE = re.compile(rf"^{_RELATION_PART}(?:\.{_RELATION_PART}){{0,2}}$")


class SparkSessionLike(Protocol):
    """Minimum surface the evaluator uses on a Spark session.

    Real ``pyspark.sql.SparkSession`` satisfies it. Tests pass a
    fake whose ``.table(name)`` returns a duck-typed DataFrame.
    """

    def table(self, name: str) -> Any:  # returns a DataFrame-like
        ...


class SparkEvaluator:
    """Evaluates DQ rules against a Spark DataFrame.

    The session is supplied at construction (the customer's
    realm-runner-managed handle; we never create one). Per
    :meth:`evaluate` we resolve the relation via ``session.table(name)``
    and apply ``filter`` + ``limit`` + ``collect``.

    Errors from the Spark side (``AnalysisException``, ``Py4JJavaError``,
    plain ``Exception``) are caught and surfaced as
    ``RuleEvaluation.status="errored"`` with the message truncated to
    512 chars — same posture as the duckdb evaluator (audit-table-safe,
    no leaked stack traces, fail-open).
    """

    def __init__(self, session: SparkSessionLike) -> None:
        self._session = session

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
            df = self._session.table(relation_name).filter(rule.predicate)

            if rule.sample_cap == 0:
                # Boolean-only fast path
                rows = df.limit(1).collect()
                fired = bool(rows)
                return RuleEvaluation(
                    rule=rule,
                    status="failed" if fired else "passed",
                    violations_seen=-1 if fired else 0,
                    violations_capped=False,
                    duration_ms=_elapsed_ms(start),
                )

            if rule.sample_cap == -1:
                # Uncapped — caller knows the relation is small
                spark_rows = df.collect()
                samples = [_row_to_dict(r) for r in spark_rows]
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
                    violations_capped=False,
                    sample=tuple(samples),
                    duration_ms=_elapsed_ms(start),
                )

            cap = rule.sample_cap
            spark_rows = df.limit(cap + 1).collect()
            samples = [_row_to_dict(r) for r in spark_rows]
            if not samples:
                return RuleEvaluation(
                    rule=rule,
                    status="passed",
                    violations_seen=0,
                    violations_capped=False,
                    duration_ms=_elapsed_ms(start),
                )
            capped = len(samples) > cap
            if capped:
                samples = samples[:cap]
            return RuleEvaluation(
                rule=rule,
                status="failed",
                violations_seen=len(samples),
                violations_capped=capped,
                sample=tuple(samples),
                duration_ms=_elapsed_ms(start),
            )
        except Exception as exc:  # — pyspark errors aren't a single base class
            return RuleEvaluation(
                rule=rule,
                status="errored",
                violations_seen=0,
                violations_capped=False,
                duration_ms=_elapsed_ms(start),
                error=str(exc)[:512],
            )


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a pyspark Row to a plain dict.

    Real ``pyspark.sql.Row`` exposes ``.asDict()``. Some test fakes
    expose a plain ``dict`` already; tolerate both.
    """
    if hasattr(row, "asDict"):
        result: dict[str, Any] = row.asDict()
        return result
    if isinstance(row, dict):
        return dict(row)
    raise TypeError(f"Cannot convert Spark row to dict: {type(row).__name__}")


def _elapsed_ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


__all__ = ["SparkEvaluator", "SparkSessionLike"]
