"""Unit tests for :class:`SparkEvaluator` (Phase 4d).

Strategy: duck-type pyspark with a tiny in-memory fake. The framework
never imports pyspark at module top, so these tests don't need a JVM
or a Spark install. Real Spark validation lives in Phase 7 smoke tests
against a Databricks cluster.

Coverage parallels test_dq_evaluator.py (the duckdb side):

* happy paths (passed / failed-with-sample / multiple violators / column capture)
* sample_cap (overflow flag, exact boundary, cap=0 bool path, cap=-1 uncapped)
* error paths (bad predicate / missing relation / invalid relation name)
* Protocol conformance with :class:`DqEvaluator`
"""

from __future__ import annotations

from typing import Any

import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    DqEvaluator,
    DqRule,
    InjectionPoint,
    Layer,
    RuleEvaluation,
    Severity,
    SparkEvaluator,
)

# ──────────────────────────────────────────────────────────────────────
# pyspark fakes — minimum surface SparkEvaluator touches
# ──────────────────────────────────────────────────────────────────────


class _FakeRow(dict[str, Any]):
    """Acts both as a pyspark Row (``.asDict()``) and a plain dict."""

    def asDict(self) -> dict[str, Any]:  # — pyspark API name
        return dict(self)


class _FakeDataFrame:
    """In-memory mock of a Spark DataFrame supporting filter/limit/collect."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        *,
        raise_on_filter: Exception | None = None,
    ) -> None:
        self._rows = rows
        self._columns = columns
        self._limit: int | None = None
        self._raise = raise_on_filter

    @property
    def columns(self) -> list[str]:
        return self._columns

    def filter(self, predicate: str) -> _FakeDataFrame:
        if self._raise is not None:
            raise self._raise
        # Tiny SQL-ish predicate evaluator covering only the patterns
        # used in this file's tests. Robust enough to drive the eval
        # branches without pulling in a SQL parser.
        filtered = [r for r in self._rows if _eval_predicate(predicate, r)]
        return _FakeDataFrame(filtered, self._columns)

    def limit(self, n: int) -> _FakeDataFrame:
        new = _FakeDataFrame(self._rows, self._columns)
        new._limit = n
        return new

    def collect(self) -> list[_FakeRow]:
        rows = self._rows if self._limit is None else self._rows[: self._limit]
        return [_FakeRow(r) for r in rows]


class _FakeSparkSession:
    """Resolves table names to fake DataFrames."""

    def __init__(
        self,
        tables: dict[str, _FakeDataFrame],
    ) -> None:
        self._tables = tables

    def table(self, name: str) -> _FakeDataFrame:
        if name not in self._tables:
            raise RuntimeError(f"AnalysisException: Table '{name}' not found")
        return self._tables[name]


def _eval_predicate(predicate: str, row: dict[str, Any]) -> bool:
    """Evaluate the small subset of SQL used by tests.

    Supports:
    - ``email IS NULL``
    - ``email IS NOT NULL``
    - ``country = 'US'``
    - ``customer_id < 0``
    - ``customer_id IN (2, 4)``
    - ``nonexistent_column IS NULL`` → raise (simulates AnalysisException)
    """
    pred = predicate.strip()

    if pred == "nonexistent_column IS NULL":
        raise RuntimeError("AnalysisException: cannot resolve 'nonexistent_column'")

    if pred.endswith("IS NULL"):
        col = pred.removesuffix("IS NULL").strip()
        if col not in row:
            raise RuntimeError(f"AnalysisException: cannot resolve {col!r}")
        return row[col] is None

    if pred.endswith("IS NOT NULL"):
        col = pred.removesuffix("IS NOT NULL").strip()
        return row[col] is not None

    if "=" in pred and "==" not in pred:
        col, _, value = pred.partition("=")
        col = col.strip()
        value = value.strip().strip("'")
        return str(row.get(col)) == value

    if "<" in pred:
        col, _, value = pred.partition("<")
        col = col.strip()
        return float(row.get(col, 0)) < float(value.strip())

    if " IN " in pred:
        col, _, rest = pred.partition(" IN ")
        col = col.strip()
        values = [v.strip().strip("'") for v in rest.strip().lstrip("(").rstrip(")").split(",")]
        return str(row.get(col)) in values

    raise ValueError(f"_FakeDataFrame predicate not supported in tests: {predicate!r}")


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def session() -> _FakeSparkSession:
    """Spark session with a `customers` table mirroring the duckdb fixture."""
    rows = [
        {"customer_id": 1, "email": "a@example.com", "country": "US"},
        {"customer_id": 2, "email": None, "country": "UK"},
        {"customer_id": 3, "email": "bad-email", "country": "US"},
        {"customer_id": 4, "email": None, "country": "IN"},
        {"customer_id": 5, "email": "d@example.com", "country": "US"},
    ]
    df = _FakeDataFrame(rows, columns=["customer_id", "email", "country"])
    return _FakeSparkSession({"customers": df})


def _rule(
    predicate: str,
    *,
    sample_cap: int = 100,
) -> DqRule:
    return DqRule(
        name="r",
        dataset_name="customers",
        layer=Layer.SILVER,
        predicate=predicate,
        injection_point=InjectionPoint.POST_PROCESSOR,
        severity=Severity.WARNING,
        action=Action.CONTINUE_LOG,
        sample_cap=sample_cap,
    )


# ──────────────────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────────────────


def test_passed_when_no_violators(session: _FakeSparkSession) -> None:
    result = SparkEvaluator(session).evaluate(_rule("customer_id < 0"), relation_name="customers")
    assert result.status == "passed"
    assert result.violations_seen == 0
    assert result.sample == ()


def test_failed_with_single_violator(session: _FakeSparkSession) -> None:
    """email IS NULL matches 2 rows in the fixture."""
    result = SparkEvaluator(session).evaluate(_rule("email IS NULL"), relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 2
    assert {row["customer_id"] for row in result.sample} == {2, 4}


def test_sample_contains_every_column(session: _FakeSparkSession) -> None:
    result = SparkEvaluator(session).evaluate(_rule("country = 'IN'"), relation_name="customers")
    assert result.sample[0].keys() == {"customer_id", "email", "country"}


# ──────────────────────────────────────────────────────────────────────
# sample_cap behaviour
# ──────────────────────────────────────────────────────────────────────


def test_sample_cap_truncates_and_flags_capped(session: _FakeSparkSession) -> None:
    # 3 US customers, cap=2 → capped flag fires
    result = SparkEvaluator(session).evaluate(
        _rule("country = 'US'", sample_cap=2), relation_name="customers"
    )
    assert result.status == "failed"
    assert result.violations_seen == 2
    assert result.violations_capped is True


def test_sample_cap_at_exact_boundary_is_not_flagged(
    session: _FakeSparkSession,
) -> None:
    # 3 US customers, cap=3 → exactly at boundary
    result = SparkEvaluator(session).evaluate(
        _rule("country = 'US'", sample_cap=3), relation_name="customers"
    )
    assert result.violations_seen == 3
    assert result.violations_capped is False


def test_sample_cap_zero_returns_minus_one_when_fired(
    session: _FakeSparkSession,
) -> None:
    result = SparkEvaluator(session).evaluate(
        _rule("email IS NULL", sample_cap=0), relation_name="customers"
    )
    assert result.status == "failed"
    assert result.violations_seen == -1
    assert result.sample == ()


def test_sample_cap_zero_returns_zero_when_passed(
    session: _FakeSparkSession,
) -> None:
    result = SparkEvaluator(session).evaluate(
        _rule("customer_id < 0", sample_cap=0), relation_name="customers"
    )
    assert result.status == "passed"
    assert result.violations_seen == 0


def test_sample_cap_uncapped_returns_all(session: _FakeSparkSession) -> None:
    result = SparkEvaluator(session).evaluate(
        _rule("country = 'US'", sample_cap=-1), relation_name="customers"
    )
    assert result.violations_seen == 3
    assert result.violations_capped is False


# ──────────────────────────────────────────────────────────────────────
# Error paths
# ──────────────────────────────────────────────────────────────────────


def test_unknown_column_returns_errored(session: _FakeSparkSession) -> None:
    """Spark raises AnalysisException → wrap as status=errored."""
    result = SparkEvaluator(session).evaluate(
        _rule("nonexistent_column IS NULL"), relation_name="customers"
    )
    assert result.status == "errored"
    assert result.error is not None
    assert "nonexistent_column" in result.error


def test_missing_relation_returns_errored(session: _FakeSparkSession) -> None:
    result = SparkEvaluator(session).evaluate(_rule("email IS NULL"), relation_name="not_a_table")
    assert result.status == "errored"
    assert result.error is not None


@pytest.mark.parametrize(
    "bad_name",
    ["bad name", "bad-name", "1starts_with_digit", "", "drop;table"],
)
def test_invalid_relation_name_rejected_before_query(
    session: _FakeSparkSession, bad_name: str
) -> None:
    result = SparkEvaluator(session).evaluate(_rule("email IS NULL"), relation_name=bad_name)
    assert result.status == "errored"
    assert result.error is not None
    assert "invalid relation name" in result.error


def test_error_message_truncated_at_512_chars(
    session: _FakeSparkSession,
) -> None:
    """Long error from a Spark exception is capped at 512 chars."""
    long_msg = "x" * 1000
    failing_df = _FakeDataFrame([], [], raise_on_filter=RuntimeError(long_msg))
    s = _FakeSparkSession({"customers": failing_df})

    result = SparkEvaluator(s).evaluate(_rule("email IS NULL"), relation_name="customers")
    assert result.status == "errored"
    assert result.error is not None
    assert len(result.error) <= 512


# ──────────────────────────────────────────────────────────────────────
# Protocol conformance — drop-in for DqEvaluator
# ──────────────────────────────────────────────────────────────────────


def test_spark_evaluator_satisfies_protocol(session: _FakeSparkSession) -> None:
    evaluator: DqEvaluator = SparkEvaluator(session)
    rule = _rule("country = 'IN'")
    result: RuleEvaluation = evaluator.evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 1
