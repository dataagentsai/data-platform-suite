"""Unit tests for the DQ evaluator (Phase 2 — duckdb container).

Covers the cheap-eval contract:

* sample_cap > 0 → ``LIMIT cap+1`` overflow probe → capped flag
* sample_cap == 0 → ``EXISTS`` bool path (violations_seen=-1 when fired)
* sample_cap == -1 → uncapped (rare)
* errored predicates → status=errored (no crash, no leaked stack)
* invalid relation names rejected before SQL emission

Per-action / per-severity / per-layer behaviour is exercised to prove
the evaluator is purely fact-finding — interpretation lives in the
Phase 3 action engine.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import duckdb
import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    DqEvaluator,
    DqRule,
    DuckdbEvaluator,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    RuleEvaluation,
    Severity,
)

# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """In-memory duckdb connection seeded with a small customers table."""
    c = duckdb.connect(":memory:")
    c.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER,
            email VARCHAR,
            country VARCHAR
        )
        """
    )
    c.execute(
        """
        INSERT INTO customers VALUES
            (1, 'a@example.com', 'US'),
            (2, NULL,            'UK'),
            (3, 'bad-email',     'US'),
            (4, 'c@example.com', 'IN'),
            (5, 'd@example.com', 'US')
        """
    )
    yield c
    c.close()


@pytest.fixture()
def empty_conn() -> Iterator[duckdb.DuckDBPyConnection]:
    """duckdb connection with the schema but no rows."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE customers (customer_id INTEGER, email VARCHAR)")
    yield c
    c.close()


def _rule(
    predicate: str,
    *,
    name: str = "test_rule",
    sample_cap: int = 100,
    action: Action = Action.CONTINUE_LOG,
    severity: Severity = Severity.WARNING,
    layer: Layer = Layer.SILVER,
    injection_point: InjectionPoint = InjectionPoint.POST_PROCESSOR,
    quarantine_strategy: QuarantineStrategy | None = None,
) -> DqRule:
    return DqRule(
        name=name,
        dataset_name="customers",
        layer=layer,
        predicate=predicate,
        injection_point=injection_point,
        severity=severity,
        action=action,
        quarantine_strategy=quarantine_strategy,
        sample_cap=sample_cap,
    )


# ──────────────────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────────────────


def test_evaluate_passed_when_no_violators(conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("customer_id < 0")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "passed"
    assert result.violations_seen == 0
    assert result.violations_capped is False
    assert result.sample == ()
    assert result.error is None


def test_evaluate_failed_with_single_violator(conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("email IS NULL")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 1
    assert result.violations_capped is False
    assert len(result.sample) == 1
    assert result.sample[0]["customer_id"] == 2
    assert result.sample[0]["email"] is None


def test_evaluate_failed_with_multiple_violators(conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("country = 'US'")  # 3 of 5 rows
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 3
    assert result.violations_capped is False


def test_sample_contains_every_column(conn: duckdb.DuckDBPyConnection) -> None:
    """SELECT * captures all source columns; consumers can render any."""
    rule = _rule("customer_id = 1")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.sample[0].keys() == {"customer_id", "email", "country"}


# ──────────────────────────────────────────────────────────────────────
# sample_cap behaviour
# ──────────────────────────────────────────────────────────────────────


def test_sample_cap_truncates_and_flags_capped(conn: duckdb.DuckDBPyConnection) -> None:
    # 3 US customers, cap=2 → capped flag fires
    rule = _rule("country = 'US'", sample_cap=2)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 2
    assert result.violations_capped is True
    assert len(result.sample) == 2


def test_sample_cap_at_exact_boundary_is_not_flagged(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    # 3 US customers, cap=3 → exactly at boundary; no overflow probe
    rule = _rule("country = 'US'", sample_cap=3)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.violations_seen == 3
    assert result.violations_capped is False


def test_sample_cap_zero_uses_boolean_path_when_fired(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _rule("email IS NULL", sample_cap=0)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == -1  # bool-only path
    assert result.violations_capped is False
    assert result.sample == ()


def test_sample_cap_zero_returns_passed_when_no_violators(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _rule("customer_id < 0", sample_cap=0)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "passed"
    assert result.violations_seen == 0
    assert result.sample == ()


def test_sample_cap_uncapped_returns_all_violators(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _rule("country = 'US'", sample_cap=-1)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.violations_seen == 3
    assert result.violations_capped is False  # we got everything; no overflow possible
    assert len(result.sample) == 3


# ──────────────────────────────────────────────────────────────────────
# Error paths
# ──────────────────────────────────────────────────────────────────────


def test_unknown_column_in_predicate_returns_errored(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _rule("nonexistent_column IS NULL")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "errored"
    assert result.violations_seen == 0
    assert result.error is not None
    assert "nonexistent_column" in result.error


def test_missing_relation_returns_errored(conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("customer_id IS NULL")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="not_a_table")
    assert result.status == "errored"
    assert result.error is not None


@pytest.mark.parametrize(
    "bad_name",
    [
        "bad name",  # space
        "bad-name",  # hyphen
        "1starts_with_digit",
        "",
        ".",
        "a.b.c.d",  # 4 parts > max 3
        "drop;table",
        "name'; DROP",
    ],
)
def test_invalid_relation_name_returns_errored_before_query(
    conn: duckdb.DuckDBPyConnection, bad_name: str
) -> None:
    rule = _rule("customer_id IS NULL")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name=bad_name)
    assert result.status == "errored"
    assert result.error is not None
    assert "invalid relation name" in result.error
    # No SQL was attempted — no leaked engine error
    assert "duckdb" not in (result.error or "").lower()


def test_error_message_truncated_at_512_chars(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    # Build a predicate that produces a long error message via a long
    # column name — duckdb echoes the identifier in the binder error.
    long_col = "x_" + "a" * 600
    rule = _rule(f"{long_col} IS NULL")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "errored"
    assert result.error is not None
    assert len(result.error) <= 512


# ──────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────


def test_empty_relation_returns_passed(empty_conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("email IS NULL")
    result = DuckdbEvaluator(empty_conn).evaluate(rule, relation_name="customers")
    assert result.status == "passed"
    assert result.violations_seen == 0


def test_predicate_with_subquery_works(conn: duckdb.DuckDBPyConnection) -> None:
    """Non-trivial SQL — caller's predicate, not auto-built — must
    pass through. The cheap-eval lint warned about subqueries at sync
    time; the engine still runs them."""
    rule = _rule("customer_id IN (SELECT customer_id FROM customers WHERE country = 'US')")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 3


def test_dotted_relation_name_works(conn: duckdb.DuckDBPyConnection) -> None:
    """3-part names like ``catalog.schema.table`` are accepted. We
    register one as a 2-part name (``main.customers``) to prove the
    quoting works without a real catalog."""
    rule = _rule("customer_id = 1")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="main.customers")
    assert result.status == "failed"
    assert result.violations_seen == 1


def test_duration_ms_is_captured(conn: duckdb.DuckDBPyConnection) -> None:
    rule = _rule("customer_id < 1000")
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.duration_ms >= 0  # may be 0 on very fast machines


# ──────────────────────────────────────────────────────────────────────
# Action / severity / layer / injection invariants — evaluator is
# purely fact-finding; these axes belong to action engine + reporting.
# ──────────────────────────────────────────────────────────────────────


def test_quarantine_action_evaluates_identically(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """quarantine_record affects what happens *after* eval; the eval
    output (violations + sample) is the same."""
    rule = _rule(
        "country = 'US'",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 3


def test_stop_pipeline_action_evaluates_identically(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _rule("country = 'US'", action=Action.STOP_PIPELINE, severity=Severity.ERROR)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 3


@pytest.mark.parametrize("severity", [Severity.INFO, Severity.WARNING, Severity.ERROR])
def test_severity_does_not_alter_evaluation(
    conn: duckdb.DuckDBPyConnection, severity: Severity
) -> None:
    rule = _rule("email IS NULL", severity=severity)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"
    assert result.violations_seen == 1


@pytest.mark.parametrize("layer", [Layer.BRONZE, Layer.SILVER, Layer.GOLD])
def test_layer_does_not_alter_evaluation(conn: duckdb.DuckDBPyConnection, layer: Layer) -> None:
    rule = _rule("email IS NULL", layer=layer)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"


@pytest.mark.parametrize(
    "injection_point",
    [InjectionPoint.POST_READER, InjectionPoint.POST_PROCESSOR, InjectionPoint.POST_WRITE],
)
def test_injection_point_does_not_alter_evaluation(
    conn: duckdb.DuckDBPyConnection, injection_point: InjectionPoint
) -> None:
    rule = _rule("email IS NULL", injection_point=injection_point)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.status == "failed"


# ──────────────────────────────────────────────────────────────────────
# Protocol conformance
# ──────────────────────────────────────────────────────────────────────


def test_duckdb_evaluator_satisfies_protocol(conn: duckdb.DuckDBPyConnection) -> None:
    """The action engine programs against :class:`DqEvaluator`. Static
    typing should accept ``DuckdbEvaluator`` anywhere ``DqEvaluator``
    is required."""
    evaluator: DqEvaluator = DuckdbEvaluator(conn)
    rule = _rule("customer_id = 1")
    result: RuleEvaluation = evaluator.evaluate(rule, relation_name="customers")
    assert result.status == "failed"


# ──────────────────────────────────────────────────────────────────────
# RuleEvaluation surface
# ──────────────────────────────────────────────────────────────────────


def test_rule_evaluation_is_frozen() -> None:
    from dataclasses import FrozenInstanceError

    rule = _rule("email IS NULL")
    result = RuleEvaluation(
        rule=rule,
        status="passed",
        violations_seen=0,
        violations_capped=False,
    )
    with pytest.raises(FrozenInstanceError):
        result.violations_seen = 5  # type: ignore[misc]


def test_rule_evaluation_carries_rule_back(conn: duckdb.DuckDBPyConnection) -> None:
    """Phase 3's action engine reads the rule off the evaluation —
    so the round-trip identity matters."""
    rule = _rule("email IS NULL", name="email_not_null", severity=Severity.ERROR)
    result = DuckdbEvaluator(conn).evaluate(rule, relation_name="customers")
    assert result.rule is rule


# ──────────────────────────────────────────────────────────────────────
# null handling — predicate semantics matter; these guard against
# accidental "IS NULL means empty string"-style regressions.
# ──────────────────────────────────────────────────────────────────────


def test_is_null_matches_only_null(conn: duckdb.DuckDBPyConnection) -> None:
    """Sanity check on SQL semantics — emails 'a@b.com' / 'bad-email'
    must not match IS NULL; only customer_id=2 has NULL email."""
    rule = _rule("email IS NULL")
    samples: tuple[dict[str, Any], ...] = (
        DuckdbEvaluator(conn).evaluate(rule, relation_name="customers").sample
    )
    assert {row["customer_id"] for row in samples} == {2}
