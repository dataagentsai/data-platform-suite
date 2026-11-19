"""Unit tests for the DQ action engine (Phase 3).

Covers the dispatch contract:

* ``passed``  → audit row with action_taken=none; no violations
* ``errored`` → audit row with error_message; **no halt** (fail-open)
* ``failed`` with each authored action:
   - ``stop_pipeline`` → audit + sample → :class:`DqHaltError`
   - ``continue_log`` → audit + sample
   - ``quarantine_record`` x``log_only``  → audit + sample only
   - ``quarantine_record`` x``side_table`` → + writer side-effect

Plus repository + quarantine round-trip checks (data actually lands
where the engine claims it did).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest
from lake_ingestion_framework.core.dq import (
    Action,
    ActionEngine,
    ActionTaken,
    DeltaSideTableWriter,
    DqHaltError,
    DqRule,
    DuckdbQuarantineWriter,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    QuarantineWriter,
    RuleEvaluation,
    Severity,
)
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType

# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo_with_rule() -> Iterator[tuple[SqliteMetadataRepository, int, int]]:
    """SQLite repo seeded with one source + dataset + DQ rule.

    Yields ``(repo, dataset_id, rule_id)``.
    """
    r = SqliteMetadataRepository(":memory:")
    try:
        r.bootstrap_schema()
        source_id = r.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"connection_pattern": "azure_basic", "auth_method": "managed_identity"},
        )
        dataset_id = r.upsert_dataset(
            source_id=source_id,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "load_type": "INCREMENTAL",
                "target_path_template": "raw/{source}/{object}",
                "schema_name": "dbo",
                "object_name": "customers",
                "watermark_column": "modified_at",
            },
            columns=[
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="customer_id",
                    data_type="INT",
                    is_nullable=False,
                    ordinal_position=1,
                    is_primary_key=True,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                ),
            ],
        )
        # The actual rule_id is assigned by upsert_dq_rule_set; we
        # short-circuit by directly inserting since this fixture is
        # only seeding state for the action engine to write against.
        from lake_ingestion_framework.core.dq import DqRuleSet

        rs = DqRuleSet(
            dataset_name="customers",
            layer=Layer.SILVER,
            rules=(_make_rule("email IS NULL", action=Action.CONTINUE_LOG),),
        )
        r.upsert_dq_rule_set(rs, dataset_id=dataset_id)
        rules = r.list_dq_rules(dataset_id=dataset_id, layer=Layer.SILVER)
        yield r, dataset_id, rules[0].rule_id
    finally:
        r.close()


@pytest.fixture()
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE customers (customer_id INTEGER, email VARCHAR, country VARCHAR)")
    c.execute(
        """
        INSERT INTO customers VALUES
            (1, 'a@example.com', 'US'),
            (2, NULL,            'UK'),
            (3, 'bad-email',     'US'),
            (4, NULL,            'IN'),
            (5, 'd@example.com', 'US')
        """
    )
    yield c
    c.close()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _make_rule(
    predicate: str,
    *,
    name: str = "email_not_null",
    action: Action = Action.CONTINUE_LOG,
    quarantine_strategy: QuarantineStrategy | None = None,
    sample_cap: int = 100,
    severity: Severity = Severity.WARNING,
) -> DqRule:
    return DqRule(
        name=name,
        dataset_name="customers",
        layer=Layer.SILVER,
        predicate=predicate,
        injection_point=InjectionPoint.POST_PROCESSOR,
        severity=severity,
        action=action,
        quarantine_strategy=quarantine_strategy,
        sample_cap=sample_cap,
    )


def _evaluation(
    rule: DqRule,
    *,
    status: str = "failed",
    sample: tuple[dict[str, Any], ...] = (),
    violations_seen: int = 0,
    error: str | None = None,
) -> RuleEvaluation:
    """Build a synthetic RuleEvaluation. Tests want full control over
    the evaluator output without re-deriving it from a duckdb table."""
    return RuleEvaluation(
        rule=rule,
        status=status,  # type: ignore[arg-type]
        violations_seen=violations_seen,
        violations_capped=False,
        sample=sample,
        duration_ms=12,
        error=error,
    )


# ──────────────────────────────────────────────────────────────────────
# Status branches
# ──────────────────────────────────────────────────────────────────────


def test_passed_records_audit_row_only(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL")
    eval_ = _evaluation(rule, status="passed")

    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-1")

    assert outcome.action_taken == ActionTaken.NONE
    assert outcome.violators_total == 0
    assert outcome.sample_captured == 0

    results = repo.list_dq_results(rule_id=rule_id)
    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].action_taken == "none"
    assert results[0].error_message is None
    # No violations rows on a pass.
    assert repo.list_dq_violations(result_id=results[0].result_id) == []


def test_errored_records_audit_with_error_no_halt(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("nonexistent_column IS NULL", action=Action.STOP_PIPELINE)
    eval_ = _evaluation(rule, status="errored", error="binder error: unknown column")

    # Even a stop_pipeline rule does NOT halt when the evaluator errored —
    # this is the fail-open contract.
    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-2")

    assert outcome.action_taken == ActionTaken.ERRORED
    results = repo.list_dq_results(rule_id=rule_id)
    assert results[0].passed is False
    assert results[0].action_taken == "errored"
    assert results[0].error_message == "binder error: unknown column"


def test_failed_continue_log_records_sample(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL", action=Action.CONTINUE_LOG)
    sample = (
        {"customer_id": 2, "email": None, "country": "UK"},
        {"customer_id": 4, "email": None, "country": "IN"},
    )
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=2)

    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-3")

    assert outcome.action_taken == ActionTaken.CONTINUE_LOG
    assert outcome.violators_total == 2
    assert outcome.sample_captured == 2

    violations = repo.list_dq_violations(result_id=outcome.result_id)
    assert len(violations) == 2
    # JSON round-trips — pk_columns_json is parseable.
    parsed = [json.loads(v.pk_columns_json) for v in violations]
    assert {row["customer_id"] for row in parsed} == {2, 4}


def test_failed_stop_pipeline_writes_audit_then_raises(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL", action=Action.STOP_PIPELINE)
    sample = ({"customer_id": 2, "email": None, "country": "UK"},)
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=1)

    with pytest.raises(DqHaltError) as exc_info:
        ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-4")

    # Audit row landed BEFORE the halt — caller can correlate the
    # halt to the result_id in the exception.
    outcome = exc_info.value.outcome
    assert outcome.action_taken == ActionTaken.STOP_PIPELINE
    assert outcome.violators_total == 1

    results = repo.list_dq_results(rule_id=rule_id)
    assert len(results) == 1
    assert results[0].action_taken == "stop_pipeline"
    assert results[0].passed is False
    # Sample wrote to dq_violations even though we halted.
    assert len(repo.list_dq_violations(result_id=results[0].result_id)) == 1


def test_failed_quarantine_log_only_writes_sample_only(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.LOG_ONLY,
    )
    sample = ({"customer_id": 2, "email": None, "country": "UK"},)
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=1)

    # log_only: writer is configured but should NOT be called.
    writer = _SpyQuarantineWriter(real=DuckdbQuarantineWriter(conn))
    outcome = ActionEngine(repo, quarantine_writer=writer).apply(
        eval_, rule_id=rule_id, run_id="r-5", relation_name="customers"
    )

    assert outcome.action_taken == ActionTaken.QUARANTINE_LOG_ONLY
    assert outcome.quarantined_to_side_table == 0
    assert writer.calls == []  # writer never invoked
    assert len(repo.list_dq_violations(result_id=outcome.result_id)) == 1  # audit sample landed


def test_failed_quarantine_side_table_invokes_writer(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
        sample_cap=1,  # cap audit sample; side table should still get all
    )
    # Evaluator returned only 1 (capped) sample row, but the writer
    # runs its own SELECT WHERE pred and pulls every match.
    sample = ({"customer_id": 2, "email": None, "country": "UK"},)
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=1)

    writer = DuckdbQuarantineWriter(conn)
    outcome = ActionEngine(repo, quarantine_writer=writer).apply(
        eval_, rule_id=rule_id, run_id="r-6", relation_name="customers"
    )

    assert outcome.action_taken == ActionTaken.QUARANTINE_SIDE_TABLE
    # 2 actual NULL emails in the seed; cap doesn't apply to side table.
    assert outcome.quarantined_to_side_table == 2

    # Side table exists with both violators + DQ metadata columns.
    rows = conn.execute(
        "SELECT customer_id, dq_run_id, dq_rule_id FROM customers__quarantine ORDER BY customer_id"
    ).fetchall()
    assert [r[0] for r in rows] == [2, 4]
    assert {r[1] for r in rows} == {"r-6"}
    assert {r[2] for r in rows} == {rule_id}


# ──────────────────────────────────────────────────────────────────────
# Side-table guardrails
# ──────────────────────────────────────────────────────────────────────


def test_side_table_without_relation_name_raises(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    eval_ = _evaluation(
        rule,
        status="failed",
        sample=({"customer_id": 2},),
        violations_seen=1,
    )
    with pytest.raises(ValueError, match="relation_name"):
        ActionEngine(repo, quarantine_writer=DuckdbQuarantineWriter(conn)).apply(
            eval_, rule_id=rule_id, run_id="r-7"
        )


def test_side_table_without_writer_raises(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    eval_ = _evaluation(
        rule,
        status="failed",
        sample=({"customer_id": 2},),
        violations_seen=1,
    )
    # No writer configured.
    with pytest.raises(ValueError, match="quarantine_writer"):
        ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-8", relation_name="customers")


# ──────────────────────────────────────────────────────────────────────
# pk_columns slicing
# ──────────────────────────────────────────────────────────────────────


def test_pk_columns_slices_sample_to_pks_only(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL")
    sample = ({"customer_id": 2, "email": None, "country": "UK", "ssn": "111-22-3333"},)
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=1)

    outcome = ActionEngine(repo).apply(
        eval_,
        rule_id=rule_id,
        run_id="r-9",
        pk_columns=("customer_id",),
    )

    violations = repo.list_dq_violations(result_id=outcome.result_id)
    payload = json.loads(violations[0].pk_columns_json)
    assert payload == {"customer_id": 2}
    assert "ssn" not in payload  # PII-class column not stored


def test_no_pk_columns_stores_full_row(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL")
    sample = ({"customer_id": 2, "email": None, "country": "UK"},)
    eval_ = _evaluation(rule, status="failed", sample=sample, violations_seen=1)

    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-10")

    violations = repo.list_dq_violations(result_id=outcome.result_id)
    payload = json.loads(violations[0].pk_columns_json)
    assert payload == {"customer_id": 2, "email": None, "country": "UK"}


# ──────────────────────────────────────────────────────────────────────
# sample_cap=0 — bool-only path through the action engine
# ──────────────────────────────────────────────────────────────────────


def test_sample_cap_zero_failed_records_minus_one(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL", sample_cap=0)
    # Bool path: violations_seen=-1, sample empty.
    eval_ = _evaluation(rule, status="failed", sample=(), violations_seen=-1)

    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-11")

    assert outcome.violators_total == -1
    assert outcome.sample_captured == 0

    results = repo.list_dq_results(rule_id=rule_id)
    assert results[0].violators_total == -1
    assert repo.list_dq_violations(result_id=results[0].result_id) == []


# ──────────────────────────────────────────────────────────────────────
# Round-trip
# ──────────────────────────────────────────────────────────────────────


def test_multiple_apply_calls_create_distinct_results(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL")
    engine = ActionEngine(repo)
    o1 = engine.apply(_evaluation(rule, status="passed"), rule_id=rule_id, run_id="r-A")
    o2 = engine.apply(
        _evaluation(rule, status="failed", sample=({"customer_id": 2},), violations_seen=1),
        rule_id=rule_id,
        run_id="r-B",
    )
    assert o1.result_id != o2.result_id
    assert {r.run_id for r in repo.list_dq_results(rule_id=rule_id)} == {"r-A", "r-B"}


def test_run_id_round_trips_into_results_and_violations(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL")
    eval_ = _evaluation(rule, status="failed", sample=({"customer_id": 2},), violations_seen=1)
    outcome = ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="run-xyz")

    results = repo.list_dq_results(run_id="run-xyz")
    assert [r.result_id for r in results] == [outcome.result_id]


# ──────────────────────────────────────────────────────────────────────
# DqHaltError surface
# ──────────────────────────────────────────────────────────────────────


def test_dq_halt_error_message_contains_rule_and_count(
    repo_with_rule: tuple[SqliteMetadataRepository, int, int],
) -> None:
    repo, _, rule_id = repo_with_rule
    rule = _make_rule("email IS NULL", action=Action.STOP_PIPELINE, name="email_not_null_strict")
    eval_ = _evaluation(rule, status="failed", sample=({"customer_id": 2},), violations_seen=7)
    with pytest.raises(DqHaltError) as exc_info:
        ActionEngine(repo).apply(eval_, rule_id=rule_id, run_id="r-halt")
    msg = str(exc_info.value)
    assert "email_not_null_strict" in msg
    assert "7" in msg  # violator count surfaced


# ──────────────────────────────────────────────────────────────────────
# DuckdbQuarantineWriter direct tests (writer in isolation)
# ──────────────────────────────────────────────────────────────────────


def test_quarantine_writer_creates_table_and_inserts(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    written = DuckdbQuarantineWriter(conn).write_violators(
        rule=rule, relation_name="customers", rule_id=42, run_id="run-q"
    )
    assert written == 2

    # Side table exists; columns include DQ metadata.
    schema = conn.execute("DESCRIBE customers__quarantine").fetchall()
    column_names = {row[0] for row in schema}
    assert {"customer_id", "email", "country"}.issubset(column_names)
    assert {"dq_run_id", "dq_rule_id", "dq_captured_at"}.issubset(column_names)


def test_quarantine_writer_idempotent_for_table_creation(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Two calls — second one should append, not blow up on duplicate
    CREATE TABLE."""
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    writer = DuckdbQuarantineWriter(conn)
    writer.write_violators(rule=rule, relation_name="customers", rule_id=1, run_id="A")
    writer.write_violators(rule=rule, relation_name="customers", rule_id=1, run_id="B")
    # 2 violators x 2 runs = 4 rows
    count = conn.execute("SELECT COUNT(*) FROM customers__quarantine").fetchone()
    assert count is not None
    assert count[0] == 4


# ──────────────────────────────────────────────────────────────────────
# DeltaSideTableWriter — Phase 4b
# ──────────────────────────────────────────────────────────────────────


def test_delta_side_table_writer_writes_violators(
    conn: duckdb.DuckDBPyConnection, tmp_path: pytest.TempPathFactory
) -> None:
    """Predicate matches 2 rows → those 2 land in a Delta directory
    under target_root, with DQ-metadata cols appended."""
    import deltalake

    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    writer = DeltaSideTableWriter(conn, target_root=tmp_path)  # type: ignore[arg-type]

    written = writer.write_violators(
        rule=rule, relation_name="customers", rule_id=42, run_id="run-Q"
    )
    assert written == 2

    # Read back via deltalake to confirm the rows landed with metadata cols.
    target_dir = tmp_path / "customers__quarantine"  # type: ignore[operator]
    rows = deltalake.DeltaTable(str(target_dir)).to_pyarrow_table().to_pylist()
    assert len(rows) == 2
    assert {r["customer_id"] for r in rows} == {2, 4}
    # DQ metadata cols appended
    for row in rows:
        assert row["dq_run_id"] == "run-Q"
        assert row["dq_rule_id"] == 42
        assert row["dq_captured_at"] is not None


def test_delta_side_table_writer_zero_violators_is_no_op(
    conn: duckdb.DuckDBPyConnection, tmp_path: pytest.TempPathFactory
) -> None:
    """Predicate that matches no rows → return 0 and don't create the
    Delta directory (avoids empty quarantine spam)."""
    rule = _make_rule(
        "customer_id < 0",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    writer = DeltaSideTableWriter(conn, target_root=tmp_path)  # type: ignore[arg-type]
    written = writer.write_violators(
        rule=rule, relation_name="customers", rule_id=1, run_id="run-N"
    )
    assert written == 0
    assert not (tmp_path / "customers__quarantine").exists()  # type: ignore[operator]


def test_delta_side_table_writer_appends_across_runs(
    conn: duckdb.DuckDBPyConnection, tmp_path: pytest.TempPathFactory
) -> None:
    """Two write_violators calls → 2 + 2 = 4 rows in the side table.

    Proves the Delta append semantics — each run accumulates audit data."""
    import deltalake

    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    writer = DeltaSideTableWriter(conn, target_root=tmp_path)  # type: ignore[arg-type]
    writer.write_violators(rule=rule, relation_name="customers", rule_id=1, run_id="run-A")
    writer.write_violators(rule=rule, relation_name="customers", rule_id=1, run_id="run-B")

    target_dir = tmp_path / "customers__quarantine"  # type: ignore[operator]
    rows = deltalake.DeltaTable(str(target_dir)).to_pyarrow_table().to_pylist()
    assert len(rows) == 4
    assert {r["dq_run_id"] for r in rows} == {"run-A", "run-B"}


def test_delta_side_table_writer_rejects_invalid_relation_name(
    conn: duckdb.DuckDBPyConnection, tmp_path: pytest.TempPathFactory
) -> None:
    rule = _make_rule(
        "email IS NULL",
        action=Action.QUARANTINE_RECORD,
        quarantine_strategy=QuarantineStrategy.SIDE_TABLE,
    )
    writer = DeltaSideTableWriter(conn, target_root=tmp_path)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid relation name"):
        writer.write_violators(
            rule=rule,
            relation_name="bad name; DROP TABLE x",
            rule_id=1,
            run_id="run-X",
        )


# ──────────────────────────────────────────────────────────────────────
# Test helpers
# ──────────────────────────────────────────────────────────────────────


class _SpyQuarantineWriter:
    """Wraps a real writer and records the calls made to it.

    Lets log-only tests prove the writer was *not* invoked while
    side-table tests can still use the real duckdb writer underneath.
    """

    def __init__(self, real: QuarantineWriter) -> None:
        self._real = real
        self.calls: list[dict[str, Any]] = []

    def write_violators(
        self, *, rule: DqRule, relation_name: str, rule_id: int, run_id: str
    ) -> int:
        self.calls.append(
            {
                "rule": rule.name,
                "relation_name": relation_name,
                "rule_id": rule_id,
                "run_id": run_id,
            }
        )
        return self._real.write_violators(
            rule=rule, relation_name=relation_name, rule_id=rule_id, run_id=run_id
        )
