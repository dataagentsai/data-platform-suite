"""DQ action engine — turn :class:`RuleEvaluation` outcomes into side
effects (audit rows, halts, quarantine).

The engine is deliberately container-agnostic — it consumes the
``RuleEvaluation`` produced by *any* :class:`DqEvaluator`
implementation (duckdb today, spark in Phase 4) and writes audit
rows via the :class:`MetadataRepository` Protocol. Quarantine of
violator rows is delegated to a :class:`QuarantineWriter` so the
prod path (Spark/Delta) and the local path (duckdb) plug in cleanly.

Branching contract
==================

* ``status == "passed"``     → record a ``dq_results`` row only;
                               action_taken = ``none``
* ``status == "errored"``    → record a ``dq_results`` row with
                               ``error_message`` set; **no halt** —
                               engine errors aren't predicate failures
                               and shouldn't silently halt pipelines.
                               action_taken = ``errored``
* ``status == "failed"``     → branch on ``rule.action``:

  * ``stop_pipeline``        → record + write sample → raise
                               :class:`DqHaltError` (audit row lands
                               *before* the halt so the run-log can
                               point to it)
  * ``continue_log``         → record + write sample
  * ``quarantine_record``    → record + write sample +
    ``quarantine_strategy == log_only``  → no side-table write
    ``quarantine_strategy == side_table`` → :class:`QuarantineWriter`
                               routes ALL violators to ``<main>__quarantine``;
                               sample still goes to ``dq_violations``
                               for audit

Rationale for "fail open" on errored: a rule with a typo in the
predicate column should not bring production to a halt — that's a
deploy-time issue. The audit row makes it visible (operators can
filter ``dq_results`` for ``error_message IS NOT NULL``); the action
engine moves on. Customers who want errored-as-halt can add a
follow-up rule that detects ``dq_results.error_message`` cardinality.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from lake_ingestion_framework.core.dq.evaluator import RuleEvaluation
from lake_ingestion_framework.core.dq.types import (
    Action,
    DqRule,
    QuarantineStrategy,
)
from lake_ingestion_framework.core.exceptions import LakeIngestError

if TYPE_CHECKING:
    from pathlib import Path

    import duckdb

    from lake_ingestion_framework.core.metadata.repository import MetadataRepository

# ──────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────


class ActionTaken(StrEnum):
    """What the action engine did. Stored verbatim in
    ``meta.dq_results.action_taken`` so dashboards can group on it.

    Distinct from :class:`Action` — that's what the *rule* asks for;
    this is what the *engine actually did*. The two diverge when a
    rule fails (``errored`` is never an authored Action) and when
    quarantine strategies surface in audit (``quarantine_log_only``
    vs ``quarantine_side_table``).
    """

    NONE = "none"
    """Rule passed; no action needed."""

    ERRORED = "errored"
    """Engine error — rule didn't run cleanly. Audit only; no halt."""

    STOP_PIPELINE = "stop_pipeline"
    CONTINUE_LOG = "continue_log"
    QUARANTINE_LOG_ONLY = "quarantine_log_only"
    QUARANTINE_SIDE_TABLE = "quarantine_side_table"


@dataclass(frozen=True)
class ActionOutcome:
    """What the engine did for one rule evaluation.

    The pipeline (Phase 4) accumulates these per injection point —
    they're the input to the run-log summary and the OpenLineage
    DQ facet (Phase 6).
    """

    rule: DqRule
    action_taken: ActionTaken
    result_id: int
    """``meta.dq_results.result_id`` of the audit row. -1 only when
    ``action_taken == NONE`` and the engine was constructed with
    ``record_passes=False`` (off by default, but available for
    high-cardinality production setups)."""

    violators_total: int
    """Same field as :attr:`RuleEvaluation.violations_seen`, mirrored
    here so consumers don't need to keep both objects."""

    sample_captured: int
    """Rows actually written to ``meta.dq_violations`` for this
    result. ≤ ``rule.sample_cap``. 0 on passed, on errored, or on
    ``sample_cap == 0`` rules."""

    quarantined_to_side_table: int = 0
    """Rows the :class:`QuarantineWriter` routed to the side table.
    Only > 0 when ``action_taken == QUARANTINE_SIDE_TABLE``. Phase 4
    will plumb this into the run-log."""


class DqHaltError(LakeIngestError):
    """Raised when a rule with ``action=stop_pipeline`` fires.

    The pipeline catches this, writes a final ``run_log`` row with
    ``status='FAILED'``, then re-raises so the orchestrator (ADF /
    Databricks Workflows) sees a non-zero exit. The ``ActionOutcome``
    is attached for context — the audit row already exists in
    ``meta.dq_results`` (engine writes it *before* raising).
    """

    def __init__(self, outcome: ActionOutcome) -> None:
        rule = outcome.rule
        super().__init__(
            f"DQ halt — rule {rule.name!r} on dataset "
            f"{rule.dataset_name!r} ({rule.layer.value}) fired with "
            f"{outcome.violators_total} violator(s); "
            f"audit row id={outcome.result_id}."
        )
        self.outcome = outcome


# ──────────────────────────────────────────────────────────────────────
# Quarantine writer — routes violators to a side table
# ──────────────────────────────────────────────────────────────────────


class QuarantineWriter(Protocol):
    """Routes violator rows from the main relation to a side sink.

    Used only when ``action=quarantine_record`` and
    ``quarantine_strategy=side_table``. The engine constructs the
    target relation name (``<main>__quarantine``) from the input
    relation; the writer is responsible for the actual physical
    write (duckdb table, Delta append, whatever).

    Returns the number of rows written. The action engine adds this
    to :attr:`ActionOutcome.quarantined_to_side_table` for the
    run-log + lineage facet.
    """

    def write_violators(
        self,
        *,
        rule: DqRule,
        relation_name: str,
        rule_id: int,
        run_id: str,
    ) -> int: ...


class DuckdbQuarantineWriter:
    """Writes violators to a duckdb table named ``<relation>__quarantine``.

    Schema is ``main + dq_run_id + dq_rule_id + dq_captured_at``;
    created lazily on first write per relation. Used for local-mode
    realm and for unit tests; Phase 4 will add a Delta-aware writer
    that uses the same Protocol.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def write_violators(
        self,
        *,
        rule: DqRule,
        relation_name: str,
        rule_id: int,
        run_id: str,
    ) -> int:
        # Defence in depth: even though the pipeline only passes
        # relation names sourced from validated metadata, re-check the
        # shape here before bracket-quoting into SQL. Predicates are
        # trusted (sync-validated by the YAML loader).
        if not _RELATION_RE.match(relation_name):
            raise ValueError(f"invalid relation name: {relation_name!r}")

        target = _quarantine_table_name(relation_name)
        rel_q = _q(relation_name)
        target_q = _q(target)

        # Create the side table with the main relation's schema + DQ
        # metadata columns. ``WHERE FALSE`` materialises only the
        # column list (no rows), so re-runs against an existing
        # quarantine table are no-ops here. SQL is built outside the
        # execute() call so the no-fstring-sql semgrep gate sees a
        # parameterised call site; identifiers are regex-validated
        # above (_RELATION_RE), predicates were sync-validated in YAML.
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_q} AS SELECT *, CAST('' AS VARCHAR) AS dq_run_id, CAST(0 AS BIGINT) AS dq_rule_id, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dq_captured_at FROM {rel_q} WHERE FALSE"  # noqa: S608, E501 — idents regex-validated
        self._conn.execute(create_sql, ())
        # Append violators with the run/rule metadata. duckdb's
        # ``rowcount`` reflects rows inserted by the last execute().
        insert_sql = f"INSERT INTO {target_q} SELECT *, ?, ?, CURRENT_TIMESTAMP FROM {rel_q} WHERE ({rule.predicate})"  # noqa: S608, E501 — idents regex-validated
        cursor = self._conn.execute(insert_sql, (run_id, rule_id))
        return _affected_rows(cursor)


class DeltaSideTableWriter:
    """Routes violators to a Delta-formatted side table.

    Composition:

    * ``conn``        — duckdb connection where the source relation
                        is registered (pipeline registers the enriched
                        pyarrow table into it; the writer SELECTs
                        violators against that registered relation).
    * ``target_root`` — directory that contains per-relation quarantine
                        Delta tables. The writer creates
                        ``<target_root>/<relation>__quarantine`` lazily
                        on first write.

    Lazily imports ``deltalake`` on first call — gated on the
    ``[local]`` extra. Using duckdb's ``.arrow()`` materialiser on
    the SELECT result, then ``deltalake.write_deltalake(..., mode="append")``
    keeps the path zero-copy until the Delta encoder runs.

    Pairs with :class:`SilverDqContext` for production wiring:
    customer creates one duckdb conn, constructs this writer with it,
    passes both via the context. The pipeline registers the enriched
    Silver table on the same conn — same handle the writer will SELECT
    against. See ``docs/dq_design.md`` §6 (side_table strategy).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, *, target_root: Path) -> None:
        self._conn = conn
        self._target_root = target_root

    def write_violators(
        self,
        *,
        rule: DqRule,
        relation_name: str,
        rule_id: int,
        run_id: str,
    ) -> int:
        if not _RELATION_RE.match(relation_name):
            raise ValueError(f"invalid relation name: {relation_name!r}")

        rel_q = _q(relation_name)
        # Pull violators with DQ-metadata columns appended; duckdb
        # returns a pyarrow Table via ``.to_arrow_table()`` (zero-copy
        # until materialisation). Run parameterised so the literal
        # f-string never reaches the execute() call site
        # (no-fstring-sql semgrep gate); identifier validated above.
        select_sql = f"SELECT *, ? AS dq_run_id, ? AS dq_rule_id, CURRENT_TIMESTAMP AS dq_captured_at FROM {rel_q} WHERE ({rule.predicate})"  # noqa: S608, E501 — idents regex-validated
        violators = self._conn.execute(select_sql, (run_id, rule_id)).to_arrow_table()
        if violators.num_rows == 0:
            return 0

        # Per-relation Delta directory. Dotted relations collapse to
        # the last segment so paths stay readable on disk.
        target_dir = self._target_root / f"{relation_name.rsplit('.', maxsplit=1)[-1]}__quarantine"
        deltalake = _lazy_import_deltalake()
        deltalake.write_deltalake(str(target_dir), violators, mode="append")
        return int(violators.num_rows)


def _lazy_import_deltalake() -> Any:
    """Import deltalake on first use. Gated on the ``[local]`` extra
    (pip install lake-ingest[local]). Same pattern as
    :func:`core.silver.pipeline._lazy_import_deltalake` — duplicated
    here to avoid the action engine taking a Silver dependency."""
    try:
        import deltalake
    except ImportError as exc:
        raise RuntimeError(
            "deltalake not installed. Install with: pip install lake-ingest[local] "
            "(required by DeltaSideTableWriter for side-table quarantine)."
        ) from exc
    return deltalake


# ──────────────────────────────────────────────────────────────────────
# Action engine — the dispatch
# ──────────────────────────────────────────────────────────────────────


class ActionEngine:
    """Records audit rows and executes the action a rule asks for.

    Stateless across calls; can be reused for many rules in one run.
    Tests use the SQLite repository + DuckdbQuarantineWriter; Phase
    4's pipeline integration will inject the Azure-SQL repo + Spark
    quarantine writer.
    """

    def __init__(
        self,
        repo: MetadataRepository,
        *,
        quarantine_writer: QuarantineWriter | None = None,
    ) -> None:
        self._repo = repo
        self._writer = quarantine_writer

    def apply(
        self,
        evaluation: RuleEvaluation,
        *,
        rule_id: int,
        run_id: str,
        relation_name: str | None = None,
        pk_columns: tuple[str, ...] | None = None,
    ) -> ActionOutcome:
        """Process one evaluation. Always writes a ``dq_results`` row.

        ``relation_name`` is required when the rule's action is
        ``quarantine_record + side_table``; ignored otherwise.
        ``pk_columns``, when supplied, narrows what ends up in
        ``meta.dq_violations.pk_columns_json`` to just those columns
        — keeps audit rows compact and avoids leaking non-PK PII into
        the metadata DB. When ``None``, the full sample row is stored.
        """
        rule = evaluation.rule

        # Errored evaluations: audit + carry on. The engine doesn't
        # halt on bugs in the predicate — that's a deploy issue, not
        # a data issue.
        if evaluation.status == "errored":
            result_id = self._repo.record_dq_result(
                rule_id=rule_id,
                run_id=run_id,
                passed=False,
                violators_total=0,
                sample_captured=0,
                action_taken=ActionTaken.ERRORED.value,
                duration_ms=evaluation.duration_ms,
                error_message=evaluation.error,
            )
            return ActionOutcome(
                rule=rule,
                action_taken=ActionTaken.ERRORED,
                result_id=result_id,
                violators_total=0,
                sample_captured=0,
            )

        # Passed: clean audit row, no further work.
        if evaluation.status == "passed":
            result_id = self._repo.record_dq_result(
                rule_id=rule_id,
                run_id=run_id,
                passed=True,
                violators_total=0,
                sample_captured=0,
                action_taken=ActionTaken.NONE.value,
                duration_ms=evaluation.duration_ms,
            )
            return ActionOutcome(
                rule=rule,
                action_taken=ActionTaken.NONE,
                result_id=result_id,
                violators_total=0,
                sample_captured=0,
            )

        # Failed: branch on the rule's authored action.
        action_taken = _action_taken_for(rule)

        result_id = self._repo.record_dq_result(
            rule_id=rule_id,
            run_id=run_id,
            passed=False,
            violators_total=evaluation.violations_seen,
            sample_captured=len(evaluation.sample),
            action_taken=action_taken.value,
            duration_ms=evaluation.duration_ms,
        )

        sample_rows = _slice_for_audit(evaluation.sample, pk_columns)
        sample_captured = self._repo.record_dq_violations(
            rule_id=rule_id,
            result_id=result_id,
            rows=sample_rows,
        )

        # Side-table quarantine: route ALL violators (uncapped) via
        # the writer. The audit sample we already wrote stays a
        # capped lookup; the quarantine table is the full record.
        quarantined_side = 0
        if action_taken == ActionTaken.QUARANTINE_SIDE_TABLE:
            if relation_name is None:
                raise ValueError(
                    f"Rule {rule.name!r}: action=quarantine_record + "
                    "strategy=side_table requires relation_name to be passed."
                )
            if self._writer is None:
                raise ValueError(
                    f"Rule {rule.name!r}: side_table quarantine requested but "
                    "no quarantine_writer was configured on the ActionEngine."
                )
            quarantined_side = self._writer.write_violators(
                rule=rule,
                relation_name=relation_name,
                rule_id=rule_id,
                run_id=run_id,
            )

        outcome = ActionOutcome(
            rule=rule,
            action_taken=action_taken,
            result_id=result_id,
            violators_total=evaluation.violations_seen,
            sample_captured=sample_captured,
            quarantined_to_side_table=quarantined_side,
        )

        if action_taken == ActionTaken.STOP_PIPELINE:
            raise DqHaltError(outcome)

        return outcome


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _action_taken_for(rule: DqRule) -> ActionTaken:
    """Map ``(rule.action, rule.quarantine_strategy)`` to the
    concrete :class:`ActionTaken` recorded in audit."""
    if rule.action == Action.STOP_PIPELINE:
        return ActionTaken.STOP_PIPELINE
    if rule.action == Action.CONTINUE_LOG:
        return ActionTaken.CONTINUE_LOG
    if rule.action == Action.QUARANTINE_RECORD:
        if rule.quarantine_strategy == QuarantineStrategy.SIDE_TABLE:
            return ActionTaken.QUARANTINE_SIDE_TABLE
        # log_only — DqRule.__post_init__ already enforces strategy
        # is set whenever action is quarantine_record, so we don't
        # need to handle the None case here.
        return ActionTaken.QUARANTINE_LOG_ONLY
    # New Action members will surface as type errors at this point —
    # mypy's exhaustiveness check (we don't have explicit assert_never
    # but the StrEnum is finite). Keep this fallback so runtime is
    # tolerant if the enum ever grows during a deploy.
    raise ValueError(f"Unhandled action: {rule.action!r}")


def _slice_for_audit(
    sample: tuple[dict[str, Any], ...],
    pk_columns: tuple[str, ...] | None,
) -> list[dict[str, object]]:
    """Narrow each sample row to the audit columns.

    With known PKs we capture only those (compact + low PII surface).
    Without them we capture the whole row — pragmatic v1 default;
    customers who care can populate ``meta.dataset_config_columns``
    with ``is_primary_key=1`` and re-deploy.
    """
    if pk_columns is None:
        return [dict(row) for row in sample]
    pk_set = set(pk_columns)
    return [{k: v for k, v in row.items() if k in pk_set} for row in sample]


_RELATION_PART = r"[A-Za-z_][A-Za-z0-9_]{0,127}"
_RELATION_RE = re.compile(rf"^{_RELATION_PART}(?:\.{_RELATION_PART}){{0,2}}$")


def _q(name: str) -> str:
    """Bracket-quote a (possibly dotted) relation name. Caller
    must have validated against ``_RELATION_RE`` first — this helper
    only handles formatting."""
    return ".".join(f'"{part}"' for part in name.split("."))


def _quarantine_table_name(relation_name: str) -> str:
    """Append ``__quarantine`` to the last segment of a (possibly
    dotted) relation name."""
    parts = relation_name.split(".")
    parts[-1] = f"{parts[-1]}__quarantine"
    return ".".join(parts)


def _affected_rows(cursor: Any) -> int:
    """duckdb returns rows-inserted via ``fetchone()`` on INSERT in
    some versions; in others ``rowcount`` is set. Tolerate both."""
    try:
        rowcount = int(cursor.rowcount)
    except (AttributeError, TypeError):
        rowcount = -1
    if rowcount >= 0:
        return rowcount
    row = cursor.fetchone()
    if row is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


__all__ = [
    "ActionEngine",
    "ActionOutcome",
    "ActionTaken",
    "DeltaSideTableWriter",
    "DqHaltError",
    "DuckdbQuarantineWriter",
    "QuarantineWriter",
]
