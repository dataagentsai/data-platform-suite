"""Bronze → Silver pipeline.

The pipeline reads a Bronze Delta table, drops Bronze framework metadata,
adds Silver framework metadata (with a recomputed ``_record_hash``), and
writes the result to the Silver Delta table per the configured strategy.

Strategies (per ``docs/silver_layer_design.md`` §4):

- ``append`` — write rows as-is; no merge logic. Suitable for immutable
  events or when downstream consumers tolerate duplicates.
- ``scd1``  — current-state upsert. Initial run creates the target;
  subsequent runs MERGE source rows on ``business_keys`` (insert new,
  update existing — every column overwritten on match). No history.
- ``scd2``  — versioned history. Each row carries ``_is_current``,
  ``_valid_from``, ``_valid_to``. The customer-supplied
  ``scd2_order_by`` column populates ``_valid_from`` for new versions
  and ``_valid_to`` when a previous current row is closed. Subsequent
  runs classify source rows by natural-key match + hash compare:
  changed rows close the previous version + insert a new one; new
  natural keys insert as current; unchanged rows are skipped.
- ``upsert`` — same write semantics as ``scd1`` (MERGE on natural key;
  current state, no history). Distinct enum value because the design
  doc names it as the auto-classification "safe fallback" when the
  framework can't infer a strategy from metadata. Routes through the
  same ``_write_scd1`` helper.

Why deltalake (Rust binding) and not delta-spark: per ``LocalDeltaWriter``
(Phase 0b), no JVM, no Spark cold-start. Same lazy-import pattern.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from lake_ingestion_framework.core.dq import (
    ActionEngine,
    ActionTaken,
    DqRule,
    DuckdbEvaluator,
    InjectionPoint,
    Layer,
    QuarantineWriter,
)
from lake_ingestion_framework.core.exceptions import (
    LakeIngestError,
    SinkUnavailableError,
    ValidationError,
    WriteFailedError,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_METADATA_COLS,
    SILVER_IS_CURRENT,
    SILVER_LOADED_AT,
    SILVER_RECORD_HASH,
    SILVER_RUN_ID,
    SILVER_VALID_FROM,
    SILVER_VALID_TO,
    compute_record_hash,
)
from lake_ingestion_framework.core.processors import (
    DedupConfig,
    ProcessorEngine,
    ProcessorRunReport,
)

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

    from lake_ingestion_framework.core.metadata.repository import (
        DqRuleRow,
        MetadataRepository,
    )


# ──────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────


class SilverStrategy(StrEnum):
    """Write strategies the pipeline supports.

    StrEnum so callers can pass either ``"append"`` or
    ``SilverStrategy.APPEND`` interchangeably (both compare equal).
    """

    APPEND = "append"
    SCD1 = "scd1"
    SCD2 = "scd2"
    UPSERT = "upsert"  # same write semantics as SCD1; distinct for classification


@dataclass(frozen=True)
class SilverDqContext:
    """Inputs needed to run DQ at the ``post_processor`` injection point.

    Pass via :attr:`SilverConfig.dq_context` to enable DQ evaluation;
    leave as ``None`` to skip DQ entirely (existing behaviour preserved).

    The pipeline evaluates rules **before** writing Silver — so a
    ``stop_pipeline`` rule halts cleanly without leaving partial data
    behind. Other actions (``continue_log``, ``quarantine_record``)
    log via :class:`ActionEngine` and the Silver write proceeds.

    Phase 4 v1 supports ``continue_log``, ``stop_pipeline``, and
    ``quarantine_record + log_only``. ``quarantine_record + side_table``
    requires a customer-supplied :class:`QuarantineWriter` that
    persists violators outside duckdb (e.g., a Delta-aware writer);
    no default Delta writer ships in this phase.
    """

    repo: MetadataRepository
    """Where DQ rules live and where audit rows land."""

    dataset_id: int
    """Filters ``meta.dq_rules`` to rules attached to this dataset."""

    dataset_name: str
    """Used to construct :class:`DqRule` instances from ``DqRuleRow``
    (the natural-key surface the evaluator reads). Should match
    ``dataset_master.dataset_name`` for the row identified by
    ``dataset_id``."""

    relation_name: str = "silver"
    """Name to register the in-memory pyarrow Table as in duckdb. The
    evaluator's ``SELECT ... FROM <relation_name>`` queries against
    this. Default ``"silver"`` is convenient for tests; production
    wiring should pass the dataset name (e.g., ``"customers"``) so
    audit messages are readable."""

    pk_columns: tuple[str, ...] = ()
    """When non-empty, only these columns of each violator are stored
    in ``meta.dq_violations.pk_columns_json``. Empty → store the full
    sample row (pragmatic v1 default)."""

    quarantine_writer: QuarantineWriter | None = None
    """Required iff a rule has ``action=quarantine_record`` +
    ``strategy=side_table``. Pair with :attr:`conn` (the writer needs
    a duckdb conn to materialise violators); see
    :class:`DeltaSideTableWriter` for the production-grade impl."""

    conn: duckdb.DuckDBPyConnection | None = None
    """Optional duckdb connection. When supplied, the pipeline
    registers the enriched table into it and the customer remains
    responsible for closing it. When ``None``, the pipeline creates a
    private ``:memory:`` conn for the run and closes it at the end —
    the Phase 4a default. Pass a customer-managed conn when wiring a
    :class:`DeltaSideTableWriter` (or any other writer that needs the
    conn at construction)."""


@dataclass(frozen=True)
class SilverProcessorContext:
    """Inputs the Wave-1 processor engine needs from the metadata DB.

    Pass via :attr:`SilverConfig.processor_context` to enable the
    standard processor groups (hygiene / type_safety / etc.). Leave
    as ``None`` to skip — pipelines that don't carry per-column
    metadata see no behaviour change.

    See ``docs/standard_processors.md``.
    """

    repo: MetadataRepository
    """Where dataset_config_columns + processor-group config live."""

    dataset_id: int
    """Filters meta.dataset_config_columns to one dataset."""

    platform_groups: dict[str, str] | None = None
    """Resolved platform_config processor_groups KV (env-wide
    defaults). Caller looks them up; the engine merges them with
    dataset-level overrides."""

    dataset_groups: dict[str, str] | None = None
    """Resolved dataset_config processor_groups KV (per-dataset
    overrides)."""

    env: str | None = None
    """Environment name (``'dev'`` / ``'uat'`` / ``'prod'``). Required
    when the dataset has any PII columns — the engine looks up
    per-class actions in ``meta.pii_policy`` keyed by env. When None,
    PII processors are skipped (Wave 1 behaviour)."""


@dataclass(frozen=True)
class SilverConfig:
    """Inputs to one Silver run."""

    bronze_path: Path
    silver_path: Path
    run_id: str
    strategy: SilverStrategy = SilverStrategy.APPEND
    business_keys: tuple[str, ...] | None = None
    """Natural-key columns used as the MERGE predicate for SCD1 / SCD2.
    Ignored for ``append`` strategy."""
    hash_columns: tuple[str, ...] | None = None
    """Columns participating in ``_record_hash``. Default (when ``None``):
    every non-metadata source column. Override to exclude noisy fields
    (audit timestamps, sequence numbers) where stable change detection
    matters more than literal full-row hashing."""
    partition_by: tuple[str, ...] | None = None
    scd2_order_by: str | None = None
    """Source column whose value populates ``_valid_from`` for new
    SCD2 versions and ``_valid_to`` when a previous current row is
    closed. Required for SCD2; ignored for other strategies. Typically
    a TIMESTAMP column like ``updated_at`` or ``effective_from``."""
    dq_context: SilverDqContext | None = None
    """Optional Phase 4 hook — when set, the pipeline runs DQ rules
    attached to this dataset's Silver layer before the write. Leave
    ``None`` for pipelines that don't (yet) carry DQ rules."""

    processor_context: SilverProcessorContext | None = None
    """Optional Wave-1 hook — when set, the pipeline runs the standard
    processor groups (hygiene / type_safety / ...) inside ``_enrich``.
    Leave as ``None`` for pipelines without per-column metadata."""

    dedup: DedupConfig | None = None
    """Optional Wave-3 hook — row-level dedup applied after column
    processors and BEFORE ``_record_hash`` recompute. None = no dedup
    (default, preserves prior behaviour)."""


class _RunCounts(NamedTuple):
    """Per-strategy row counts captured by each ``_write_*`` writer.

    All values come from in-process state (input-table row count, MERGE
    return metrics, or SCD2 classification). No extra full-table scans.
    """

    inserted: int = 0
    updated: int = 0  # MERGE matches that were updated; SCD2 close-outs
    unchanged: int = 0  # SCD2-only: hash matched, skipped


@dataclass(frozen=True)
class _DqSummary:
    """Aggregate of all rule outcomes for one Silver run.

    Used to populate the DQ fields on :class:`SilverRunStats`. Internal
    type — public surface is the SilverRunStats fields.
    """

    rules_evaluated: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    rules_errored: int = 0
    violations_logged: int = 0
    """Sample rows that landed in ``meta.dq_violations`` across all rules."""
    rows_quarantined: int = 0
    """Rows routed to side tables across all rules. 0 unless a rule has
    ``quarantine_record + side_table`` AND a writer is configured."""


@dataclass(frozen=True)
class SilverRunStats:
    """Counts + light instrumentation emitted by a successful run.

    All metrics come either from in-process state or from a single
    ``_delta_log`` metadata read — no extra full-table scans.
    """

    rows_read: int
    rows_written: int
    strategy: SilverStrategy
    silver_path: Path
    rows_inserted: int = 0
    rows_updated: int = 0  # MERGE matches updated; SCD2 close-outs
    rows_unchanged: int = 0  # SCD2 only — source row hash matched current
    rows_deduped: int = 0
    """Wave-3 dedup. Rows dropped (or flagged) by row-level dedup
    BEFORE the Silver write. 0 when ``SilverConfig.dedup`` is unset
    or all rows survived."""
    silver_version_after: int | None = None
    duration_seconds: float = 0.0
    silver_columns: tuple[str, ...] = ()
    """Output column names (in write order). Populated from the
    enriched pyarrow table at write time so downstream consumers
    (lineage, dq) don't have to re-read the Delta table for schema."""
    bronze_path: Path | None = None
    """Source path — pulled from ``SilverConfig`` at run time so the
    stats object is self-describing for emitters that don't see the
    config (e.g., the lineage Silver emitter)."""
    run_id: str = ""
    """Run ID supplied by the orchestrator (mirrors
    ``SilverConfig.run_id``). Carried through so emitters can correlate
    framework run logs to OL run events."""
    dq_rules_evaluated: int = 0
    """Phase 4 — DQ summary fields. Zero when ``dq_context`` is unset
    or the dataset has no enabled rules at the Silver / post_processor
    injection point."""
    dq_rules_passed: int = 0
    dq_rules_failed: int = 0
    dq_rules_errored: int = 0
    dq_violations_logged: int = 0
    """Total sample rows written to ``meta.dq_violations`` this run
    across all rules — bounded by ``Σ rule.sample_cap``."""
    dq_rows_quarantined: int = 0
    """Total rows routed to side-table quarantine this run. > 0 only
    when a rule used ``quarantine_record + side_table`` with a
    customer-supplied writer."""

    processors_fired: int = 0
    """Wave-1 processor stats. Number of processor invocations that
    actually transformed at least one row (across all groups + all
    columns). Zero when no ``processor_context`` is configured or
    every applicable column was already clean."""

    processor_rows_affected: int = 0
    """Sum of ``rows_affected`` across all fired processors. Best-effort
    count; some processors report ``-1`` when row-level counting would
    cost an extra scan — those don't contribute here."""


class UnsupportedSilverStrategyError(LakeIngestError):
    """The configured strategy isn't implemented yet (or at all)."""


# ──────────────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────────────


class SilverPipeline:
    """Run one Bronze → Silver execution.

    Stateless; safe to instantiate once per process and reuse. Each
    ``run()`` call is independent — opens a fresh DeltaTable for read,
    constructs a new pyarrow Table, writes via ``write_deltalake``.
    """

    def run(self, config: SilverConfig) -> SilverRunStats:
        # 1. Validate strategy preconditions (fail before any I/O)
        _validate(config)

        started_at = time.monotonic()

        deltalake = _lazy_import_deltalake()
        pa = _lazy_import_pyarrow()

        # 2. Read Bronze
        try:
            source = deltalake.DeltaTable(str(config.bronze_path)).to_pyarrow_table()
        except Exception as exc:
            raise SinkUnavailableError(
                f"Cannot read Bronze Delta table at {config.bronze_path}: {exc}"
            ) from exc

        # 2b. Phase 4c — post_reader DQ rules fire against the raw
        # Bronze pyarrow table BEFORE any Silver transformation. "Is
        # what we read sane?" — schema-level invariants about source
        # data. Halts here mean nothing landed downstream.
        dq_pre = self._run_dq(config, source, injection_point=InjectionPoint.POST_READER)

        # 3. Transform
        enriched, processor_report, rows_deduped = self._enrich(source, config, pa)

        # 3b. Phase 4a — post_processor DQ rules fire against the
        # enriched table BEFORE the write. Halts here keep Silver
        # consistent (no partial commit).
        dq_processor = self._run_dq(config, enriched, injection_point=InjectionPoint.POST_PROCESSOR)

        # 4. Write per strategy. Each writer returns _RunCounts captured from
        # in-process state (input row count / MERGE return metrics / SCD2
        # classification) — no extra full-table scans.
        try:
            if config.strategy == SilverStrategy.APPEND:
                counts = _write_append(deltalake, enriched, config)
            elif config.strategy in {SilverStrategy.SCD1, SilverStrategy.UPSERT}:
                # UPSERT shares write semantics with SCD1 (MERGE on natural key;
                # current state, no history). Same code path; same preconditions.
                counts = _write_scd1(deltalake, enriched, config)
            elif config.strategy == SilverStrategy.SCD2:
                counts = _write_scd2(deltalake, pa, enriched, config)
            else:
                raise UnsupportedSilverStrategyError(
                    f"Silver strategy {config.strategy!r} is not implemented yet."
                )
        except (SinkUnavailableError, ValidationError, UnsupportedSilverStrategyError):
            raise  # framework errors propagate; only generic failures get wrapped
        except Exception as exc:
            raise WriteFailedError(f"Silver write failed for {config.silver_path}: {exc}") from exc

        # 4b. Phase 4c — post_write DQ rules fire AFTER successful
        # write. v1 simplification: predicates evaluate against the
        # ``enriched`` table (the data we sent to write) rather than
        # re-reading the post-commit Delta. Halts here can't undo the
        # write but stop downstream consumers.
        dq_post = self._run_dq(config, enriched, injection_point=InjectionPoint.POST_WRITE)
        dq_summary = _merge_dq_summaries(dq_pre, dq_processor, dq_post)

        # Single _delta_log read for the post-commit version — cheap (no data scan).
        silver_version = _read_silver_version(deltalake, config.silver_path)

        return SilverRunStats(
            rows_read=source.num_rows,
            rows_written=enriched.num_rows,
            strategy=config.strategy,
            silver_path=config.silver_path,
            bronze_path=config.bronze_path,
            run_id=config.run_id,
            silver_columns=tuple(enriched.column_names),
            rows_inserted=counts.inserted,
            rows_updated=counts.updated,
            rows_unchanged=counts.unchanged,
            silver_version_after=silver_version,
            duration_seconds=time.monotonic() - started_at,
            dq_rules_evaluated=dq_summary.rules_evaluated,
            dq_rules_passed=dq_summary.rules_passed,
            dq_rules_failed=dq_summary.rules_failed,
            dq_rules_errored=dq_summary.rules_errored,
            dq_violations_logged=dq_summary.violations_logged,
            dq_rows_quarantined=dq_summary.rows_quarantined,
            processors_fired=processor_report.processors_fired,
            processor_rows_affected=processor_report.total_rows_affected,
            rows_deduped=rows_deduped,
        )

    # ── Internals

    def _run_dq(
        self,
        config: SilverConfig,
        table: pa.Table,
        *,
        injection_point: InjectionPoint,
    ) -> _DqSummary:
        """Evaluate rules attached to this dataset's Silver layer at
        the given injection point against the supplied pyarrow table.

        Called up to three times per run (post_reader against raw
        Bronze, post_processor against enriched, post_write against
        enriched-after-write). ``stop_pipeline`` halts via
        :class:`DqHaltError`; the pipeline doesn't catch — caller
        decides what to do with partial state.

        No-op when no ``dq_context`` is configured or no enabled rules
        match the dataset / layer / injection_point — keeps the
        existing pure-pipeline tests untouched and avoids duckdb
        startup when there's no DQ work to do.
        """
        ctx = config.dq_context
        if ctx is None:
            return _DqSummary()

        all_rules = ctx.repo.list_dq_rules(
            dataset_id=ctx.dataset_id,
            layer=Layer.SILVER,
            only_enabled=True,
        )
        rules = [r for r in all_rules if r.injection_point == injection_point]
        if not rules:
            return _DqSummary()

        # Conn ownership: when the customer supplies one (paired with a
        # DeltaSideTableWriter or similar), we register into it but
        # leave its lifetime to them. When ``None``, we own a private
        # in-memory conn for the run.
        if ctx.conn is not None:
            conn = ctx.conn
            owns_conn = False
        else:
            duckdb = _lazy_import_duckdb()
            conn = duckdb.connect(":memory:")
            owns_conn = True
        try:
            # Register the in-memory pyarrow table — zero-copy via
            # the Arrow C interface. The relation is queryable as
            # ``ctx.relation_name`` for the lifetime of the conn.
            # Re-registering replaces the previous binding, which is
            # how successive injection-point calls swap their data.
            conn.register(ctx.relation_name, table)
            evaluator = DuckdbEvaluator(conn)
            engine = ActionEngine(ctx.repo, quarantine_writer=ctx.quarantine_writer)

            passed = 0
            failed = 0
            errored = 0
            violations = 0
            quarantined = 0

            for row in rules:
                rule = _row_to_dqrule(row, ctx.dataset_name)
                evaluation = evaluator.evaluate(rule, relation_name=ctx.relation_name)
                outcome = engine.apply(
                    evaluation,
                    rule_id=row.rule_id,
                    run_id=config.run_id,
                    relation_name=ctx.relation_name,
                    pk_columns=ctx.pk_columns or None,
                )
                # The action engine raises DqHaltError on stop_pipeline
                # before we reach here — so when we *are* here, the
                # rule has been recorded and the pipeline can carry on.
                if outcome.action_taken == ActionTaken.NONE:
                    passed += 1
                elif outcome.action_taken == ActionTaken.ERRORED:
                    errored += 1
                else:
                    failed += 1
                    violations += outcome.sample_captured
                    quarantined += outcome.quarantined_to_side_table

            return _DqSummary(
                rules_evaluated=len(rules),
                rules_passed=passed,
                rules_failed=failed,
                rules_errored=errored,
                violations_logged=violations,
                rows_quarantined=quarantined,
            )
        finally:
            if owns_conn:
                conn.close()

    def _enrich(
        self, source: pa.Table, config: SilverConfig, pa_module: Any
    ) -> tuple[pa.Table, ProcessorRunReport, int]:
        """Drop Bronze metadata cols, run standard processors,
        run row-level dedup, recompute hash, add Silver metadata cols.

        Returns ``(enriched_table, processor_report, rows_deduped)``.
        ``rows_deduped`` is 0 when no dedup config is supplied.
        """
        # Drop Bronze metadata if present (left over from LocalDeltaWriter).
        business = _drop_columns(source, BRONZE_METADATA_COLS)

        # Wave 1+2+3 — run column groups against the business columns
        # BEFORE we compute the hash, so the hash is over the
        # post-transform shape (stable for SCD2 across runs).
        business, processor_report = self._run_processors(business, config)

        # Wave 3 — row-level dedup runs AFTER column groups (PII
        # mask-equivalence respected) and BEFORE hash recompute (hash
        # only over surviving rows).
        rows_deduped = 0
        if config.dedup is not None:
            engine = ProcessorEngine()
            business, dedup_result = engine.apply_dedup(business, config.dedup)
            rows_deduped = dedup_result.rows_dropped + dedup_result.flagged_only

        hash_cols = _resolve_hash_columns(business.column_names, config.hash_columns)

        loaded_at = datetime.now(UTC)
        run_id_col = pa_module.array([config.run_id] * business.num_rows, type=pa_module.string())
        loaded_at_col = pa_module.array(
            [loaded_at] * business.num_rows, type=pa_module.timestamp("us", tz="UTC")
        )
        hash_col = pa_module.array(_hashes(business, hash_cols), type=pa_module.string())

        enriched = (
            business.append_column(SILVER_RUN_ID, run_id_col)
            .append_column(SILVER_LOADED_AT, loaded_at_col)
            .append_column(SILVER_RECORD_HASH, hash_col)
        )
        return enriched, processor_report, rows_deduped

    def _run_processors(
        self, table: pa.Table, config: SilverConfig
    ) -> tuple[pa.Table, ProcessorRunReport]:
        """Apply the standard processor groups against the business
        table. No-op when no ``processor_context`` is configured."""
        ctx = config.processor_context
        if ctx is None:
            return table, ProcessorRunReport()

        engine = ProcessorEngine()
        engine_config = engine.resolve_config(
            ctx.repo,
            dataset_id=ctx.dataset_id,
            platform_groups=ctx.platform_groups,
            dataset_groups=ctx.dataset_groups,
            env=ctx.env,
        )
        return engine.apply_pre_hash_groups(table, engine_config)


# ──────────────────────────────────────────────────────────────────────
# Helpers (free functions — easy to unit-test)
# ──────────────────────────────────────────────────────────────────────


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


_MERGE_STRATEGIES = frozenset({SilverStrategy.SCD1, SilverStrategy.UPSERT, SilverStrategy.SCD2})


def _validate(config: SilverConfig) -> None:
    """Strategy-specific preconditions; raises before any I/O if violated.

    - SCD1 / UPSERT / SCD2 all require non-empty ``business_keys``
      (natural key for the MERGE predicate).
    - SCD2 additionally requires ``scd2_order_by``.
    - All identifiers (business_keys + scd2_order_by) must match the
      framework's safe-identifier regex, since they're spliced into
      MERGE predicates / update expressions.
    """
    if config.strategy in _MERGE_STRATEGIES and not config.business_keys:
        raise ValidationError(
            f"Silver strategy {config.strategy.value!r} requires non-empty "
            "business_keys for the MERGE predicate."
        )
    if config.strategy == SilverStrategy.SCD2:
        if not config.business_keys:
            raise ValidationError(
                "Silver strategy 'scd2' requires non-empty business_keys for natural-key match."
            )
        if not config.scd2_order_by:
            raise ValidationError(
                "Silver strategy 'scd2' requires scd2_order_by (column populating "
                "_valid_from / _valid_to)."
            )
        if not _IDENTIFIER_RE.match(config.scd2_order_by):
            raise ValidationError(
                f"scd2_order_by {config.scd2_order_by!r} is not a safe SQL identifier."
            )
    for key in config.business_keys or ():
        if not _IDENTIFIER_RE.match(key):
            raise ValidationError(
                f"business_key {key!r} is not a safe SQL identifier "
                "(must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$)."
            )


def _write_append(deltalake: Any, table: pa.Table, config: SilverConfig) -> _RunCounts:
    """Append rows to the target Delta table (creates it on first call)."""
    deltalake.write_deltalake(
        str(config.silver_path),
        table,
        mode="append",
        partition_by=list(config.partition_by) if config.partition_by else None,
    )
    # Append always inserts every input row; never updates anything.
    return _RunCounts(inserted=table.num_rows)


def _write_scd1(deltalake: Any, table: pa.Table, config: SilverConfig) -> _RunCounts:
    """SCD1 upsert: initial run creates the table; subsequent runs MERGE.

    Existence check uses the filesystem (presence of ``_delta_log/``) rather
    than catching an exception class, since deltalake's exception types
    have churned across versions.
    """
    if not _silver_table_exists(config.silver_path):
        # Initial load — no MERGE possible. Falls back to a write_deltalake.
        deltalake.write_deltalake(
            str(config.silver_path),
            table,
            mode="append",
            partition_by=list(config.partition_by) if config.partition_by else None,
        )
        return _RunCounts(inserted=table.num_rows)

    # business_keys is non-empty + identifier-safe (validated above).
    keys = config.business_keys or ()
    predicate = " AND ".join(f"target.{c} = source.{c}" for c in keys)
    target = deltalake.DeltaTable(str(config.silver_path))
    metrics = (
        target.merge(
            source=table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    return _RunCounts(
        inserted=_int(metrics, "num_target_rows_inserted"),
        updated=_int(metrics, "num_target_rows_updated"),
    )


def _int(metrics: Any, key: str) -> int:
    """Pull an int from deltalake's MERGE return value (string-typed in current versions)."""
    if not isinstance(metrics, dict):
        return 0
    raw = metrics.get(key, 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _silver_table_exists(silver_path: Path) -> bool:
    """A Delta table is identified by the ``_delta_log/`` directory."""
    return (silver_path / "_delta_log").is_dir()


def _read_silver_version(deltalake: Any, silver_path: Path) -> int | None:
    """Return the latest commit version, or ``None`` if not retrievable.

    Reads only ``_delta_log`` metadata (no data scan). Cheap. Defensive
    against version skew in deltalake's API surface — any failure
    yields ``None`` rather than raising.
    """
    try:
        return int(deltalake.DeltaTable(str(silver_path)).version())
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────
# SCD2 writer
# ──────────────────────────────────────────────────────────────────────


def _write_scd2(
    deltalake: Any, pa_module: Any, enriched: pa.Table, config: SilverConfig
) -> _RunCounts:
    """SCD2 versioned-history upsert.

    Algorithm:

    1. Stamp every source row with SCD2 metadata (``_is_current=true``,
       ``_valid_from=source[scd2_order_by]``, ``_valid_to=NULL``).
    2. If the target doesn't exist yet → write the stamped source as
       the initial set of current versions.
    3. Otherwise: read target's current rows, classify source rows by
       natural-key match + ``_record_hash`` compare:

       - new natural key      → insert as new current version
       - hash differs         → close the existing current row
                                (``_is_current=false``,
                                ``_valid_to=source[scd2_order_by]``)
                                + insert the new current version
       - hash unchanged       → no-op
    """
    order_by = config.scd2_order_by
    if order_by is None:
        # Already validated; defensive guard for type narrowing.
        raise ValidationError("scd2_order_by missing at write time (validation bug).")
    if order_by not in enriched.column_names:
        raise ValidationError(
            f"scd2_order_by column {order_by!r} not present in source. "
            f"Source columns: {enriched.column_names}"
        )

    stamped = _add_scd2_columns(pa_module, enriched, order_by)

    # Initial load — no MERGE possible.
    if not _silver_table_exists(config.silver_path):
        deltalake.write_deltalake(
            str(config.silver_path),
            stamped,
            mode="append",
            partition_by=list(config.partition_by) if config.partition_by else None,
        )
        return _RunCounts(inserted=stamped.num_rows)

    # Subsequent load — classify against target.current.
    target_dt = deltalake.DeltaTable(str(config.silver_path))
    target_table = target_dt.to_pyarrow_table()
    target_current = _filter_current(target_table)

    business_keys = config.business_keys or ()
    new_idx, changed_idx = _classify_against_target(stamped, target_current, business_keys)

    # Close out the existing current rows whose hash will change.
    if changed_idx:
        close_table = _build_close_table(pa_module, stamped, changed_idx, business_keys, order_by)
        _execute_close_merge(target_dt, close_table, business_keys)

    # Append new versions (new + changed).
    insert_idx = sorted(new_idx + changed_idx)
    if insert_idx:
        new_versions = stamped.take(insert_idx)
        deltalake.write_deltalake(
            str(config.silver_path),
            new_versions,
            mode="append",
            partition_by=list(config.partition_by) if config.partition_by else None,
        )

    return _RunCounts(
        inserted=len(insert_idx),
        updated=len(changed_idx),
        unchanged=enriched.num_rows - len(new_idx) - len(changed_idx),
    )


def _add_scd2_columns(pa_module: Any, table: pa.Table, order_by_col: str) -> pa.Table:
    """Stamp ``_is_current=true``, ``_valid_from=table[order_by_col]``, ``_valid_to=NULL``."""
    n = table.num_rows
    is_current_col = pa_module.array([True] * n, type=pa_module.bool_())
    valid_from_col = table.column(order_by_col)
    # _valid_to: NULL array with the same type as order_by column for schema consistency
    valid_to_col = pa_module.nulls(n, type=valid_from_col.type)
    return (
        table.append_column(SILVER_IS_CURRENT, is_current_col)
        .append_column(SILVER_VALID_FROM, valid_from_col)
        .append_column(SILVER_VALID_TO, valid_to_col)
    )


def _filter_current(table: pa.Table) -> pa.Table:
    """Return rows where ``_is_current=true`` (or empty if column missing)."""
    if SILVER_IS_CURRENT not in table.column_names:
        return table.slice(0, 0)
    mask = table.column(SILVER_IS_CURRENT)
    return table.filter(mask)


def _classify_against_target(
    source: pa.Table,
    target_current: pa.Table,
    business_keys: tuple[str, ...],
) -> tuple[list[int], list[int]]:
    """Return (new_indices, changed_indices) for source rows."""
    target_key_to_hash = _build_key_hash_map(target_current, business_keys)
    source_key_lists = [source.column(k).to_pylist() for k in business_keys]
    source_hashes = source.column(SILVER_RECORD_HASH).to_pylist()

    new_indices: list[int] = []
    changed_indices: list[int] = []
    for i in range(source.num_rows):
        key = tuple(source_key_lists[c][i] for c in range(len(business_keys)))
        target_hash = target_key_to_hash.get(key)
        if target_hash is None:
            new_indices.append(i)
        elif target_hash != source_hashes[i]:
            changed_indices.append(i)
        # else: unchanged — skip
    return new_indices, changed_indices


def _build_key_hash_map(
    table: pa.Table, business_keys: tuple[str, ...]
) -> dict[tuple[Any, ...], str]:
    """Map (natural_key_tuple) → ``_record_hash`` for the rows in ``table``."""
    if table.num_rows == 0 or SILVER_RECORD_HASH not in table.column_names:
        return {}
    key_lists = [table.column(k).to_pylist() for k in business_keys]
    hashes = table.column(SILVER_RECORD_HASH).to_pylist()
    return {
        tuple(key_lists[c][i] for c in range(len(business_keys))): hashes[i]
        for i in range(table.num_rows)
    }


def _build_close_table(
    pa_module: Any,
    stamped_source: pa.Table,
    changed_indices: list[int],
    business_keys: tuple[str, ...],
    order_by_col: str,
) -> pa.Table:
    """Build a (business_keys + new_valid_to) table for the close MERGE."""
    subset = stamped_source.take(changed_indices)
    arrays = [subset.column(k) for k in business_keys] + [subset.column(order_by_col)]
    names = [*business_keys, "new_valid_to"]
    return pa_module.Table.from_arrays(arrays, names=names)


def _execute_close_merge(
    target_dt: Any, close_table: pa.Table, business_keys: tuple[str, ...]
) -> None:
    """MERGE: set ``_is_current=false``, ``_valid_to=source.new_valid_to`` on matched rows."""
    key_predicate = " AND ".join(f"target.{c} = source.{c}" for c in business_keys)
    predicate = f"{key_predicate} AND target.{SILVER_IS_CURRENT} = true"
    (
        target_dt.merge(
            source=close_table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            updates={
                SILVER_IS_CURRENT: "false",
                SILVER_VALID_TO: "source.new_valid_to",
            }
        )
        .execute()
    )


def _drop_columns(table: pa.Table, cols_to_drop: tuple[str, ...]) -> pa.Table:
    """Return ``table`` without any of the named columns (no-op if absent)."""
    present = [c for c in cols_to_drop if c in table.column_names]
    return table.drop_columns(present) if present else table


def _resolve_hash_columns(available: list[str], hash_columns: tuple[str, ...] | None) -> list[str]:
    """Pick which columns participate in the per-row hash.

    Default: all non-metadata source columns. When ``hash_columns`` is
    supplied, those (filtered to columns that actually exist) win.
    """
    if hash_columns is None:
        return list(available)
    return [c for c in hash_columns if c in available]


def _hashes(table: pa.Table, columns: list[str]) -> list[str]:
    """Compute the per-row record hash, ordered by row position."""
    if not columns:
        return [compute_record_hash(())] * table.num_rows
    column_data = [table.column(c).to_pylist() for c in columns]
    return [
        compute_record_hash(tuple(column_data[i][row_idx] for i in range(len(columns))))
        for row_idx in range(table.num_rows)
    ]


# ──────────────────────────────────────────────────────────────────────
# Lazy imports — same pattern as LocalDeltaWriter
# ──────────────────────────────────────────────────────────────────────


def _lazy_import_deltalake() -> Any:
    """Import deltalake on first use; surface a clean error if missing."""
    try:
        import deltalake
    except ImportError as exc:
        raise SinkUnavailableError(
            "deltalake not installed. Install with: pip install lake-ingest[local]"
        ) from exc
    return deltalake


def _lazy_import_pyarrow() -> Any:
    """Import pyarrow on first use; surface a clean error if missing."""
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise SinkUnavailableError(
            "pyarrow not installed. Install with: pip install lake-ingest[local]"
        ) from exc
    return pa


def _lazy_import_duckdb() -> Any:
    """Import duckdb on first use; gated on the ``[dq]`` extra so the
    framework remains importable without it."""
    try:
        import duckdb
    except ImportError as exc:
        raise SinkUnavailableError(
            "duckdb not installed. Install with: pip install lake-ingest[dq] "
            "(required to evaluate DQ rules in SilverPipeline)."
        ) from exc
    return duckdb


def _merge_dq_summaries(*summaries: _DqSummary) -> _DqSummary:
    """Sum DQ summaries across injection points (post_reader +
    post_processor + post_write) into one for the run-stats."""
    return _DqSummary(
        rules_evaluated=sum(s.rules_evaluated for s in summaries),
        rules_passed=sum(s.rules_passed for s in summaries),
        rules_failed=sum(s.rules_failed for s in summaries),
        rules_errored=sum(s.rules_errored for s in summaries),
        violations_logged=sum(s.violations_logged for s in summaries),
        rows_quarantined=sum(s.rows_quarantined for s in summaries),
    )


def _row_to_dqrule(row: DqRuleRow, dataset_name: str) -> DqRule:
    """Adapt a ``DqRuleRow`` (DB-side, surrogate-keyed) into a
    :class:`DqRule` (natural-keyed; what the evaluator consumes).

    ``dataset_name`` is required because :class:`DqRule` carries the
    name (portable), not the surrogate id. The pipeline's
    :class:`SilverDqContext` ferries it in.
    """
    return DqRule(
        name=row.rule_name,
        dataset_name=dataset_name,
        layer=row.layer,
        predicate=row.predicate,
        injection_point=row.injection_point,
        severity=row.severity,
        action=row.action,
        quarantine_strategy=row.quarantine_strategy,
        rule_type=row.rule_type,
        tech=row.tech,
        container=row.container,
        sample_cap=row.sample_cap,
        is_enabled=row.is_enabled,
        source=row.source,
        authored_by=row.authored_by,
        description=row.description,
    )


__all__ = [
    "SilverConfig",
    "SilverDqContext",
    "SilverPipeline",
    "SilverRunStats",
    "SilverStrategy",
    "UnsupportedSilverStrategyError",
]
