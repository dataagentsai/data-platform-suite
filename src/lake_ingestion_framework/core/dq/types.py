"""Typed domain model for the DQ engine (v1).

Every value here is a contract — adding an enum member is additive
(minor bump); removing or renaming is a breaking change. Mirrors the
``meta.dq_rules`` schema in ``schema_sqlite.sql``.

Why a separate module from ``types/enums.py``: DQ-specific enums
shouldn't bloat the cross-cutting enum file. They cohabit with
:class:`DqRule` here so the rule's typed surface is one read.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from lake_ingestion_framework.core.exceptions import LakeIngestError

# ──────────────────────────────────────────────────────────────────────
# Enums — every safe-listed value the rule store accepts
# ──────────────────────────────────────────────────────────────────────


class Layer(StrEnum):
    """Which layer the rule attaches to.

    Mirrors the medallion + Source. Rules at Bronze rarely make sense
    (use Source-side checks instead), but supported for completeness.
    """

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class RuleType(StrEnum):
    """v1 ships ``predicate`` only — a SQL expression where ``TRUE``
    rows are violators. Future types (existence_check / freshness /
    reconciliation / staleness) come from later DQ sub-components."""

    PREDICATE = "predicate"


class Tech(StrEnum):
    """Authoring syntax. v1: ``sql`` only — every engine speaks it,
    least-surprise to review, simplest validator."""

    SQL = "sql"


class Container(StrEnum):
    """Where the rule executes. ``duckdb`` for local mode (cheap, fast,
    no Spark needed); ``spark`` for production realms wired to
    Databricks Workflows."""

    DUCKDB = "duckdb"
    SPARK = "spark"


class InjectionPoint(StrEnum):
    """When the rule fires inside the pipeline.

    Multiple rules can attach to the same point; ordering within a
    point is by ``rule_id`` ascending (deterministic).
    """

    POST_READER = "post_reader"
    """Right after Bronze has been read; "is what we read sane?"."""

    POST_PROCESSOR = "post_processor"
    """After Silver/Gold business logic; "did transformations preserve invariants?"."""

    POST_WRITE = "post_write"
    """Last gate after the write commit; row-count match, partition coverage."""


class Severity(StrEnum):
    """Label only — what the rule "means". Decoupled from
    :class:`Action` so customers can have e.g.
    ``severity=error + action=continue_log`` (mark as critical but
    don't break the pipeline). v1 drops ``critical`` until paging is
    wired."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Action(StrEnum):
    """What the engine does when the rule fires (i.e. violators
    found). v1 ships three; the fuller set
    (reroute / block_partition / notify_only / continue_warn) is
    deferred."""

    STOP_PIPELINE = "stop_pipeline"
    """Halt; downstream tasks blocked. The pipeline raises
    :class:`DqHaltError` (defined in the action engine, Phase 1.4)."""

    CONTINUE_LOG = "continue_log"
    """Log the violation to ``meta.dq_results`` + ``meta.dq_violations``;
    let the pipeline continue. Default for warnings."""

    QUARANTINE_RECORD = "quarantine_record"
    """Move violator rows out of the main path; continue. The strategy
    (side_table vs log_only) is per-rule via :class:`QuarantineStrategy`."""


class QuarantineStrategy(StrEnum):
    """How violator rows are stored when ``action=quarantine_record``.

    ``side_table`` is the default per design doc §6: violator rows go
    to a separate ``<main>__quarantine`` Delta table. ``log_only`` is
    the cheap option: only the PKs land in ``meta.dq_violations``.
    ``flag_column`` (third pattern in the design) is deferred.
    """

    SIDE_TABLE = "side_table"
    LOG_ONLY = "log_only"


class RuleSource(StrEnum):
    """Origin of a rule — distinguishes user-authored from
    framework-generated entries. Drives review workflows; the
    Recommender writes ``auto_recommended`` rules; SCD2 etc. write
    ``framework_invariant`` ones; YAML-authored rules are
    ``user_authored``."""

    USER_AUTHORED = "user_authored"
    AUTO_RECOMMENDED = "auto_recommended"
    FRAMEWORK_INVARIANT = "framework_invariant"


# ──────────────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────────────


class DqValidationError(LakeIngestError):
    """A DQ rule (in YAML or in metadata) violates a framework invariant.

    Raised at config-sync / metadata-load time, **not at runtime** —
    the same posture the rest of the framework takes (validate at
    boundaries, trust internally).
    """


# ──────────────────────────────────────────────────────────────────────
# DqRule — the canonical typed shape
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DqRule:
    """One DQ rule — mirrors a ``meta.dq_rules`` row.

    Constructed by the YAML loader; validated at construction; consumed
    by the evaluator (Phase 1.2) and the action engine (Phase 1.4).
    """

    name: str
    """Human-meaningful name. Unique per ``(dataset_id, layer)``;
    used as the natural key for upsert."""

    dataset_name: str
    """Dataset name as registered in ``dataset_master.dataset_name``.
    Resolved to ``dataset_id`` by the rules repository at sync time —
    the dataclass holds the name, not the ID, so YAML round-trips
    cleanly."""

    layer: Layer
    predicate: str
    """SQL expression. Rows where this evaluates TRUE are violators.
    Run via duckdb (local) or Spark (prod). Validated for cheap-eval
    + safe-identifier shape at sync time."""

    injection_point: InjectionPoint
    severity: Severity
    action: Action

    quarantine_strategy: QuarantineStrategy | None = None
    """Required when ``action == QUARANTINE_RECORD``; ignored
    otherwise. Validated at construction."""

    rule_type: RuleType = RuleType.PREDICATE
    tech: Tech = Tech.SQL
    container: Container = Container.DUCKDB

    sample_cap: int = 100
    """Max number of violator rows to capture per run (in
    ``meta.dq_violations`` for ``log_only``, or for the side-table
    sample). 0 disables capture entirely; -1 means uncapped (rare)."""

    is_enabled: bool = True
    source: RuleSource = RuleSource.USER_AUTHORED
    authored_by: str | None = None
    description: str = ""

    def __post_init__(self) -> None:
        # Light, type-only invariants. Predicate / identifier safety
        # is enforced by the YAML loader (richer error messages there).
        if not self.name:
            raise DqValidationError("DqRule.name must be non-empty.")
        if not self.dataset_name:
            raise DqValidationError(f"Rule {self.name!r}: dataset_name must be non-empty.")
        if not self.predicate.strip():
            raise DqValidationError(f"Rule {self.name!r}: predicate must be non-empty.")

        if self.action == Action.QUARANTINE_RECORD and self.quarantine_strategy is None:
            raise DqValidationError(
                f"Rule {self.name!r}: action=quarantine_record requires "
                "quarantine_strategy to be set (side_table | log_only)."
            )
        if self.action != Action.QUARANTINE_RECORD and self.quarantine_strategy is not None:
            raise DqValidationError(
                f"Rule {self.name!r}: quarantine_strategy is only valid when "
                f"action=quarantine_record; got action={self.action.value!r}."
            )
        if self.sample_cap < -1:
            raise DqValidationError(
                f"Rule {self.name!r}: sample_cap must be >= -1 (-1 = uncapped, 0 = none); "
                f"got {self.sample_cap}."
            )


@dataclass(frozen=True)
class DqRuleSet:
    """All rules attached to one ``(layer, dataset)`` pair — the unit
    a YAML file represents.

    File path convention: ``config/dq/<layer>_<dataset>.yaml``.
    Customer can author multiple rules per file; each parses into a
    :class:`DqRule` and the bundle becomes a ``DqRuleSet``.
    """

    dataset_name: str
    layer: Layer
    rules: tuple[DqRule, ...] = field(default_factory=tuple)
    """All rules in this set must share ``dataset_name`` + ``layer`` —
    enforced by the loader."""

    source_path: str | None = None
    """Filesystem path the rules were loaded from, for error messages
    + audit. ``None`` when constructed in code (tests)."""

    def __post_init__(self) -> None:
        for rule in self.rules:
            if rule.dataset_name != self.dataset_name:
                raise DqValidationError(
                    f"Rule {rule.name!r} dataset_name={rule.dataset_name!r} doesn't match "
                    f"set's dataset_name={self.dataset_name!r}."
                )
            if rule.layer != self.layer:
                raise DqValidationError(
                    f"Rule {rule.name!r} layer={rule.layer.value!r} doesn't match "
                    f"set's layer={self.layer.value!r}."
                )
        # Names must be unique within the set (also unique in DB by the
        # natural key, but better to fail at load time with a clearer message).
        seen: set[str] = set()
        for rule in self.rules:
            if rule.name in seen:
                raise DqValidationError(
                    f"Duplicate rule name {rule.name!r} in dataset={self.dataset_name!r} "
                    f"layer={self.layer.value!r}."
                )
            seen.add(rule.name)
