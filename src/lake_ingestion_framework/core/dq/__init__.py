"""Data Quality engine — Inline Checks (v1 slim cut).

Per ``docs/dq_design.md`` + the v1 addendum at the top of that doc:

- v1 ships **Inline Checks only** (Profiler / Recon / TrendCheck /
  Staleness deferred)
- v1 ships **sql tech only** (pydeequ / great_expectations / soda_core
  deferred)
- v1 ships **3 actions** (stop_pipeline / continue_log /
  quarantine_record); **2 quarantine strategies** (side_table +
  log_only)
- Authoring is YAML in ``<customer>-ingestion/config/dq/``;
  ``lake-ingest config sync`` pushes to ``meta.dq_rules``; runtime
  reads only from the metadata DB

Public API:

- :class:`DqRule` — typed rule dataclass (mirrors ``meta.dq_rules`` row)
- :class:`DqRuleSet` — bundle of rules belonging to one
  ``(layer, dataset)`` pair (the unit of a YAML file)
- :func:`load_rules_from_yaml` — parse + validate
  ``config/dq/*.yaml`` into :class:`DqRule` instances
- Enums: :class:`Layer`, :class:`RuleType`, :class:`Tech`,
  :class:`Container`, :class:`InjectionPoint`, :class:`Severity`,
  :class:`Action`, :class:`QuarantineStrategy`, :class:`RuleSource`
- :class:`DqValidationError` — single error type for malformed rules
- :class:`DqEvaluator` — Protocol for SQL evaluators (Phase 2);
  :class:`DuckdbEvaluator` is the local container, :class:`RuleEvaluation`
  is the per-rule result
"""

from lake_ingestion_framework.core.dq.actions import (
    ActionEngine,
    ActionOutcome,
    ActionTaken,
    DeltaSideTableWriter,
    DqHaltError,
    DuckdbQuarantineWriter,
    QuarantineWriter,
)
from lake_ingestion_framework.core.dq.evaluator import (
    DqEvaluator,
    DuckdbEvaluator,
    EvaluationStatus,
    RuleEvaluation,
)
from lake_ingestion_framework.core.dq.spark_evaluator import (
    SparkEvaluator,
    SparkSessionLike,
)
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
from lake_ingestion_framework.core.dq.yaml_loader import (
    CheapEvalWarning,
    load_rules_from_yaml,
)

__all__ = [
    "Action",
    "ActionEngine",
    "ActionOutcome",
    "ActionTaken",
    "CheapEvalWarning",
    "Container",
    "DeltaSideTableWriter",
    "DqEvaluator",
    "DqHaltError",
    "DqRule",
    "DqRuleSet",
    "DqValidationError",
    "DuckdbEvaluator",
    "DuckdbQuarantineWriter",
    "EvaluationStatus",
    "InjectionPoint",
    "Layer",
    "QuarantineStrategy",
    "QuarantineWriter",
    "RuleEvaluation",
    "RuleSource",
    "RuleType",
    "Severity",
    "SparkEvaluator",
    "SparkSessionLike",
    "Tech",
    "load_rules_from_yaml",
]
