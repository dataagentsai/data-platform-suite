"""Typed surface for the standard processors layer.

Eight groups, ~30 processors total. See ``docs/standard_processors.md``
for the full design.

The principle: **1:1, no external dependencies, data-format-only.**
A processor takes one column → one column, needs no lookups, applies
universal/data-format logic only. Everything else is dbt's job.

This module declares the typed surface — the ``Processor`` Protocol,
the ``ProcessorGroup`` enum, and the ``ColumnTransformContext`` /
``GroupConfig`` dataclasses the engine consumes. Concrete processors
live in sibling modules (``hygiene.py``, ``type_safety.py``, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa


class ProcessorGroup(StrEnum):
    """The eight groups of standard processors.

    Group enable/disable is the primary selection lever — customers
    flip a group on/off in YAML and ~7 processors fire or don't.
    Per-processor overrides are available but secondary.
    """

    HYGIENE = "hygiene"
    TYPE_SAFETY = "type_safety"
    CASE_NORMALIZE = "case_normalize"
    TIMESTAMP_NORMALIZE = "timestamp_normalize"
    STANDARDIZATION = "standardization"
    PII = "pii"
    DEDUP = "dedup"
    PROVENANCE = "provenance"


class GroupMode(StrEnum):
    """How a group is configured at the dataset/platform level.

    * ``ENABLED`` — every processor in the group runs (per its own default)
    * ``DISABLED`` — group is fully bypassed; no processor in it fires
    * ``OPT_IN`` — only processors explicitly listed in
      :attr:`DatasetColumnRow.transforms` fire. Used for groups like
      ``standardization`` where defaults would surprise.
    * ``DATASET_DECIDES`` — the platform leaves the call to each
      dataset's own ``processor_groups`` config (or ``DISABLED`` if
      the dataset is silent).
    """

    ENABLED = "enabled"
    DISABLED = "disabled"
    OPT_IN = "opt_in"
    DATASET_DECIDES = "dataset_decides"


@dataclass(frozen=True)
class ColumnTransformContext:
    """Per-column context the engine hands to each :class:`Processor`.

    Carries everything a processor needs to decide whether it should
    fire and how. Column metadata comes from
    ``meta.dataset_config_columns``; group + processor toggles come
    from the engine's resolved config.
    """

    bronze_column_name: str
    """Source-side column name."""

    silver_column_name: str
    """Target column name (post-rename)."""

    bronze_data_type: str
    """As recorded in ``dataset_config_columns.data_type``."""

    silver_data_type: str | None
    """Target type for safe_cast — None inherits Bronze."""

    cast_safety: str
    """``'strict'`` or ``'try_cast'``."""

    pii_class: str
    """``'none'`` / ``'email'`` / ``'phone'`` / ``'ssn'`` / ..."""

    column_role: str | None
    """Optional role tag (``'code'``, ``'identifier'``, ...)."""

    null_replacement: str | None
    """Literal substituted for NULL post-transform."""


@dataclass(frozen=True)
class ProcessorRunResult:
    """Outcome of running one processor against one column.

    Engine collects these per-(group, processor, column) for the
    ``processors describe`` CLI and the lineage facet (Wave 4+).
    """

    group: ProcessorGroup
    processor_name: str
    column_name: str
    fired: bool
    """``True`` when the processor actually transformed at least one
    cell. ``False`` when it skipped (column type didn't match,
    column was already clean, etc.)."""

    rows_affected: int = 0
    """Best-effort count of cells modified; ``-1`` when the processor
    can't compute it cheaply."""


class Processor(Protocol):
    """Container-agnostic column processor.

    Each processor declares which group it belongs to + which data
    types it applies to. The engine routes columns through groups in
    order; per processor, it checks ``applies_to(ctx)`` and only fires
    if the predicate returns True. Implementations should be pure:
    no I/O, no side effects beyond the column transform.

    Three concrete substrate kinds will exist over the four waves:

    * pyarrow Table mutation (Wave 1+) — the local / batch path
    * duckdb SQL emission (future Wave 5) — for streaming
    * Spark DataFrame transform (future Wave 5) — for prod realms

    Wave 1 ships only the pyarrow path; the Protocol shape leaves the
    door open for the others without breaking callers.
    """

    name: str
    """Stable processor name; appears in YAML, in
    ``ProcessorRunResult``, and in ``processors describe`` output."""

    group: ProcessorGroup
    """Which group this processor belongs to."""

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        """Whether the processor wants to run for this column.

        Cheap; called once per column per processor. Return False
        when the column type / role / pii_class doesn't match.
        """
        ...

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        """Apply the transform to a pyarrow Table.

        Returns ``(transformed_table, result)``. The table is a
        fresh pyarrow Table; the implementation must not mutate the
        input in place. The result captures whether the processor
        fired and a rows-affected count where cheap to compute.
        """
        ...


@dataclass(frozen=True)
class GroupConfig:
    """Resolved per-group configuration after platform + dataset merge.

    The engine holds one of these per group during a run.
    """

    group: ProcessorGroup
    mode: GroupMode
    skip_processors: tuple[str, ...] = ()
    """Processor names within this group that the dataset has
    explicitly disabled (e.g. ``hygiene.skip: [strip_bom]``)."""


@dataclass(frozen=True)
class EngineConfig:
    """The full resolved configuration the engine runs against."""

    groups: tuple[GroupConfig, ...]
    """Eight :class:`GroupConfig` entries, one per group, in fixed
    application order."""

    columns: tuple[ColumnTransformContext, ...]
    """Per-column contexts in ordinal order."""

    column_skip_groups: dict[str, frozenset[str]]
    """``column_name → {group_name, ...}`` — per-column group bypass."""

    column_skip_processors: dict[str, frozenset[str]]
    """``column_name → {processor_name, ...}`` — per-column processor bypass."""

    column_opt_in_processors: dict[str, frozenset[str]]
    """``column_name → {processor_name, ...}`` — per-column opt-in
    list (for OPT_IN-mode groups like ``standardization``)."""


__all__ = [
    "ColumnTransformContext",
    "EngineConfig",
    "GroupConfig",
    "GroupMode",
    "Processor",
    "ProcessorGroup",
    "ProcessorRunResult",
]
