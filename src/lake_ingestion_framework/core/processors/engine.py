"""ProcessorEngine — applies the eight standard groups in order.

Per ``docs/standard_processors.md`` the application order is:

  1. column renames + drops (handled by SilverPipeline._enrich)
  2. hygiene
  3. type_safety
  4. case_normalize           (Wave 3)
  5. timestamp_normalize      (Wave 3)
  6. standardization          (Wave 4)
  7. per-column custom transforms (null_replacement, opt-in transforms)
  8. pii                       (Wave 2)
  9. dedup                     (Wave 3)
 10. _record_hash recompute (handled in SilverPipeline._enrich)
 11. provenance (Silver metadata cols — handled in SilverPipeline._enrich)

Wave 1 ships hygiene + type_safety + provenance describe-only.
Subsequent waves register more groups against this same engine
without re-touching the wiring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.case_normalize import (
    CASE_NORMALIZE_DEFAULT_ON,
    CASE_NORMALIZE_OPT_IN,
)
from lake_ingestion_framework.core.processors.dedup import (
    DedupConfig,
    DedupResult,
    apply_dedup,
)
from lake_ingestion_framework.core.processors.hygiene import (
    HYGIENE_DEFAULT_ON,
    HYGIENE_OPT_IN,
)
from lake_ingestion_framework.core.processors.pii import (
    PiiPolicy,
    build_pii_group,
    resolve_salt,
)
from lake_ingestion_framework.core.processors.provenance import (
    PROVENANCE_DEFAULT_ON,
)
from lake_ingestion_framework.core.processors.standardization import (
    STANDARDIZATION_DEFAULT_ON,
    STANDARDIZATION_OPT_IN,
)
from lake_ingestion_framework.core.processors.timestamp_normalize import (
    TIMESTAMP_NORMALIZE_DEFAULT_ON,
    TIMESTAMP_NORMALIZE_OPT_IN,
)
from lake_ingestion_framework.core.processors.type_safety import (
    TYPE_SAFETY_DEFAULT_ON,
    TYPE_SAFETY_OPT_IN,
)
from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    EngineConfig,
    GroupConfig,
    GroupMode,
    Processor,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from lake_ingestion_framework.core.metadata.repository import (
        DatasetColumnRow,
        MetadataRepository,
    )


# Order matters — see module docstring + docs/standard_processors.md.
_GROUP_ORDER = (
    ProcessorGroup.HYGIENE,
    ProcessorGroup.TYPE_SAFETY,
    # case_normalize / timestamp_normalize / standardization land in Wave 3-4
    ProcessorGroup.PII,  # Wave 2
    ProcessorGroup.DEDUP,  # Wave 3
)


# Group → (default-on processors, opt-in processor map)
# Procesor instances are typed loosely as ``object`` in the imports
# because each module's processor classes are independent — they only
# satisfy the Protocol structurally. The cast at use site narrows.
_GROUP_REGISTRY: dict[ProcessorGroup, tuple[tuple[object, ...], dict[str, object]]] = {
    ProcessorGroup.HYGIENE: (HYGIENE_DEFAULT_ON, HYGIENE_OPT_IN),
    ProcessorGroup.TYPE_SAFETY: (TYPE_SAFETY_DEFAULT_ON, TYPE_SAFETY_OPT_IN),
    ProcessorGroup.CASE_NORMALIZE: (CASE_NORMALIZE_DEFAULT_ON, CASE_NORMALIZE_OPT_IN),
    ProcessorGroup.TIMESTAMP_NORMALIZE: (
        TIMESTAMP_NORMALIZE_DEFAULT_ON,
        TIMESTAMP_NORMALIZE_OPT_IN,
    ),
    ProcessorGroup.STANDARDIZATION: (
        STANDARDIZATION_DEFAULT_ON,
        STANDARDIZATION_OPT_IN,
    ),
    ProcessorGroup.PROVENANCE: (PROVENANCE_DEFAULT_ON, {}),
}


@dataclass
class ProcessorRunReport:
    """Aggregated results across all groups + columns for one run.

    Surfaced via :meth:`SilverRunStats.processor_summary` (Wave 1.4)
    and via the lineage facet (deferred).
    """

    results: list[ProcessorRunResult] = field(default_factory=list)

    @property
    def processors_fired(self) -> int:
        return sum(1 for r in self.results if r.fired)

    @property
    def total_rows_affected(self) -> int:
        return sum(r.rows_affected for r in self.results if r.rows_affected > 0)

    def by_group(self) -> dict[ProcessorGroup, list[ProcessorRunResult]]:
        out: dict[ProcessorGroup, list[ProcessorRunResult]] = {}
        for r in self.results:
            out.setdefault(r.group, []).append(r)
        return out


class ProcessorEngine:
    """Applies registered processor groups against a pyarrow Table.

    Stateless across calls — safe to reuse one instance for many
    runs. Each ``apply()`` call resolves the engine config from the
    repository + the dataset, then walks the groups in fixed order.

    Per ``docs/standard_processors.md``, the engine is the *only*
    place groups are applied. SilverPipeline calls
    ``apply_pre_hash_groups()`` between rename and ``_record_hash``
    computation; provenance + post-hash work stays in the pipeline
    until the engine takes ownership in a later wave.
    """

    def __init__(self) -> None:
        # Per-run state populated by resolve_config:
        self._pii_processors: tuple[object, ...] = ()

    # ── Config resolution ───────────────────────────────────────────

    def resolve_config(
        self,
        repo: MetadataRepository,
        dataset_id: int,
        platform_groups: dict[str, str] | None = None,
        dataset_groups: dict[str, str] | None = None,
        env: str | None = None,
    ) -> EngineConfig:
        """Build an :class:`EngineConfig` by merging:

        * platform-level group settings (``platform_groups``)
        * dataset-level overrides (``dataset_groups``)
        * per-column metadata (from ``meta.dataset_config_columns``)

        Both ``platform_groups`` and ``dataset_groups`` use raw
        strings (``"enabled"`` / ``"disabled"`` / ``"opt_in"`` /
        ``"dataset_decides"``) — what comes out of the KV registry.
        """
        column_rows = repo.list_dataset_columns(dataset_id)
        contexts = tuple(_to_context(c) for c in column_rows if not c.is_excluded)
        skip_groups = {
            c.column_name: frozenset(c.skip_groups) for c in column_rows if c.skip_groups
        }
        skip_processors = {
            c.column_name: frozenset(c.skip_processors) for c in column_rows if c.skip_processors
        }
        opt_in = {c.column_name: frozenset(c.transforms) for c in column_rows if c.transforms}

        groups: list[GroupConfig] = []
        for group in ProcessorGroup:
            mode = _resolve_group_mode(group, platform_groups, dataset_groups)
            groups.append(GroupConfig(group=group, mode=mode))

        # Wave 2 — instantiate the pii group with resolved policy if env supplied.
        # PII processors are dynamic (depend on customer policy), so they live
        # in a per-engine attribute rather than the static _GROUP_REGISTRY.
        self._pii_processors = ()  # reset for this resolve_config call
        if env is not None:
            policy_rows = repo.list_pii_policy(env=env)
            policies = {
                r.pii_class: PiiPolicy(
                    pii_class=r.pii_class,
                    env=r.env,
                    action=r.action,
                    salt=resolve_salt(r.salt_kv_ref),
                    redact_value=r.redact_value or "[redacted]",
                    mask_visible=r.mask_visible,
                )
                for r in policy_rows
            }
            if policies:
                self._pii_processors = build_pii_group(policies, env=env)

        return EngineConfig(
            groups=tuple(groups),
            columns=contexts,
            column_skip_groups=skip_groups,
            column_skip_processors=skip_processors,
            column_opt_in_processors=opt_in,
        )

    # ── Application ─────────────────────────────────────────────────

    def apply_pre_hash_groups(
        self, table: pa.Table, config: EngineConfig
    ) -> tuple[pa.Table, ProcessorRunReport]:
        """Apply hygiene + type_safety + pii against the in-memory
        table — every group whose result must land in the
        ``_record_hash`` recompute downstream.

        PII runs LAST among these so the hash is over the
        masked/hashed value. Two clear-text values that mask to the
        same thing produce the same row hash → SCD2 dedup works
        correctly.
        """
        report = ProcessorRunReport()
        # Wave 1 + Wave 3 column groups, in order
        for group in (
            ProcessorGroup.HYGIENE,
            ProcessorGroup.TYPE_SAFETY,
            ProcessorGroup.CASE_NORMALIZE,
            ProcessorGroup.TIMESTAMP_NORMALIZE,
            ProcessorGroup.STANDARDIZATION,
        ):
            table, results = self._apply_group(table, group, config)
            report.results.extend(results)

        # PII — Wave 2. Processors are dynamic (per resolved policy),
        # so they don't live in the static _GROUP_REGISTRY.
        table, pii_results = self._apply_pii(table, config)
        report.results.extend(pii_results)

        return table, report

    def apply_dedup(
        self, table: pa.Table, dedup_config: DedupConfig
    ) -> tuple[pa.Table, DedupResult]:
        """Apply the dataset's dedup policy to the in-memory table.

        Called by SilverPipeline AFTER ``apply_pre_hash_groups`` and
        BEFORE ``_record_hash`` recompute — so dedup respects PII /
        cast / case-normalization equivalence, and the hash runs only
        over the surviving rows.

        No-op when ``dedup_config.mode == 'none'`` (Phase-4 behaviour
        preserved when customer doesn't opt in).
        """
        return apply_dedup(table, dedup_config)

    def _apply_pii(
        self, table: pa.Table, config: EngineConfig
    ) -> tuple[pa.Table, list[ProcessorRunResult]]:
        """Apply the resolved pii_processors against eligible columns.

        No-op when the engine wasn't given an ``env`` at config-resolve
        time, or no policy rows matched, or the pii group is DISABLED.
        """
        if not self._pii_processors:
            return table, []
        pii_cfg = next(g for g in config.groups if g.group == ProcessorGroup.PII)
        if pii_cfg.mode == GroupMode.DISABLED:
            return table, []

        results: list[ProcessorRunResult] = []
        for ctx in config.columns:
            if "pii" in config.column_skip_groups.get(ctx.bronze_column_name, ()):
                continue
            for raw_proc in self._pii_processors:
                proc = _as_processor(raw_proc)
                if proc.name in config.column_skip_processors.get(ctx.bronze_column_name, ()):
                    continue
                if not proc.applies_to(ctx):
                    continue
                table, result = proc.run_arrow(table, ctx)
                results.append(result)
        return table, results

    def describe(self, config: EngineConfig) -> dict[ProcessorGroup, list[str]]:
        """Return ``group → [active processor names]`` for the
        ``processors describe`` CLI. Doesn't run any processor;
        purely a config inspection.
        """
        out: dict[ProcessorGroup, list[str]] = {}
        for group_cfg in config.groups:
            if group_cfg.group not in _GROUP_REGISTRY:
                continue
            default_on, _ = _GROUP_REGISTRY[group_cfg.group]
            if group_cfg.mode == GroupMode.DISABLED:
                out[group_cfg.group] = []
                continue
            out[group_cfg.group] = [_as_processor(p).name for p in default_on]
        return out

    # ── Internals ───────────────────────────────────────────────────

    def _apply_group(
        self,
        table: pa.Table,
        group: ProcessorGroup,
        config: EngineConfig,
    ) -> tuple[pa.Table, list[ProcessorRunResult]]:
        if group not in _GROUP_REGISTRY:
            return table, []
        group_cfg = next(g for g in config.groups if g.group == group)
        if group_cfg.mode == GroupMode.DISABLED:
            return table, []

        default_on, opt_in_map = _GROUP_REGISTRY[group]
        all_results: list[ProcessorRunResult] = []

        for ctx in config.columns:
            # column-level group skip
            if group.value in config.column_skip_groups.get(ctx.bronze_column_name, ()):
                continue

            # default-on processors fire when the group is ENABLED
            if group_cfg.mode == GroupMode.ENABLED:
                for raw_proc in default_on:
                    proc = _as_processor(raw_proc)
                    if proc.name in config.column_skip_processors.get(ctx.bronze_column_name, ()):
                        continue
                    if not proc.applies_to(ctx):
                        continue
                    table, result = proc.run_arrow(table, ctx)
                    all_results.append(result)

            # opt-in processors fire when the column lists them
            opt_ins = config.column_opt_in_processors.get(ctx.bronze_column_name, frozenset())
            for proc_name in opt_ins:
                raw_opt = opt_in_map.get(proc_name)
                if raw_opt is None:
                    continue
                proc = _as_processor(raw_opt)
                if not proc.applies_to(ctx):
                    continue
                table, result = proc.run_arrow(table, ctx)
                all_results.append(result)

        return table, all_results


def _as_processor(obj: object) -> Processor:
    """Narrow ``object`` to the structural :class:`Processor` Protocol.

    Static-typing escape hatch — the registry holds processor classes
    from independent modules whose only common contract is the
    Protocol. mypy needs the cast; runtime is duck-typed.
    """
    return obj  # type: ignore[return-value]


def _resolve_group_mode(
    group: ProcessorGroup,
    platform_groups: dict[str, str] | None,
    dataset_groups: dict[str, str] | None,
) -> GroupMode:
    """Merge platform + dataset group settings into a final mode.

    Priority: dataset > platform > built-in default. ``dataset_decides``
    at platform level means "use dataset's choice (or DISABLED if
    dataset is silent)."
    """
    dataset_value = (dataset_groups or {}).get(group.value)
    if dataset_value is not None:
        return _parse_mode(dataset_value)

    platform_value = (platform_groups or {}).get(group.value)
    if platform_value is not None:
        mode = _parse_mode(platform_value)
        if mode == GroupMode.DATASET_DECIDES:
            # No dataset override → disabled
            return GroupMode.DISABLED
        return mode

    return _BUILTIN_DEFAULTS.get(group, GroupMode.DISABLED)


def _parse_mode(value: str) -> GroupMode:
    try:
        return GroupMode(value)
    except ValueError:
        return GroupMode.DISABLED


# Built-in defaults per docs/standard_processors.md "defaults summary".
_BUILTIN_DEFAULTS: dict[ProcessorGroup, GroupMode] = {
    ProcessorGroup.HYGIENE: GroupMode.ENABLED,
    ProcessorGroup.TYPE_SAFETY: GroupMode.ENABLED,
    ProcessorGroup.CASE_NORMALIZE: GroupMode.ENABLED,
    ProcessorGroup.TIMESTAMP_NORMALIZE: GroupMode.ENABLED,
    ProcessorGroup.STANDARDIZATION: GroupMode.OPT_IN,
    ProcessorGroup.PII: GroupMode.ENABLED,
    ProcessorGroup.DEDUP: GroupMode.DISABLED,
    ProcessorGroup.PROVENANCE: GroupMode.ENABLED,
}


def _to_context(row: DatasetColumnRow) -> ColumnTransformContext:
    """Convert a metadata row to a :class:`ColumnTransformContext`."""
    return ColumnTransformContext(
        bronze_column_name=row.column_name,
        silver_column_name=row.silver_column_name or row.column_name,
        bronze_data_type=row.data_type,
        silver_data_type=row.silver_data_type,
        cast_safety=row.cast_safety,
        pii_class=row.pii_class,
        column_role=row.column_role,
        null_replacement=row.null_replacement,
    )


__all__ = [
    "ProcessorEngine",
    "ProcessorRunReport",
]
