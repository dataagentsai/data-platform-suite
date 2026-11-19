"""``lake-ingest config sync`` — push customer YAML configs to metadata DB.

Reads the customer repo's ``config/`` tree and upserts rows into
``platform_config``, ``source_master`` / ``source_config``, and
``dataset_master`` / ``dataset_config`` (plus optional column rows).

Workflow:

1. Resolve target environment → which ``platform_config/<env>.yaml`` to load.
2. Bootstrap metadata schema (idempotent).
3. Plan all upserts; report or apply.

Plan/apply split mirrors ``init.py`` — dry-run is the default in the
CLI and tests assert the plan without touching a real DB.

See ``docs/customer_profile.md`` and ``docs/repo_structure.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from lake_ingestion_framework.core.config_yaml import (
    DatasetConfigFile,
    PlatformConfigFile,
    SourceConfigFile,
    load_dataset_yaml,
    load_platform_config_yaml,
    load_source_yaml,
)
from lake_ingestion_framework.core.dq import (
    CheapEvalWarning,
    DqRuleSet,
    load_rules_from_yaml,
)
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    DqRuleSyncResult,
    MetadataRepository,
)


@dataclass(frozen=True)
class ConfigSyncPlan:
    """Resolved view of what ``config sync`` will write.

    Path lists keep filesystem locations for diagnostic output; the
    parsed Pydantic objects carry the data the repository needs.
    """

    env: str
    platform_config: PlatformConfigFile
    platform_config_path: Path
    sources: tuple[SourceConfigFile, ...] = ()
    source_paths: tuple[Path, ...] = ()
    datasets: tuple[DatasetConfigFile, ...] = ()
    dataset_paths: tuple[Path, ...] = ()
    dq_rule_sets: tuple[DqRuleSet, ...] = ()
    """DQ rules grouped by ``(layer, dataset)``. One entry per
    ``config/dq/*.yaml`` file."""
    dq_warnings: tuple[CheapEvalWarning, ...] = field(default_factory=tuple)
    """Cheap-eval lints across all DQ rule files. Non-fatal — surfaced
    so operators see them at sync time."""
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ConfigSyncResult:
    """Counts written by ``apply_config_sync``."""

    platform_config_keys: int
    sources_upserted: int
    datasets_upserted: int
    dq_rules_inserted: int = 0
    dq_rules_updated: int = 0
    dq_rules_unchanged: int = 0
    dq_rules_soft_deleted: int = 0
    dq_rules_hard_deleted: int = 0
    # Wave 1-4 — standard processors + PII
    pii_policies_upserted: int = 0
    """Rows upserted into ``meta.pii_policy``."""
    processor_group_keys_upserted: int = 0
    """``processor_groups.<name>`` KV keys written across platform +
    dataset configs."""


# ──────────────────────────────────────────────────────────────────────
# Plan
# ──────────────────────────────────────────────────────────────────────


def plan_config_sync(  # noqa: PLR0912 — branches mirror the optional config sub-dirs
    *,
    customer_repo: Path,
    env: str,
) -> ConfigSyncPlan:
    """Read the customer repo's ``config/`` tree for one environment.

    Pure: no DB writes, no schema validation against the key registry
    (that happens in ``apply_config_sync`` via the repository).
    """
    if not customer_repo.is_dir():
        raise ValidationError(f"Customer repo path is not a directory: {customer_repo}")

    config_dir = customer_repo / "config"
    if not config_dir.is_dir():
        raise ValidationError(f"Missing config/ directory under {customer_repo}.")

    # ── platform_config/<env>.yaml — required
    pc_path = config_dir / "platform_config" / f"{env}.yaml"
    if not pc_path.is_file():
        raise ValidationError(
            f"No platform_config for env={env!r}: expected {pc_path}. "
            "Run `lake-ingest init` first or create the file by hand."
        )
    platform_config = load_platform_config_yaml(pc_path)

    # ── sources/*.yaml — optional, but warn if missing
    sources: list[SourceConfigFile] = []
    source_paths: list[Path] = []
    sources_dir = config_dir / "sources"
    if sources_dir.is_dir():
        for yaml_path in sorted(sources_dir.glob("*.yaml")):
            sources.append(load_source_yaml(yaml_path))
            source_paths.append(yaml_path)

    # ── datasets/<source_name>/*.yaml — optional
    datasets: list[DatasetConfigFile] = []
    dataset_paths: list[Path] = []
    datasets_dir = config_dir / "datasets"
    if datasets_dir.is_dir():
        for yaml_path in sorted(datasets_dir.rglob("*.yaml")):
            datasets.append(load_dataset_yaml(yaml_path))
            dataset_paths.append(yaml_path)

    # ── dq/<layer>_<dataset>.yaml — optional (DQ engine v1, Phase 1)
    dq_rule_sets: list[DqRuleSet] = []
    dq_warnings: list[CheapEvalWarning] = []
    dq_dir = config_dir / "dq"
    if dq_dir.is_dir():
        for yaml_path in sorted(dq_dir.glob("*.yaml")):
            rule_set, file_warnings = load_rules_from_yaml(yaml_path)
            dq_rule_sets.append(rule_set)
            dq_warnings.extend(file_warnings)

    warnings: list[str] = []

    # Datasets reference (source_name, source_env); flag any orphans
    source_keys = {(s.source_name, s.source_env) for s in sources}
    for ds, ds_path in zip(datasets, dataset_paths, strict=True):
        if (ds.source_name, ds.source_env) not in source_keys:
            warnings.append(
                f"{ds_path}: references source ({ds.source_name}, {ds.source_env}) "
                "not declared in config/sources/."
            )

    # DQ rules reference dataset names; flag rules whose dataset isn't declared.
    declared_dataset_names = {ds.dataset_name for ds in datasets}
    for rule_set in dq_rule_sets:
        if rule_set.dataset_name not in declared_dataset_names:
            warnings.append(
                f"{rule_set.source_path}: references dataset "
                f"{rule_set.dataset_name!r} not declared in config/datasets/."
            )

    return ConfigSyncPlan(
        env=env,
        platform_config=platform_config,
        platform_config_path=pc_path,
        sources=tuple(sources),
        source_paths=tuple(source_paths),
        datasets=tuple(datasets),
        dataset_paths=tuple(dataset_paths),
        dq_rule_sets=tuple(dq_rule_sets),
        dq_warnings=tuple(dq_warnings),
        warnings=tuple(warnings),
    )


# ──────────────────────────────────────────────────────────────────────
# Apply
# ──────────────────────────────────────────────────────────────────────


def apply_config_sync(
    plan: ConfigSyncPlan,
    repo: MetadataRepository,
    *,
    delete_missing_dq_rules: bool = False,
) -> ConfigSyncResult:
    """Write the planned rows to the metadata DB.

    Bootstraps the schema first; then writes platform_config, sources,
    datasets, and DQ rules in dependency order.

    ``delete_missing_dq_rules=True`` hard-deletes DQ rules in the DB
    that aren't in any YAML; default ``False`` soft-deletes them
    (``is_enabled=0``) so the audit trail survives.
    """
    repo.bootstrap_schema()

    # Platform-level kv (legacy flat) + processor_groups (expanded
    # to processor_groups.<name> keys) — both go into platform_config.
    platform_kv = dict(plan.platform_config.kv)
    platform_pg_keys = _processor_group_keys(plan.platform_config.processor_groups)
    platform_kv.update(platform_pg_keys)
    repo.upsert_platform_config(platform_kv)

    # Wave 2 — pii_policy rows are per-env; the env in the plan is
    # the env this YAML was loaded for, so every entry shares it.
    pii_policies_upserted = 0
    for entry in plan.platform_config.pii_policy:
        repo.upsert_pii_policy(
            pii_class=entry.pii_class,
            env=plan.env,
            action=entry.action,
            salt_kv_ref=entry.salt_kv_ref,
            redact_value=entry.redact_value,
            mask_visible=entry.mask_visible,
        )
        pii_policies_upserted += 1

    processor_group_keys_upserted = len(platform_pg_keys)

    source_id_by_key: dict[tuple[str, str], int] = {}
    for src in plan.sources:
        source_id = repo.upsert_source(
            source_name=src.source_name,
            source_env=src.source_env,
            source_type=src.source_type,
            is_enabled=src.is_enabled,
            config=src.config,
        )
        source_id_by_key[(src.source_name, src.source_env)] = source_id

    datasets_written = 0
    dataset_id_by_name: dict[str, int] = {}
    for ds in plan.datasets:
        ds_source_id = source_id_by_key.get((ds.source_name, ds.source_env))
        if ds_source_id is None:
            # Orphan dataset — already warned in plan; skip silently here so
            # operators can fix one-off references without blocking the rest.
            continue

        columns = (
            tuple(
                DatasetColumnRow(
                    dataset_id=0,  # set by repository on insert
                    column_name=c.column_name,
                    data_type=c.data_type,
                    is_nullable=c.is_nullable,
                    ordinal_position=c.ordinal_position,
                    is_primary_key=c.is_primary_key,
                    is_excluded=c.is_excluded,
                    pii_class=c.pii_class,
                    source_path=c.source_path,
                    silver_column_name=c.silver_column_name,
                    silver_data_type=c.silver_data_type,
                    cast_safety=c.cast_safety,
                    null_replacement=c.null_replacement,
                    transforms=c.transforms,
                    skip_groups=c.skip_groups,
                    skip_processors=c.skip_processors,
                    column_role=c.column_role,
                )
                for c in ds.columns
            )
            if ds.columns
            else None
        )

        # Per-dataset processor_groups overrides + dedup config are
        # synced into the dataset_config KV (with processor_groups.*
        # and dedup.* keys). The base ds.config dict is the customer's
        # already-flat KV; we union them.
        ds_config_kv = dict(ds.config)
        ds_pg_keys = _processor_group_keys(ds.processor_groups)
        ds_config_kv.update(ds_pg_keys)
        if ds.dedup is not None:
            ds_config_kv.update(_dedup_keys(ds.dedup))
        processor_group_keys_upserted += len(ds_pg_keys)

        dataset_id = repo.upsert_dataset(
            source_id=ds_source_id,
            dataset_name=ds.dataset_name,
            is_enabled=ds.is_enabled,
            config=ds_config_kv,
            columns=list(columns) if columns is not None else None,
        )
        dataset_id_by_name[ds.dataset_name] = dataset_id
        datasets_written += 1

    # ── DQ rule sets (one upsert per (layer, dataset))
    # Skip rule sets whose dataset isn't in the metadata DB — they were
    # warned in plan; treating them as orphans here matches the dataset/source
    # orphan posture (be permissive at apply time; let plan flag the issue).
    dq_totals = DqRuleSyncResult()
    for rule_set in plan.dq_rule_sets:
        rule_dataset_id = dataset_id_by_name.get(rule_set.dataset_name)
        if rule_dataset_id is None:
            continue
        rs_result = repo.upsert_dq_rule_set(
            rule_set,
            dataset_id=rule_dataset_id,
            delete_missing=delete_missing_dq_rules,
        )
        dq_totals = DqRuleSyncResult(
            inserted=dq_totals.inserted + rs_result.inserted,
            updated=dq_totals.updated + rs_result.updated,
            unchanged=dq_totals.unchanged + rs_result.unchanged,
            soft_deleted=dq_totals.soft_deleted + rs_result.soft_deleted,
            hard_deleted=dq_totals.hard_deleted + rs_result.hard_deleted,
        )

    return ConfigSyncResult(
        platform_config_keys=len(platform_kv),
        sources_upserted=len(plan.sources),
        datasets_upserted=datasets_written,
        dq_rules_inserted=dq_totals.inserted,
        dq_rules_updated=dq_totals.updated,
        dq_rules_unchanged=dq_totals.unchanged,
        dq_rules_soft_deleted=dq_totals.soft_deleted,
        dq_rules_hard_deleted=dq_totals.hard_deleted,
        pii_policies_upserted=pii_policies_upserted,
        processor_group_keys_upserted=processor_group_keys_upserted,
    )


def _processor_group_keys(groups: dict[str, str]) -> dict[str, str]:
    """Expand a ``{group_name: mode}`` dict into KV keys
    ``processor_groups.<group_name>: <mode>`` for the registry."""
    return {f"processor_groups.{name}": mode for name, mode in groups.items()}


def _dedup_keys(dedup: object) -> dict[str, str]:
    """Flatten the ``DatasetDedupConfig`` to KV keys.

    Stored as ``dedup.mode`` / ``dedup.business_keys`` (CSV) /
    ``dedup.order_by``. None values are omitted (no key written).
    """
    # Imported at function scope to avoid a circular import — config_sync
    # already imports DatasetConfigFile transitively.
    from lake_ingestion_framework.core.config_yaml import DatasetDedupConfig

    if not isinstance(dedup, DatasetDedupConfig):
        return {}
    out: dict[str, str] = {"dedup.mode": dedup.mode}
    if dedup.business_keys:
        out["dedup.business_keys"] = ",".join(dedup.business_keys)
    if dedup.order_by:
        out["dedup.order_by"] = dedup.order_by
    return out


__all__ = [
    "ConfigSyncPlan",
    "ConfigSyncResult",
    "apply_config_sync",
    "plan_config_sync",
]
