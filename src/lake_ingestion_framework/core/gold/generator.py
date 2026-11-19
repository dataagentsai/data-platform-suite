"""dbt project generator for the Gold layer.

Reads a :class:`GoldProjectConfig` (sources + models) and emits a
deterministic, byte-identical dbt project tree:

    <output_dir>/
      dbt_project.yml
      profiles.yml
      models/
        sources.yml
        marts/<domain>/<model>.sql
        marts/<domain>/<model>.yml

Phase 1 scope (per ``docs/gold_layer_design.md`` §21.1 phases 1-2):

- Strategy: ``dbt`` only (PySpark Workflow escape hatch is Phase 9).
- Materializations: ``view`` / ``table`` / ``incremental`` (with
  ``merge`` strategy + dbt-managed watermark, design §6.2).
- Tests: ``not_null`` + ``unique`` on each model's ``business_keys``.
- Re-running with the same config produces byte-identical output
  (sorted YAML, stable file iteration order — golden-diff ready).

Deferred (call sites raise ``UnsupportedGoldFeatureError`` when used):

- ``snapshot`` / ``materialized_view`` materializations
- ``pyspark_workflow`` strategy
- post_hook metadata-column macros, DQ test emission, recon, lineage,
  framework-passed watermark vars (patterns 2 + 3)

The generator is **realm-agnostic** — it lives in ``core/`` because dbt
projects are realm-neutral. The customer's realm picks how to run the
project (Databricks Workflows, ADF Execute Databricks Notebook, etc.).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from lake_ingestion_framework.core.exceptions import (
    LakeIngestError,
    ValidationError,
)

# ──────────────────────────────────────────────────────────────────────
# Public types
# ──────────────────────────────────────────────────────────────────────


class GoldStrategy(StrEnum):
    """Per-model engine choice (design §3.1).

    v1 ships ``dbt`` only; ``pyspark_workflow`` is recognised at the
    config layer so future phases can add the writer without reshaping
    the API.
    """

    DBT = "dbt"
    PYSPARK_WORKFLOW = "pyspark_workflow"


class Materialization(StrEnum):
    """dbt materialization (design §5.1).

    Phase 1 supports VIEW / TABLE / INCREMENTAL. SNAPSHOT and
    MATERIALIZED_VIEW are recognised values but unsupported by the
    generator (deferred to phases 6 + later).
    """

    VIEW = "view"
    TABLE = "table"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"
    MATERIALIZED_VIEW = "materialized_view"


class MartShape(StrEnum):
    """Mart-shape classification (design §5.2 / §14).

    Drives the default materialization and folder layout. Optional —
    the generator falls back to the explicit ``materialization`` value
    when ``mart_shape`` is unset.
    """

    FCT = "fct"
    DIM = "dim"
    OBT = "obt"
    AGG = "agg"
    SNAP = "snap"


class SnapshotStrategy(StrEnum):
    """dbt snapshot detection strategy (design §8.2).

    ``timestamp``: track changes via a column whose value rises on each
    update. Cheap — dbt only re-checks rows whose ``updated_at`` is
    newer than what's in the snapshot.

    ``check``: checksum on a listed set of columns. Use when the source
    has no reliable monotonic update column.
    """

    TIMESTAMP = "timestamp"
    CHECK = "check"


@dataclass(frozen=True)
class GoldSource:
    """One Silver table declared as a dbt source.

    Renders into ``models/sources.yml`` under a single ``silver``
    source group. ``loaded_at_field`` defaults to ``_silver_loaded_at``
    — the framework's standard Silver metadata column.
    """

    name: str
    description: str = ""
    loaded_at_field: str = "_silver_loaded_at"
    freshness_warn_after_hours: int = 6
    freshness_error_after_hours: int = 24


@dataclass(frozen=True)
class GoldModelConfig:
    """One Gold model — emits an ``.sql`` + ``.yml`` pair under marts/.

    Single-source models (one Silver table) get a templated SELECT.
    Multi-source models (joins across multiple Silver tables, derived
    dims, point-in-time logic) supply ``custom_sql`` — the framework
    deliberately doesn't render JOIN logic; that's where AE judgment
    lives (which keys, inner vs left, dedupe rules, window functions).

    The generator template assembles the SQL: a ``{{ config(...) }}``
    block driven by ``materialization`` + ``incremental_strategy``,
    followed by ``SELECT * FROM {{ source('silver', '<source>') }}``,
    optionally guarded by an ``is_incremental()`` predicate.

    For non-trivial transforms a customer can supply ``custom_sql``;
    when present, the generator skips its templated SELECT and embeds
    that body verbatim under the config block.
    """

    name: str
    """Model identifier — drives the ``.sql`` file name. Conventionally
    starts with ``fct_`` / ``dim_`` / ``obt_`` / ``agg_`` / ``snap_``."""

    domain: str
    """Subfolder under ``models/marts/``. Group models by business
    domain (e.g., ``finance``, ``product``)."""

    silver_sources: tuple[str, ...]
    """Upstream Silver sources. Every entry must appear in
    :attr:`GoldProjectConfig.sources` by ``name``. Single-source models
    use the default templated SELECT; multi-source models must supply
    :attr:`custom_sql` (the framework doesn't render JOIN logic)."""

    business_keys: tuple[str, ...]
    """Natural-key columns. ``not_null`` test on each; ``unique`` test
    on the (single) first key. Drives ``unique_key`` for incremental."""

    strategy: GoldStrategy = GoldStrategy.DBT
    """Engine. Phase 1 supports ``DBT`` only; ``PYSPARK_WORKFLOW`` is
    deferred to Phase 9."""

    materialization: Materialization = Materialization.INCREMENTAL
    mart_shape: MartShape | None = None

    description: str = ""
    incremental_watermark_column: str | None = None
    """Column carrying the incremental cursor (e.g., ``updated_at``).
    Required when ``materialization == INCREMENTAL``. Spliced into the
    ``WHERE updated_at > (SELECT MAX(...) FROM {{ this }})`` predicate."""

    incremental_lookback_days: int = 3
    """Days subtracted from the watermark to catch late-arriving rows
    (design §6.2). 0 disables the lookback."""

    custom_sql: str | None = None
    """Hand-authored model body (everything after the ``{{ config }}``
    block). Used when SQL doesn't fit the templated SELECT — design
    §4.2 / open question Q7."""


@dataclass(frozen=True)
class GoldSnapshotConfig:
    """One dbt snapshot — emits a ``.sql`` file under ``snapshots/``.

    Snapshots track row-version history for *Gold-level* state — most
    SCD2 belongs at Silver (design §8.1), so reach for a snapshot only
    when:

    - The mart itself is the grain of history (e.g., daily snapshot of
      ``obt_customer_360`` to track how a joined view evolves)
    - A computed Gold metric changes over time and you need the
      historical value
    - The upstream source has no SCD2 capability and Gold needs SCD2
      semantics

    Snapshots wrap a :class:`GoldModelConfig` (or take a custom SELECT
    body). They live in ``snapshots/`` and use dbt's ``{% snapshot %}``
    block, distinct from regular models in both file location and SQL
    syntax.
    """

    name: str
    """Snapshot identifier — drives the file name + the dbt snapshot
    block name. Conventionally starts with ``snap_``."""

    target_schema: str
    """Schema where the snapshot table lives in the warehouse.
    Conventionally a separate schema (``gold_snapshots``) so snapshot
    tables don't clutter the main Gold schema."""

    source_model: str
    """Name of the Gold model this snapshot wraps. Must appear in
    :attr:`GoldProjectConfig.models`. The default SELECT is
    ``SELECT * FROM {{ ref('<source_model>') }}``; override via
    :attr:`custom_sql`."""

    unique_key: str | tuple[str, ...]
    """Natural key (or compound key) that identifies a logical row
    across versions. dbt uses this to match new versions to existing
    history."""

    strategy: SnapshotStrategy
    """``timestamp`` (cheap, requires :attr:`updated_at_column`) or
    ``check`` (checksum, requires :attr:`check_cols`)."""

    updated_at_column: str | None = None
    """Column that monotonically rises on row updates (e.g.,
    ``_gold_loaded_at``). Required when ``strategy=TIMESTAMP``;
    ignored otherwise."""

    check_cols: tuple[str, ...] | None = None
    """Columns whose hashed value triggers a new version when changed.
    Required (non-empty) when ``strategy=CHECK``; ignored otherwise."""

    description: str = ""
    custom_sql: str | None = None
    """Override the default ``SELECT * FROM ref('<source_model>')``.
    The body still goes inside the ``{% snapshot %}`` block."""


@dataclass(frozen=True)
class GoldProjectConfig:
    """Inputs for one ``generate_dbt_project()`` call.

    ``project_name`` becomes the dbt project name + the profile name
    (kept identical for simplicity; per-customer projects always have a
    unique name anyway).
    """

    project_name: str
    sources: tuple[GoldSource, ...]
    models: tuple[GoldModelConfig, ...]
    snapshots: tuple[GoldSnapshotConfig, ...] = ()
    """Optional Gold-level SCD2 snapshots (design §8.2). Each wraps a
    Gold model in :attr:`models` and tracks row-version history."""

    silver_catalog_var: str = "silver_catalog"
    silver_schema_var: str = "silver_schema"
    """``var()`` names referenced from sources.yml — keep configurable
    so customers can pass per-environment values via dbt vars."""

    silver_catalog_default: str = "main"
    silver_schema_default: str = "silver"
    """Default values for the silver_catalog/schema vars — emitted into
    ``dbt_project.yml`` so ``dbt parse`` succeeds without ``--vars``.
    Customers override per-env via ``dbt run --vars '{...}'``."""

    profile_target: str = "prod"
    profile_threads: int = 8

    dbt_version_requirement: str = ">=1.7.0"


@dataclass(frozen=True)
class GoldGenerationStats:
    """What ``generate_dbt_project()`` emitted — for CLI output + tests.

    File counts come from ``files_written``; emit them as informational
    counters next to the ADF generator's ``artifacts_written`` to give
    operators a single mental model across realms.
    """

    project_name: str
    output_dir: Path
    files_written: tuple[Path, ...] = field(default_factory=tuple)
    sources_count: int = 0
    models_count: int = 0
    snapshots_count: int = 0


class UnsupportedGoldFeatureError(LakeIngestError):
    """A configured option exists in the schema but isn't implemented yet."""


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────


def generate_dbt_project(project: GoldProjectConfig, output_dir: Path) -> GoldGenerationStats:
    """Emit a deterministic dbt project tree under ``output_dir``.

    Idempotent: re-running with identical inputs writes byte-identical
    files (sorted YAML, stable iteration order). Fails fast on any
    config violation before touching the filesystem.
    """
    _validate(project)

    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    written.append(_write_dbt_project_yml(project, output_dir))
    written.append(_write_profiles_yml(project, output_dir))
    written.append(_write_sources_yml(project, output_dir))
    written.append(_write_packages_yml(output_dir))
    written.append(_write_gold_meta_macro(output_dir))

    for model in project.models:
        written.append(_write_model_sql(model, output_dir))
        written.append(_write_model_yml(model, output_dir))

    for snapshot in project.snapshots:
        written.append(_write_snapshot_sql(snapshot, output_dir))

    return GoldGenerationStats(
        project_name=project.project_name,
        output_dir=output_dir,
        files_written=tuple(sorted(written)),
        sources_count=len(project.sources),
        models_count=len(project.models),
        snapshots_count=len(project.snapshots),
    )


# ──────────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────────


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_DBT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
"""dbt project / model / source names must be snake_case (dbt
convention enforced at parse time; we enforce it earlier to fail
deterministically)."""


_SUPPORTED_MATERIALIZATIONS: frozenset[Materialization] = frozenset(
    {Materialization.VIEW, Materialization.TABLE, Materialization.INCREMENTAL}
)


def _validate(project: GoldProjectConfig) -> None:
    """Reject every shape we can't emit — before any I/O."""
    _validate_dbt_name("project_name", project.project_name)

    if not project.sources:
        raise ValidationError("GoldProjectConfig.sources must be non-empty.")
    if not project.models:
        raise ValidationError("GoldProjectConfig.models must be non-empty.")

    source_names = {s.name for s in project.sources}
    for source in project.sources:
        _validate_dbt_name(f"source {source.name!r}", source.name)
        _validate_identifier("loaded_at_field", source.loaded_at_field)

    for model in project.models:
        _validate_model(model, source_names)

    model_names = {m.name for m in project.models}
    snapshot_names: set[str] = set()
    for snapshot in project.snapshots:
        if snapshot.name in snapshot_names:
            raise ValidationError(f"Duplicate snapshot name {snapshot.name!r}.")
        snapshot_names.add(snapshot.name)
        _validate_snapshot(snapshot, model_names)


def _validate_model(model: GoldModelConfig, source_names: set[str]) -> None:
    _validate_dbt_name(f"model {model.name!r}", model.name)
    _validate_dbt_name(f"domain {model.domain!r}", model.domain)

    if not model.silver_sources:
        raise ValidationError(
            f"Model {model.name!r} requires at least one entry in silver_sources."
        )

    seen: set[str] = set()
    for src in model.silver_sources:
        if src not in source_names:
            raise ValidationError(
                f"Model {model.name!r} references silver source {src!r} "
                f"which is not in project.sources (have: {sorted(source_names)})."
            )
        if src in seen:
            raise ValidationError(
                f"Model {model.name!r} lists silver source {src!r} more than once."
            )
        seen.add(src)

    # Default templated SELECT only handles a single source; joins must
    # come from custom_sql. This forces JOIN choices (keys, types, dedupe)
    # to live in SQL where they belong, not in metadata.
    if model.custom_sql is None and len(model.silver_sources) > 1:
        raise ValidationError(
            f"Model {model.name!r} declares {len(model.silver_sources)} silver_sources "
            "but no custom_sql. Multi-source models must supply custom_sql with "
            "the join logic (the framework doesn't render JOINs)."
        )

    if not model.business_keys:
        raise ValidationError(f"Model {model.name!r} requires at least one business_key.")
    for key in model.business_keys:
        _validate_identifier(f"business_key in {model.name!r}", key)

    if model.strategy != GoldStrategy.DBT:
        raise UnsupportedGoldFeatureError(
            f"Gold strategy {model.strategy.value!r} is not supported in Phase 1. "
            "Only 'dbt' is wired (PySpark Workflow is Phase 9)."
        )

    if model.materialization == Materialization.SNAPSHOT:
        raise ValidationError(
            f"Model {model.name!r} sets materialization='snapshot'. Snapshots are "
            "configured via the project-level 'snapshots' field "
            "(GoldSnapshotConfig), not as a model materialization."
        )

    if model.materialization not in _SUPPORTED_MATERIALIZATIONS:
        raise UnsupportedGoldFeatureError(
            f"Materialization {model.materialization.value!r} is not supported. "
            f"Supported: {sorted(m.value for m in _SUPPORTED_MATERIALIZATIONS)} "
            "(use 'snapshots' field for SCD2)."
        )

    if model.materialization == Materialization.INCREMENTAL:
        if not model.incremental_watermark_column:
            raise ValidationError(
                f"Incremental model {model.name!r} requires "
                "incremental_watermark_column (e.g., 'updated_at')."
            )
        _validate_identifier(
            f"incremental_watermark_column in {model.name!r}",
            model.incremental_watermark_column,
        )
        if model.incremental_lookback_days < 0:
            raise ValidationError(
                f"incremental_lookback_days must be >= 0, got {model.incremental_lookback_days}."
            )


def _validate_snapshot(snapshot: GoldSnapshotConfig, model_names: set[str]) -> None:
    _validate_dbt_name(f"snapshot {snapshot.name!r}", snapshot.name)
    _validate_identifier(f"target_schema in {snapshot.name!r}", snapshot.target_schema)

    if snapshot.source_model not in model_names:
        raise ValidationError(
            f"Snapshot {snapshot.name!r} references source_model "
            f"{snapshot.source_model!r} which is not in project.models "
            f"(have: {sorted(model_names)})."
        )

    keys = (snapshot.unique_key,) if isinstance(snapshot.unique_key, str) else snapshot.unique_key
    if not keys:
        raise ValidationError(f"Snapshot {snapshot.name!r} requires a non-empty unique_key.")
    for key in keys:
        _validate_identifier(f"unique_key in {snapshot.name!r}", key)

    if snapshot.strategy == SnapshotStrategy.TIMESTAMP:
        if not snapshot.updated_at_column:
            raise ValidationError(
                f"Snapshot {snapshot.name!r} uses strategy='timestamp' and requires "
                "updated_at_column (e.g., '_gold_loaded_at')."
            )
        _validate_identifier(f"updated_at_column in {snapshot.name!r}", snapshot.updated_at_column)
    elif snapshot.strategy == SnapshotStrategy.CHECK:
        if not snapshot.check_cols:
            raise ValidationError(
                f"Snapshot {snapshot.name!r} uses strategy='check' and requires "
                "non-empty check_cols."
            )
        for col in snapshot.check_cols:
            _validate_identifier(f"check_cols entry in {snapshot.name!r}", col)


def _validate_dbt_name(role: str, value: str) -> None:
    if not _DBT_NAME_RE.match(value):
        raise ValidationError(
            f"Invalid {role}: {value!r}. dbt names must match ^[a-z][a-z0-9_]{{0,127}}$."
        )


def _validate_identifier(role: str, value: str) -> None:
    if not _IDENTIFIER_RE.match(value):
        raise ValidationError(
            f"Invalid {role}: {value!r}. Must match ^[A-Za-z_][A-Za-z0-9_]{{0,127}}$."
        )


# ──────────────────────────────────────────────────────────────────────
# Emitters
# ──────────────────────────────────────────────────────────────────────


def _write_dbt_project_yml(project: GoldProjectConfig, output_dir: Path) -> Path:
    payload: dict[str, Any] = {
        "name": project.project_name,
        "version": "1.0.0",
        "config-version": 2,
        "profile": project.project_name,
        "require-dbt-version": project.dbt_version_requirement,
        "model-paths": ["models"],
        "snapshot-paths": ["snapshots"],
        "test-paths": ["tests"],
        "macro-paths": ["macros"],
        "seed-paths": ["seeds"],
        "analysis-paths": ["analyses"],
        "target-path": "target",
        "clean-targets": ["dbt_packages", "target"],
        # Defaults for the vars sources.yml references — customers
        # override per-environment via `dbt run --vars '{...}'` or by
        # editing the generated dbt_project.yml. Defaults exist so
        # `dbt parse` succeeds without flags.
        "vars": {
            project.silver_catalog_var: project.silver_catalog_default,
            project.silver_schema_var: project.silver_schema_default,
        },
    }
    return _write_yaml(output_dir / "dbt_project.yml", payload)


def _write_profiles_yml(project: GoldProjectConfig, output_dir: Path) -> Path:
    """Emit a Databricks-adapter profile (design §4.4).

    All connection params route through ``env_var()`` so the generated
    file is safe to commit — secrets resolve at dbt-run time.
    """
    payload: dict[str, Any] = {
        project.project_name: {
            "target": project.profile_target,
            "outputs": {
                project.profile_target: {
                    "type": "databricks",
                    "host": "{{ env_var('DATABRICKS_HOST') }}",
                    "http_path": "{{ env_var('DATABRICKS_HTTP_PATH') }}",
                    "token": "{{ env_var('DATABRICKS_TOKEN') }}",
                    "catalog": "{{ env_var('GOLD_CATALOG') }}",
                    "schema": "{{ env_var('GOLD_SCHEMA') }}",
                    "threads": project.profile_threads,
                }
            },
        }
    }
    return _write_yaml(output_dir / "profiles.yml", payload)


def _write_sources_yml(project: GoldProjectConfig, output_dir: Path) -> Path:
    # ``loaded_at_field`` and ``freshness`` live under ``config:`` per
    # the dbt 1.11+ shape (PropertyMovedToConfigDeprecation otherwise).
    tables = [
        {
            "name": s.name,
            "description": s.description,
            "config": {
                "loaded_at_field": s.loaded_at_field,
                "freshness": {
                    "warn_after": {"count": s.freshness_warn_after_hours, "period": "hour"},
                    "error_after": {"count": s.freshness_error_after_hours, "period": "hour"},
                },
            },
        }
        for s in sorted(project.sources, key=lambda x: x.name)
    ]
    payload: dict[str, Any] = {
        "version": 2,
        "sources": [
            {
                "name": "silver",
                "database": f"{{{{ var('{project.silver_catalog_var}') }}}}",
                "schema": f"{{{{ var('{project.silver_schema_var}') }}}}",
                "tables": tables,
            }
        ],
    }
    sources_dir = output_dir / "models"
    sources_dir.mkdir(parents=True, exist_ok=True)
    return _write_yaml(sources_dir / "sources.yml", payload)


def _write_packages_yml(output_dir: Path) -> Path:
    """Emit packages.yml — dbt-utils is a standard Gold dependency.

    Used by ``generate_surrogate_key`` (SCD2-dim SK assignment) and a
    handful of other macros every Gold project ends up needing.
    Pinned to ``>=1.1.0`` (the version that supports Databricks
    natively).
    """
    payload: dict[str, Any] = {
        "packages": [{"package": "dbt-labs/dbt_utils", "version": ">=1.1.0"}],
    }
    return _write_yaml(output_dir / "packages.yml", payload)


def _write_gold_meta_macro(output_dir: Path) -> Path:
    """Emit ``macros/gold_meta.sql`` — per-record metadata column macro.

    Models call ``{{ gold_meta_cols() }}`` in their SELECT list to add
    ``_gold_run_id`` + ``_gold_loaded_at``. The default templated SELECT
    auto-includes it; custom_sql callers add it themselves (we don't
    auto-wrap their SQL because that would change its shape unexpectedly).

    ``_gold_run_id`` is sourced from the dbt var of the same name; the
    framework's runner sets this via ``dbt run --vars '{...}'``. When the
    var is unset (a customer running dbt directly), it defaults to
    ``'manual'`` so ad-hoc runs still produce well-formed rows.
    """
    macros_dir = output_dir / "macros"
    macros_dir.mkdir(parents=True, exist_ok=True)
    path = macros_dir / "gold_meta.sql"
    path.write_text(_GOLD_META_MACRO_BODY, encoding="utf-8")
    return path


_GOLD_META_MACRO_BODY = """\
{% macro gold_meta_cols() -%}
    '{{ var("gold_run_id", "manual") }}' AS _gold_run_id,
    CURRENT_TIMESTAMP() AS _gold_loaded_at
{%- endmacro %}
"""


def _write_model_sql(model: GoldModelConfig, output_dir: Path) -> Path:
    domain_dir = output_dir / "models" / "marts" / model.domain
    domain_dir.mkdir(parents=True, exist_ok=True)
    sql_path = domain_dir / f"{model.name}.sql"
    sql_path.write_text(_render_model_sql(model), encoding="utf-8")
    return sql_path


def _write_model_yml(model: GoldModelConfig, output_dir: Path) -> Path:
    domain_dir = output_dir / "models" / "marts" / model.domain
    domain_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "version": 2,
        "models": [
            {
                "name": model.name,
                "description": model.description,
                "columns": _model_column_specs(model),
            }
        ],
    }
    return _write_yaml(domain_dir / f"{model.name}.yml", payload)


def _write_snapshot_sql(snapshot: GoldSnapshotConfig, output_dir: Path) -> Path:
    snapshots_dir = output_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    sql_path = snapshots_dir / f"{snapshot.name}.sql"
    sql_path.write_text(_render_snapshot_sql(snapshot), encoding="utf-8")
    return sql_path


# ──────────────────────────────────────────────────────────────────────
# Rendering helpers
# ──────────────────────────────────────────────────────────────────────


def _render_model_sql(model: GoldModelConfig) -> str:
    config_block = _render_config_block(model)
    body = model.custom_sql.rstrip() if model.custom_sql else _render_default_select(model)
    return f"{config_block}\n\n{body}\n"


def _render_config_block(model: GoldModelConfig) -> str:
    """Emit ``{{ config(...) }}`` keyword-style for readability.

    Keys are alphabetised for deterministic output.
    """
    args: dict[str, Any] = {"materialized": model.materialization.value}
    if model.materialization == Materialization.INCREMENTAL:
        args["unique_key"] = (
            model.business_keys[0] if len(model.business_keys) == 1 else list(model.business_keys)
        )
        args["incremental_strategy"] = "merge"
        args["on_schema_change"] = "append_new_columns"

    rendered = ", ".join(f"{k}={_jinja_literal(v)}" for k, v in sorted(args.items()))
    return f"{{{{ config({rendered}) }}}}"


def _render_default_select(model: GoldModelConfig) -> str:
    # Validation guarantees len(silver_sources) == 1 when custom_sql is None.
    source_name = model.silver_sources[0]
    # ``src.*`` + ``{{ gold_meta_cols() }}`` — every Gold row carries
    # _gold_run_id + _gold_loaded_at automatically. Custom-SQL callers
    # add the macro themselves.
    select = (
        "SELECT\n"
        "    src.*,\n"
        "    {{ gold_meta_cols() }}\n"
        f"FROM {{{{ source('silver', '{source_name}') }}}} src"
    )
    if model.materialization != Materialization.INCREMENTAL:
        return select

    cursor = model.incremental_watermark_column
    lookback = model.incremental_lookback_days
    lookback_clause = f" - INTERVAL {lookback} DAYS" if lookback > 0 else ""
    # cursor is _IDENTIFIER_RE-validated; this string is a dbt template,
    # not a runtime query — bandit/ruff S608 is suppressed file-wide in
    # pyproject.toml since every f-string in this module is a template body.
    where_line = (
        f"  WHERE src.{cursor} > " f"(SELECT MAX({cursor}) FROM {{{{ this }}}}){lookback_clause}\n"
    )
    incremental_block = "{% if is_incremental() %}\n" + where_line + "{% endif %}"
    return f"{select}\n\n{incremental_block}"


def _render_snapshot_sql(snapshot: GoldSnapshotConfig) -> str:
    """Emit a dbt ``{% snapshot %}`` block.

    Body defaults to ``SELECT * FROM {{ ref('<source_model>') }}``;
    ``custom_sql`` overrides the SELECT but stays inside the block.
    """
    args: dict[str, Any] = {
        "target_schema": snapshot.target_schema,
        "unique_key": (
            snapshot.unique_key
            if isinstance(snapshot.unique_key, str)
            else list(snapshot.unique_key)
        ),
        "strategy": snapshot.strategy.value,
    }
    if snapshot.strategy == SnapshotStrategy.TIMESTAMP:
        # Validation guarantees presence.
        args["updated_at"] = snapshot.updated_at_column
    else:
        args["check_cols"] = list(snapshot.check_cols or ())

    config_args = ",\n        ".join(f"{k}={_jinja_literal(v)}" for k, v in sorted(args.items()))
    body = (
        snapshot.custom_sql.rstrip()
        if snapshot.custom_sql
        else f"SELECT * FROM {{{{ ref('{snapshot.source_model}') }}}}"
    )
    return (
        f"{{% snapshot {snapshot.name} %}}\n"
        f"\n"
        f"{{{{\n"
        f"    config(\n"
        f"        {config_args},\n"
        f"    )\n"
        f"}}}}\n"
        f"\n"
        f"{body}\n"
        f"\n"
        f"{{% endsnapshot %}}\n"
    )


def _jinja_literal(value: Any) -> str:
    """Render a Python value as the equivalent Jinja literal.

    Strings get single-quoted; lists become Python-list literals (dbt
    accepts both ``unique_key='id'`` and ``unique_key=['a', 'b']``).
    """
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, list):
        return "[" + ", ".join(_jinja_literal(item) for item in value) + "]"
    return repr(value)


def _model_column_specs(model: GoldModelConfig) -> list[dict[str, Any]]:
    """Schema-test entries: ``not_null`` on every key; ``unique`` on
    the first key (compound uniqueness needs ``dbt_utils.unique_combination_of_columns``,
    deferred to the tests-dedupe phase). The framework's per-record
    metadata cols are declared with descriptions so dbt docs picks
    them up; ``not_null`` on each since the macro guarantees them."""
    columns: list[dict[str, Any]] = []
    for i, key in enumerate(model.business_keys):
        tests: list[str] = ["not_null"]
        if i == 0 and len(model.business_keys) == 1:
            tests.append("unique")
        columns.append({"name": key, "tests": sorted(tests)})
    columns.extend(_GOLD_META_COLUMN_SPECS)
    return columns


_GOLD_META_COLUMN_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "_gold_run_id",
        "description": (
            "Framework Gold run identifier — sourced from the dbt var "
            "'gold_run_id' set by 'lake-ingest gold deploy'. Defaults to "
            "'manual' when run directly via dbt."
        ),
        "tests": ["not_null"],
    },
    {
        "name": "_gold_loaded_at",
        "description": "Timestamp this row was materialized at the Gold layer.",
        "tests": ["not_null"],
    },
)


def _write_yaml(path: Path, payload: dict[str, Any]) -> Path:
    """Dump YAML with sorted keys + stable formatting for golden-diff CI."""
    text = yaml.safe_dump(
        payload,
        sort_keys=True,
        default_flow_style=False,
        indent=2,
        width=120,
        allow_unicode=True,
    )
    path.write_text(text, encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────────────
# YAML config loader
# ──────────────────────────────────────────────────────────────────────


def load_project_config(path: Path) -> GoldProjectConfig:
    """Hydrate a :class:`GoldProjectConfig` from a YAML file.

    Phase 1's wiring path: customer-authored YAML in their
    ``<customer>-ingestion`` repo. Later phases hydrate from
    ``meta.dataset_config[layer='gold']`` directly.

    Schema (top-level keys mirror the dataclass field names):

    .. code-block:: yaml

        project_name: acme_gold
        sources:
          - {name: orders, description: "..."}
        models:
          - name: fct_orders
            domain: finance
            silver_source: orders
            business_keys: [order_id]
            materialization: incremental
            incremental_watermark_column: updated_at

    Unknown keys raise ``ValidationError`` so typos surface early.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValidationError(
            f"Gold config {path} must be a YAML mapping; got {type(raw).__name__}."
        )
    return _hydrate_project(raw, path)


_PROJECT_KEYS: frozenset[str] = frozenset(
    {
        "project_name",
        "sources",
        "models",
        "snapshots",
        "silver_catalog_var",
        "silver_schema_var",
        "profile_target",
        "profile_threads",
        "dbt_version_requirement",
    }
)
_SNAPSHOT_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "target_schema",
        "source_model",
        "unique_key",
        "strategy",
        "updated_at_column",
        "check_cols",
        "description",
        "custom_sql",
    }
)
_SOURCE_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "description",
        "loaded_at_field",
        "freshness_warn_after_hours",
        "freshness_error_after_hours",
    }
)
_MODEL_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "domain",
        "silver_sources",
        "business_keys",
        "strategy",
        "materialization",
        "mart_shape",
        "description",
        "incremental_watermark_column",
        "incremental_lookback_days",
        "custom_sql",
    }
)


def _hydrate_project(raw: dict[str, Any], path: Path) -> GoldProjectConfig:
    _reject_unknown_keys(raw, _PROJECT_KEYS, "project")

    project_name = _required_str(raw, "project_name", path)
    sources_raw = _required_list(raw, "sources", path)
    models_raw = _required_list(raw, "models", path)

    sources = tuple(_hydrate_source(s) for s in sources_raw)
    models = tuple(_hydrate_model(m) for m in models_raw)

    snapshots: tuple[GoldSnapshotConfig, ...] = ()
    if "snapshots" in raw:
        if not isinstance(raw["snapshots"], list):
            raise ValidationError(
                f"Gold config {path}: 'snapshots' must be a list (got "
                f"{type(raw['snapshots']).__name__})."
            )
        snapshots = tuple(_hydrate_snapshot(s) for s in raw["snapshots"])

    kwargs: dict[str, Any] = {
        "project_name": project_name,
        "sources": sources,
        "models": models,
        "snapshots": snapshots,
    }
    for opt in (
        "silver_catalog_var",
        "silver_schema_var",
        "profile_target",
        "dbt_version_requirement",
    ):
        if opt in raw:
            kwargs[opt] = raw[opt]
    if "profile_threads" in raw:
        kwargs["profile_threads"] = int(raw["profile_threads"])
    return GoldProjectConfig(**kwargs)


def _hydrate_source(raw: Any) -> GoldSource:
    if not isinstance(raw, dict):
        raise ValidationError(f"Each source entry must be a mapping; got {type(raw).__name__}.")
    _reject_unknown_keys(raw, _SOURCE_KEYS, "source")
    return GoldSource(
        name=str(raw["name"]),
        description=str(raw.get("description", "")),
        loaded_at_field=str(raw.get("loaded_at_field", "_silver_loaded_at")),
        freshness_warn_after_hours=int(raw.get("freshness_warn_after_hours", 6)),
        freshness_error_after_hours=int(raw.get("freshness_error_after_hours", 24)),
    )


def _hydrate_model(raw: Any) -> GoldModelConfig:
    if not isinstance(raw, dict):
        raise ValidationError(f"Each model entry must be a mapping; got {type(raw).__name__}.")
    _reject_unknown_keys(raw, _MODEL_KEYS, "model")

    if "silver_sources" not in raw:
        raise ValidationError(
            f"Model {raw.get('name', '<unnamed>')!r} is missing required key 'silver_sources'."
        )
    if not isinstance(raw["silver_sources"], list):
        raise ValidationError(
            f"Model {raw.get('name', '<unnamed>')!r}: silver_sources must be a list."
        )

    return GoldModelConfig(
        name=str(raw["name"]),
        domain=str(raw["domain"]),
        silver_sources=tuple(str(s) for s in raw["silver_sources"]),
        business_keys=tuple(str(k) for k in raw["business_keys"]),
        strategy=GoldStrategy(raw.get("strategy", GoldStrategy.DBT.value)),
        materialization=Materialization(
            raw.get("materialization", Materialization.INCREMENTAL.value)
        ),
        mart_shape=MartShape(raw["mart_shape"]) if raw.get("mart_shape") else None,
        description=str(raw.get("description", "")),
        incremental_watermark_column=raw.get("incremental_watermark_column"),
        incremental_lookback_days=int(raw.get("incremental_lookback_days", 3)),
        custom_sql=raw.get("custom_sql"),
    )


def _hydrate_snapshot(raw: Any) -> GoldSnapshotConfig:
    if not isinstance(raw, dict):
        raise ValidationError(f"Each snapshot entry must be a mapping; got {type(raw).__name__}.")
    _reject_unknown_keys(raw, _SNAPSHOT_KEYS, "snapshot")

    for required in ("name", "target_schema", "source_model", "unique_key", "strategy"):
        if required not in raw:
            raise ValidationError(
                f"Snapshot {raw.get('name', '<unnamed>')!r} is missing required key "
                f"{required!r}."
            )

    raw_unique = raw["unique_key"]
    unique_key: str | tuple[str, ...]
    if isinstance(raw_unique, str):
        unique_key = raw_unique
    elif isinstance(raw_unique, list):
        unique_key = tuple(str(k) for k in raw_unique)
    else:
        raise ValidationError(
            f"Snapshot {raw['name']!r}: unique_key must be a string or list, got "
            f"{type(raw_unique).__name__}."
        )

    check_cols_raw = raw.get("check_cols")
    check_cols: tuple[str, ...] | None = None
    if check_cols_raw is not None:
        if not isinstance(check_cols_raw, list):
            raise ValidationError(f"Snapshot {raw['name']!r}: check_cols must be a list.")
        check_cols = tuple(str(c) for c in check_cols_raw)

    return GoldSnapshotConfig(
        name=str(raw["name"]),
        target_schema=str(raw["target_schema"]),
        source_model=str(raw["source_model"]),
        unique_key=unique_key,
        strategy=SnapshotStrategy(raw["strategy"]),
        updated_at_column=raw.get("updated_at_column"),
        check_cols=check_cols,
        description=str(raw.get("description", "")),
        custom_sql=raw.get("custom_sql"),
    )


def _reject_unknown_keys(raw: dict[str, Any], allowed: frozenset[str], role: str) -> None:
    unknown = set(raw) - allowed
    if unknown:
        raise ValidationError(
            f"Unknown {role} key(s): {sorted(unknown)}. Allowed: {sorted(allowed)}."
        )


def _required_str(raw: dict[str, Any], key: str, path: Path) -> str:
    if key not in raw:
        raise ValidationError(f"Gold config {path} is missing required key {key!r}.")
    return str(raw[key])


def _required_list(raw: dict[str, Any], key: str, path: Path) -> list[Any]:
    if key not in raw:
        raise ValidationError(f"Gold config {path} is missing required key {key!r}.")
    if not isinstance(raw[key], list):
        raise ValidationError(f"Gold config {path}: {key!r} must be a list.")
    return list(raw[key])
