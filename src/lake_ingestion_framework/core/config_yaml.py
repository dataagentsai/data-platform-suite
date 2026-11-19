"""YAML schemas for customer-repo source/dataset config files.

The customer repo holds three trees of YAML that get synced into the
metadata DB by ``lake-ingest config sync``:

- ``config/platform_config/<env>.yaml``  → ``platform_config`` rows
- ``config/sources/<source>.yaml``       → ``source_master`` + ``source_config``
- ``config/datasets/<source>/<ds>.yaml`` → ``dataset_master`` + ``dataset_config``
                                            + optional ``dataset_config_columns``

This module defines Pydantic models for parsing those files. Per-key
validation (key-name allow-list, value-type coercion) happens later, in
the repository — the YAML schema only enforces *shape*, not the KV
contents themselves. That separation lets the same registry govern
both YAML edits and any future direct-DB writes.

See ``docs/customer_profile.md`` and ``docs/repo_structure.md``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import SourceEnv

# ──────────────────────────────────────────────────────────────────────
# platform_config/<env>.yaml — flat KV map
# ──────────────────────────────────────────────────────────────────────


_PROCESSOR_GROUPS = {
    "hygiene",
    "type_safety",
    "case_normalize",
    "timestamp_normalize",
    "standardization",
    "pii",
    "dedup",
    "provenance",
}
_PROCESSOR_GROUP_MODES = {"enabled", "disabled", "opt_in", "dataset_decides"}
_PII_ACTIONS = {"pass_through", "hash", "mask", "tokenize", "redact"}
_DEDUP_MODES = {
    "none",
    "exact",
    "business_key_first",
    "business_key_latest",
    "flag",
}
_CAST_SAFETIES = {"strict", "try_cast"}


class PiiPolicyEntry(BaseModel):
    """One ``pii_policy:`` entry in platform_config.

    ``salt_kv_ref`` is a Key Vault reference (``kv://<vault>/<secret>``)
    or a literal salt for non-prod envs. ``redact_value`` defaults to
    ``[redacted]`` when used; ``mask_visible`` defaults to 4 (last-N
    chars kept by ``mask`` action).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    pii_class: str = Field(..., min_length=1)
    action: str
    salt_kv_ref: str | None = None
    redact_value: str | None = None
    mask_visible: int = Field(default=4, ge=0, le=64)

    @field_validator("action")
    @classmethod
    def _check_action(cls, v: str) -> str:
        if v not in _PII_ACTIONS:
            raise ValueError(f"action must be one of {sorted(_PII_ACTIONS)}; got {v!r}")
        return v


class PlatformConfigFile(BaseModel):
    """Structured view of ``config/platform_config/<env>.yaml``.

    Three sections are extracted from the flat YAML:

    * ``kv``               — all top-level keys not in the special
                              sections. Synced to ``meta.platform_config``.
    * ``processor_groups`` — group → mode mapping. Expanded to
                              ``processor_groups.<name>`` KV keys.
    * ``pii_policy``       — list of :class:`PiiPolicyEntry`. Synced
                              to ``meta.pii_policy`` keyed by
                              ``(pii_class, env)``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    kv: dict[str, str] = Field(default_factory=dict)
    processor_groups: dict[str, str] = Field(default_factory=dict)
    pii_policy: tuple[PiiPolicyEntry, ...] = ()

    @field_validator("processor_groups")
    @classmethod
    def _check_groups(cls, v: dict[str, str]) -> dict[str, str]:
        for k, mode in v.items():
            if k not in _PROCESSOR_GROUPS:
                raise ValueError(
                    f"processor_groups: unknown group {k!r}; " f"valid: {sorted(_PROCESSOR_GROUPS)}"
                )
            if mode not in _PROCESSOR_GROUP_MODES:
                raise ValueError(
                    f"processor_groups[{k!r}]: invalid mode {mode!r}; "
                    f"valid: {sorted(_PROCESSOR_GROUP_MODES)}"
                )
        return v


def load_platform_config_yaml(path: Path) -> PlatformConfigFile:
    """Load a platform_config YAML file and return a structured view.

    Top-level entries split into three buckets:

    * ``processor_groups:`` block → :attr:`PlatformConfigFile.processor_groups`
    * ``pii_policy:`` list → :attr:`PlatformConfigFile.pii_policy`
    * everything else → :attr:`PlatformConfigFile.kv` (the legacy
      flat-KV bucket; values stringified the same way as before)
    """
    raw = _read_yaml_mapping(path)
    pg_raw = raw.pop("processor_groups", None) or {}
    pii_raw = raw.pop("pii_policy", None) or []

    if not isinstance(pg_raw, dict):
        raise ValidationError(
            f"{path}: processor_groups must be a mapping; got {type(pg_raw).__name__}."
        )
    if not isinstance(pii_raw, list):
        raise ValidationError(f"{path}: pii_policy must be a list; got {type(pii_raw).__name__}.")

    kv: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            raise ValidationError(f"{path}: keys must be strings; got {type(key).__name__}.")
        kv[key] = _stringify(value)

    try:
        return PlatformConfigFile(
            kv=kv,
            processor_groups={str(k): str(v) for k, v in pg_raw.items()},
            pii_policy=tuple(PiiPolicyEntry.model_validate(e) for e in pii_raw),
        )
    except Exception as exc:
        raise ValidationError(f"{path}: invalid platform_config: {exc}") from exc


# ──────────────────────────────────────────────────────────────────────
# sources/<source>.yaml
# ──────────────────────────────────────────────────────────────────────


class SourceConfigFile(BaseModel):
    """Schema for ``config/sources/<source_name>.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_name: str = Field(..., min_length=1, max_length=128)
    source_env: str = Field(default="prod")
    source_type: SourceType
    is_enabled: bool = True
    config: dict[str, str] = Field(default_factory=dict)

    @field_validator("source_env")
    @classmethod
    def _validate_source_env(cls, v: str) -> str:
        SourceEnv.parse(v)
        return v

    @field_validator("config", mode="before")
    @classmethod
    def _stringify_config_values(cls, v: object) -> dict[str, str]:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError(f"`config` must be a mapping; got {type(v).__name__}.")
        return {str(k): _stringify(val) for k, val in v.items()}


def load_source_yaml(path: Path) -> SourceConfigFile:
    data = _read_yaml_mapping(path)
    try:
        return SourceConfigFile.model_validate(data)
    except Exception as exc:
        raise ValidationError(f"{path}: invalid source config: {exc}") from exc


# ──────────────────────────────────────────────────────────────────────
# datasets/<source>/<dataset>.yaml
# ──────────────────────────────────────────────────────────────────────


class DatasetColumnEntry(BaseModel):
    """One entry under ``columns:`` in a dataset YAML.

    Wave 1-4 of standard processors added per-column transform
    metadata. All new fields default to None / ``strict`` / empty so
    columns authored before the processor track keep working.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    column_name: str = Field(..., min_length=1, max_length=128)
    data_type: str = Field(..., min_length=1)
    is_nullable: bool = True
    ordinal_position: int = Field(..., ge=0)
    is_primary_key: bool = False
    is_excluded: bool = False
    pii_class: str = "none"
    """Free-form string keyed against ``meta.pii_policy.pii_class``.

    The framework's :class:`PiiClass` enum (``none / direct / quasi /
    sensitive``) is the sensitivity-tier taxonomy emitted by
    ``discover``. The PII processor (Wave 2) supports fine-grained
    classes too (``email`` / ``ssn`` / ``phone`` / ...). Pii_policy
    can be written for either flavour; lookup is exact-match on
    pii_class string.
    """
    source_path: str | None = None
    # Wave 1-4 — column-transform metadata
    silver_column_name: str | None = None
    silver_data_type: str | None = None
    cast_safety: str = "strict"
    null_replacement: str | None = None
    transforms: tuple[str, ...] = ()
    skip_groups: tuple[str, ...] = ()
    skip_processors: tuple[str, ...] = ()
    column_role: str | None = None

    @field_validator("cast_safety")
    @classmethod
    def _check_cast_safety(cls, v: str) -> str:
        if v not in _CAST_SAFETIES:
            raise ValueError(f"cast_safety must be one of {sorted(_CAST_SAFETIES)}; got {v!r}")
        return v

    @field_validator("skip_groups")
    @classmethod
    def _check_skip_groups(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        for g in v:
            if g not in _PROCESSOR_GROUPS:
                raise ValueError(
                    f"skip_groups: unknown group {g!r}; valid: {sorted(_PROCESSOR_GROUPS)}"
                )
        return v


class DatasetDedupConfig(BaseModel):
    """``dedup:`` block in a dataset YAML — Wave 3 row-level dedup."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: str = "none"
    business_keys: tuple[str, ...] = ()
    order_by: str | None = None

    @field_validator("mode")
    @classmethod
    def _check_mode(cls, v: str) -> str:
        if v not in _DEDUP_MODES:
            raise ValueError(f"dedup.mode must be one of {sorted(_DEDUP_MODES)}; got {v!r}")
        return v


class DatasetConfigFile(BaseModel):
    """Schema for ``config/datasets/<source_name>/<dataset_name>.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_name: str = Field(..., min_length=1, max_length=128)
    source_env: str = Field(default="prod")
    dataset_name: str = Field(..., min_length=1, max_length=200)
    is_enabled: bool = False
    config: dict[str, str] = Field(default_factory=dict)
    columns: tuple[DatasetColumnEntry, ...] = ()
    # Wave 1-4 — per-dataset processor + dedup overrides
    processor_groups: dict[str, str] = Field(default_factory=dict)
    """Per-dataset overrides of platform-level group enable/disable.
    Same valid modes as platform_config.processor_groups."""
    dedup: DatasetDedupConfig | None = None

    @field_validator("processor_groups")
    @classmethod
    def _check_groups(cls, v: dict[str, str]) -> dict[str, str]:
        for k, mode in v.items():
            if k not in _PROCESSOR_GROUPS:
                raise ValueError(
                    f"processor_groups: unknown group {k!r}; " f"valid: {sorted(_PROCESSOR_GROUPS)}"
                )
            if mode not in _PROCESSOR_GROUP_MODES:
                raise ValueError(
                    f"processor_groups[{k!r}]: invalid mode {mode!r}; "
                    f"valid: {sorted(_PROCESSOR_GROUP_MODES)}"
                )
        return v

    @field_validator("source_env")
    @classmethod
    def _validate_source_env(cls, v: str) -> str:
        SourceEnv.parse(v)
        return v

    @field_validator("config", mode="before")
    @classmethod
    def _stringify_config_values(cls, v: object) -> dict[str, str]:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError(f"`config` must be a mapping; got {type(v).__name__}.")
        return {str(k): _stringify(val) for k, val in v.items()}

    @field_validator("columns")
    @classmethod
    def _unique_column_names(
        cls, v: tuple[DatasetColumnEntry, ...]
    ) -> tuple[DatasetColumnEntry, ...]:
        names = [c.column_name for c in v]
        if len(names) != len(set(names)):
            duplicates = sorted({n for n in names if names.count(n) > 1})
            raise ValueError(f"Duplicate column_name(s): {duplicates}")
        return v


def load_dataset_yaml(path: Path) -> DatasetConfigFile:
    data = _read_yaml_mapping(path)
    try:
        return DatasetConfigFile.model_validate(data)
    except Exception as exc:
        raise ValidationError(f"{path}: invalid dataset config: {exc}") from exc


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValidationError(f"Not a file: {path}")
    try:
        with path.open("r", encoding="utf-8") as fp:
            data = yaml.safe_load(fp)
    except yaml.YAMLError as exc:
        raise ValidationError(f"{path}: YAML parse error: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValidationError(
            f"{path}: top-level YAML must be a mapping; got {type(data).__name__}."
        )
    return data


def _stringify(value: object) -> str:
    """Coerce a YAML-parsed scalar into the wire string the DB stores.

    Booleans → ``true`` / ``false`` (lowercase, registry-accepted).
    None → empty string (registry rejects empty for required keys).
    Lists / dicts → JSON-encoded so a ``KeyType.JSON`` key round-trips.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    if isinstance(value, list | dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


__all__ = [
    "DatasetColumnEntry",
    "DatasetConfigFile",
    "SourceConfigFile",
    "load_dataset_yaml",
    "load_platform_config_yaml",
    "load_source_yaml",
]
