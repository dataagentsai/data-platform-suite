"""Customer profile YAML parser + validator.

The profile is a single YAML file at the customer repo root that
declares everything tailored about the deployment: identity,
environments, source types, feature toggles, constraints, compliance.
``lake-ingest init`` reads it; ``lake-ingest deploy`` enforces it.

See ``docs/customer_profile.md`` for the schema reference.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import SourceEnv

_CUSTOMER_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9-]{1,49}$")


class CustomerIdentity(BaseModel):
    """The ``customer:`` block."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(..., min_length=2, max_length=50)
    display_name: str = Field(..., min_length=1)
    primary_contact: str | None = None
    cost_center: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not _CUSTOMER_NAME_PATTERN.match(v):
            raise ValueError(
                f"customer.name {v!r} must match ^[a-z][a-z0-9-]{{1,49}}$ "
                "(lowercase, digits, hyphens; starts with letter)."
            )
        return v


class EnvironmentSpec(BaseModel):
    """One entry from the ``environments:`` block."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(..., min_length=2, max_length=20)
    storage_account: str = Field(..., min_length=3, max_length=24)
    container: str = Field(default="lake")
    metadata_db_server: str
    metadata_db_database: str
    region: str

    @field_validator("name")
    @classmethod
    def _validate_env_name(cls, v: str) -> str:
        # Reuse SourceEnv shape — same constraint
        SourceEnv.parse(v)
        return v


class SourceSpec(BaseModel):
    """One entry from the ``sources:`` block."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_type: SourceType
    connection_patterns: tuple[str, ...] = Field(..., min_length=1)
    requires_shir: bool = False


class FeaturesBlock(BaseModel):
    """Feature toggles. All optional; defaults documented in
    ``docs/customer_profile.md``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    segmented_loads: bool = True
    flattening: bool = False
    document_dataset_type: bool = False
    cross_account_files: bool = False
    rest_api_generic: bool = False
    saas_apis: bool = False
    cloud_dw: bool = False
    sap_hana: bool = False
    heartbeat_pipeline: bool = True
    auto_retry_on_partial: bool = False
    pii_classification: bool = True


class ConstraintsBlock(BaseModel):
    """Hard policy gates. Deploy validation rejects configs that violate."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    no_azure_functions: bool = False
    no_databricks: bool = False
    msi_only: bool = False
    private_endpoint_required: bool = True
    cmk_required: bool = False
    egress_to_internet: bool = True
    shir_only_for_onprem: bool = True
    no_public_blob_access: bool = True


class ComplianceBlock(BaseModel):
    """Compliance posture; framework enforces what it can."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    pii_required: bool = True
    data_residency: str | None = None
    retention_minimum_days: int = 0
    audit_log_retention_years: int = 7


class TrimBlock(BaseModel):
    """Slim-trim preferences for what gets included in the customer repo."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    include_runbooks: bool = True
    include_test_doctor: bool = True
    include_heartbeat_test: bool = True
    include_examples: bool = True


class CustomerProfile(BaseModel):
    """Top-level customer profile. The full YAML schema in one model."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    customer: CustomerIdentity
    environments: tuple[EnvironmentSpec, ...] = Field(..., min_length=1)
    sources: tuple[SourceSpec, ...] = Field(..., min_length=1)
    features: FeaturesBlock = FeaturesBlock()
    constraints: ConstraintsBlock = ConstraintsBlock()
    compliance: ComplianceBlock = ComplianceBlock()
    trim: TrimBlock = TrimBlock()

    @field_validator("environments")
    @classmethod
    def _unique_env_names(cls, v: tuple[EnvironmentSpec, ...]) -> tuple[EnvironmentSpec, ...]:
        names = [e.name for e in v]
        if len(names) != len(set(names)):
            duplicates = sorted({n for n in names if names.count(n) > 1})
            raise ValueError(f"Duplicate environment names: {duplicates}")
        return v

    @field_validator("sources")
    @classmethod
    def _unique_source_types_unless_repeated(
        cls,
        v: tuple[SourceSpec, ...],
    ) -> tuple[SourceSpec, ...]:
        # We allow the same source_type to appear if connection_patterns
        # differ — but warn if exact duplicates.
        seen: set[tuple[SourceType, tuple[str, ...]]] = set()
        for s in v:
            key = (s.source_type, s.connection_patterns)
            if key in seen:
                raise ValueError(
                    f"Duplicate source spec: {s.source_type} with "
                    f"connection_patterns={list(s.connection_patterns)}."
                )
            seen.add(key)
        return v


def load_profile(path: Path) -> CustomerProfile:
    """Read a YAML file and return a validated ``CustomerProfile``.

    Raises ``ValidationError`` on parse / validation failure.
    """
    if not path.exists():
        raise ValidationError(f"Profile file not found: {path}")
    if not path.is_file():
        raise ValidationError(f"Profile path is not a file: {path}")

    try:
        with path.open("r", encoding="utf-8") as fp:
            data: Any = yaml.safe_load(fp)
    except yaml.YAMLError as exc:
        raise ValidationError(f"Profile YAML invalid: {exc}") from exc

    if not isinstance(data, dict):
        raise ValidationError(
            f"Profile must be a YAML mapping at the root; got {type(data).__name__}."
        )

    try:
        return CustomerProfile.model_validate(data)
    except Exception as exc:
        # Pydantic raises pydantic.ValidationError; we wrap in our own
        # to keep the boundary consistent.
        raise ValidationError(f"Profile invalid: {exc}") from exc


__all__ = [
    "ComplianceBlock",
    "ConstraintsBlock",
    "CustomerIdentity",
    "CustomerProfile",
    "EnvironmentSpec",
    "FeaturesBlock",
    "SourceSpec",
    "TrimBlock",
    "load_profile",
]
