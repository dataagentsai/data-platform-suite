"""Doctor probes — health checks against a deployed environment.

A probe is a small, named, side-effect-free function that returns a
``ProbeResult``. Probes don't raise on a *negative* finding; they
return ``status=fail``. They only raise if the probe machinery itself
breaks (programming error).

Source-connectivity probes belong to source connectors (under each
source-type extra) and aren't loaded here. The probes in this module
are realm-agnostic: framework version, config schema, metadata DB
schema. See ``docs/operability.md`` → ``lake-ingest doctor``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from lake_ingestion_framework._version import __version__
from lake_ingestion_framework.core.config_yaml import (
    load_dataset_yaml,
    load_platform_config_yaml,
    load_source_yaml,
)
from lake_ingestion_framework.core.exceptions import (
    LakeIngestError,
    MetadataConnectionError,
    MetadataError,
)
from lake_ingestion_framework.core.metadata.repository import MetadataRepository


class ProbeStatus(StrEnum):
    """Outcome of one probe."""

    # The "pass" string is a probe-outcome label, not a credential.
    PASS = "pass"  # noqa: S105  # nosec B105
    WARN = "warn"
    FAIL = "fail"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ProbeResult:
    """One probe's verdict."""

    name: str
    status: ProbeStatus
    summary: str
    details: tuple[str, ...] = ()


# ──────────────────────────────────────────────────────────────────────
# Probes
# ──────────────────────────────────────────────────────────────────────


def probe_framework_version() -> ProbeResult:
    """Trivial probe: framework can import + report its version.

    Confirms the install is sane before any other probe runs.
    """
    return ProbeResult(
        name="framework-version",
        status=ProbeStatus.PASS,
        summary=f"lake-ingest {__version__}",
    )


def probe_config_schema(*, customer_repo: Path, env: str) -> ProbeResult:
    """Walk ``config/`` and validate each YAML against its Pydantic schema.

    Doesn't talk to the DB — purely syntactic. ``fail`` on any parse
    failure; ``warn`` if the customer repo has no config dir at all
    (init wasn't run yet); ``pass`` otherwise.
    """
    config_dir = customer_repo / "config"
    if not config_dir.is_dir():
        return ProbeResult(
            name="config-schema",
            status=ProbeStatus.WARN,
            summary=f"No config/ directory under {customer_repo}",
            details=("Run `lake-ingest init` first.",),
        )

    errors: list[str] = []
    counts = {"platform_config": 0, "sources": 0, "datasets": 0}

    pc_path = config_dir / "platform_config" / f"{env}.yaml"
    if pc_path.is_file():
        try:
            load_platform_config_yaml(pc_path)
            counts["platform_config"] = 1
        except LakeIngestError as exc:
            errors.append(f"{pc_path}: {exc}")
    else:
        errors.append(f"Missing platform_config for env={env!r}: {pc_path}")

    sources_dir = config_dir / "sources"
    if sources_dir.is_dir():
        for path in sorted(sources_dir.glob("*.yaml")):
            try:
                load_source_yaml(path)
                counts["sources"] += 1
            except LakeIngestError as exc:
                errors.append(str(exc))

    datasets_dir = config_dir / "datasets"
    if datasets_dir.is_dir():
        for path in sorted(datasets_dir.rglob("*.yaml")):
            try:
                load_dataset_yaml(path)
                counts["datasets"] += 1
            except LakeIngestError as exc:
                errors.append(str(exc))

    if errors:
        return ProbeResult(
            name="config-schema",
            status=ProbeStatus.FAIL,
            summary=f"{len(errors)} config file(s) failed validation",
            details=tuple(errors),
        )
    return ProbeResult(
        name="config-schema",
        status=ProbeStatus.PASS,
        summary=(
            f"{counts['platform_config']} platform_config, "
            f"{counts['sources']} sources, {counts['datasets']} datasets"
        ),
    )


def probe_metadata_db(repo: MetadataRepository) -> ProbeResult:
    """Connect to the metadata DB and read ``metadata_schema_version``.

    ``fail`` on connection error or missing version row; ``pass`` with
    the version on success.
    """
    try:
        version = repo.schema_version()
    except MetadataConnectionError as exc:
        return ProbeResult(
            name="metadata-db",
            status=ProbeStatus.FAIL,
            summary="Cannot connect to metadata DB",
            details=(str(exc),),
        )
    except MetadataError as exc:
        return ProbeResult(
            name="metadata-db",
            status=ProbeStatus.FAIL,
            summary="Metadata DB schema not bootstrapped",
            details=(str(exc), "Run `lake-ingest config sync --apply` to bootstrap."),
        )

    return ProbeResult(
        name="metadata-db",
        status=ProbeStatus.PASS,
        summary=f"connected, schema version {version}",
    )


__all__ = [
    "ProbeResult",
    "ProbeStatus",
    "probe_config_schema",
    "probe_framework_version",
    "probe_metadata_db",
]
