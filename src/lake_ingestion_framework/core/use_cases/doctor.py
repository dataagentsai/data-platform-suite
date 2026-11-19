"""``lake-ingest doctor`` — health-probe a deployed environment.

The skeleton runs three realm-agnostic probes: framework version,
config schema, metadata DB. Future realms add their own probes (Key
Vault reachability, RBAC matrix, ADLS write probe, source connectivity)
by extending this list at the composition root.

See ``docs/operability.md`` → ``lake-ingest doctor``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lake_ingestion_framework.core.metadata.repository import MetadataRepository
from lake_ingestion_framework.core.probes import (
    ProbeResult,
    ProbeStatus,
    probe_config_schema,
    probe_framework_version,
    probe_metadata_db,
)


@dataclass(frozen=True)
class DoctorReport:
    """Aggregated output of all probes."""

    results: tuple[ProbeResult, ...]

    @property
    def overall(self) -> ProbeStatus:
        """Worst status across all probes."""
        statuses = {r.status for r in self.results}
        if ProbeStatus.FAIL in statuses:
            return ProbeStatus.FAIL
        if ProbeStatus.WARN in statuses:
            return ProbeStatus.WARN
        return ProbeStatus.PASS


def run_doctor(
    *,
    customer_repo: Path,
    env: str,
    metadata_repo: MetadataRepository,
) -> DoctorReport:
    """Run all configured probes and return an aggregated report.

    Order matters for readability: cheap/local probes first; metadata
    DB last (most likely to fail in fresh environments).
    """
    results: list[ProbeResult] = [
        probe_framework_version(),
        probe_config_schema(customer_repo=customer_repo, env=env),
        probe_metadata_db(metadata_repo),
    ]
    return DoctorReport(results=tuple(results))


__all__ = ["DoctorReport", "run_doctor"]
