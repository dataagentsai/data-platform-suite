"""``lake-ingest deploy`` — generate realm-native artifacts from metadata.

Plan/apply pattern matching ``init`` / ``config_sync`` / ``discover``.
Plan walks the metadata DB and assembles the artifacts via the realm
generator; apply writes them under ``<customer-repo>/generated/<realm>/<env>/``.

``--include-procs`` (Phase 6b-1) additionally emits the realm's
runtime SQL artifacts under ``runtime_sql/`` — the metadata DB views,
log tables, and stored procedures the ADF runtime depends on. The
SQL files ship as canonical T-SQL; deploy writes them so a DBA can
apply them with sqlcmd. See
``docs/realms/adf-adls-runtime-sql.md``.

MVP supports realm 1 (`adf-adls`) only. Other realms
land per ``docs/realms_strategy.md``.

Per CLAUDE.md "Idempotent deploy": re-running against the same
metadata produces byte-identical output. The realm generator + the
JSON serializer enforce this; this use case wraps them in a
filesystem layer that respects the same contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import MetadataRepository

# ──────────────────────────────────────────────────────────────────────
# Plan / result dataclasses
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DeployPlannedFile:
    """One file to write at deploy time."""

    target_path: Path
    kind: str  # "linkedService" / "dataset" / "pipeline" / "trigger"
    name: str
    rendered: str


@dataclass(frozen=True)
class DeployPlan:
    """Full deploy plan: realm + env + the file list it would write."""

    realm: str
    env: str
    target_dir: Path
    files: tuple[DeployPlannedFile, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class DeployResult:
    """Counts written by ``apply_deploy``."""

    files_written: int
    by_kind: dict[str, int]


# ──────────────────────────────────────────────────────────────────────
# Plan
# ──────────────────────────────────────────────────────────────────────


_SUPPORTED_REALMS = frozenset({"adf-adls"})


def plan_deploy(
    *,
    realm: str,
    env: str,
    repo: MetadataRepository,
    customer_repo: Path,
    include_procs: bool = False,
) -> DeployPlan:
    """Walk the metadata DB and produce a list of files to write.

    Pure read against the metadata DB. No filesystem writes.

    When ``include_procs`` is True, additionally emits the realm's
    runtime SQL artifacts (views, tables, procs) under
    ``runtime_sql/<artifact_kind>/<name>.sql``. The ADF JSON deploy
    is independent of the SQL deploy — customers can run them on
    different cadences (procs change rarely; ADF pipelines change
    with every metadata edit).
    """
    if realm not in _SUPPORTED_REALMS:
        raise ValidationError(
            f"Unknown realm {realm!r}. Phase 5a ships {sorted(_SUPPORTED_REALMS)}; "
            "see docs/realms_strategy.md for the catalog."
        )

    # Bootstrap the metadata schema if it isn't already, so callers
    # can run plan_deploy against a fresh DB and get a clean
    # "no sources configured" warning rather than a SQL error.
    repo.bootstrap_schema()

    # Lazy import keeps the framework usable when the realm package
    # isn't on the import path (e.g. tooling runs that don't touch
    # generation).
    from lake_ingestion_framework.realms.adf_adls.generator import (
        plan_artifacts,
    )

    target_dir = customer_repo / "generated" / realm / env

    artifacts, warnings = plan_artifacts(repo)

    files: list[DeployPlannedFile] = [
        DeployPlannedFile(
            target_path=target_dir / a.relative_path,
            kind=a.kind,
            name=a.name,
            rendered=a.rendered,
        )
        for a in artifacts
    ]

    if include_procs:
        files.extend(_runtime_sql_files(target_dir=target_dir))

    return DeployPlan(
        realm=realm,
        env=env,
        target_dir=target_dir,
        files=tuple(files),
        warnings=tuple(warnings),
    )


def _runtime_sql_files(*, target_dir: Path) -> list[DeployPlannedFile]:
    """Emit the realm's runtime SQL artifacts as DeployPlannedFiles.

    Layout under ``<target_dir>/runtime_sql/``:

    - ``tables/<name>.sql``  — log tables (master_run_log, run_log)
    - ``views/<name>.sql``   — Lookup-target views
    - ``procs/<name>.sql``   — stored procs ADF calls at runtime

    Apply order (FK + reference safety): tables → views → procs.
    """
    from lake_ingestion_framework.realms.adf_adls.runtime_sql import (
        all_procs,
        all_tables,
        all_views,
    )

    out: list[DeployPlannedFile] = []
    runtime_dir = target_dir / "runtime_sql"

    for name, ddl in all_tables(dialect="azure_sql_db"):
        out.append(
            DeployPlannedFile(
                target_path=runtime_dir / "tables" / f"{name}.sql",
                kind="runtime_sql_table",
                name=name,
                rendered=ddl,
            )
        )
    for name, ddl in all_views(dialect="azure_sql_db"):
        out.append(
            DeployPlannedFile(
                target_path=runtime_dir / "views" / f"{name}.sql",
                kind="runtime_sql_view",
                name=name,
                rendered=ddl,
            )
        )
    for name, ddl in all_procs():
        out.append(
            DeployPlannedFile(
                target_path=runtime_dir / "procs" / f"{name}.sql",
                kind="runtime_sql_proc",
                name=name,
                rendered=ddl,
            )
        )
    return out


# ──────────────────────────────────────────────────────────────────────
# Apply
# ──────────────────────────────────────────────────────────────────────


def apply_deploy(plan: DeployPlan, *, overwrite: bool = True) -> DeployResult:
    """Write each planned file to disk.

    Defaults to ``overwrite=True`` because deploy artifacts are
    *generated* — they exist to be overwritten on every plan run.
    Callers wanting strict-create can flip ``overwrite=False``.
    """
    plan.target_dir.mkdir(parents=True, exist_ok=True)
    by_kind: dict[str, int] = {}

    for pf in plan.files:
        pf.target_path.parent.mkdir(parents=True, exist_ok=True)
        if pf.target_path.exists() and not overwrite:
            raise ValidationError(f"File already exists: {pf.target_path}")
        pf.target_path.write_text(pf.rendered, encoding="utf-8")
        by_kind[pf.kind] = by_kind.get(pf.kind, 0) + 1

    return DeployResult(
        files_written=len(plan.files),
        by_kind=by_kind,
    )


__all__ = [
    "DeployPlan",
    "DeployPlannedFile",
    "DeployResult",
    "apply_deploy",
    "plan_deploy",
]
