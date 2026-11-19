"""Realm-agnostic local-mode dispatcher.

Walks the deploy output directory, loads the ADF artifacts into an
``ArtifactRegistry``, and invokes the adf-adls mimic to run a named
pipeline. Future realms get their own dispatcher branch (or a
strategy lookup) — for v1 this knows about adf-adls only.

CLI shape (per docs/adf_mimic_design.md §10):

    lake-ingest run --mode local --pipeline PL_INGEST_POSTGRES \\
        --artifacts-root <customer>-ingestion/adf \\
        --metadata-db <customer>-ingestion/local/meta.db \\
        --lakes-root ~/.lake-ingest/lakes/<env> \\
        --params p_server=localhost p_user=postgres ...

The CLI hooks ``cli.py`` to ``run_pipeline_local`` directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lake_ingestion_framework.realms.adf_adls.local.mimic import (
    ADFMimicRunner,
    ArtifactRegistry,
    ExecutionContext,
    PipelineResult,
)


@dataclass(frozen=True)
class LocalRunReport:
    """Outcome of one local-mode pipeline invocation."""

    pipeline: PipelineResult
    artifacts_root: Path
    metadata_db: Path
    lakes_root: Path

    @property
    def succeeded(self) -> bool:
        return self.pipeline.status == "Succeeded"


def run_pipeline_local(
    *,
    pipeline_name: str,
    artifacts_root: Path,
    metadata_db: Path,
    lakes_root: Path,
    parameters: dict[str, Any] | None = None,
) -> LocalRunReport:
    """Run a single pipeline locally end-to-end.

    Loads artifacts from ``artifacts_root`` (the deploy output, with
    ``linkedServices/``, ``datasets/``, ``pipelines/`` subdirs), invokes
    the adf-adls mimic against ``metadata_db`` (a SQLite file) and
    writes Bronze Delta into ``lakes_root``.
    """
    registry = ArtifactRegistry.from_directory(artifacts_root)
    runner = ADFMimicRunner()
    ctx = ExecutionContext(
        registry=registry,
        metadata_db_path=metadata_db,
        lakes_root=lakes_root,
        runner=runner,
    )
    pipeline_result = runner.run_pipeline(
        pipeline_name=pipeline_name,
        parameters=parameters or {},
        ctx=ctx,
    )
    return LocalRunReport(
        pipeline=pipeline_result,
        artifacts_root=artifacts_root,
        metadata_db=metadata_db,
        lakes_root=lakes_root,
    )


__all__ = ["LocalRunReport", "run_pipeline_local"]
