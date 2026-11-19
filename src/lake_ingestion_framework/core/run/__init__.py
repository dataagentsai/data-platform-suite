"""Realm-agnostic local-mode dispatcher.

The dispatcher loads the deployed ADF artifacts from disk and invokes
the realm's mimic. Today only adf-adls has a mimic; future realms get
their own under ``realms/<realm>/local/mimic/``.

Public entry point: :func:`run_pipeline_local`.
"""

from lake_ingestion_framework.core.run.local_runner import (
    LocalRunReport,
    run_pipeline_local,
)

__all__ = ["LocalRunReport", "run_pipeline_local"]
