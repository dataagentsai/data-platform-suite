"""ADF Mimic — minimum-viable local executor for ADF pipeline JSON.

See ``docs/adf_mimic_design.md`` for the full design. This package
implements seven activity types the generator emits today: Lookup,
ForEach, Copy, SqlServerStoredProcedure, ExecutePipeline, SetVariable,
Switch.

Public entry point: ``ADFMimicRunner``. Most callers should use the
``core/run/local_runner.py`` dispatcher rather than instantiating the
runner directly.
"""

from lake_ingestion_framework.realms.adf_adls.local.mimic.activities import (
    ActivityResult,
    ExecutionContext,
)
from lake_ingestion_framework.realms.adf_adls.local.mimic.resolvers import (
    ArtifactRegistry,
)
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState
from lake_ingestion_framework.realms.adf_adls.local.mimic.runner import (
    ADFMimicRunner,
    PipelineResult,
)

__all__ = [
    "ADFMimicRunner",
    "ActivityResult",
    "ArtifactRegistry",
    "ExecutionContext",
    "PipelineResult",
    "RunState",
]
