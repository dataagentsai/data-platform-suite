"""Standard processors — out-of-the-box column transforms.

See ``docs/standard_processors.md`` for the full design and the
principle that bounds this layer (1:1, no external dependencies,
data-format-only).

Wave 1 ships:

* ``hygiene`` group — 7 default-on + 2 opt-in processors
* ``type_safety`` group — 4 default-on processors
* ``provenance`` group — formalises the existing Silver enrich

Subsequent waves register more groups against the same engine
without touching the wiring.
"""

from lake_ingestion_framework.core.processors.dedup import (
    DedupConfig,
    DedupMode,
    DedupResult,
)
from lake_ingestion_framework.core.processors.engine import (
    ProcessorEngine,
    ProcessorRunReport,
)
from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    EngineConfig,
    GroupConfig,
    GroupMode,
    Processor,
    ProcessorGroup,
    ProcessorRunResult,
)

__all__ = [
    "ColumnTransformContext",
    "DedupConfig",
    "DedupMode",
    "DedupResult",
    "EngineConfig",
    "GroupConfig",
    "GroupMode",
    "Processor",
    "ProcessorEngine",
    "ProcessorGroup",
    "ProcessorRunReport",
    "ProcessorRunResult",
]
