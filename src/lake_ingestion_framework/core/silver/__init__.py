"""Bronze → Silver pipeline.

Reads a Bronze Delta table, drops Bronze framework metadata columns,
applies the configured Silver write strategy, and persists to a Silver
Delta table with Silver metadata columns.

Public API: :class:`SilverPipeline`, :class:`SilverConfig`, :class:`SilverRunStats`.

See ``docs/silver_layer_design.md`` for the design.
"""

from lake_ingestion_framework.core.silver.pipeline import (
    SilverConfig,
    SilverDqContext,
    SilverPipeline,
    SilverProcessorContext,
    SilverRunStats,
    SilverStrategy,
)

__all__ = [
    "SilverConfig",
    "SilverDqContext",
    "SilverPipeline",
    "SilverProcessorContext",
    "SilverRunStats",
    "SilverStrategy",
]
