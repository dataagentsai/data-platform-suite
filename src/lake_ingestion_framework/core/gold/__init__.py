"""Silver → Gold layer: metadata-driven dbt project generation.

The framework's role at Gold is **generation**, not execution: emit a
deterministic, byte-identical dbt project from metadata. dbt itself runs
the project against the customer's warehouse.

Public API: :func:`generate_dbt_project`, :class:`GoldProjectConfig`,
:class:`GoldModelConfig`, :class:`GoldSource`, :class:`GoldStrategy`,
:class:`Materialization`, :class:`MartShape`,
:class:`GoldGenerationStats`.

See ``docs/gold_layer_design.md`` for the design.
"""

from lake_ingestion_framework.core.gold.generator import (
    GoldGenerationStats,
    GoldModelConfig,
    GoldProjectConfig,
    GoldSnapshotConfig,
    GoldSource,
    GoldStrategy,
    MartShape,
    Materialization,
    SnapshotStrategy,
    UnsupportedGoldFeatureError,
    generate_dbt_project,
    load_project_config,
)

__all__ = [
    "GoldGenerationStats",
    "GoldModelConfig",
    "GoldProjectConfig",
    "GoldSnapshotConfig",
    "GoldSource",
    "GoldStrategy",
    "MartShape",
    "Materialization",
    "SnapshotStrategy",
    "UnsupportedGoldFeatureError",
    "generate_dbt_project",
    "load_project_config",
]
