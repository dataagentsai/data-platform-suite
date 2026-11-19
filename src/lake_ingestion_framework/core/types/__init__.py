"""Typed value objects + enums.

Pydantic models live alongside the enums when they grow beyond simple
``StrEnum``. For now: enums only.
"""

from lake_ingestion_framework.core.types.enums import (
    AuthMethod,
    CompressionCodec,
    DatasetType,
    FileFormat,
    FlatteningEngine,
    FlatteningMode,
    LoadProfile,
    LoadType,
    MasterRunStatus,
    PiiClass,
    Realm,
    RunStatus,
    RunType,
    SegmentationStrategy,
    SourceType,
    TriggerType,
)

__all__ = [
    "AuthMethod",
    "CompressionCodec",
    "DatasetType",
    "FileFormat",
    "FlatteningEngine",
    "FlatteningMode",
    "LoadProfile",
    "LoadType",
    "MasterRunStatus",
    "PiiClass",
    "Realm",
    "RunStatus",
    "RunType",
    "SegmentationStrategy",
    "SourceType",
    "TriggerType",
]
