"""Metadata-driven framework for ingesting data into a lakehouse.

The public API is exposed at the package root for the few stable
contracts. Internal modules (`_*` prefixed or buried under `core/`,
`realms/`, etc.) are not part of the contract and may change in any
release.
"""

from lake_ingestion_framework._version import __version__
from lake_ingestion_framework.core.exceptions import (
    LakeIngestError,
    MetadataError,
    RuntimeStateError,
    SinkError,
    SourceError,
    ValidationError,
)
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
    "LakeIngestError",
    "LoadProfile",
    "LoadType",
    "MasterRunStatus",
    "MetadataError",
    "PiiClass",
    "Realm",
    "RunStatus",
    "RunType",
    "RuntimeStateError",
    "SegmentationStrategy",
    "SinkError",
    "SourceError",
    "SourceType",
    "TriggerType",
    "ValidationError",
    "__version__",
]
