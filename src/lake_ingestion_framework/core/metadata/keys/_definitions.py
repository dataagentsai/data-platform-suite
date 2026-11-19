"""Type definitions for the key registry.

A ``KeyDef`` describes one valid key in a KV table. Drives validation
when Python writes to ``platform_config``, ``source_config``, or
``dataset_config``.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from lake_ingestion_framework.core.types.enums import SourceType


class KeyTable(StrEnum):
    """Which KV table a key belongs to."""

    PLATFORM_CONFIG = "platform_config"
    SOURCE_CONFIG = "source_config"
    DATASET_CONFIG = "dataset_config"


class KeyType(StrEnum):
    """Logical type of the parsed value.

    Wire form is always ``NVARCHAR(MAX)`` in the DB; the type drives
    parsing at the boundary.
    """

    STRING = "string"
    INT = "int"
    BOOL = "bool"
    ENUM = "enum"
    KV_REF = "kv-ref"
    PATH_TEMPLATE = "path-template"
    JSON = "json"
    CRON = "cron"
    URI = "uri"
    IDENTIFIER = "identifier"


class KeyDef(BaseModel):
    """A single registered key. Immutable; fields locked at construction."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(..., min_length=1, max_length=100)
    table: KeyTable
    type: KeyType
    required: bool = False
    default: str | int | bool | None = None
    enum_values: tuple[str, ...] | None = None
    description: str = ""
    # source_config keys are scoped to specific source types; None = any
    source_type_scope: tuple[SourceType, ...] | None = None
    # informational — for the doc generator
    cross_key_dependency: str | None = None

    def applies_to_source_type(self, source_type: SourceType | None) -> bool:
        """Return True if this key is valid for the given source_type.

        For non-``source_config`` keys, scope is always wide open (the
        scope field is meaningless).
        """
        if self.table is not KeyTable.SOURCE_CONFIG:
            return True
        if self.source_type_scope is None:
            return True
        if source_type is None:
            return False
        return source_type in self.source_type_scope
