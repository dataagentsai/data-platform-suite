"""KV-key registry — what keys are valid in ``platform_config``,
``source_config``, and ``dataset_config``.

The single source of truth for what Python will accept when writing to
KV tables. Mirrors ``docs/metadata_keys.md``. Adding a new key is a
PR that touches both the doc and ``_registry.py``; CI catches drift.

Public surface:

- ``KeyDef``, ``KeyTable``, ``KeyType`` — the model
- ``lookup_key()`` — find a key def for a (table, key) pair; raises
  ``UnknownKeyError`` if not registered
- ``parse_value()`` — parse the raw ``[value]`` string into a typed
  Python value per the key's declared type
- ``ALL_KEYS`` — the registry as a flat list (for tests + docs)
"""

from lake_ingestion_framework.core.metadata.keys._definitions import (
    KeyDef,
    KeyTable,
    KeyType,
)
from lake_ingestion_framework.core.metadata.keys._lookup import (
    lookup_key as lookup_key,
)
from lake_ingestion_framework.core.metadata.keys._lookup import (
    parse_value as parse_value,
)
from lake_ingestion_framework.core.metadata.keys._registry import (
    ALL_KEYS as ALL_KEYS,
)

__all__ = [
    "ALL_KEYS",
    "KeyDef",
    "KeyTable",
    "KeyType",
    "lookup_key",
    "parse_value",
]
