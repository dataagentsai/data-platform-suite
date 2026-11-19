"""Framework metadata column conventions.

Each layer (Bronze, Silver, Gold) prepends a small set of metadata columns
to its rows so downstream consumers can trace lineage, detect changes,
and identify the run that produced a given row.

Bronze: ``_run_id``, ``_loaded_at``, ``_record_hash``
Silver: ``_silver_run_id``, ``_silver_loaded_at``, ``_record_hash``
        plus SCD2 columns when applicable: ``_is_current``, ``_valid_from``, ``_valid_to``

The ``_record_hash`` recipe is identical across layers — SHA256 over selected
columns' string-cast values, with a null sentinel between fields. Identical
recipe = portable change detection.
"""

from __future__ import annotations

import hashlib
from typing import Any

# ──────────────────────────────────────────────────────────────────────
# Hash recipe (shared)
# ──────────────────────────────────────────────────────────────────────

# Sentinel inserted between values and as a null replacement, so that
# ``("a", None) → "a\0NULL\0\0NULL\0"`` doesn't collide with
# ``("a\0NULL\0",) → "a\0NULL\0"``.
NULL_SENTINEL = "\0NULL\0"


def compute_record_hash(values: tuple[Any, ...]) -> str:
    """SHA256 hex digest over ``values`` joined by the null sentinel.

    Each value is string-cast; ``None`` becomes the null sentinel itself.
    Caller controls which columns participate by passing the right tuple.
    """
    parts = [NULL_SENTINEL if v is None else str(v) for v in values]
    encoded = NULL_SENTINEL.join(parts).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ──────────────────────────────────────────────────────────────────────
# Bronze metadata columns
# ──────────────────────────────────────────────────────────────────────

BRONZE_RUN_ID = "_run_id"
BRONZE_LOADED_AT = "_loaded_at"
BRONZE_RECORD_HASH = "_record_hash"

BRONZE_METADATA_COLS: tuple[str, ...] = (
    BRONZE_RUN_ID,
    BRONZE_LOADED_AT,
    BRONZE_RECORD_HASH,
)


# ──────────────────────────────────────────────────────────────────────
# Silver metadata columns
# ──────────────────────────────────────────────────────────────────────

SILVER_RUN_ID = "_silver_run_id"
SILVER_LOADED_AT = "_silver_loaded_at"
SILVER_RECORD_HASH = "_record_hash"  # same column name; recomputed at Silver

# SCD2-only columns; absent under append / scd1 / upsert strategies.
SILVER_IS_CURRENT = "_is_current"
SILVER_VALID_FROM = "_valid_from"
SILVER_VALID_TO = "_valid_to"

SILVER_METADATA_COLS: tuple[str, ...] = (
    SILVER_RUN_ID,
    SILVER_LOADED_AT,
    SILVER_RECORD_HASH,
)


__all__ = [
    "BRONZE_LOADED_AT",
    "BRONZE_METADATA_COLS",
    "BRONZE_RECORD_HASH",
    "BRONZE_RUN_ID",
    "NULL_SENTINEL",
    "SILVER_IS_CURRENT",
    "SILVER_LOADED_AT",
    "SILVER_METADATA_COLS",
    "SILVER_RECORD_HASH",
    "SILVER_RUN_ID",
    "SILVER_VALID_FROM",
    "SILVER_VALID_TO",
    "compute_record_hash",
]
