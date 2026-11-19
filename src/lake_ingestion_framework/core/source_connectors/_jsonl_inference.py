"""Shared column-type inference for JSON-shaped record streams.

Used by both the SFTP connector (JSON Lines files) and the REST API
connector (JSON object responses). Given a list of dict records,
produce ``ColumnInfo`` for each key in their union.

Pure functions — no I/O, no driver imports. Easily unit-testable.

Inference rules (conservative):
- All non-null values are bool → ``boolean``
- All non-null values are int (and not bool) → ``integer``
- All non-null values are int or float (and not bool) → ``numeric``
- All non-null values are list → ``array``
- All non-null values are dict → ``object``
- Else → ``string`` (default)

Nullability: a column is nullable if any record has a None value or
omits the key entirely.
"""

from __future__ import annotations

from typing import Any

from lake_ingestion_framework.core.source_connectors.protocol import ColumnInfo

# Strings that count as boolean values in mixed-type columns.
_BOOL_VALUES = frozenset({"true", "false", "yes", "no", "1", "0", "t", "f"})


def infer_columns_from_records(records: list[dict[str, Any]]) -> list[ColumnInfo]:
    """Return ``ColumnInfo`` for the union of keys across ``records``.

    Records are sampled in order; the first occurrence of each key
    sets its ``ordinal_position``. Empty input → empty output.
    """
    if not records:
        return []

    seen: list[str] = []
    seen_set: set[str] = set()
    for obj in records:
        for key in obj:
            if key not in seen_set:
                seen.append(key)
                seen_set.add(key)

    columns: list[ColumnInfo] = []
    for idx, key in enumerate(seen):
        values = [obj.get(key) for obj in records if key in obj]
        is_nullable = any(v is None for v in values) or any(key not in obj for obj in records)
        non_null = [v for v in values if v is not None]
        columns.append(
            ColumnInfo(
                name=key,
                data_type=_infer_value_type(non_null),
                is_nullable=is_nullable,
                ordinal_position=idx + 1,
            )
        )
    return columns


def _infer_value_type(values: list[Any]) -> str:
    """Type-classify a non-null list of Python values from JSON."""
    if not values:
        return "string"
    if all(
        isinstance(v, bool) or (isinstance(v, str) and v.lower() in _BOOL_VALUES) for v in values
    ):
        return "boolean"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return "integer"
    if all(isinstance(v, int | float) and not isinstance(v, bool) for v in values):
        return "numeric"
    if all(isinstance(v, list) for v in values):
        return "array"
    if all(isinstance(v, dict) for v in values):
        return "object"
    return "string"


__all__ = ["infer_columns_from_records"]
