"""Public lookup + parse functions for the key registry.

Built on the KeyDef list in ``_registry.py``. Keep the surface tiny —
two functions handle every caller's needs.
"""

from __future__ import annotations

import json

from lake_ingestion_framework.core.exceptions import (
    UnknownKeyError,
    ValidationError,
)
from lake_ingestion_framework.core.metadata.keys._definitions import (
    KeyDef,
    KeyTable,
    KeyType,
)
from lake_ingestion_framework.core.metadata.keys._registry import ALL_KEYS
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import (
    Identifier,
    KvRef,
)

# Index: (table, key) → KeyDef
# For source_config, multiple defs can share the same key with different
# source_type_scope tuples; we keep all defs with the same key in a list.
_INDEX: dict[tuple[KeyTable, str], list[KeyDef]] = {}
for _key_def in ALL_KEYS:
    _INDEX.setdefault((_key_def.table, _key_def.name), []).append(_key_def)


def lookup_key(
    table: KeyTable,
    key: str,
    *,
    source_type: SourceType | None = None,
) -> KeyDef:
    """Return the ``KeyDef`` for the (table, key) pair.

    For ``source_config`` keys, ``source_type`` selects between defs
    that share a name but apply to different source types. ``None``
    matches keys that are valid for any source type.
    """
    candidates = _INDEX.get((table, key))
    if not candidates:
        raise UnknownKeyError(table=table.value, key=key)

    if table is not KeyTable.SOURCE_CONFIG:
        return candidates[0]

    # SOURCE_CONFIG: match against source_type
    for kd in candidates:
        if kd.applies_to_source_type(source_type):
            return kd

    raise UnknownKeyError(
        table=table.value,
        key=f"{key} (not valid for source_type={source_type})",
    )


def parse_value(key_def: KeyDef, raw: str) -> object:
    """Parse a raw string ``[value]`` into a typed Python value.

    Raises ``ValidationError`` if the raw value is incompatible with
    the key's declared type. Caller is responsible for handling NULL
    values from the DB before calling — this function expects a
    non-None string.
    """
    match key_def.type:
        case KeyType.STRING | KeyType.PATH_TEMPLATE | KeyType.CRON | KeyType.URI:
            return raw

        case KeyType.INT:
            try:
                return int(raw)
            except ValueError as exc:
                raise ValidationError(
                    f"Key {key_def.name!r}: cannot parse {raw!r} as int."
                ) from exc

        case KeyType.BOOL:
            normalized = raw.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
            raise ValidationError(
                f"Key {key_def.name!r}: cannot parse {raw!r} as bool. "
                "Expected one of: true/false/1/0/yes/no/on/off."
            )

        case KeyType.ENUM:
            allowed = key_def.enum_values or ()
            if raw not in allowed:
                raise ValidationError(
                    f"Key {key_def.name!r}: {raw!r} is not in the safe-list {list(allowed)}."
                )
            return raw

        case KeyType.KV_REF:
            return KvRef.parse(raw)

        case KeyType.IDENTIFIER:
            return Identifier.parse(raw, role=key_def.name)

        case KeyType.JSON:
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValidationError(f"Key {key_def.name!r}: invalid JSON ({exc.msg}).") from exc

        case _:
            # mypy --strict ensures this is unreachable, but keep a sentinel
            raise ValidationError(  # pragma: no cover
                f"Key {key_def.name!r}: unknown KeyType {key_def.type!r}."
            )
