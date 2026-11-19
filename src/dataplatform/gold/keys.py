"""Deterministic hash surrogate keys with reserved members (ADR-004, DPC-0081, DPC-0046).

A member's key is a hash of (the source dataset, its business key, and for a history dimension the version start),
so every rebuild and backfill gives the same key on any engine. The recipe is plain text so each engine can repeat it:

    text = "<source.dataset>|<key value 1>|...|<version start as yyyy-MM-ddTHH:mm:ss.ffffffZ, UTC>"
    key  = the first 15 hex digits of SHA-256(text) as a whole number (60 bits: positive in any 64-bit integer)

Reserved members: -1 unknown (the fact's member has not arrived yet, LATE-02) and -2 not applicable (the fact has no
business key for this dimension). Collisions are possible in principle, so the harness checks the key is unique per
dimension (KEY-01).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

UNKNOWN = -1
NOT_APPLICABLE = -2
HEX_DIGITS = 15
SEP = "|"
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"  # the engine adapter renders a timestamp the same way


def canonical_time(t: datetime) -> str:
    """A version start as the recipe writes it (a naive time is taken as UTC)."""
    aware = t if t.tzinfo else t.replace(tzinfo=UTC)
    return aware.astimezone(UTC).strftime(TS_FORMAT)


def key_text(dataset: str, business_key: tuple[object, ...], version_start: datetime | None = None) -> str:
    parts = [dataset, *("" if v is None else str(v) for v in business_key)]
    if version_start is not None:
        parts.append(canonical_time(version_start))
    # shiftleft: allow SL-COST-CODE-14 -- a string join (str.join), not a dataframe join
    return SEP.join(parts)


def surrogate_key(dataset: str, business_key: tuple[object, ...], version_start: datetime | None = None) -> int:
    """The member's key; the same inputs always give the same number."""
    digest = hashlib.sha256(key_text(dataset, business_key, version_start).encode()).hexdigest()
    return int(digest[:HEX_DIGITS], 16)


def member(business_key: tuple[object, ...], found: int | None) -> int:
    """The key a fact row points at: not applicable without a business key, unknown when no member matched."""
    if not business_key or all(v is None for v in business_key):
        return NOT_APPLICABLE
    return UNKNOWN if found is None else found
