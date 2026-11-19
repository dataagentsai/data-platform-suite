"""Heuristics for ``lake-ingest discover``.

Pure logic — no I/O. Given a list of ``ColumnInfo`` for a table,
heuristics propose:

- a watermark column candidate (for incremental load eligibility)
- a PII classification per column (none / direct / quasi / sensitive)

Per ``docs/discovery.md`` → "PII / watermark / partition heuristics".
These are *suggestions* the operator reviews; nothing is auto-enabled.
Heuristics are intentionally conservative — false negatives are fine
(operator overrides via YAML edit), false positives are *not*
(silently labeling something as PII when it isn't damages trust).

Token-aware matching. A "token" is a snake_case segment: ``email`` in
``email_address``, ``user_email``, etc. Patterns use
``(?:^|_)keyword(?:_|$)`` style boundaries because Python's ``\\b``
doesn't fire between word characters and underscores.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from lake_ingestion_framework.core.source_connectors.protocol import ColumnInfo
from lake_ingestion_framework.core.types.enums import PiiClass


def _token_pattern(keyword: str) -> re.Pattern[str]:
    """Compile a keyword into a token-aware pattern.

    Matches the keyword as a whole snake_case token: as the entire
    name, prefixed by ``_``, suffixed by ``_``, or surrounded by ``_``.
    Case-insensitive.
    """
    return re.compile(rf"(?:^|_){re.escape(keyword)}(?:_|$)", re.IGNORECASE)


# ──────────────────────────────────────────────────────────────────────
# Watermark candidate
# ──────────────────────────────────────────────────────────────────────

# Column-name patterns we accept as watermark candidates, in priority order.
# Higher number = stronger signal.
_WATERMARK_NAME_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    # Exact temporal-modification names — strongest signal
    (re.compile(r"^modified_at$", re.IGNORECASE), 100),
    (re.compile(r"^updated_at$", re.IGNORECASE), 100),
    (re.compile(r"^last_modified$", re.IGNORECASE), 100),
    (re.compile(r"^last_modified_at$", re.IGNORECASE), 100),
    (re.compile(r"^last_updated$", re.IGNORECASE), 100),
    (re.compile(r"^last_updated_at$", re.IGNORECASE), 100),
    (re.compile(r"^record_modified_dt$", re.IGNORECASE), 100),
    # Suffix forms — anything containing "_modified_at" / "_updated_at"
    (re.compile(r"_modified_(?:at|dt|time|timestamp)$", re.IGNORECASE), 90),
    (re.compile(r"_updated_(?:at|dt|time|timestamp)$", re.IGNORECASE), 90),
    # Generic mtime-style names
    (re.compile(r"^mtime$", re.IGNORECASE), 80),
    (re.compile(r"^row_version$", re.IGNORECASE), 80),
    (re.compile(r"^etag$", re.IGNORECASE), 70),
    # Created-at — usable for append-only tables (lower priority)
    (re.compile(r"^created_at$", re.IGNORECASE), 50),
    (re.compile(r"^inserted_at$", re.IGNORECASE), 50),
)

_TEMPORAL_TYPE_FRAGMENTS = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "rowversion",
)


@dataclass(frozen=True)
class WatermarkCandidate:
    """One watermark column proposal for a table."""

    column_name: str
    confidence: int  # 0-100
    reason: str


def find_watermark_candidates(columns: list[ColumnInfo]) -> list[WatermarkCandidate]:
    """Return watermark candidates ordered by descending confidence.

    Empty list = no candidate; operator should set ``load_type=FULL``
    or supply a watermark column manually.
    """
    candidates: list[WatermarkCandidate] = []
    for col in columns:
        confidence = _watermark_confidence_from_name(col.name)
        if confidence == 0:
            continue
        if not _is_temporal_type(col.data_type):
            continue
        if col.is_nullable:
            confidence = max(confidence - 20, 1)
            reason = f"name match, temporal type {col.data_type!r}, nullable (caveat)"
        else:
            reason = f"name match, temporal type {col.data_type!r}, NOT NULL"
        candidates.append(
            WatermarkCandidate(
                column_name=col.name,
                confidence=confidence,
                reason=reason,
            )
        )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates


def _watermark_confidence_from_name(name: str) -> int:
    for pattern, score in _WATERMARK_NAME_PATTERNS:
        if pattern.search(name):
            return score
    return 0


def _is_temporal_type(data_type: str) -> bool:
    lowered = data_type.lower()
    return any(frag in lowered for frag in _TEMPORAL_TYPE_FRAGMENTS)


# ──────────────────────────────────────────────────────────────────────
# PII classification
# ──────────────────────────────────────────────────────────────────────

# Direct identifiers — column names that almost certainly hold PII.
# Patterns match snake_case tokens; multi-word identifiers use the
# underscore-joined form.
_PII_DIRECT_PATTERNS: tuple[re.Pattern[str], ...] = (
    _token_pattern("ssn"),
    _token_pattern("social_security"),
    _token_pattern("passport"),
    _token_pattern("aadhaar"),
    _token_pattern("nin"),
    _token_pattern("tax_id"),
    _token_pattern("tin"),
    _token_pattern("ein"),
    _token_pattern("email"),
    _token_pattern("email_address"),
    _token_pattern("phone"),
    _token_pattern("phone_number"),
    _token_pattern("mobile"),
    _token_pattern("mobile_number"),
    _token_pattern("credit_card"),
    _token_pattern("card_number"),
    _token_pattern("iban"),
    _token_pattern("bank_account"),
    _token_pattern("first_name"),
    _token_pattern("last_name"),
    _token_pattern("full_name"),
    _token_pattern("username"),
    _token_pattern("date_of_birth"),
    _token_pattern("dob"),
    _token_pattern("birthdate"),
)

# Quasi-identifiers — useful for re-identification when combined.
_PII_QUASI_PATTERNS: tuple[re.Pattern[str], ...] = (
    _token_pattern("zip"),
    _token_pattern("postal_code"),
    _token_pattern("postcode"),
    _token_pattern("gender"),
    _token_pattern("ethnicity"),
    _token_pattern("nationality"),
    _token_pattern("address"),
    _token_pattern("street_address"),
    _token_pattern("street"),
    _token_pattern("city"),
    _token_pattern("state"),
    _token_pattern("country"),
    _token_pattern("age"),
)

# Sensitive — special-category data under GDPR-style rules.
_PII_SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    _token_pattern("health"),
    _token_pattern("medical"),
    _token_pattern("medical_history"),
    _token_pattern("diagnosis"),
    _token_pattern("religion"),
    _token_pattern("religious"),
    _token_pattern("salary"),
    _token_pattern("compensation"),
    _token_pattern("wage"),
    _token_pattern("password"),
    _token_pattern("pwd"),
    _token_pattern("secret"),
    _token_pattern("api_key"),
    _token_pattern("biometric"),
)


def classify_pii(column_name: str) -> PiiClass:
    """Best-effort PII classification by column name.

    Conservative — returns ``NONE`` unless a token pattern strongly
    matches. Order: SENSITIVE > DIRECT > QUASI > NONE.
    """
    if any(p.search(column_name) for p in _PII_SENSITIVE_PATTERNS):
        return PiiClass.SENSITIVE
    if any(p.search(column_name) for p in _PII_DIRECT_PATTERNS):
        return PiiClass.DIRECT
    if any(p.search(column_name) for p in _PII_QUASI_PATTERNS):
        return PiiClass.QUASI
    return PiiClass.NONE


__all__ = [
    "WatermarkCandidate",
    "classify_pii",
    "find_watermark_candidates",
]
