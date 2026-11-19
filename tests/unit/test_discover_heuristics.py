"""Unit tests for discover heuristics (watermark + PII)."""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.source_connectors.protocol import ColumnInfo
from lake_ingestion_framework.core.types.enums import PiiClass
from lake_ingestion_framework.core.use_cases._discover_heuristics import (
    classify_pii,
    find_watermark_candidates,
)


def _col(
    name: str,
    data_type: str = "timestamp without time zone",
    is_nullable: bool = False,
    pos: int = 1,
) -> ColumnInfo:
    return ColumnInfo(
        name=name,
        data_type=data_type,
        is_nullable=is_nullable,
        ordinal_position=pos,
    )


# ──────────────────────────────────────────────────────────────────────
# Watermark heuristics
# ──────────────────────────────────────────────────────────────────────


class TestWatermarkCandidates:
    def test_finds_modified_at(self) -> None:
        cols = [_col("id", "integer", False, 1), _col("modified_at", "timestamp", False, 2)]
        candidates = find_watermark_candidates(cols)
        assert len(candidates) == 1
        assert candidates[0].column_name == "modified_at"
        assert candidates[0].confidence == 100

    def test_finds_updated_at(self) -> None:
        cols = [_col("updated_at", "timestamp", False, 1)]
        candidates = find_watermark_candidates(cols)
        assert candidates[0].column_name == "updated_at"

    def test_finds_record_modified_dt_with_date_type(self) -> None:
        cols = [_col("record_modified_dt", "date", False, 1)]
        candidates = find_watermark_candidates(cols)
        assert candidates[0].column_name == "record_modified_dt"

    def test_rejects_temporal_name_with_non_temporal_type(self) -> None:
        # Name matches but data type is INT — not a watermark candidate
        cols = [_col("modified_at", "integer", False, 1)]
        assert find_watermark_candidates(cols) == []

    def test_rejects_non_temporal_name_with_temporal_type(self) -> None:
        cols = [_col("created_count", "timestamp", False, 1)]
        assert find_watermark_candidates(cols) == []

    def test_orders_by_confidence(self) -> None:
        cols = [
            _col("created_at", "timestamp", False, 1),  # confidence 50
            _col("modified_at", "timestamp", False, 2),  # confidence 100
        ]
        candidates = find_watermark_candidates(cols)
        assert candidates[0].column_name == "modified_at"
        assert candidates[1].column_name == "created_at"

    def test_nullable_column_penalised(self) -> None:
        cols = [_col("modified_at", "timestamp", is_nullable=True, pos=1)]
        candidates = find_watermark_candidates(cols)
        assert candidates[0].confidence < 100  # penalty applied
        assert "nullable" in candidates[0].reason

    def test_no_match_returns_empty(self) -> None:
        cols = [_col("id", "integer", False, 1), _col("name", "text", False, 2)]
        assert find_watermark_candidates(cols) == []

    def test_modified_suffix_matches(self) -> None:
        cols = [_col("row_modified_at", "timestamp", False, 1)]
        candidates = find_watermark_candidates(cols)
        assert candidates[0].column_name == "row_modified_at"


# ──────────────────────────────────────────────────────────────────────
# PII classification
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        # Direct PII
        ("email", PiiClass.DIRECT),
        ("email_address", PiiClass.DIRECT),
        ("phone_number", PiiClass.DIRECT),
        ("ssn", PiiClass.DIRECT),
        ("first_name", PiiClass.DIRECT),
        ("last_name", PiiClass.DIRECT),
        ("dob", PiiClass.DIRECT),
        ("date_of_birth", PiiClass.DIRECT),
        ("credit_card", PiiClass.DIRECT),
        ("iban", PiiClass.DIRECT),
        ("username", PiiClass.DIRECT),
        # Quasi
        ("zip", PiiClass.QUASI),
        ("postal_code", PiiClass.QUASI),
        ("street_address", PiiClass.QUASI),
        ("city", PiiClass.QUASI),
        ("country", PiiClass.QUASI),
        ("gender", PiiClass.QUASI),
        ("age", PiiClass.QUASI),
        # Sensitive
        ("password", PiiClass.SENSITIVE),
        ("api_key", PiiClass.SENSITIVE),
        ("salary", PiiClass.SENSITIVE),
        ("medical_history", PiiClass.SENSITIVE),
        ("religion", PiiClass.SENSITIVE),
        # None
        ("id", PiiClass.NONE),
        ("order_total", PiiClass.NONE),
        ("created_at", PiiClass.NONE),
        ("status", PiiClass.NONE),
        ("description", PiiClass.NONE),
    ],
)
def test_classify_pii(name: str, expected: PiiClass) -> None:
    assert classify_pii(name) is expected


def test_sensitive_takes_priority_over_direct() -> None:
    # 'password' in a column name should classify as SENSITIVE even
    # though "name" patterns might compete
    assert classify_pii("password_hash") is PiiClass.SENSITIVE


def test_case_insensitive() -> None:
    assert classify_pii("EMAIL") is PiiClass.DIRECT
    assert classify_pii("First_Name") is PiiClass.DIRECT
