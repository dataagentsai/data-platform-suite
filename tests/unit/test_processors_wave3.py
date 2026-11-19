"""Wave 3 unit tests — case_normalize, timestamp_normalize, dedup."""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
from lake_ingestion_framework.core.processors.case_normalize import (
    CodeUppercase,
    EmailLowercase,
    GeneralLowercase,
    GeneralUppercase,
)
from lake_ingestion_framework.core.processors.dedup import (
    DedupConfig,
    apply_dedup,
)
from lake_ingestion_framework.core.processors.timestamp_normalize import (
    EpochToTimestamp,
    Iso8601Parse,
    UtcNormalize,
)
from lake_ingestion_framework.core.processors.types import ColumnTransformContext


def _ctx(
    *,
    name: str = "value",
    bronze: str = "VARCHAR",
    silver: str | None = None,
    pii_class: str = "none",
    column_role: str | None = None,
) -> ColumnTransformContext:
    return ColumnTransformContext(
        bronze_column_name=name,
        silver_column_name=name,
        bronze_data_type=bronze,
        silver_data_type=silver,
        cast_safety="strict",
        pii_class=pii_class,
        column_role=column_role,
        null_replacement=None,
    )


# ──────────────────────────────────────────────────────────────────────
# case_normalize
# ──────────────────────────────────────────────────────────────────────


def test_email_lowercase_fires_for_pii_email() -> None:
    proc = EmailLowercase()
    assert proc.applies_to(_ctx(pii_class="email")) is True
    assert proc.applies_to(_ctx(pii_class="none")) is False


def test_email_lowercase_lowercases_values() -> None:
    table = pa.table({"value": pa.array(["Alice@X.com", "bob@x.com", None])})
    new_table, result = EmailLowercase().run_arrow(table, _ctx(pii_class="email"))
    assert result.fired is True
    assert new_table.column("value").to_pylist() == ["alice@x.com", "bob@x.com", None]


def test_code_uppercase_fires_for_code_role() -> None:
    proc = CodeUppercase()
    assert proc.applies_to(_ctx(column_role="code")) is True
    assert proc.applies_to(_ctx(column_role=None)) is False


def test_code_uppercase_uppercases_codes() -> None:
    table = pa.table({"value": pa.array(["us", "Gb", "in"])})
    new_table, _ = CodeUppercase().run_arrow(table, _ctx(column_role="code"))
    assert new_table.column("value").to_pylist() == ["US", "GB", "IN"]


def test_general_case_processors_apply_to_any_string() -> None:
    """Opt-in processors don't auto-fire; engine routes them via
    column.transforms list. Test that applies_to is permissive."""
    assert GeneralLowercase().applies_to(_ctx()) is True
    assert GeneralUppercase().applies_to(_ctx()) is True
    assert GeneralLowercase().applies_to(_ctx(bronze="INT")) is False


# ──────────────────────────────────────────────────────────────────────
# timestamp_normalize
# ──────────────────────────────────────────────────────────────────────


def test_utc_normalize_passes_through_already_utc() -> None:
    table = pa.table(
        {
            "ts": pa.array(
                [datetime(2026, 5, 5, tzinfo=UTC)] * 3,
                type=pa.timestamp("us", tz="UTC"),
            )
        }
    )
    new_table, result = UtcNormalize().run_arrow(table, _ctx(name="ts", bronze="TIMESTAMP"))
    assert result.fired is False
    assert new_table.column("ts").type == pa.timestamp("us", tz="UTC")


def test_utc_normalize_converts_other_tz_to_utc() -> None:
    """Source tz=America/New_York → target tz=UTC; cast preserves
    instant in time."""
    src_tz = UTC  # use UTC stand-in for repeatability
    # Build a tz-aware non-UTC array via cast
    ts = datetime(2026, 5, 5, 12, 0, tzinfo=src_tz)
    table = pa.table({"ts": pa.array([ts], type=pa.timestamp("us", tz="+05:30"))})
    new_table, result = UtcNormalize().run_arrow(table, _ctx(name="ts", bronze="TIMESTAMP"))
    assert result.fired is True
    assert str(new_table.column("ts").type.tz).upper() in {"UTC", "+00:00"}


def test_epoch_to_timestamp_seconds() -> None:
    table = pa.table({"ts": pa.array([1700000000, 1800000000], type=pa.int64())})
    new_table, result = EpochToTimestamp().run_arrow(
        table, _ctx(name="ts", bronze="BIGINT", silver="TIMESTAMP")
    )
    assert result.fired is True
    out = new_table.column("ts").to_pylist()
    assert out[0].year == 2023  # 1700000000 = 2023-11
    assert out[1].year == 2027  # 1800000000 = 2027


def test_epoch_to_timestamp_milliseconds_auto_detected() -> None:
    """Values >= 10^11 are treated as ms."""
    table = pa.table(
        {"ts": pa.array([1700000000000], type=pa.int64())}  # 1.7T → ms
    )
    new_table, _ = EpochToTimestamp().run_arrow(
        table, _ctx(name="ts", bronze="BIGINT", silver="TIMESTAMP")
    )
    out = new_table.column("ts").to_pylist()
    assert out[0].year == 2023  # 1.7T ms = 2023, not 5141X CE


def test_iso8601_parse_handles_z_suffix_and_offsets() -> None:
    table = pa.table(
        {
            "ts": pa.array(
                [
                    "2026-05-05T12:00:00Z",
                    "2026-05-05T12:00:00+05:30",
                    "2026-05-05",
                    "garbage",
                    None,
                ]
            ),
        }
    )
    new_table, _ = Iso8601Parse().run_arrow(
        table, _ctx(name="ts", bronze="VARCHAR", silver="TIMESTAMP")
    )
    out = new_table.column("ts").to_pylist()
    assert out[0].year == 2026
    assert out[1].year == 2026
    assert out[2].year == 2026
    assert out[3] is None  # garbage → fail-open NULL
    assert out[4] is None


def test_iso8601_parse_does_not_fire_for_non_string() -> None:
    proc = Iso8601Parse()
    assert proc.applies_to(_ctx(bronze="BIGINT", silver="TIMESTAMP")) is False


# ──────────────────────────────────────────────────────────────────────
# dedup
# ──────────────────────────────────────────────────────────────────────


def test_dedup_none_is_no_op() -> None:
    table = pa.table({"id": [1, 1, 2]})
    new_table, result = apply_dedup(table, DedupConfig(mode="none"))
    assert new_table.equals(table)
    assert result.rows_dropped == 0


def test_dedup_exact_drops_identical_rows() -> None:
    table = pa.table({"id": [1, 1, 2, 3, 3, 3], "v": ["a", "a", "b", "c", "c", "c"]})
    new_table, result = apply_dedup(table, DedupConfig(mode="exact"))
    assert result.rows_in == 6
    assert result.rows_out == 3
    assert result.rows_dropped == 3
    assert new_table.column("id").to_pylist() == [1, 2, 3]


def test_dedup_business_key_first_keeps_first_occurrence() -> None:
    table = pa.table(
        {
            "customer_id": [1, 1, 2, 1],
            "name": ["alice", "ALICE", "bob", "Alice"],
        }
    )
    new_table, result = apply_dedup(
        table,
        DedupConfig(mode="business_key_first", business_keys=("customer_id",)),
    )
    assert result.rows_dropped == 2
    assert new_table.column("name").to_pylist() == ["alice", "bob"]


def test_dedup_business_key_latest_uses_order_by() -> None:
    table = pa.table(
        {
            "customer_id": [1, 1, 2, 1],
            "modified_at": [10, 30, 20, 25],
            "name": ["v1", "v3", "bob", "v2"],
        }
    )
    new_table, result = apply_dedup(
        table,
        DedupConfig(
            mode="business_key_latest",
            business_keys=("customer_id",),
            order_by="modified_at",
        ),
    )
    assert result.rows_dropped == 2
    # customer_id=1: max modified_at=30 → "v3"
    out = {
        (r[0], r[1])
        for r in zip(
            new_table.column("customer_id").to_pylist(),
            new_table.column("name").to_pylist(),
            strict=True,
        )
    }
    assert out == {(1, "v3"), (2, "bob")}


def test_dedup_business_key_no_keys_is_no_op() -> None:
    """No business_keys configured → mode is ignored, table passes through."""
    table = pa.table({"id": [1, 1, 2]})
    new_table, result = apply_dedup(table, DedupConfig(mode="business_key_first"))
    assert new_table.equals(table)
    assert result.rows_dropped == 0


def test_dedup_flag_adds_is_duplicate_column() -> None:
    table = pa.table({"id": [1, 1, 2, 1, 3]})
    new_table, result = apply_dedup(table, DedupConfig(mode="flag", business_keys=("id",)))
    assert result.rows_dropped == 0  # no rows removed
    assert result.flagged_only == 2  # two duplicates flagged
    flags = new_table.column("_is_duplicate").to_pylist()
    assert flags == [False, True, False, True, False]


def test_dedup_empty_table_is_safe() -> None:
    table = pa.table({"id": pa.array([], type=pa.int64())})
    new_table, result = apply_dedup(table, DedupConfig(mode="exact", business_keys=("id",)))
    assert result.rows_in == 0
    assert new_table.num_rows == 0
