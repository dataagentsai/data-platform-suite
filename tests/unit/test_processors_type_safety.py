"""Unit tests for the type_safety group (Wave 1)."""

from __future__ import annotations

import pyarrow as pa
from lake_ingestion_framework.core.processors.type_safety import (
    BooleanCoercion,
    DecimalScaleNormalize,
    IntPromotion,
    SafeCast,
)
from lake_ingestion_framework.core.processors.types import ColumnTransformContext


def _ctx(
    *,
    bronze: str = "STRING",
    silver: str | None = None,
    cast_safety: str = "strict",
    name: str = "value",
) -> ColumnTransformContext:
    return ColumnTransformContext(
        bronze_column_name=name,
        silver_column_name=name,
        bronze_data_type=bronze,
        silver_data_type=silver,
        cast_safety=cast_safety,
        pii_class="none",
        column_role=None,
        null_replacement=None,
    )


# ──────────────────────────────────────────────────────────────────────
# safe_cast
# ──────────────────────────────────────────────────────────────────────


def test_safe_cast_only_fires_when_try_cast() -> None:
    proc = SafeCast()
    assert proc.applies_to(_ctx(silver="BIGINT", cast_safety="strict")) is False
    assert proc.applies_to(_ctx(silver="BIGINT", cast_safety="try_cast")) is True
    assert proc.applies_to(_ctx(silver=None, cast_safety="try_cast")) is False


def test_safe_cast_string_to_bigint_with_try_cast() -> None:
    table = pa.table({"value": pa.array(["1", "2", "bad", "4"], type=pa.string())})
    ctx = _ctx(bronze="STRING", silver="BIGINT", cast_safety="try_cast")
    new_table, result = SafeCast().run_arrow(table, ctx)
    assert result.fired is True
    out = new_table.column("value").to_pylist()
    assert out == [1, 2, None, 4]


# ──────────────────────────────────────────────────────────────────────
# int_promotion
# ──────────────────────────────────────────────────────────────────────


def test_int_promotion_int32_to_int64() -> None:
    table = pa.table({"value": pa.array([1, 2, 3], type=pa.int32())})
    ctx = _ctx(bronze="INT", silver=None)
    new_table, result = IntPromotion().run_arrow(table, ctx)
    assert result.fired is True
    assert new_table.column("value").type == pa.int64()


def test_int_promotion_skips_when_silver_type_explicit() -> None:
    """Customer set silver_data_type → that wins; int_promotion stays out."""
    proc = IntPromotion()
    assert proc.applies_to(_ctx(bronze="INT", silver="INT")) is False
    assert proc.applies_to(_ctx(bronze="INT", silver=None)) is True


def test_int_promotion_skips_already_bigint() -> None:
    table = pa.table({"value": pa.array([1, 2, 3], type=pa.int64())})
    ctx = _ctx(bronze="BIGINT", silver=None)
    _, result = IntPromotion().run_arrow(table, ctx)
    # applies_to is True (BIGINT → INT family for the regex), but the
    # actual cast is a no-op; result.fired is False
    assert result.fired is False


# ──────────────────────────────────────────────────────────────────────
# decimal_scale_normalize — Wave 1 placeholder
# ──────────────────────────────────────────────────────────────────────


def test_decimal_scale_normalize_is_no_op_in_wave_1() -> None:
    """Wave 1 ships the placeholder; behaviour is no-op until Wave 3.

    The processor reports fired=False so describe-CLI surfaces it.
    """
    table = pa.table({"value": pa.array([1.0, 2.5, 3.0])})
    ctx = _ctx(bronze="DECIMAL(10,2)", silver=None)
    new_table, result = DecimalScaleNormalize().run_arrow(table, ctx)
    assert result.fired is False
    assert new_table.equals(table)


# ──────────────────────────────────────────────────────────────────────
# boolean_coercion
# ──────────────────────────────────────────────────────────────────────


def test_boolean_coercion_yes_no() -> None:
    table = pa.table({"flag": pa.array(["Y", "N", "yes", "NO", "1", "0", "true", "false"])})
    ctx = _ctx(bronze="VARCHAR", silver="BOOLEAN", name="flag")
    new_table, result = BooleanCoercion().run_arrow(table, ctx)
    assert result.fired is True
    assert new_table.column("flag").to_pylist() == [
        True,
        False,
        True,
        False,
        True,
        False,
        True,
        False,
    ]


def test_boolean_coercion_unknown_to_null() -> None:
    table = pa.table({"flag": pa.array(["Y", "MAYBE", "X"])})
    ctx = _ctx(bronze="VARCHAR", silver="BOOLEAN", name="flag")
    new_table, _ = BooleanCoercion().run_arrow(table, ctx)
    assert new_table.column("flag").to_pylist() == [True, None, None]


def test_boolean_coercion_only_fires_when_target_is_boolean() -> None:
    proc = BooleanCoercion()
    assert proc.applies_to(_ctx(silver="BOOLEAN")) is True
    assert proc.applies_to(_ctx(silver="STRING")) is False
    assert proc.applies_to(_ctx(silver=None)) is False
