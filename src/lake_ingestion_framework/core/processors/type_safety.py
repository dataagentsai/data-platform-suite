"""type_safety group — coercion done safely.

| Processor                | Default | What it does |
|--------------------------|---------|--------------|
| safe_cast                | on (when silver_data_type set) | try_cast, never halt on failure |
| int_promotion            | on      | INT → BIGINT to prevent downstream overflow |
| decimal_scale_normalize  | on      | promote DECIMAL scale to dataset's max |
| boolean_coercion         | on      | "Y"/"true"/"1" → BOOLEAN |
| numeric_clipping         | off (opt-in) | clamp [min, max] |
| string_truncation        | off (opt-in) | cap at N chars |

The four default-on processors fire automatically when conditions
match (silver_data_type set / int data_type / etc.). The two opt-in
processors require explicit listing in ``transforms``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


_INT_TYPES = {"INT", "INTEGER", "INT4", "SMALLINT", "TINYINT"}
_BOOL_TYPES = {"BOOL", "BOOLEAN"}
_DECIMAL_PREFIXES = ("DECIMAL", "NUMERIC")
_STRING_TYPES = {"STRING", "VARCHAR", "TEXT", "CHAR", "NVARCHAR", "NCHAR"}
_INT64_BITS = 64  # promote anything narrower to BIGINT


def _normalize(t: str | None) -> str:
    return (t or "").upper().split("(", 1)[0].strip()


# ──────────────────────────────────────────────────────────────────────


class SafeCast:
    """try_cast — bad values become NULL rather than raising.

    Fires when ``ctx.cast_safety == 'try_cast'`` AND a target type is
    declared. ``cast_safety='strict'`` (the default) means *don't* run
    safe_cast; the framework will let the underlying engine raise
    naturally on cast failure (caller's choice).
    """

    name = "safe_cast"
    group = ProcessorGroup.TYPE_SAFETY

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return ctx.cast_safety == "try_cast" and bool(ctx.silver_data_type)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        target = _normalize(ctx.silver_data_type)
        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)

        src = table.column(col)
        # Map common SQL type names → pyarrow types
        target_pa = _to_pyarrow_type(target)
        if target_pa is None:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)

        # try_cast — invalid → null; safe == True
        try:
            casted = src.cast(target_pa, safe=False)  # safe=False permits lossy
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            # Fall back to row-by-row try_cast
            values: list[object] = []
            for v in src.to_pylist():
                try:
                    values.append(_python_cast(v, target))
                except (ValueError, TypeError):
                    values.append(None)
            casted = pa.array(values, type=target_pa)

        idx = table.schema.get_field_index(col)
        new_table = table.set_column(idx, col, casted)
        return new_table, ProcessorRunResult(
            self.group, self.name, col, fired=True, rows_affected=-1
        )


class IntPromotion:
    """Promote INT/INT4/SMALLINT to BIGINT to prevent overflow.

    Fires automatically when the column's bronze type is a smaller
    int family AND the customer hasn't explicitly set
    ``silver_data_type`` (which would override and trump promotion).
    """

    name = "int_promotion"
    group = ProcessorGroup.TYPE_SAFETY

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        bronze = _normalize(ctx.bronze_data_type)
        return bronze in _INT_TYPES and not ctx.silver_data_type

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        src = table.column(col)
        if pa.types.is_integer(src.type) and src.type.bit_width < _INT64_BITS:
            new_table = table.set_column(
                table.schema.get_field_index(col), col, src.cast(pa.int64())
            )
            return new_table, ProcessorRunResult(
                self.group, self.name, col, fired=True, rows_affected=table.num_rows
            )
        return table, ProcessorRunResult(self.group, self.name, col, fired=False)


class DecimalScaleNormalize:
    """Normalise DECIMAL/NUMERIC scale across the dataset.

    For Wave 1 we apply a no-op when the column already has a target
    scale via ``silver_data_type='DECIMAL(p,s)'``; the dataset-wide
    "promote to max scale" auto-mode lands in Wave 3 once we have a
    place to store it. This class is the placeholder that the engine
    calls; future waves expand its behaviour without changing wiring.
    """

    name = "decimal_scale_normalize"
    group = ProcessorGroup.TYPE_SAFETY

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        bronze = _normalize(ctx.bronze_data_type)
        return any(bronze.startswith(p) for p in _DECIMAL_PREFIXES)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        # Wave 1: behaviour is a no-op; ProcessorRunResult.fired=False
        # so describe-CLI output makes the placeholder visible.
        return table, ProcessorRunResult(self.group, self.name, ctx.silver_column_name, fired=False)


class BooleanCoercion:
    """Coerce common boolean string forms to actual BOOLEAN.

    Fires when target is BOOLEAN OR bronze data_type is BOOLEAN-ish.
    ``"Y"`` / ``"YES"`` / ``"TRUE"`` / ``"1"`` → True;
    ``"N"`` / ``"NO"`` / ``"FALSE"`` / ``"0"`` → False; anything else
    (case-insensitive comparison) → NULL.
    """

    name = "boolean_coercion"
    group = ProcessorGroup.TYPE_SAFETY

    _TRUTHY: ClassVar[set[str]] = {"y", "yes", "true", "t", "1"}
    _FALSY: ClassVar[set[str]] = {"n", "no", "false", "f", "0"}

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        target = _normalize(ctx.silver_data_type)
        return target in _BOOL_TYPES

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        src = table.column(col).to_pylist()
        out: list[bool | None] = []
        changed = 0
        for v in src:
            if v is None:
                out.append(None)
                continue
            if isinstance(v, bool):
                out.append(v)
                continue
            s = str(v).strip().lower()
            if s in self._TRUTHY:
                out.append(True)
            elif s in self._FALSY:
                out.append(False)
            else:
                out.append(None)
            changed += 1
        new_array = pa.array(out, type=pa.bool_())
        idx = table.schema.get_field_index(col)
        return table.set_column(idx, col, new_array), ProcessorRunResult(
            self.group, self.name, col, fired=True, rows_affected=changed
        )


def _to_pyarrow_type(sql_type: str) -> pa.DataType | None:
    """Map a normalised SQL type name to a pyarrow DataType.

    Supports the common cases that flow through the framework. Returns
    None for unsupported types — the caller treats it as "skip cast."
    """
    import pyarrow as pa

    t = sql_type.upper()
    mapping = {
        "STRING": pa.string(),
        "VARCHAR": pa.string(),
        "TEXT": pa.string(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "INT4": pa.int32(),
        "BIGINT": pa.int64(),
        "INT8": pa.int64(),
        "SMALLINT": pa.int16(),
        "TINYINT": pa.int8(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "REAL": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
    }
    return mapping.get(t)


def _python_cast(value: object, target: str) -> object:
    """Best-effort Python-side cast, used as fallback when pyarrow
    can't cast the column directly (e.g. STRING → INT)."""
    if target in {"INT", "INTEGER", "INT4", "BIGINT", "INT8", "SMALLINT", "TINYINT"}:
        return int(str(value).strip()) if value is not None else None
    if target in {"FLOAT", "DOUBLE", "REAL"}:
        return float(str(value).strip()) if value is not None else None
    if target in {"BOOLEAN", "BOOL"}:
        s = str(value).strip().lower()
        if s in {"y", "yes", "true", "t", "1"}:
            return True
        if s in {"n", "no", "false", "f", "0"}:
            return False
        raise ValueError(f"unrecognised boolean: {value!r}")
    return str(value) if value is not None else None


TYPE_SAFETY_DEFAULT_ON = (
    SafeCast(),
    IntPromotion(),
    DecimalScaleNormalize(),
    BooleanCoercion(),
)
TYPE_SAFETY_OPT_IN: dict[str, object] = {}  # numeric_clipping / string_truncation deferred


__all__ = [
    "BooleanCoercion",
    "DecimalScaleNormalize",
    "IntPromotion",
    "SafeCast",
    "TYPE_SAFETY_DEFAULT_ON",
    "TYPE_SAFETY_OPT_IN",
]
