"""case_normalize group — case discipline.

| Processor          | Default | What it does |
|--------------------|---------|--------------|
| email_lowercase    | on (when pii_class=email) | Auto when classified |
| code_uppercase     | on (when column_role='code') | Country/currency/status codes |
| general_lowercase  | off (opt-in) | Per-column opt-in |
| general_uppercase  | off (opt-in) | Per-column opt-in |

Runs AFTER hygiene + type_safety, BEFORE pii — so pii hashing
captures the case-normalized value (alice@x.com vs Alice@x.com map
to the same hash).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.hygiene import _apply_string_op
from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


_STRING_TYPES = {"STRING", "VARCHAR", "TEXT", "CHAR", "NVARCHAR", "NCHAR"}


def _is_string_column(ctx: ColumnTransformContext) -> bool:
    bronze = (ctx.bronze_data_type or "").upper()
    silver = (ctx.silver_data_type or "").upper() if ctx.silver_data_type else ""
    return any(t in _STRING_TYPES for t in (bronze, silver) if t)


# ──────────────────────────────────────────────────────────────────────


class EmailLowercase:
    name = "email_lowercase"
    group = ProcessorGroup.CASE_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return ctx.pii_class == "email" and _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(table, ctx.silver_column_name, str.lower)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class CodeUppercase:
    name = "code_uppercase"
    group = ProcessorGroup.CASE_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return ctx.column_role == "code" and _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(table, ctx.silver_column_name, str.upper)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class GeneralLowercase:
    """Opt-in: per-column ``transforms: [general_lowercase]``."""

    name = "general_lowercase"
    group = ProcessorGroup.CASE_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(table, ctx.silver_column_name, str.lower)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class GeneralUppercase:
    """Opt-in: per-column ``transforms: [general_uppercase]``."""

    name = "general_uppercase"
    group = ProcessorGroup.CASE_NORMALIZE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(table, ctx.silver_column_name, str.upper)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


CASE_NORMALIZE_DEFAULT_ON = (EmailLowercase(), CodeUppercase())
CASE_NORMALIZE_OPT_IN: dict[str, object] = {
    "general_lowercase": GeneralLowercase(),
    "general_uppercase": GeneralUppercase(),
}


__all__ = [
    "CASE_NORMALIZE_DEFAULT_ON",
    "CASE_NORMALIZE_OPT_IN",
    "CodeUppercase",
    "EmailLowercase",
    "GeneralLowercase",
    "GeneralUppercase",
]
