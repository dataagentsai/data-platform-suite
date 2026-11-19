"""Hygiene group — string cleanup processors.

Seven processors active by default + two opt-in. All operate on
STRING / VARCHAR columns; non-string columns are silently skipped via
``applies_to``.

| Processor                        | Default | What it does |
|----------------------------------|---------|--------------|
| trim_whitespace                  | on      | strip leading/trailing |
| normalize_internal_whitespace    | on      | collapse multi-space to single |
| empty_string_to_null             | on      | "" → NULL |
| strip_control_chars              | on      | strip \\x00-\\x1f except \\t \\n \\r |
| strip_bom                        | on      | drop leading byte-order-mark |
| normalize_unicode (NFC default)  | on      | unicode normalization form |
| normalize_line_endings           | on      | CRLF / CR → LF |
| strip_zero_width                 | off     | zero-width chars; opt in |
| decode_html_entities             | off     | &amp; → & ; opt in |

All processors are pure — they take a pyarrow Table + a column
context and return a new Table. No I/O, no side effects.
"""

from __future__ import annotations

import re
import unicodedata
from typing import TYPE_CHECKING

from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


# Control chars: \x00-\x1f except \t \n \r (which are legitimately whitespace)
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_BOM = "﻿"
_ZERO_WIDTH_RE = re.compile(r"[\u200b-‍﻿]")
_INTERNAL_WS_RE = re.compile(r"\s+")


def _is_string_column(ctx: ColumnTransformContext) -> bool:
    """Bronze + Silver type checks — skip non-string columns."""
    bronze_t = (ctx.bronze_data_type or "").upper()
    silver_t = (ctx.silver_data_type or "").upper() if ctx.silver_data_type else ""
    return any(
        t in {"STRING", "VARCHAR", "TEXT", "CHAR", "NVARCHAR", "NCHAR"}
        for t in (bronze_t, silver_t)
        if t
    )


def _apply_string_op(
    table: pa.Table,
    column: str,
    op: object,
) -> tuple[pa.Table, int]:
    """Apply ``op`` (a callable str → str | None) to every value of a
    string column. Returns the new table + number of cells changed.

    Robust against direct invocation on non-string columns — returns
    the table unchanged when the column type isn't string-like. The
    engine routes via ``applies_to`` first, but tests sometimes call
    ``run_arrow`` directly.

    NULLs pass through unchanged; ``op`` returning ``None`` substitutes
    NULL.
    """
    import pyarrow as pa

    if column not in table.column_names:
        return table, 0
    src = table.column(column)
    if not pa.types.is_string(src.type) and not pa.types.is_large_string(src.type):
        # Non-string column → no-op, no change count.
        return table, 0
    values = src.to_pylist()
    changed = 0
    out: list[str | None] = []
    for v in values:
        if v is None:
            out.append(None)
            continue
        new = op(v)  # type: ignore[operator]
        if new != v:
            changed += 1
        out.append(new)
    new_array = pa.array(out, type=pa.string())
    idx = table.schema.get_field_index(column)
    return table.set_column(idx, column, new_array), changed


# ──────────────────────────────────────────────────────────────────────
# Processor classes (one per transform)
# ──────────────────────────────────────────────────────────────────────


class TrimWhitespace:
    name = "trim_whitespace"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(table, ctx.silver_column_name, str.strip)
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class NormalizeInternalWhitespace:
    name = "normalize_internal_whitespace"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(
            table, ctx.silver_column_name, lambda s: _INTERNAL_WS_RE.sub(" ", s)
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class EmptyStringToNull:
    name = "empty_string_to_null"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        def empty_to_none(s: str) -> str | None:
            return None if s == "" else s

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, empty_to_none)
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class StripControlChars:
    name = "strip_control_chars"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(
            table, ctx.silver_column_name, lambda s: _CTRL_RE.sub("", s)
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class StripBom:
    name = "strip_bom"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(
            table, ctx.silver_column_name, lambda s: s.lstrip(_BOM)
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class NormalizeUnicode:
    name = "normalize_unicode"
    group = ProcessorGroup.HYGIENE

    def __init__(self, form: str = "NFC") -> None:
        if form not in {"NFC", "NFD", "NFKC", "NFKD"}:
            raise ValueError(f"normalize_unicode form must be NFC/NFD/NFKC/NFKD; got {form!r}")
        self._form = form

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        from typing import Literal, cast

        form = cast("Literal['NFC', 'NFD', 'NFKC', 'NFKD']", self._form)
        new_table, changed = _apply_string_op(
            table, ctx.silver_column_name, lambda s: unicodedata.normalize(form, s)
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class NormalizeLineEndings:
    name = "normalize_line_endings"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(
            table,
            ctx.silver_column_name,
            lambda s: s.replace("\r\n", "\n").replace("\r", "\n"),
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class StripZeroWidth:
    """Opt-in (default off): zero-width chars are sometimes legitimate
    (Arabic / Indic scripts) so removing them is destructive in those
    contexts. Customers explicitly opt in via ``transforms``."""

    name = "strip_zero_width"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        new_table, changed = _apply_string_op(
            table, ctx.silver_column_name, lambda s: _ZERO_WIDTH_RE.sub("", s)
        )
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class DecodeHtmlEntities:
    """Opt-in (default off): scraped data sometimes carries
    ``&amp;`` / ``&#39;`` etc. Most clean DB extracts don't. Customer
    enables per column via ``transforms: [decode_html_entities]``."""

    name = "decode_html_entities"
    group = ProcessorGroup.HYGIENE

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import html

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, html.unescape)
        return new_table, ProcessorRunResult(
            group=self.group,
            processor_name=self.name,
            column_name=ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


# Default-on processors (fire when group is enabled, no opt-in needed).
HYGIENE_DEFAULT_ON = (
    TrimWhitespace(),
    NormalizeInternalWhitespace(),
    EmptyStringToNull(),
    StripControlChars(),
    StripBom(),
    NormalizeUnicode(),
    NormalizeLineEndings(),
)

# Opt-in processors — only fire when the column lists them in
# ``transforms``. The engine routes them via the same group.
HYGIENE_OPT_IN: dict[str, object] = {
    "strip_zero_width": StripZeroWidth(),
    "decode_html_entities": DecodeHtmlEntities(),
}


__all__ = [
    "DecodeHtmlEntities",
    "EmptyStringToNull",
    "HYGIENE_DEFAULT_ON",
    "HYGIENE_OPT_IN",
    "NormalizeInternalWhitespace",
    "NormalizeLineEndings",
    "NormalizeUnicode",
    "StripBom",
    "StripControlChars",
    "StripZeroWidth",
    "TrimWhitespace",
]
