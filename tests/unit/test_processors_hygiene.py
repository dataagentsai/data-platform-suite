"""Unit tests for the hygiene group (Wave 1).

Each processor takes a pyarrow Table + column context and returns a
new Table. Tests run against in-memory pyarrow — no metadata DB.
"""

from __future__ import annotations

import pyarrow as pa
from lake_ingestion_framework.core.processors.hygiene import (
    DecodeHtmlEntities,
    EmptyStringToNull,
    NormalizeInternalWhitespace,
    NormalizeLineEndings,
    NormalizeUnicode,
    StripBom,
    StripControlChars,
    StripZeroWidth,
    TrimWhitespace,
)
from lake_ingestion_framework.core.processors.types import ColumnTransformContext


def _ctx(name: str = "name", *, data_type: str = "STRING") -> ColumnTransformContext:
    return ColumnTransformContext(
        bronze_column_name=name,
        silver_column_name=name,
        bronze_data_type=data_type,
        silver_data_type=None,
        cast_safety="strict",
        pii_class="none",
        column_role=None,
        null_replacement=None,
    )


def _t(values: list[str | None], col: str = "name") -> pa.Table:
    return pa.table({col: pa.array(values, type=pa.string())})


# ──────────────────────────────────────────────────────────────────────


def test_trim_whitespace_strips_both_sides() -> None:
    table = _t(["  hello", "world  ", " both ", "clean", None])
    new_table, result = TrimWhitespace().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [
        "hello",
        "world",
        "both",
        "clean",
        None,
    ]
    assert result.fired is True
    assert result.rows_affected == 3


def test_trim_whitespace_no_op_returns_unchanged() -> None:
    table = _t(["already", "clean", None])
    new_table, result = TrimWhitespace().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == ["already", "clean", None]
    assert result.fired is False


def test_normalize_internal_whitespace_collapses_runs() -> None:
    table = _t(["one  space", "many   spaces", "tabs\t\t\there"])
    new_table, _ = NormalizeInternalWhitespace().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [
        "one space",
        "many spaces",
        "tabs here",
    ]


def test_empty_string_to_null_substitutes() -> None:
    table = _t(["", "non-empty", "", None])
    new_table, result = EmptyStringToNull().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [None, "non-empty", None, None]
    assert result.rows_affected == 2


def test_strip_control_chars_keeps_tab_newline_carriage() -> None:
    """Control chars 0x00-0x1f are stripped EXCEPT \\t (0x09), \\n (0x0a), \\r (0x0d)."""
    table = _t(["bad\x07char", "tab\there", "newline\nok", "clean"])
    new_table, _ = StripControlChars().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [
        "badchar",
        "tab\there",
        "newline\nok",
        "clean",
    ]


def test_strip_bom_removes_only_leading() -> None:
    table = _t(["﻿text", "no-bom", "﻿two﻿first"])
    new_table, _ = StripBom().run_arrow(table, _ctx())
    # lstrip strips runs of BOM at the start; mid-string BOMs survive
    assert new_table.column("name").to_pylist() == [
        "text",
        "no-bom",
        "two﻿first",
    ]


def test_normalize_unicode_default_nfc() -> None:
    """é can be NFD (e + combining acute) or NFC (single codepoint).
    Default NFC composes; both representations should compare equal post-normalize."""
    nfd = "é"  # 'e' + combining acute
    nfc = "é"  # precomposed 'é'
    table = _t([nfd])
    new_table, _ = NormalizeUnicode().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [nfc]


def test_normalize_line_endings_to_lf() -> None:
    table = _t(["crlf\r\nthere", "cr\rsolo", "lf\nok", "clean"])
    new_table, _ = NormalizeLineEndings().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [
        "crlf\nthere",
        "cr\nsolo",
        "lf\nok",
        "clean",
    ]


def test_strip_zero_width_default_off_but_works_when_invoked() -> None:
    table = _t(["zwsp\u200btext", "zwj‍here", "clean"])
    new_table, _ = StripZeroWidth().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == ["zwsptext", "zwjhere", "clean"]


def test_decode_html_entities_decodes_common() -> None:
    table = _t(["a&amp;b", "&lt;tag&gt;", "no-entities"])
    new_table, _ = DecodeHtmlEntities().run_arrow(table, _ctx())
    assert new_table.column("name").to_pylist() == [
        "a&b",
        "<tag>",
        "no-entities",
    ]


def test_processors_skip_non_string_columns() -> None:
    """Hygiene processors only target STRING columns; INT cols pass through."""
    int_ctx = _ctx(data_type="INT")
    table = pa.table({"name": pa.array([1, 2, 3], type=pa.int32())})
    new_table, result = TrimWhitespace().run_arrow(table, int_ctx)
    # applies_to is False so caller skips; if invoked anyway, _apply_string_op
    # still returns table unchanged because the column isn't string-typed
    assert TrimWhitespace().applies_to(int_ctx) is False
    assert new_table.column("name").to_pylist() == [1, 2, 3]
    assert result.fired is False
