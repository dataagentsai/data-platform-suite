"""Source discovery (B3): formats from bytes, days present and missing; table-driven."""

from __future__ import annotations

import pytest

from dataplatform.ingest.discovery import discover, sniff

SNIFF_CASES = [
    ("parquet", b"PAR1\x15\x04", "parquet"),
    ("avro", b"Obj\x01\x04\x14avro", "avro"),
    ("gzip", b"\x1f\x8b\x08\x00", "gzip"),
    ("json lines", b'{"a": 1}\n{"a": 2}', "json_lines"),
    ("csv", b"id,qty\n1,2\n", "csv"),
    ("pipe", b"id|qty\n", "csv"),
    ("empty", b"", "unknown"),
    ("binary noise", b"\xff\xfe\x00", "unknown"),
]


@pytest.mark.parametrize(("case", "head", "want"), SNIFF_CASES, ids=[c[0] for c in SNIFF_CASES])
def test_sniff(case: str, head: bytes, want: str) -> None:
    assert sniff(head) == want


def test_discover_finds_formats_and_missing_days() -> None:
    d = discover([("2026-11-01", b"PAR1", 100), ("2026-11-01", b"PAR1", 50), ("2026-11-02", b"PAR1", 120),
                  ("2026-11-05T10", b'{"a":1}', None)])
    assert dict(d.formats) == {"parquet": 3, "json_lines": 1}
    assert d.files_per_day == {"2026-11-01": 2, "2026-11-02": 1, "2026-11-05": 1}
    assert d.rows_per_day["2026-11-01"] == 150
    assert d.missing_days == ["2026-11-03", "2026-11-04"]
    assert (d.first, d.last) == ("2026-11-01", "2026-11-05")


def test_discover_nothing() -> None:
    d = discover([])
    assert (d.first, d.missing_days) == ("", [])
