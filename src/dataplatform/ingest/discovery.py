"""Source discovery (B3): before a feed is onboarded, look at what actually arrives. For the client's own drops (format
unknown, history completeness unknown) this answers: which formats, which windows, how many files and rows a day,
and which days are missing. Read-only, standard library; the format is read from the bytes, not the file name.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

MAGIC = (
    (b"PAR1", "parquet"),
    (b"Obj\x01", "avro"),
    (b"ORC", "orc"),
    (b"\x1f\x8b", "gzip"),
    (b"PK\x03\x04", "zip"),
)


def sniff(head: bytes) -> str:
    """The format of a file from its first bytes: parquet, avro, orc, gzip, zip, json_lines, csv, or unknown."""
    for magic, name in MAGIC:
        if head.startswith(magic):
            return name
    text = head.lstrip()[:1]
    if text in (b"{", b"["):
        return "json_lines"
    try:
        first = head.decode("utf-8").splitlines()[0] if head else ""
    except UnicodeDecodeError:
        return "unknown"
    return "csv" if any(d in first for d in (",", "|", "\t")) else "unknown"


@dataclass
class Discovery:
    formats: Counter[str] = field(default_factory=Counter)
    files_per_day: dict[str, int] = field(default_factory=dict)
    rows_per_day: dict[str, int] = field(default_factory=dict)
    missing_days: list[str] = field(default_factory=list)

    @property
    def first(self) -> str:
        return min(self.files_per_day) if self.files_per_day else ""

    @property
    def last(self) -> str:
        return max(self.files_per_day) if self.files_per_day else ""


def discover(files: list[tuple[str, bytes, int | None]]) -> Discovery:
    """`files` = (window, first bytes, declared rows or None) for every delivery seen. Days are the windows' first
    ten characters; a day between the first and the last with no file is missing."""
    d = Discovery()
    for window, head, rows in files:
        day = window[:10]
        d.formats[sniff(head)] += 1
        d.files_per_day[day] = d.files_per_day.get(day, 0) + 1
        d.rows_per_day[day] = d.rows_per_day.get(day, 0) + (rows or 0)
    if d.files_per_day:
        start, end = date.fromisoformat(d.first), date.fromisoformat(d.last)
        days = (end - start).days
        d.missing_days = [str(start + timedelta(n)) for n in range(days + 1)
                          if str(start + timedelta(n)) not in d.files_per_day]
    return d
