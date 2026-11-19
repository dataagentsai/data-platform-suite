"""The ops tables as a typed, portable file, so a realm can publish them where its dashboards read (B13).

The ops builder (obs/ops.py) makes plain rows in memory. A realm that serves dashboards from its catalog needs each
column's type, which plain rows only show by their values. `export` gives every table its columns and one type per
column (taken from the first value that is not empty; a column with no value at all is text), and values a file can
carry (times as ISO text). `restore` turns them back into Python values for the realm's writer.

    types   text | integer | decimal | flag | time | date
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

TYPES = ("text", "integer", "decimal", "flag", "time", "date")


def type_of(value: Any) -> str:
    if isinstance(value, bool):
        return "flag"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, datetime):
        return "time"
    if isinstance(value, date):
        return "date"
    return "text"


def column_types(rows: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """Columns in first-seen order, each typed by its first non-empty value; integer widens to decimal if mixed."""
    out: dict[str, str] = {}
    for row in rows:
        for col, value in row.items():
            if value is None:
                out.setdefault(col, "")
                continue
            seen, now = out.get(col, ""), type_of(value)
            out[col] = now if not seen or (seen, now) == ("integer", "decimal") else seen
    return [(c, t or "text") for c, t in out.items()]


def _carry(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind in ("time", "date"):
        return value.isoformat()
    if kind == "decimal":
        return float(value)
    if kind == "text" and not isinstance(value, str):
        return json.dumps(value, default=str, sort_keys=True) if isinstance(value, list | tuple | dict) else str(value)
    return value


def export(tables: dict[str, list[dict[str, Any]]],
           declared: dict[str, tuple[tuple[str, str], ...]] | None = None) -> dict[str, Any]:
    """Declared columns win (so an empty table keeps its shape); a column the rows have beyond them is added, typed
    by its values."""
    out: dict[str, Any] = {}
    for name in sorted(tables):
        cols = list((declared or {}).get(name, ()))
        cols += [c for c in column_types(tables[name]) if c[0] not in {d for d, _ in cols}]
        out[name] = {"columns": [list(c) for c in cols],
                     "rows": [[_carry(r.get(c), t) for c, t in cols] for r in tables[name]]}
    return out


def _back(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "time":
        return datetime.fromisoformat(value)
    if kind == "date":
        return date.fromisoformat(value)
    return value


def restore(table: dict[str, Any]) -> tuple[list[tuple[str, str]], list[tuple[Any, ...]]]:
    cols = [(c, t) for c, t in table["columns"]]
    return cols, [tuple(_back(v, t) for v, (_, t) in zip(row, cols, strict=True)) for row in table["rows"]]
