"""Profiling (X5, DPC-0051, B9): every loaded batch profiled at the lander, the dataset run's profile compared with
the last one, and drift reported in the run log. Standard library only.

    profile_rows    rows, then per column: nulls, distinct values (exact up to DISTINCT_CAP), min and max for numbers,
                    dates and timestamps. Text columns get counts only, so a profile never holds a value from a text
                    column (personal data is tokenised before this, but a profile should not need to rely on that).
    merge           the batches of one run into one profile
    drift           what changed against the last profile: a column's null share jumping, volume swinging, a column
                    appearing or going quiet
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

DISTINCT_CAP = 10_000
ORDERED = ("integer", "number", "date", "timestamp")


def _text(v: Any) -> str:
    return v.isoformat() if isinstance(v, (date, datetime)) else str(v)


def profile_rows(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> dict[str, Any]:
    cols: dict[str, dict[str, Any]] = {}
    for name, logical in columns:
        values = [r.get(name) for r in rows]
        present = [v for v in values if v is not None and v != ""]
        seen: set[str] = set()
        for v in present:
            if len(seen) >= DISTINCT_CAP:
                break
            seen.add(_text(v))
        col: dict[str, Any] = {"nulls": len(values) - len(present), "distinct": len(seen)}
        if logical in ORDERED and present:
            col["min"], col["max"] = _text(min(present)), _text(max(present))
        cols[name] = col
    return {"rows": len(rows), "columns": cols}


def merge(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    """One profile for a run's batches: rows and nulls add up, distinct is the largest batch's (a lower bound), min and
    max widen."""
    out: dict[str, Any] = {"rows": 0, "columns": {}}
    for p in profiles:
        out["rows"] += p["rows"]
        for name, c in p["columns"].items():
            m = out["columns"].setdefault(name, {"nulls": 0, "distinct": 0})
            m["nulls"] += c["nulls"]
            m["distinct"] = max(m["distinct"], c["distinct"])
            for k, pick in (("min", min), ("max", max)):
                if k in c:
                    m[k] = pick(m[k], c[k]) if k in m else c[k]
    return out


def null_share(p: dict[str, Any], column: str) -> float:
    rows = p.get("rows") or 0
    return (p["columns"].get(column, {}).get("nulls", 0) / rows) if rows else 0.0


def drift(now: dict[str, Any], before: dict[str, Any] | None, null_jump: float = 0.2,
          volume_ratio: float = 3.0) -> list[str]:
    """Findings, each one plain sentence; empty when nothing moved beyond the limits or there is no earlier profile."""
    if not before or not before.get("rows") or not now.get("rows"):
        return []
    found = []
    ratio = now["rows"] / before["rows"]
    if ratio >= volume_ratio or ratio <= 1 / volume_ratio:
        found.append(f"volume moved from {before['rows']} to {now['rows']} rows")
    for name in sorted(now["columns"]):
        if name not in before["columns"]:
            found.append(f"{name} is new")
            continue
        a, b = null_share(before, name), null_share(now, name)
        if b - a >= null_jump:
            found.append(f"{name} empty in {b:.0%} of rows, was {a:.0%}")
    for name in sorted(set(before["columns"]) - set(now["columns"])):
        found.append(f"{name} no longer delivered")
    return found
