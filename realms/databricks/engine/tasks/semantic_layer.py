"""After a proof run: create the business metric views (the semantic generator's committed SQL for this environment)
and ask each certified known answer of the DEPLOYED views, the way a dashboard tile or a Genie answer would.

A view's number must equal both the same question asked of gold in Python (semantic/answers.py, the run's
gold_known_answers) and the world's truth (the run's known_answers, `world`). A number the world does not have yet
(a window not complete inside a short run, a day after the run) must be empty in the view too. The spec's own values
hold at scale 1.0 after 14 days, so they are reported, not judged here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

TOLERANCE = 1e-6


def _statements(text: str) -> str:
    return "\n".join(ln for ln in text.splitlines() if not ln.startswith("--")).strip().rstrip(";")


def create_views(spark: Any, generated: Path) -> list[str]:
    notes = []
    for path in sorted((generated / "semantic").glob("mv_*.sql")):
        spark.sql(_statements(path.read_text()))
        notes.append(f"{path.stem}: metric view created")
    return notes


def _same(a: float | None, b: float | None) -> bool:
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= TOLERANCE * max(1, abs(b)))


def check(spark: Any, generated: Path, report: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per certified known answer: the deployed view's number, gold's (Python) and the world's."""
    rows = spark.sql(_statements((generated / "semantic" / "known_answers.sql").read_text())).collect()
    gold = report.get("gold_known_answers") or {}
    world = report.get("known_answers") or {}
    out = []
    for r in rows:
        view, measure = str(r["view_name"]), str(r["measure"])
        got = None if r["got"] is None else float(r["got"])
        g = next((v for k, v in gold.items() if k.startswith(f"{view}|{measure}|")), None)
        base = view.removesuffix("_preview")  # a preview view answers the certified view's question
        w = next((v.get("world") for k, v in world.items() if k.startswith(f"{base} {measure} ")), None)
        ok = _same(got, g) and _same(got, None if w is None else float(w))
        out.append({"view": view, "measure": measure, "address": str(r["address"]), "view_value": got,
                    "gold_value": g, "world_value": w, "spec_value": float(r["expected"]), "ok": ok})
    return out
