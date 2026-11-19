"""The local ops page (B10): one self-contained HTML file written after a local run, so a person can see what happened
without a workspace. It reads only the ops tables (obs/ops.py, the same rows the ops metric views and dashboards
read) plus the run's incident verdicts and per-layer checks against the oracle. Pure: rows in, HTML text out.
White page, no scripts, no external files; opens from disk in any browser.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from html import escape
from typing import Any

Row = dict[str, Any]

CSS = """
body{background:#fff;color:#1d2433;font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;margin:0;padding:24px 32px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:28px 0 8px;border-bottom:1px solid #e3e7ee;padding-bottom:4px}
.sub{color:#5b6475;margin-bottom:16px}.tiles{display:flex;flex-wrap:wrap;gap:12px;margin:12px 0}
.tile{border:1px solid #e3e7ee;border-radius:8px;padding:10px 14px;min-width:130px}
.tile b{display:block;font-size:22px}.tile span{color:#5b6475;font-size:12px}
table{border-collapse:collapse;width:100%;margin:6px 0 4px;font-size:13px}
th{text-align:left;background:#f5f7fa;border-bottom:1px solid #d9dee7;padding:5px 8px;white-space:nowrap}
td{border-bottom:1px solid #eef1f5;padding:4px 8px;vertical-align:top}
.ok{color:#1a7f37;font-weight:600}.warn{color:#9a6700;font-weight:600}.bad{color:#cf222e;font-weight:600}
.note{color:#5b6475;font-size:12px}code{background:#f5f7fa;padding:0 3px;border-radius:3px}
"""
GOOD = {"succeeded", "handled", "loaded", "ok", "true", "met", "resolved"}
SOFT = {"warned", "partial", "flagged", "deferred", "skipped", "skipped_duplicate", "grouped", "snoozed"}


def _cls(value: Any) -> str:
    v = str(value).lower()
    if v in GOOD:
        return "ok"
    if v in SOFT:
        return "warn"
    if v in {"failed", "missed", "false", "error", "fired", "rejected_contract", "rejected_volume",
             "held_mass_delete", "breached"}:
        return "bad"
    return ""


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        value = round(value, 1)
    text = escape(str(value))
    c = _cls(value)
    return f'<td class="{c}">{text}</td>' if c else f"<td>{text}</td>"


def table(rows: Sequence[Row], columns: Sequence[str] | None = None, limit: int = 200) -> str:
    """Rows as an HTML table; columns default to every key of the first row. More than `limit` rows are cut, and the
    cut is said."""
    if not rows:
        return '<p class="note">Nothing recorded.</p>'
    cols = list(columns or rows[0].keys())
    head = "".join(f"<th>{escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(_cell(r.get(c)) for c in cols) + "</tr>" for r in rows[:limit])
    more = f'<p class="note">{len(rows) - limit} more rows not shown.</p>' if len(rows) > limit else ""
    return f"<table><tr>{head}</tr>{body}</table>{more}"


def _tile(value: Any, label: str, cls: str = "") -> str:
    return f'<div class="tile"><b class="{cls}">{escape(str(value))}</b><span>{escape(label)}</span></div>'


def _sum(rows: Iterable[Row], key: str) -> float:
    return sum(float(r.get(key) or 0) for r in rows)


def render(tables: dict[str, list[Row]], title: str, subtitle: str, layer_checks: Sequence[Row] = (),  # noqa: PLR0917
           incidents: Sequence[Row] = (), hops: Sequence[Row] = ()) -> str:
    """The page. `layer_checks`: {day, layer, equals_oracle}; `incidents`: {day, layer, incident, detector, dataset,
    verdict, evidence}; `hops`: {dataset, hop, n, p50, p95, p99, basis}."""
    runs, ds_runs = tables.get("ops_runs", []), tables.get("ops_dataset_runs", [])
    held, alerts = tables.get("ops_files_held", []), tables.get("ops_alerts", [])
    failed = sum(1 for r in ds_runs if r.get("status") == "failed")
    checks_ok = all(bool(c.get("equals_oracle")) for c in layer_checks) if layer_checks else None
    missed = sum(1 for i in incidents if str(i.get("verdict")).lower() == "missed")
    warn_steps = [r for r in tables.get("ops_steps", []) if str(r.get("status")) in ("warned", "failed")]
    by_step: dict[str, float] = {}
    for r in tables.get("ops_step_durations", []):
        by_step[str(r.get("step"))] = by_step.get(str(r.get("step")), 0.0) + float(r.get("duration_s") or 0)
    slowest = [{"step": k, "seconds": v} for k, v in sorted(by_step.items(), key=lambda kv: -kv[1])[:12]]
    tiles = "".join([
        _tile(len(runs), "runs"),
        _tile(len(ds_runs), "dataset runs"),
        _tile(failed, "dataset runs failed", "bad" if failed else "ok"),
        _tile(int(_sum(tables.get("ops_ingestion_daily", []), "rows_loaded")), "rows loaded"),
        _tile(len(held), "files held or skipped", "warn" if held else "ok"),
        _tile(len(alerts), "alert events", "warn" if alerts else "ok"),
        _tile(f"{len(incidents) - missed}/{len(incidents)}", "planted incidents caught", "bad" if missed else "ok"),
        _tile("yes" if checks_ok else ("no" if checks_ok is False else "n/a"), "every layer equals the oracle",
              "ok" if checks_ok else "bad"),
    ])
    parts = [
        f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
        f"<title>{escape(title)}</title><style>{CSS}</style></head><body>",
        f"<h1>{escape(title)}</h1><div class='sub'>{escape(subtitle)}</div><div class='tiles'>{tiles}</div>",
        "<h2>Every layer against the world's own totals</h2>",
        table(layer_checks, ("day", "layer", "equals_oracle")),
        "<h2>Planted incidents and whether the platform caught them</h2>",
        table(incidents, ("day", "layer", "incident", "detector", "dataset", "verdict", "evidence")),
        "<h2>Runs</h2>", table(runs),
        "<h2>Dataset runs</h2>", table(ds_runs),
        "<h2>Where the time goes (seconds per step, all runs)</h2>", table(slowest, ("step", "seconds")),
        "<p class='note'>Time from the previous record to this step's record. Silver records its stages together "
        "after the run, so a silver run's whole time shows on its first stage.</p>",
        "<h2>Delay per hop (ADR-034)</h2>",
        table(hops, ("dataset", "hop", "n", "p50", "p95", "p99", "basis")),
        "<h2>Files held or skipped, with the reason</h2>", table(held),
        "<h2>Steps that warned or failed (quality, profile drift, detectors)</h2>", table(warn_steps),
        "<h2>Ingestion per day</h2>", table(tables.get("ops_ingestion_daily", [])),
        "<h2>Freshness checks</h2>", table(tables.get("ops_freshness", [])),
        "<h2>Alerts</h2>", table(alerts),
        "<p class='note'>Built from the ops tables (obs/ops.py), the same rows the ops metric views and dashboards "
        "read in the platform. Numbers are measured on this run unless a row says modeled.</p></body></html>",
    ]
    return "".join(parts)
