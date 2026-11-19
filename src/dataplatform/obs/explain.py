"""A run explained in plain words, deterministically: the run's report and ops tables in, a Markdown narrative out.

No model, no network, no clock: the same files give the same text, so the text can be diffed between runs and checked in
a test. Every sentence comes from a template below and a number the run recorded. Nothing is guessed: a count that is
zero says nothing, and a layer the run did not judge is said not to have been judged.

    explain(report, tables, modes) -> str
        report = report.json, tables = ops_tables.json, modes = {dataset: "push/files"}

The narrative follows one day at a time, in the order the platform ran it: what the world planted, ingestion, silver,
gold, and the verdicts. Each layer ends with where its code starts and the page that explains it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

SITE = "https://cleandataengineering.ai/learn/data-platform-suite/"
STARTS = {  # layer -> (what it is, where the code starts, the explainer page)
    "ingestion": ("landing or source to bronze", "ingest/runner.py: ingest", "ingest-runner"),
    "silver": ("bronze to silver", "silver/runner.py: build_silver", "silver"),
    "gold": ("silver to gold", "gold/harness.py: run_gold", "gold"),
}
# (key in a dataset's counts, template). Only a non-zero count produces a sentence.
INGEST_FACTS: list[tuple[str, str]] = [
    ("loaded", "{n} file(s) loaded"),
    ("skipped_duplicate", "{n} file(s) skipped because they had been loaded before"),
    ("rejected_contract", "{n} file(s) rejected because they broke the contract"),
    ("rejected_volume", "{n} file(s) rejected because their size was far from normal"),
    ("held_mass_delete", "{n} copy(ies) held because too many keys were missing"),
]
SILVER_FACTS: list[tuple[str, str]] = [
    ("batches", "{n} batch(es) taken"),
    ("rows_in", "{n} row(s) in"),
    ("duplicates.dropped", "{n} duplicate(s) dropped"),
    ("duplicates.replayed", "{n} replayed row(s) dropped"),
    ("quality.flagged", "{n} row(s) flagged"),
    ("quality.critical", "{n} row(s) broke a critical rule, so the batch was held"),
    ("rows_removed", "{n} row(s) removed"),
    ("rows_written", "{n} row(s) written"),
]
GOLD_FACTS: list[tuple[str, str]] = [
    ("members", "{n} member(s)"),
    ("rows", "{n} row(s)"),
    ("orphan_rows", "{n} row(s) on the unknown member"),
    ("windows_rebuilt", "{n} window(s) rebuilt"),
    ("windows_replaced", "{n} window(s) replaced"),
    ("windows_incomplete", "{n} window(s) left incomplete because an input had not caught up"),
]
VERDICT_WORDS = {
    "handled": "handled",
    "flagged": "flagged for the next layer",
    "deferred": "left to a later layer",
    "missed": "MISSED",
}


def facts(counts: Mapping[str, Any], table: list[tuple[str, str]]) -> str:
    """The non-zero facts of one dataset or table, in the table's order."""
    parts = [t.format(n=f"{int(counts[k]):,}") for k, t in table if counts.get(k)]
    return ", ".join(parts) if parts else "nothing to do"


def sentence(subject: str, status: str, said: str) -> str:
    return f"- **{subject}** ({status}): {said}."


def ingest_lines(day: Mapping[str, Any], modes: Mapping[str, str]) -> list[str]:
    out = []
    for d in sorted(day["datasets"], key=lambda x: x["dataset"]):
        how = modes.get(d["dataset"], "")
        said = facts(d["counts"], INGEST_FACTS)
        rows = int(d["counts"].get("rows_loaded", 0))
        said += f", {rows:,} row(s) into bronze" if rows else ""
        out.append(sentence(d["dataset"] + (f" [{how}]" if how else ""), d["status"], said))
    return out


def silver_lines(day: Mapping[str, Any]) -> list[str]:
    return [
        sentence(
            f"{d['source']}.{d['dataset']}",
            d["status"],
            facts(d["counts"], SILVER_FACTS) + (f" in {d['seconds']} s" if d.get("seconds") else ""),
        )
        for d in sorted(day["datasets"], key=lambda x: (x["source"], x["dataset"]))
    ]


def gold_lines(day: Mapping[str, Any]) -> list[str]:
    out = []
    for name, t in sorted(day["tables"].items()):
        said = facts(t["counts"], GOLD_FACTS)
        if t.get("restated"):
            said += "; restated " + ", ".join(f"{r['window']} ({r['detector']})" for r in t["restated"])
        out.append(sentence(name, t["status"], said))
    return out


def differences(measures: Mapping[str, Mapping[str, Mapping[str, float]]], side: str) -> list[str]:
    return [
        f"{ds}.{m}: {v[side]:g} against the truth's {v['oracle']:g}"
        for ds, ms in sorted(measures.items())
        for m, v in sorted(ms.items())
        if v[side] != v["oracle"]
    ]


def bronze_differences(day: Mapping[str, Any]) -> list[str]:
    return [
        f"{ds}: {v['bronze']:,} against the truth's {v['oracle']:,}"
        for ds, v in sorted(day["bronze"].items())
        if v["bronze"] != v["oracle"]
    ]


def verdict_lines(days: list[Mapping[str, Any]], layer: str) -> list[str]:
    return [
        f"- {layer}, {v['incident']} ({v['detector']}, {v['dataset']}): "
        f"**{VERDICT_WORDS.get(v['verdict'], v['verdict'])}**"
        f" {v['evidence']}"
        for d in days
        for v in d.get("incidents", [])
    ]


def held_lines(tables: Mapping[str, Any], day: str) -> list[str]:
    cols = [c[0] for c in tables.get("ops_files_held", {}).get("columns", [])]
    rows = [dict(zip(cols, r, strict=False)) for r in tables.get("ops_files_held", {}).get("rows", [])]
    return [
        f"- held: {r['file_name']} of {r['dataset']} ({r['status']}): {r['reason']}" for r in rows if r["day"] == day
    ]


def layer_block(layer: str, lines: list[str], differing: list[str], verdicts: list[str]) -> str:
    what, start, page = STARTS[layer]
    head = f"### {layer.capitalize()}: {what}"
    judged = (
        "Equal to the truth."
        if not differing
        else "Differs from the truth:\n" + "\n".join(f"  - {x}" for x in differing)
    )
    tail = f"_Code starts at `{start}`. Explained at {SITE}{page}.html_"
    return "\n".join([head, *lines, "", judged, *(["", *verdicts] if verdicts else []), "", tail, ""])


def explain(report: Mapping[str, Any], tables: Mapping[str, Any], modes: Mapping[str, str] | None = None) -> str:
    modes = modes or {}
    out = [
        f"# One run, explained: {report['days']} day(s) at scale {report['scale']} "
        f"on {report.get('engine') or 'no engine'}",
        "",
        "Each day is told in the order the platform ran it. Every number was recorded by the run.",
        "",
    ]
    layers: list[
        tuple[str, str, Callable[[Mapping[str, Any]], list[str]], Callable[[Mapping[str, Any]], list[str]]]
    ] = [
        ("ingestion", "per_day", lambda d: ingest_lines(d, modes), bronze_differences),
        ("silver", "silver", silver_lines, lambda d: differences(d["measures"], "silver")),
        ("gold", "gold", gold_lines, lambda d: differences(d["measures"], "gold")),
    ]
    for n in range(1, int(report["days"]) + 1):
        date = report["per_day"][n - 1]["date"]
        out += [f"## Day {n}, {date}", ""]
        for layer, key, lines, diff in layers:
            days = report.get(key) or []
            day = next((d for d in days if d["day"] == n), None)
            if day is None:
                out += [f"### {layer.capitalize()}", f"This run did not judge {layer}.", ""]
                continue
            block = layer_block(layer, lines(day), diff(day), verdict_lines([day], layer))
            out.append(block)
        held = held_lines(tables, date)
        if held:
            out += ["### Files held or skipped", *held, ""]
    ok = {k: report.get(f"{k}_equals_oracle") for k in ("bronze", "silver", "gold")}
    out += ["## Overall", "", *[f"- {k} equals the truth on every day: {v}" for k, v in ok.items()], ""]
    return "\n".join(out)
