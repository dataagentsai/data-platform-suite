"""A dataset's metadata and the repository's checks explained in plain words, deterministically: no model, no network.

    explain_dataset(manifest, contract) -> str     what the manifest says the platform will do with this dataset
    explain_cicd(help_lines, hooks, workflows)     what a change passes through before it is trusted

Every sentence is a template filled from a value the file holds. A setting that is absent says nothing, and a value the
templates do not know is printed as written, never guessed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dataplatform import contract as contract_doc
from dataplatform.silver.plan import HISTORY_BY_KIND

HOW_IT_ARRIVES = {
    (
        "push",
        "change_feed",
    ): "The source's change capture writes change files to landing, and the platform takes them to bronze.",
    ("push", "files"): "The source drops files into a delivery folder, and the platform takes them to bronze.",
    ("pull", "full_copy"): "The platform reads the whole table from the source on every run.",
    ("pull", "pages"): "The platform asks the source's interface for pages of records until it has them all.",
    (
        "stream",
        "events",
    ): "The platform reads the events that have arrived since its last position, then stops. "
    "It never runs continuously.",
}
HISTORY_WORDS = {
    "scd2": "keeps every version of a key, so any past day can be answered (type 2)",
    "scd1": "keeps only the latest state of a key (type 1)",
    "append": "appends each row once and never changes it",
    "snapshot": "treats each file as the whole of its window, and the newest file for a window replaces the older rows",
    "source_versioned": "keeps the versions the source already has, and checks them instead of rebuilding them",
}
DEDUP_WORDS = {
    "business_key_latest": "When a key appears more than once in a batch, the row with the highest order value is "
    "kept.",
    "business_key_first": "When a key appears more than once, the first row is kept and the later ones are dropped "
    "as replays.",
    "full_row": "A row is a duplicate only when every column is the same.",
    "none": "Rows are not de-duplicated.",
}
DETECT_WORDS = {
    "change_feed": "the change feed says so with a delete row",
    "key_compare": "a key that is in silver but missing from a new full copy is treated as deleted",
    "none": "no delete is detected",
}
APPLY_WORDS = {
    "mark_deleted": "the row stays in silver, flagged deleted, with the time of the delete",
    "remove": "the key's rows are removed from silver",
    "none": "deletes are ignored",
}


def listed(items: Sequence[Any]) -> str:
    items = [str(i) for i in items]
    return ", ".join(items[:-1]) + (" and " if len(items) > 1 else "") + items[-1] if items else ""


def _arrival(m: Mapping[str, Any]) -> list[str]:
    cfg, ing = m.get("config") or {}, m.get("ingest") or {}
    out = ["", "## How it gets in"]
    out.append(
        HOW_IT_ARRIVES.get(
            (ing.get("mode"), ing.get("read")),
            f"Mode {ing.get('mode', '?')}, read {ing.get('read', '?')}: no template for this combination.",
        )
    )
    if cfg.get("schedule_cron"):
        out.append(
            f"It is scheduled by {cfg.get('schedule_trigger', 'a')} trigger, `{cfg['schedule_cron']}`, "
            f"in {cfg.get('schedule_timezone', 'UTC')}."
        )
    if cfg.get("watermark_column"):
        out.append(
            f"Its watermark column is `{cfg['watermark_column']}`, "
            f"and the load type is {cfg.get('load_type', 'unstated')}."
        )
    if cfg.get("sla_freshness_hours"):
        out.append(
            f"The data is promised to be at most {cfg['sla_freshness_hours']} hour(s) old. "
            f"It is an error at {cfg.get('sla_freshness_error_hours', '?')} hour(s)."
        )
    return out


def _promise(contract: Mapping[str, Any] | None) -> list[str]:
    if not contract:
        return []
    cols = contract_doc.columns(dict(contract))
    key = listed([f"`{k}`" for k in contract_doc.primary_key(dict(contract))]) or "not stated"
    return ["", "## What it promises", f"The contract lists {len(cols)} column(s). The primary key is {key}."]


def _history(m: Mapping[str, Any]) -> list[str]:
    hist, load, dedup = m.get("history") or {}, m.get("load") or {}, m.get("dedup") or {}
    htype = str(hist.get("type") or HISTORY_BY_KIND.get(str(hist.get("kind") or ""), ""))
    out = [
        "",
        "## What happens to it in silver",
        f"Its history {HISTORY_WORDS.get(htype, 'is of a type this explainer has no words for: ' + htype)}.",
    ]
    if hist.get("track_columns"):
        cols = hist["track_columns"]
        verb = "changes" if len(cols) == 1 else "change"
        out.append(f"A new version opens only when {listed([f'`{c}`' for c in cols])} {verb}.")
    order = load.get("history_order_by") or dedup.get("order_by")
    if order:
        out.append(
            f"Changes are laid out in the order of `{order}`, never in the order they arrived, "
            "so a late change lands in its place."
        )
    if dedup.get("mode"):
        keys = listed([f"`{k}`" for k in dedup.get("business_keys") or []])
        out.append(
            DEDUP_WORDS.get(str(dedup["mode"]), f"Duplicate rule {dedup['mode']}.")
            + (f" The business key is {keys}." if keys else "")
        )
    return out


def _loading(m: Mapping[str, Any]) -> list[str]:
    load = m.get("load") or {}
    deletes = load.get("deletes") or {}
    out = []
    if deletes:
        out.append(
            f"Deletes: {DETECT_WORDS.get(str(deletes.get('detect')), str(deletes.get('detect')))}, and "
            f"{APPLY_WORDS.get(str(deletes.get('apply')), str(deletes.get('apply')))}."
        )
    if load.get("late_lookback_days") is not None:
        out.append(f"A replayed row is recognised for {load['late_lookback_days']} day(s) after it first arrived.")
    if load.get("new_columns"):
        out.append(f"A column that appears at the source is `{load['new_columns']}`.")
    if load.get("quarantine_stop_share") is not None:
        out.append(
            f"If more than {float(load['quarantine_stop_share']):.0%} of a batch is set aside, the batch is held."
        )
    return out


def _bad_rows(m: Mapping[str, Any]) -> list[str]:
    actions = (m.get("quality") or {}).get("actions") or {}
    if not actions:
        return []
    out = ["", "## What happens to bad rows"]
    for sev in sorted(actions):
        a = actions[sev] or {}
        out.append(
            f"- A {sev} rule failing: rows are **{a.get('rows', 'unstated')}**, "
            f"the dataset is **{a.get('dataset', 'unstated')}**{', and someone is paged' if a.get('page') else ''}."
        )
    return out


def _checked_and_owned(m: Mapping[str, Any]) -> list[str]:
    rec, owners = m.get("reconciliation") or {}, m.get("owners") or {}
    out = []
    if rec.get("measures"):
        out += [
            "",
            "## How it is checked between layers",
            f"Compared at {listed(rec.get('hops') or ['the stated hops'])}: "
            f"{listed([x['name'] for x in rec['measures']])}.",
        ]
        tol = rec.get("tolerance") or {}
        if tol:
            out.append("Tolerance: " + ", ".join(f"{k} {v}" for k, v in sorted(tol.items())) + ".")
    if owners:
        out += ["", "## Who owns it", "; ".join(f"{k}: {v}" for k, v in sorted(owners.items())) + "."]
    return out


def explain_dataset(manifest: Mapping[str, Any], contract: Mapping[str, Any] | None = None) -> str:
    """The Markdown narrative of one dataset manifest (and its contract, when given)."""
    name = f"{manifest.get('source_name', '?')}.{manifest.get('dataset_name', '?')}"
    hist = manifest.get("history") or {}
    kind = str(hist.get("kind") or "")
    htype = str(hist.get("type") or HISTORY_BY_KIND.get(kind, ""))
    out = [f"# {name}, explained", "", f"Read from its manifest. It is a **{kind or 'dataset of an unstated kind'}**."]
    out += _arrival(manifest) + _promise(contract) + _history(manifest) + _loading(manifest)
    out += _bad_rows(manifest) + _checked_and_owned(manifest)
    out += [
        "",
        "## Where to read the code that does this",
        "- The plan is built by `silver/plan.py: plan_from_view`, and refuses anything it cannot run.",
        f"- Its history is `stages/history.py: {htype}`." if htype else "- Its history type is not stated.",
        "- Ingestion for it is `ingest/runner.py: ingest`.",
    ]
    return "\n".join(out) + "\n"


def _triggers(wf: Mapping[str, Any]) -> list[str]:
    on = wf.get("on", wf.get(True, {}))  # a bare `on:` key is read as the boolean True by some YAML loaders
    if isinstance(on, Mapping):
        return sorted(on)
    return [on] if isinstance(on, str) else sorted(on or [])


def _step_name(step: Mapping[str, Any]) -> str:
    return step.get("name") or step.get("run", "").split("\n")[0][:60] or step.get("uses", "")


def _workflow(fname: str, wf: Mapping[str, Any]) -> list[str]:
    out = [f"### {wf.get('name', fname)} (`{fname}`), on {listed(_triggers(wf)) or 'no stated trigger'}"]
    for job, spec in sorted((wf.get("jobs") or {}).items()):
        steps = [_step_name(s) for s in spec.get("steps") or []]
        out.append(f"- **{spec.get('name', job)}**: " + ("; ".join(x for x in steps if x) or "no steps"))
    return out


def explain_cicd(
    help_lines: Sequence[str], hooks: Sequence[tuple[str, str]], workflows: Mapping[str, Mapping[str, Any]]
) -> str:
    """The checks a change passes. help_lines: the Makefile's `make <target> <what it does>` lines; hooks: (repo,
    hook id) pairs; workflows: file name -> parsed workflow."""
    out = ["# What a change passes through", "", "## On your machine, with make"]
    out += [f"- `{line}`" for line in help_lines] or ["- no make targets are described in this checkout"]
    out += ["", "## On commit (pre-commit hooks)"]
    out += [f"- `{hid}` from {repo}" for repo, hid in hooks] or ["- no hooks are configured in this checkout"]
    out += ["", "## On push or pull request (workflows)"]
    out += [x for fname in sorted(workflows) for x in _workflow(fname, workflows[fname])] or [
        "- no workflow files are in this checkout"
    ]
    return "\n".join(out) + "\n"
