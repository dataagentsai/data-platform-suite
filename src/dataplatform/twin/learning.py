"""The learning loop (G10): every production failure becomes one more scenario.

From a failed run's report (the store's run_report: the run, its dataset runs, steps and errors) this writes a
scenario file that names where the failure came from. It starts pending: it waits for a calendar entry in the world
that reproduces the failure (someone writes the incident's `params`), and for the layer that must catch it. Once the
entry exists, the scenario's `incident` is set and it runs like the planted ones. Nothing is lost between the
incident review and the test suite.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from dataplatform.twin.scenario import API_VERSION, FOLDER, parse

LEARNED = FOLDER / "learned"
LAYER_OF_STEP = (("land", "ingestion"), ("bronze", "ingestion"), ("contract", "ingestion"), ("silver", "silver"),
                 ("gold", "gold"), ("quality", "des"), ("recon", "des"), ("profile", "des"))


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60]


def layer_for(step: str) -> str:
    head = step.split(".", 1)[0]
    for prefix, layer in LAYER_OF_STEP:
        if head.startswith(prefix):
            return layer
    return "ingestion"


def failures(report: dict[str, Any]) -> list[dict[str, Any]]:
    """What failed in one run: failed or blocked steps, then errors, then failed dataset runs with no step to show."""
    out = [{"source": s["source"], "dataset": s["dataset"], "step": s["step"], "error_class": "StepFailed",
            "message": s["message"]} for s in report.get("steps", []) if s["status"] == "failed"]
    out += [{"source": e["source"], "dataset": e["dataset"], "step": e["step"], "error_class": e["error_class"],
             "message": e["message"]} for e in report.get("errors", [])]
    named = {(f["source"], f["dataset"]) for f in out}
    out += [{"source": d["source"], "dataset": d["dataset"], "step": "dataset", "error_class": "DatasetFailed",
             "message": d["message"]} for d in report.get("datasets", [])
            if d["status"] == "failed" and (d["source"], d["dataset"]) not in named]
    return out


def learned_scenario(report: dict[str, Any], failure: dict[str, Any], world: str) -> dict[str, Any]:
    run_id = report["run"]["run_id"]
    where = f"{failure['source']}.{failure['dataset']}" if failure.get("source") else "the run"
    sid = _slug(f"learned {where} {failure['error_class']} {run_id[:8]}")
    doc = {
        "apiVersion": API_VERSION,
        "scenario": sid,
        "incident": None,
        "story": "learned",
        "world": world,
        "origin": {"run_id": run_id, "env": report["run"]["env"], **failure},
        "discharges": ["TEST-05"],
        "checks": [
            {"id": "caught", "layer": layer_for(str(failure["step"])), "kind": "engine",
             "says": f"the platform catches this again: {failure['error_class']} at step {failure['step']} on "
                     f"{where}"},
        ],
    }
    parse(doc, sid)  # refuse to write a scenario the runner could not load
    return doc


def write_learned(report: dict[str, Any], world: str, folder: Path = LEARNED) -> list[Path]:
    """One scenario file per failure in the run; an existing file is left as it is (someone may have edited it)."""
    import yaml  # noqa: PLC0415 - local only, as in manifest.py

    folder.mkdir(parents=True, exist_ok=True)
    written = []
    for failure in failures(report):
        doc = learned_scenario(report, failure, world)
        path = folder / f"{doc['scenario']}.yaml"
        if not path.exists():
            path.write_text("# Learned from a production failure (G10 learning loop). Pending until the world has a "
                            "calendar entry that reproduces it.\n" + yaml.safe_dump(doc, sort_keys=False))
            written.append(path)
    return written
