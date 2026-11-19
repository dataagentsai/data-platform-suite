"""End to end: every layer against the simulated company's true answers, day by day (the daily loop, DAC TEST-05).

The platform and the tyre world run together for a few days on a laptop (DuckDB, SQLite sources, the in-memory metadata
store, the compressed calendar that puts every incident in days 2 and 3): the same script a deployed proof job runs
(scripts/run_platform.py). Its report is then judged here, one table per question, so a failure names the layer and the
day. Set DP_E2E_REPORT to a report.json from another run (the 14-day proof on Databricks, say) and the same tables judge
that run instead of starting one.

Tables, not loops in the test body: LAYERS (which part of the report holds each layer's verdicts), INCIDENT_LAYERS
(each layer's incident verdicts must all be handled), RUNS (every layer's run succeeded each day).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
DAYS, SCALE = 3, 0.1  # the 3-day loop; below about 0.1 the compressed calendar cannot find enough lines to delete
pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def report(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    given = os.environ.get("DP_E2E_REPORT")
    if given:
        return json.loads(Path(given).read_text())
    pytest.importorskip("deltalake")
    pytest.importorskip("sqlframe")
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog")
    out = tmp_path_factory.mktemp("e2e")
    cmd = [sys.executable, str(ROOT / "scripts/run_platform.py"), "--days", str(DAYS), "--scale", str(SCALE),
           "--db", "sqlite", "--meta", "memory", "--engine", "duckdb", "--calendar", "compressed", "--out", str(out)]
    # a fixed argument list built from this file's own constants, no shell; a failed run is judged by its report below
    done = subprocess.run(cmd, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src")},  # noqa: S603
                          capture_output=True, text=True, timeout=1800, check=False)
    tail = f"{done.stdout[-2000:]}\n{done.stderr[-2000:]}"
    assert (out / "report.json").exists(), f"the run wrote no report (exit {done.returncode}):\n{tail}"
    return json.loads((out / "report.json").read_text())


def differences(measures: dict[str, dict[str, dict[str, float]]], side: str) -> list[str]:
    """Every measure whose value differs from the oracle's, named dataset.measure."""
    return [f"{ds}.{m}: {v[side]} != oracle {v['oracle']}" for ds, ms in measures.items() for m, v in ms.items()
            if v[side] != v["oracle"]]


def bronze_differences(day: dict[str, Any]) -> list[str]:
    return [f"{ds}: {v['bronze']} != oracle {v['oracle']}" for ds, v in day["bronze"].items()
            if v["bronze"] != v["oracle"]]


# (layer, the report's list of days, how a day's mismatches are listed)
LAYERS = [
    ("bronze", "per_day", bronze_differences),
    ("silver", "silver", lambda d: differences(d["measures"], "silver")),
    ("gold", "gold", lambda d: differences(d["measures"], "gold")),
]


@pytest.mark.parametrize(("layer", "key", "mismatches"), LAYERS, ids=[x[0] for x in LAYERS])
def test_the_layer_equals_the_oracle_every_day(report, layer, key, mismatches):
    days = report.get(key) or []
    assert days, f"the report has no {layer} days: that layer did not run"
    bad = {f"day {d['day']}": m for d in days if (m := mismatches(d))}
    assert bad == {}, f"{layer} differs from the oracle"


INCIDENT_LAYERS = [("ingestion", "per_day"), ("silver", "silver"), ("gold", "gold")]


@pytest.mark.parametrize(("layer", "key"), INCIDENT_LAYERS, ids=[x[0] for x in INCIDENT_LAYERS])
def test_every_incident_the_layer_judges_is_handled(report, layer, key):
    verdicts = [(d["day"], v) for d in report.get(key) or [] for v in d.get("incidents", [])]
    bad = [f"day {day}: {v['incident']} ({v['detector']}) {v['verdict']}: {v['evidence']}" for day, v in verdicts
           if v["verdict"] not in ("handled", "flagged", "deferred")]
    assert bad == [], f"incidents the {layer} layer missed"


def test_incidents_were_actually_planted(report):
    """A green run that planted nothing proves nothing: every kind of fault must be judged in the run."""
    judged = {v["incident"] for key in ("per_day", "silver", "gold") for d in report.get(key) or []
              for v in d.get("incidents", [])}
    assert len(judged) >= 11, f"only {len(judged)} kinds of incident were judged: {sorted(judged)}"


RUNS = [("ingestion", "per_day", "run"), ("silver", "silver", "run"), ("gold", "gold", "run")]


@pytest.mark.parametrize(("layer", "key", "field"), RUNS, ids=[x[0] for x in RUNS])
def test_every_run_succeeded(report, layer, key, field):
    bad = {f"day {d['day']}": d[field] for d in report.get(key) or [] if d[field] not in ("succeeded", "partial")}
    assert bad == {}, f"{layer} runs that did not succeed"


def test_the_known_answers_match_the_specification_at_full_scale(report):
    """The specification's known answers are for scale 1.0 after 14 days; a shorter or smaller run cannot meet them, and
    its layers are judged against the oracle day by day instead (the first table)."""
    if report["days"] < 14 or report["scale"] < 1.0:
        run = f"{report['days']} days at {report['scale']}"
        pytest.skip(f"known answers hold at scale 1.0 after 14 days; this run: {run}")
    bad = {k: v for k, v in (report.get("known_answers") or {}).items() if not v["match"]}
    assert bad == {}, "a known answer differs from the simulated company's value"


def test_the_metrics_were_asked_of_built_gold(report):
    answers = report.get("gold_known_answers") or {}
    assert answers, "no metric was asked a known question of built gold"
    assert any(v is not None for v in answers.values()), "every metric answered nothing"
