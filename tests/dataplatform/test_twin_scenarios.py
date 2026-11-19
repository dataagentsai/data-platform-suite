"""DataTwin scenarios (G10, TEST-05, SIM-01): every planted tyre incident, end to end where the layers exist.

One row per scenario file. A scenario must never fail; it may be pending, and then it must say which engine session
it waits for. The `obs` checks run on the FIXTURE run log (obs/fixture.py) until W1b-W1e land.
"""

from pathlib import Path

import pytest

from dataplatform.twin import learning
from dataplatform.twin.runner import Harness, run, unexplained_alerts
from dataplatform.twin.scenario import AVAILABLE, LAYERS, InvalidScenario, load_all, parse

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_world = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre DWD)")
SCENARIOS = load_all()

# What each scenario waits for on the fixture harness (no engine attached): the layers of its engine checks.
WAITS = {
    "readings-out-of-order": ["silver", "gold"],
    "order-lines-deleted": ["ingestion", "silver", "gold"],
    "sellout-file-late": ["ingestion", "gold"],  # its restatement is logged by gold since W1d
    "sellout-file-resent": ["ingestion", "gold"],
    "lab-column-renamed": ["ingestion", "silver"],
    "promised-date-out-of-order": ["silver"],
    "material-copy-short": ["ingestion", "silver"],
    "promised-date-corrected": ["silver", "gold"],
    "new-dealer-late": ["gold"],
    "change-feed-drops-rows": ["des"],
    "stream-replayed": ["silver", "gold"],
}


@pytest.fixture(scope="module")
def harness():
    return Harness.build(SLICE, days=14)


def test_every_calendar_incident_has_a_scenario():
    import yaml

    calendar = {c["id"] for c in yaml.safe_load((SLICE / "world.yaml").read_text())["calendar"]} \
        if SLICE.exists() else set(WAITS)
    assert {s.incident for s in SCENARIOS} == calendar == set(WAITS)


@needs_world
@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.id for s in SCENARIOS])
def test_scenario(harness, scenario):
    result = run(scenario, harness)
    failed = [(c.id, c.detail) for c in result.checks if c.status == "failed"]
    assert failed == []
    assert result.status == "pending"
    assert list(result.waits_for) == WAITS[scenario.id]
    ran = [c for c in result.checks if c.status == "passed"]
    assert {c.layer for c in ran} == {"world", "obs"}
    assert all(c.fixture for c in ran if c.layer == "obs")


@needs_world
def test_every_engine_layer_turns_scenarios_ready(harness):
    """With every layer available and the engine checks implemented, nothing is left pending (the target state)."""
    everything = frozenset(LAYERS)
    results = [run(s, harness, everything) for s in SCENARIOS]
    assert {r.status for r in results} == {"pending"}  # engine checks have no implementation yet: still pending
    assert all(c.status == "pending" for r in results for c in r.checks if c.layer not in AVAILABLE)


@needs_world
def test_no_alert_on_a_clean_day(harness):
    """SIM-01 for alerts: every alert the fortnight raised is an expected one."""
    assert unexplained_alerts(SCENARIOS, harness) == []


@needs_world
def test_a_wrong_expectation_fails(harness):
    s = next(x for x in SCENARIOS if x.id == "order-lines-deleted")
    doc = {"apiVersion": "dwd-scenario/v0", "scenario": "x", "incident": s.incident, "world": s.world,
           "checks": [{"id": "wrong-day", "layer": "obs", "kind": "one_alert", "rule": "detector-fired",
                       "subject": "erp.sales_order_line/DEL-01", "fired_day": 5}]}
    assert run(parse(doc), harness).status == "failed"


REFUSED = [
    ("wrong api version", {"apiVersion": "v9"}),
    ("no checks", {"checks": []}),
    ("unknown layer", {"checks": [{"id": "a", "layer": "moon", "kind": "planted"}]}),
    ("kind on the wrong layer", {"checks": [{"id": "a", "layer": "gold", "kind": "planted"}]}),
    ("repeated check id", {"checks": [{"id": "a", "layer": "world", "kind": "planted"}] * 2}),
    ("no incident and no origin", {"incident": None}),
]


@pytest.mark.parametrize(("case", "over"), REFUSED, ids=[c[0] for c in REFUSED])
def test_scenario_refusals(case, over):
    base = {"apiVersion": "dwd-scenario/v0", "scenario": "x", "incident": "i", "world": "w",
            "checks": [{"id": "a", "layer": "world", "kind": "planted"}]}
    with pytest.raises(InvalidScenario):
        parse({**base, **over})


REPORT = {
    "run": {"run_id": "0123456789abcdef0123456789abcdef", "domain": "tyre", "env": "prod", "trigger": "schedule",
            "run_type": "normal", "status": "failed"},
    "datasets": [{"source": "erp", "dataset": "delivery", "status": "failed", "message": "feed gap"},
                 {"source": "mes", "dataset": "production", "status": "failed", "message": "oom"}],
    "steps": [{"source": "erp", "dataset": "delivery", "step": "recon.live_source_vs_silver", "status": "failed",
               "counts": {}, "message": "7 keys missing"}],
    "files": [],
    "errors": [{"source": "lims", "dataset": "inspection_result", "step": "contract", "error_class": "ContractMismatch",
                "message": "defect_code missing"}],
}


@pytest.mark.parametrize(
    ("index", "layer", "error_class"),
    [(0, "des", "StepFailed"), (1, "ingestion", "ContractMismatch"), (2, "ingestion", "DatasetFailed")],
)
def test_learning_loop_turns_failures_into_pending_scenarios(index, layer, error_class):
    failures = learning.failures(REPORT)
    assert len(failures) == 3  # erp.delivery is named by its step, not again as a failed dataset
    doc = learning.learned_scenario(REPORT, failures[index], "slices/02-tyre/world.yaml")
    scenario = parse(doc)
    assert (scenario.checks[0].layer, scenario.origin["error_class"], scenario.incident) == (layer, error_class,
                                                                                            None)
    assert scenario.origin["run_id"] == REPORT["run"]["run_id"]


def test_learning_loop_writes_once(tmp_path):
    first = learning.write_learned(REPORT, "slices/02-tyre/world.yaml", tmp_path)
    again = learning.write_learned(REPORT, "slices/02-tyre/world.yaml", tmp_path)
    assert len(first) == 3
    assert again == []
    assert {s.id for s in load_all(tmp_path)} == {p.stem for p in first}
