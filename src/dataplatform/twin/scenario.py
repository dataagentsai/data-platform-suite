"""A DataTwin scenario as a file: `apiVersion: dwd-scenario/v0` (goal G10, TEST-05).

The shape is AgentTwin's scenario file (awd-scenario/v0) applied to a data world: a scenario names the world and the
calendar entry it plays, what it discharges, and checks judged from the world's own truth (R9, SIM-01), never from
what the platform says about itself. AgentTwin is the base we follow, not a dependency: its runner drives an agent,
ours drives a data platform; the file shape, "judge by state" and "a scenario never names a product" carry over.

    apiVersion: dwd-scenario/v0
    scenario: sellout-file-late
    incident: sellout-file-late          # a calendar id in the world (or null for a learned scenario, see learning)
    world: slices/02-tyre/world.yaml
    discharges: [LATE-01, OPS-01, OPS-06]
    checks:
      - {id: planted, layer: world, kind: planted}
      - {id: breach-shown, layer: obs, kind: on_dashboard, dashboard: freshness_slos, widget: breach_list,
         match: {dataset: partner.dealer_sellout, key: D-0042}}
      - {id: one-alert, layer: obs, kind: one_alert, rule: freshness-breach,
         subject: partner.dealer_sellout/D-0042, fired_day: 1, resolved_day: 5}
      - {id: restated, layer: gold, kind: engine, says: October for D-0042 is rebuilt and marked complete}

Layers say what must exist for a check to run. `world` and `obs` always exist (obs on the fixture runner). An engine
layer exists when a run of it is attached to the harness (`Harness.attach`; ingestion and silver today, see
twin/engine.py: ingestion, silver and gold); the data expectation services (W1e) wait for their session. A check on a missing
layer is reported as pending with the session it waits for.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

API_VERSION = "dwd-scenario/v0"
LAYERS = {"world": "the tyre world (G4)", "obs": "observability (W3; fixture runs)",
          "ingestion": "W1b ingestion", "silver": "W1c silver", "gold": "W1d gold",
          "des": "W1e data expectation services"}
ENGINE = ("ingestion", "silver", "gold", "des")
KINDS = {"planted": ("world",), "oracle": ("world",), "on_dashboard": ("obs",), "one_alert": ("obs",),
         "engine": ENGINE}
AVAILABLE = frozenset({"world", "obs"})
FOLDER = Path(__file__).with_name("scenarios")


class InvalidScenario(ValueError):
    """Said at load, where it is cheap, rather than mid-run."""


@dataclass(frozen=True)
class Check:
    id: str
    layer: str
    kind: str
    args: dict[str, Any] = field(default_factory=dict)

    @property
    def waits_for(self) -> str:
        return LAYERS[self.layer]


@dataclass(frozen=True)
class Scenario:
    id: str
    incident: str | None
    world: str
    discharges: tuple[str, ...]
    checks: tuple[Check, ...]
    story: str = ""
    origin: dict[str, Any] = field(default_factory=dict)

    @property
    def layers(self) -> set[str]:
        return {c.layer for c in self.checks}

    def pending_layers(self, available: frozenset[str] = AVAILABLE) -> list[str]:
        return sorted({c.layer for c in self.checks if c.layer not in available}, key=list(LAYERS).index)


def parse(doc: dict[str, Any], where: str = "scenario") -> Scenario:
    if doc.get("apiVersion") != API_VERSION:
        raise InvalidScenario(f"{where}: apiVersion must be {API_VERSION}")
    for key in ("scenario", "world", "checks"):
        if not doc.get(key):
            raise InvalidScenario(f"{where}: '{key}' is required")
    checks = []
    for c in doc["checks"]:
        cid, layer, kind = c.get("id"), c.get("layer"), c.get("kind")
        if not cid or layer not in LAYERS or kind not in KINDS:
            raise InvalidScenario(f"{where}: check {cid!r} needs an id, a layer in {sorted(LAYERS)} and a kind in "
                                  f"{sorted(KINDS)}")
        if layer not in KINDS[kind]:
            raise InvalidScenario(f"{where}: check {cid}: kind {kind} runs on layer {KINDS[kind]}, not {layer}")
        checks.append(Check(str(cid), str(layer), str(kind),
                            {k: v for k, v in c.items() if k not in ("id", "layer", "kind")}))
    if len({c.id for c in checks}) != len(checks):
        raise InvalidScenario(f"{where}: check ids repeat")
    incident = doc.get("incident")
    if incident is None and not doc.get("origin"):
        raise InvalidScenario(f"{where}: a scenario without an incident must say its origin")
    return Scenario(str(doc["scenario"]), incident, str(doc["world"]), tuple(doc.get("discharges") or ()),
                    tuple(checks), str(doc.get("story", "")), dict(doc.get("origin") or {}))


def load(path: str | Path) -> Scenario:
    import yaml  # noqa: PLC0415 - local only, as in manifest.py

    return parse(yaml.safe_load(Path(path).read_text()), str(path))


def load_all(folder: str | Path = FOLDER) -> list[Scenario]:
    return [load(p) for p in sorted(Path(folder).glob("*.yaml"))]
