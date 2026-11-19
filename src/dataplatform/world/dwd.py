"""Reads a DWD world (dwd/v0) and the specification it cites: dataset manifests and their ODCS contracts.

The world never declares the domain (AWD rule kept in DWD): table columns and keys come from the contracts, how each
dataset is read comes from its manifest. Anything the world needs and the spec does not say is an error here,
not a silent default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dataplatform import contract as odcs
from dataplatform.source_ports import TableSpec

SYSTEM_KINDS = {"database", "file_drop", "rest_api", "stream"}


class DwdError(ValueError):
    pass


@dataclass
class Dataset:
    name: str  # source.dataset, e.g. erp.sales_order_line
    system: str  # the DWD system that holds it, e.g. erp_db
    manifest: dict[str, Any]
    contract: dict[str, Any]
    table: TableSpec

    @property
    def table_name(self) -> str:
        return self.name.split(".", 1)[1]


@dataclass
class Dwd:
    world: dict[str, Any]
    platform: dict[str, Any]
    datasets: dict[str, Dataset] = field(default_factory=dict)
    metrics: dict[str, dict[str, Any]] = field(default_factory=dict)  # metric views by name (their known answers)

    @property
    def systems(self) -> dict[str, dict[str, Any]]:
        return self.world["systems"]

    def kind_of(self, system: str) -> str:
        return str(self.systems[system]["kind"])

    def in_system(self, system: str) -> list[Dataset]:
        return [d for d in self.datasets.values() if d.system == system]


def _yaml(path: Path) -> Any:
    import yaml  # local only, as in manifest.py

    return yaml.safe_load(path.read_text())


# Day params an incident reads, and how each must sit against the incident's own day.
BEFORE = ("from_day", "due_day")  # the source day the incident reaches back to
AFTER = ("activate_day", "appears_day")  # a later effect of the incident
NOT_BEFORE = ("until_day",)  # the incident lasts to this day


def apply_calendar(world: dict[str, Any], name: str) -> dict[str, Any]:
    """A named calendar from the world's `calendars:` block moves incidents to other days (for a short run that still
    meets every incident). Every incident must be moved, a moved key must be `day` or one of its day params, and the
    order of days must hold; anything else is an error, never a silent default."""
    variants = world.get("calendars") or {}
    if name not in variants:
        raise DwdError(f"no calendar {name!r} in the world (have: {sorted(variants)})")
    moves = variants[name].get("moves") or {}
    ids = [e["id"] for e in world.get("calendar", [])]
    if sorted(moves) != sorted(ids):
        raise DwdError(f"calendar {name}: moves must name every incident; missing {sorted(set(ids) - set(moves))}, "
                       f"unknown {sorted(set(moves) - set(ids))}")
    out = []
    for entry in world["calendar"]:
        move, params = dict(moves[entry["id"]]), dict(entry.get("params") or {})
        unknown = [k for k in move if k != "day" and k not in params]
        if unknown:
            raise DwdError(f"calendar {name}, {entry['id']}: {unknown} are not this incident's params")
        day = int(move.pop("day", entry["day"]))
        params.update(move)
        wrong = [k for k in BEFORE if k in params and not int(params[k]) < day]
        wrong += [k for k in AFTER if k in params and not int(params[k]) > day]
        wrong += [k for k in NOT_BEFORE if k in params and not int(params[k]) >= day]
        if wrong or day < 1:
            raise DwdError(f"calendar {name}, {entry['id']}: day {day} breaks the order of {wrong or ['day']}")
        out.append({**entry, "day": day, "params": params})
    changed = _set_paths(world, variants[name].get("world") or {}, name)
    return {**changed, "calendar": out, "calendar_name": name}


def _set_paths(world: dict[str, Any], sets: dict[str, Any], name: str) -> dict[str, Any]:
    """`world:` in a calendar changes existing settings by dotted path (systems.partner_drop.file_window_days); a path
    that is not already in the world, or a value of another type, is an error."""
    import copy  # noqa: PLC0415 - only when a calendar changes settings

    out = copy.deepcopy(world)
    for path, value in sets.items():
        *parents, leaf = path.split(".")
        node = out
        for part in parents:
            node = node.get(part) if isinstance(node, dict) else None
            if node is None:
                break
        if not isinstance(node, dict) or leaf not in node:
            raise DwdError(f"calendar {name}: world setting {path} does not exist")
        if type(node[leaf]) is not type(value):
            raise DwdError(f"calendar {name}: {path} is {type(node[leaf]).__name__}, not {type(value).__name__}")
        node[leaf] = value
    return out


def load(slice_dir: str | Path, calendar: str | None = None) -> Dwd:
    root = Path(slice_dir)
    world = _yaml(root / "world.yaml")
    if calendar:
        world = apply_calendar(world, calendar)
    cites = world.get("cites", {})
    dwd = Dwd(world, _yaml(root / cites["platform"]["path"]))
    manifests = {
        f"{m['source_name']}.{m['dataset_name']}": (p, m)
        for p in sorted((root / cites["manifests"]).glob("*.yaml"))
        for m in [_yaml(p)]
    }
    for system, spec in world["systems"].items():
        if spec.get("kind") not in SYSTEM_KINDS:
            raise DwdError(f"system {system}: kind {spec.get('kind')!r} is not one of {sorted(SYSTEM_KINDS)}")
        for name in spec["datasets"]:
            if name not in manifests:
                raise DwdError(f"system {system} names {name}, which has no manifest under {cites['manifests']}")
            path, manifest = manifests[name]
            contract = _yaml(path.parent / manifest["contract"])
            dwd.datasets[name] = Dataset(name, system, manifest, contract, _table(name, spec, manifest, contract))
    if cites.get("metrics"):
        dwd.metrics = {m["name"]: m for p in sorted((root / cites["metrics"]).glob("*.yaml")) for m in [_yaml(p)]}
    for entry in world.get("calendar", []):
        if entry["dataset"] not in dwd.datasets:
            raise DwdError(f"incident {entry['id']} names {entry['dataset']}, which no system holds")
    return dwd


def _table(name: str, system: dict[str, Any], manifest: dict[str, Any], contract: dict[str, Any]) -> TableSpec:
    read = manifest.get("ingest", {}).get("read")
    return TableSpec(
        name=name.split(".", 1)[1],
        columns=tuple((p["name"], p["logicalType"]) for p in odcs.properties(contract)),
        key=tuple(odcs.primary_key(contract)),
        change_feed=bool(system.get("change_feed")) and read == "change_feed",
        full_copy=read == "full_copy",
    )
