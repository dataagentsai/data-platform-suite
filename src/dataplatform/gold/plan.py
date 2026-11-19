"""A gold table's plan, read from its DPM gold manifest (manifest/gold/<name>.yaml) and its ODCS contract.

Two kinds (ADR-003):
    one_to_one  a gold dimension: one silver table reshaped by the common pipeline (keys, history, reserved members);
                no build may be declared
    modelled    a gold fact: one pure build() (a SQL model or a dataframe function, D18) inside the harness; a build
                must be declared

Pure and product-free (R1): settings only, no dataframe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

KINDS = {"gold_dimension": "one_to_one", "gold_fact": "modelled"}
BUILD_KINDS = ("sql", "dataframe")
UNITS = ("hour", "day", "month")
ON_INCOMPLETE = ("flag", "hold")
HARNESS_COLUMNS = ("_is_complete", "_run_id")  # filled by the harness on every fact row, never by a build


class GoldPlanError(ValueError):
    """A gold manifest the harness cannot run (the DPM linter's rules, ADR-003 and ADR-006 confirmations)."""


@dataclass(frozen=True)
class DimRef:
    """A dimension a fact points at: its surrogate key column, the business key in the fact, and the fact column
    the as-of join reads (ADR-005)."""

    name: str
    key: str
    business_key: tuple[str, ...]
    as_of: str


@dataclass(frozen=True)
class Window:
    column: str
    unit: str
    lookback: int  # in units, counted back from the run's business date


@dataclass(frozen=True)
class Input:
    dataset: str  # source.dataset (silver)
    role: str = "driver"
    bookmark: str = ""

    @property
    def source(self) -> str:
        return self.dataset.split(".", 1)[0]

    @property
    def name(self) -> str:
        return self.dataset.split(".", 1)[1]


@dataclass(frozen=True)
class Reconcile:
    measures: tuple[tuple[str, str], ...]  # (measure name, column)
    tolerance: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class GoldPlan:
    name: str
    domain: str
    kind: str  # gold_dimension | gold_fact
    pipeline: str  # one_to_one | modelled
    columns: tuple[tuple[str, str], ...]  # contract (name, logical type), in contract order
    grain: tuple[str, ...]
    inputs: tuple[Input, ...]
    dimensions: tuple[DimRef, ...] = ()
    history: str = ""
    window: Window | None = None
    build_kind: str = ""
    build_ref: str = ""
    corrections: bool = False
    on_incomplete: str = "flag"
    grain_unique: bool = True
    orphan_rate_max: float | None = None
    reconcile: Reconcile | None = None
    unknown_member: int = -1
    not_applicable: int = -2
    business_key: tuple[str, ...] = ()  # a dimension's: its input's primary key (the store says it, not the manifest)

    @property
    def schema(self) -> str:
        """The gold schema (ADR-012): gold_<domain>; locally a folder prefix, on the engine `<env>_gold.<domain>`."""
        return f"gold_{self.domain}"

    @property
    def table(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c for c, _ in self.columns)

    @property
    def surrogate(self) -> str:
        """A dimension's own surrogate key column (its grain)."""
        return self.grain[0] if self.kind == "gold_dimension" else ""

    @property
    def measure_columns(self) -> tuple[str, ...]:
        """Number columns of a fact that are not keys: what a window's numbers are made of."""
        keys = {d.key for d in self.dimensions} | set(self.grain)
        return tuple(c for c, t in self.columns if t in ("bigint", "double") and c not in keys)


def _dims(raw: list[dict[str, Any]]) -> tuple[DimRef, ...]:
    return tuple(DimRef(str(d["name"]), str(d["key"]), tuple(d.get("business_key") or ()), str(d.get("as_of", "")))
                 for d in raw)


def _reconcile(checks: dict[str, Any]) -> Reconcile | None:
    rec = checks.get("reconcile")
    if not rec:
        return None
    return Reconcile(tuple((str(m["name"]), str(m["column"])) for m in rec.get("measures") or ()),
                     {k: float(v) for k, v in (rec.get("tolerance") or {}).items()})


def plan_from_view(view: dict[str, Any], input_keys: dict[str, list[str]] | None = None) -> GoldPlan:
    """The plan from the metadata store's view of a gold dataset (its manifest and contract columns)."""
    from dataplatform.contract import TYPES

    cols = [(str(c["name"]), TYPES[str(c["logical_type"])]) for c in view["columns"]]
    return plan_from_manifest(view["manifest"], cols, input_keys)


def plan_from_manifest(manifest: dict[str, Any], columns: list[tuple[str, str]],
                       input_keys: dict[str, list[str]] | None = None) -> GoldPlan:
    """The plan for one gold manifest. Refuses what the DPM linter refuses (ADR-003, ADR-006). `input_keys` maps an
    input dataset to its primary key (from the metadata store): a dimension's business key is its input's key."""
    name, kind = str(manifest.get("name")), str(manifest.get("kind"))
    if kind not in KINDS:
        raise GoldPlanError(f"{name}: kind {kind!r} is not one of {sorted(KINDS)}")
    pipeline = str(manifest.get("pipeline", KINDS[kind]))
    build = manifest.get("build") or {}
    if pipeline == "one_to_one" and build:
        raise GoldPlanError(f"{name}: a one-to-one table declares no build (ADR-003)")
    if pipeline == "modelled" and not build.get("ref"):
        raise GoldPlanError(f"{name}: a modelled fact needs a build reference (ADR-003)")
    if build and build.get("kind") not in BUILD_KINDS:
        raise GoldPlanError(f"{name}: build kind {build.get('kind')!r} is not one of {BUILD_KINDS}")
    if manifest.get("materialize", "table") != "table" and (
        len(manifest.get("inputs") or []) > 1 or manifest.get("corrections")
    ):
        raise GoldPlanError(f"{name}: a materialised view has one input and no corrections (ADR-006)")
    w = manifest.get("window")
    window = Window(str(w["column"]), str(w["unit"]), int(w.get("lookback", 0))) if w else None
    if window and window.unit not in UNITS:
        raise GoldPlanError(f"{name}: window unit {window.unit!r} is not one of {UNITS}")
    if pipeline == "modelled" and window is None:
        raise GoldPlanError(f"{name}: a fact built by window recompute needs a window (ADR-006)")
    cols = tuple(columns)
    names = {c for c, _ in cols}
    if window and window.column not in names:
        raise GoldPlanError(f"{name}: window column {window.column} is not a contract column")
    dims = _dims(manifest.get("dimensions") or [])
    missing = [d.key for d in dims if d.key not in names]
    if missing:
        raise GoldPlanError(f"{name}: dimension keys {missing} are not contract columns")
    checks = manifest.get("checks") or {}
    keys = manifest.get("keys") or {}
    on_inc = str((manifest.get("completeness") or {}).get("on_incomplete", "flag"))
    if on_inc not in ON_INCOMPLETE:
        raise GoldPlanError(f"{name}: completeness.on_incomplete {on_inc!r} is not one of {ON_INCOMPLETE}")
    return GoldPlan(
        name=name,
        domain=str(manifest["domain"]),
        kind=kind,
        pipeline=pipeline,
        columns=cols,
        grain=tuple(manifest.get("grain") or ()),
        inputs=tuple(Input(str(i["dataset"]), str(i.get("role", "driver")), str(i.get("bookmark", "")))
                     for i in manifest.get("inputs") or ()),
        dimensions=dims,
        history=str(manifest.get("history", "")),
        window=window,
        build_kind=str(build.get("kind", "")),
        build_ref=str(build.get("ref", "")),
        corrections=bool(manifest.get("corrections", False)),
        on_incomplete=on_inc,
        grain_unique=bool(checks.get("grain_unique", True)),
        orphan_rate_max=float(checks["orphan_rate_max"]) if "orphan_rate_max" in checks else None,
        reconcile=_reconcile(checks),
        unknown_member=int(keys.get("unknown_member", -1)),
        not_applicable=int(keys.get("not_applicable", -2)),
        business_key=_dim_key(name, kind, manifest, names, input_keys or {}),
    )


def _dim_key(name: str, kind: str, manifest: dict[str, Any], names: set[str],
             input_keys: dict[str, list[str]]) -> tuple[str, ...]:
    if kind != "gold_dimension":
        return ()
    driver = str((manifest.get("inputs") or [{}])[0].get("dataset", ""))
    key = tuple(input_keys.get(driver) or ())
    if not key or set(key) - names:
        raise GoldPlanError(f"{name}: its input {driver}'s primary key {list(key)} is not in the dimension's contract")
    return key


def order(plans: list[GoldPlan]) -> list[GoldPlan]:
    """Dimensions first, then facts; each group by name. A fact naming a dimension no plan builds is refused."""
    dims = {p.name for p in plans if p.kind == "gold_dimension"}
    for p in plans:
        unknown = [d.name for d in p.dimensions if d.name not in dims]
        if unknown:
            raise GoldPlanError(f"{p.name}: dimensions {unknown} have no gold manifest")
    return sorted(plans, key=lambda p: (p.kind != "gold_dimension", p.name))
