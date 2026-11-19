"""Metric views from the manifest's metric files (DPM v0.2 kind metric_view; ADR-022, DPC-0073, SERV-04, SERV-06).

A metric is defined once, in its metric file. This module reads those files into plain records, lints them, and
splits each into the view the conversation interface and dashboards may read (certified measures only, under the
manifest's own name) and a preview of the measures not yet certified (for their owners to review). Deprecated
measures go nowhere.

Known answers are parsed into dimension filters, so the same address can be asked of a metric view (gold) and looked
up in the world's oracle: one number everywhere.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

LIFECYCLE = ("draft", "reviewed", "certified", "deprecated")
VIEW_NAME = re.compile(r"^mv_[a-z][a-z0-9_]*$")  # ADR-012: mv_ for metric views, lower snake case
FACT_NAME = re.compile(r"^fact_[a-z][a-z0-9_]*$")
DIM_NAME = re.compile(r"^dim_[a-z][a-z0-9_]*$")
PREVIEW_SUFFIX = "_preview"
TIME = "time"  # conformed_from value of the time dimension

_MONTH = re.compile(r"^\d{4}-\d{2}$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_QUOTED = re.compile(r"^'([^']*)'$")
_JOIN_REF = re.compile(r"^(dim_[a-z0-9_]+)\.[a-z_][a-z0-9_]*$")


class MetricError(ValueError):
    """A metric file that cannot be read into a metric view."""


@dataclass(frozen=True)
class Dimension:
    name: str
    expr: str
    conformed_from: str  # TIME, the dim_ table it comes from, or "" for a column of the fact itself


@dataclass(frozen=True)
class Filter:
    dimension: str
    value: str
    grain: str  # month | day | equals


@dataclass(frozen=True)
class KnownAnswer:
    filters: tuple[Filter, ...]
    value: float
    tolerance: float


@dataclass(frozen=True)
class Measure:
    name: str
    expr: str
    additive: bool
    time_dimension: str
    certification: str
    synonyms: tuple[str, ...]
    description: str
    known_answer: KnownAnswer | None


@dataclass(frozen=True)
class Join:
    name: str  # the dim_ table
    key: str  # the surrogate key column shared by the fact and the dimension
    domain: str  # the dimension's domain: its gold schema (ADR-012)


@dataclass(frozen=True)
class MetricView:
    name: str
    domain: str
    source: str  # the gold fact
    source_domain: str
    joins: tuple[Join, ...]
    dimensions: tuple[Dimension, ...]
    measures: tuple[Measure, ...]
    owner: str
    certified_only: bool = True  # False for the preview view

    def dimension(self, name: str) -> Dimension:
        return next(d for d in self.dimensions if d.name == name)

    @property
    def certification(self) -> str:
        """The lowest state of any measure the view exposes (the view's tag)."""
        states = {m.certification for m in self.measures}
        return next((s for s in LIFECYCLE if s in states), "certified")


# ---------------------------------------------------------------------------------------------------- reading


def parse_where(where: str, dimensions: Iterable[Dimension]) -> tuple[Filter, ...]:
    """'region = 'south' AND promised month = 2026-11' -> filters on the view's dimensions (names ignore case)."""
    by_lower = {d.name.lower(): d.name for d in dimensions}
    filters = []
    for part in re.split(r"\s+AND\s+", where.strip(), flags=re.IGNORECASE):
        lhs, sep, rhs = part.partition("=")
        if not sep:
            raise MetricError(f"known answer filter '{part}' is not 'dimension = value'")
        dim = by_lower.get(lhs.strip().lower())
        if dim is None:
            raise MetricError(f"known answer filter names unknown dimension '{lhs.strip()}'")
        raw = rhs.strip()
        quoted = _QUOTED.match(raw)
        if quoted:
            filters.append(Filter(dim, quoted.group(1), "equals"))
        elif _DAY.match(raw):
            filters.append(Filter(dim, raw, "day"))
        elif _MONTH.match(raw):
            filters.append(Filter(dim, raw, "month"))
        else:
            raise MetricError(f"known answer value '{raw}' is neither 'text', YYYY-MM nor YYYY-MM-DD")
    return tuple(filters)


def _dimension(raw: Mapping[str, Any], time_dims: set[str]) -> Dimension:
    name, expr = str(raw["name"]), str(raw["expr"])
    ref = _JOIN_REF.match(expr.strip())
    conformed = TIME if name in time_dims else (ref.group(1) if ref else "")
    return Dimension(name, expr, conformed)


def _measure(raw: Mapping[str, Any], dimensions: tuple[Dimension, ...]) -> Measure:
    ka_raw = raw.get("known_answer")
    ka = None
    if ka_raw:
        ka = KnownAnswer(
            parse_where(str(ka_raw["where"]), dimensions), float(ka_raw["value"]), float(ka_raw.get("tolerance", 0))
        )
    return Measure(
        name=str(raw["name"]),
        expr=str(raw["expr"]),
        additive=bool(raw.get("additive", False)),
        time_dimension=str(raw.get("time_dimension", "")),
        certification=str(raw.get("certification", "draft")),
        synonyms=tuple(str(s) for s in raw.get("synonyms") or ()),
        description=str(raw.get("description", "")),
        known_answer=ka,
    )


def metric_view(doc: Mapping[str, Any], gold: Mapping[str, Mapping[str, Any]]) -> MetricView:
    """One metric file -> a metric view. `gold` maps gold manifest names (facts and dimensions) to their documents;
    each join's key and schema come from there, so the metric file names a dimension and nothing more."""
    if doc.get("kind") != "metric_view":
        raise MetricError(f"{doc.get('name')}: kind is not metric_view")
    source = str(doc["source"])
    fact = gold.get(source)
    if fact is None:
        raise MetricError(f"{doc['name']}: source '{source}' is not a gold manifest")
    keys = {str(d["name"]): str(d["key"]) for d in fact.get("dimensions") or ()}
    joins = []
    for j in doc.get("joins") or ():
        if j not in keys or j not in gold:
            raise MetricError(f"{doc['name']}: join '{j}' is not a dimension of {source}")
        joins.append(Join(str(j), keys[j], str(gold[j]["domain"])))
    time_dims = {str(m.get("time_dimension")) for m in doc["measures"] if m.get("time_dimension")}
    dims = tuple(_dimension(d, time_dims) for d in doc["dimensions"])
    return MetricView(
        name=str(doc["name"]),
        domain=str(doc["domain"]),
        source=source,
        source_domain=str(fact["domain"]),
        joins=tuple(joins),
        dimensions=dims,
        measures=tuple(_measure(m, dims) for m in doc["measures"]),
        owner=str((doc.get("owners") or {}).get("dataset", "")),
    )


# ---------------------------------------------------------------------------------------------------- linting


def lint(views: Iterable[MetricView]) -> list[str]:
    """Problems that stop generation. Empty means every view may be published."""
    views = list(views)
    errors: list[str] = []
    seen_measure: dict[str, str] = {}
    seen_word: dict[str, str] = {}
    for v in views:
        if not VIEW_NAME.match(v.name):
            errors.append(f"{v.name}: metric view names are mv_ + lower snake case (ADR-012, NAME-01)")
        if not FACT_NAME.match(v.source):
            errors.append(f"{v.name}: source '{v.source}' is not a fact_ table")
        errors += [f"{v.name}: join '{j.name}' is not a dim_ table" for j in v.joins if not DIM_NAME.match(j.name)]
        if not v.owner:
            errors.append(f"{v.name}: no owner")
        dim_names = {d.name for d in v.dimensions}
        for m in v.measures:
            where = f"{v.name}.{m.name}"
            if m.certification not in LIFECYCLE:
                errors.append(f"{where}: certification '{m.certification}' is not one of {', '.join(LIFECYCLE)}")
            if m.time_dimension not in dim_names:
                errors.append(f"{where}: time dimension '{m.time_dimension}' is not a dimension of the view")
            if m.certification == "certified" and m.known_answer is None:
                errors.append(f"{where}: certified without a known-answer test (ADR-022, SL-SEM-07)")
            if m.name in seen_measure:
                errors.append(f"{where}: also defined in {seen_measure[m.name]} (SERV-04: defined once)")
                continue  # its words would only repeat the same finding
            seen_measure[m.name] = v.name
            if m.certification != "certified":
                continue
            for word in (m.name, *m.synonyms):
                key = word.lower()
                if key in seen_word and seen_word[key] != where:
                    errors.append(f"{where}: '{word}' already means {seen_word[key]} (one word, one metric)")
                seen_word.setdefault(key, where)
    return errors


# ---------------------------------------------------------------------------------------------------- publishing


def published(view: MetricView) -> list[MetricView]:
    """The views to create: the certified view under the manifest's name (only certified measures; what dashboards
    and the conversation interface read, SERV-06), and, if any measure is draft or reviewed, a _preview view with
    every measure that is not deprecated, tagged with its lowest state. A view with no certified measure publishes
    only its preview."""
    live = tuple(m for m in view.measures if m.certification != "deprecated")
    certified = tuple(m for m in live if m.certification == "certified")
    out = []
    if certified:
        out.append(replace(view, measures=certified, certified_only=True))
    if len(certified) < len(live):
        out.append(replace(view, name=view.name + PREVIEW_SUFFIX, measures=live, certified_only=False))
    return out


# ---------------------------------------------------------------------------------------------------- known answers


@dataclass(frozen=True)
class KnownAnswerCase:
    view: str
    measure: str
    time_dimension: str
    answer: KnownAnswer

    @property
    def label(self) -> str:
        return f"{self.view} {self.measure} " + " ".join(f.value for f in self.answer.filters)

    @property
    def oracle_key(self) -> str:
        """The world oracle's address for this answer: the time value first, then the other filter values in order
        ('2026-11/south', '2026-11-12/P-CHN', '2026-11')."""
        time = [f.value for f in self.answer.filters if f.dimension == self.time_dimension]
        rest = [f.value for f in self.answer.filters if f.dimension != self.time_dimension]
        return "/".join(time + rest)


@dataclass(frozen=True)
class Verdict:
    case: KnownAnswerCase
    got: float | None
    ok: bool


def known_answer_cases(views: Iterable[MetricView]) -> list[KnownAnswerCase]:
    """Every certified measure's known answer, in view and measure order."""
    return [
        KnownAnswerCase(v.name, m.name, m.time_dimension, m.known_answer)
        for v in views
        for m in v.measures
        if m.certification == "certified" and m.known_answer
    ]


def oracle_value(metrics: Mapping[str, Mapping[str, Any]], case: KnownAnswerCase) -> float | None:
    """Look a known answer up in the oracle's per-day metrics ({measure: {address: number | {expected: number}}})."""
    cell = (metrics.get(case.measure) or {}).get(case.oracle_key)
    if isinstance(cell, Mapping):
        cell = cell.get("expected")
    return None if cell is None else float(cell)


def check_known_answers(
    cases: Iterable[KnownAnswerCase], read: Callable[[KnownAnswerCase], float | None]
) -> list[Verdict]:
    """Ask `read` (a metric view, the oracle, a dashboard's dataset) each case; a missing value fails."""
    out = []
    for c in cases:
        got = read(c)
        ok = got is not None and abs(got - c.answer.value) <= c.answer.tolerance + 1e-9
        out.append(Verdict(c, got, ok))
    return out
