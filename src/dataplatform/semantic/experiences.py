"""Dashboards and the conversation space, generated from the certified metric views (DPC-0076, SERV-04, SERV-06).

Nothing here is drawn by hand: a dashboard per domain with a page per certified view and, per certified measure, a
headline number, a trend over its time dimension and a breakdown by each other dimension; one conversation space over
the certified views only, with sample questions made from the known answers and synonyms, and instructions that name
the measures it must decline. A realm turns these into its product's files.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from dataplatform.semantic.metrics import MetricView, known_answer_cases

TILE_KINDS = ("counter", "trend", "breakdown")


@dataclass(frozen=True)
class Tile:
    kind: str  # counter | trend | breakdown
    view: str
    measure: str
    dimension: str | None  # None for a counter

    @property
    def title(self) -> str:
        if self.kind == "counter":
            return self.measure
        return f"{self.measure} by {self.dimension}"


@dataclass(frozen=True)
class Page:
    view: str
    title: str
    tiles: tuple[Tile, ...]


@dataclass(frozen=True)
class Dashboard:
    name: str
    title: str
    domain: str
    pages: tuple[Page, ...]


@dataclass(frozen=True)
class ConversationSpace:
    name: str
    title: str
    views: tuple[str, ...]  # certified views only
    instructions: tuple[str, ...]
    sample_questions: tuple[str, ...]
    declined: tuple[str, ...]  # measures that exist but are not certified


def _page(view: MetricView) -> Page:
    tiles: list[Tile] = []
    for m in view.measures:
        tiles.append(Tile("counter", view.name, m.name, None))
        tiles.append(Tile("trend", view.name, m.name, m.time_dimension))
        tiles += [Tile("breakdown", view.name, m.name, d.name) for d in view.dimensions if d.name != m.time_dimension]
    title = view.name.removeprefix("mv_").replace("_", " ").capitalize()
    return Page(view.name, title, tuple(tiles))


def dashboards(views: Iterable[MetricView]) -> list[Dashboard]:
    """One dashboard per domain, from certified views only (a preview view never reaches a dashboard)."""
    by_domain: dict[str, list[MetricView]] = {}
    for v in views:
        if v.certified_only:
            by_domain.setdefault(v.domain, []).append(v)
    return [
        Dashboard(
            f"{d}_metrics",
            f"{d.capitalize()} metrics (certified)",
            d,
            tuple(_page(v) for v in sorted(vs, key=lambda x: x.name)),
        )
        for d, vs in sorted(by_domain.items())
    ]


def _describe(value: str, grain: str) -> str:
    return {"month": f"in {value}", "day": f"on {value}"}.get(grain, value)


def sample_questions(views: Iterable[MetricView]) -> list[str]:
    """A question per known answer (its exact address) and one per certified measure in its first synonym's words."""
    views = [v for v in views if v.certified_only]
    out = []
    for c in known_answer_cases(views):
        where = [f"{f.dimension.lower()} {f.value}" for f in c.answer.filters if f.grain == "equals"]
        when = [_describe(f.value, f.grain) for f in c.answer.filters if f.grain != "equals"]
        out.append(" ".join([f"What was {c.measure}", *(["for " + " and ".join(where)] if where else []), *when]) + "?")
    for v in views:
        for m in v.measures:
            other = next((d.name for d in v.dimensions if d.name != m.time_dimension), m.time_dimension)
            word = m.synonyms[0] if m.synonyms else m.name
            out.append(f"Show {word} by {other.lower()} for each {m.time_dimension.lower()}.")
    return out


def conversation_space(
    name: str, title: str, views: Iterable[MetricView], business_timezone: str, all_views: Iterable[MetricView]
) -> ConversationSpace:
    """`views` are the published views; only the certified ones are attached. `all_views` (as read from the manifest,
    before publishing) supply the measures to decline."""
    certified = sorted((v for v in views if v.certified_only), key=lambda v: v.name)
    allowed = {m.name for v in certified for m in v.measures}
    declined = sorted({m.name for v in all_views for m in v.measures if m.name not in allowed})
    words = [f"{m.name}: {', '.join(m.synonyms)}" for v in certified for m in v.measures if m.synonyms]
    instructions = [
        "Answer only from the attached metric views. Every measure in them is certified (ADR-022); never write your "
        "own formula for a metric and never read any other table.",
        "Show the metric view, the measure and the query with every answer.",
        f"Dates and months are in the business time zone {business_timezone}.",
        "Business words and the measure they mean: " + "; ".join(words) + ".",
    ]
    if declined:
        instructions.append(
            "These measures exist but are not certified; if a question needs one, say it is not "
            "certified yet and do not estimate it: " + ", ".join(declined) + "."
        )
    return ConversationSpace(
        name,
        title,
        tuple(v.name for v in certified),
        tuple(instructions),
        tuple(sample_questions(certified)),
        tuple(declined),
    )
