"""Dashboards and the conversation space come from the certified views only (SERV-06, DPC-0076). Table-driven."""

import pytest

from dataplatform.semantic.experiences import TILE_KINDS, conversation_space, dashboards, sample_questions
from dataplatform.semantic.metrics import metric_view, published

GOLD = {
    "fact_o": {"kind": "gold_fact", "domain": "sales", "dimensions": [{"name": "dim_d", "key": "d_sk"}]},
    "fact_s": {"kind": "gold_fact", "domain": "make", "dimensions": [{"name": "dim_d", "key": "d_sk"}]},
    "dim_d": {"kind": "gold_dimension", "domain": "sales"},
}


def m(name, cert="certified", synonyms=("w",)):
    out = {
        "name": name,
        "expr": "COUNT(1)",
        "time_dimension": "Month",
        "certification": cert,
        "synonyms": [f"{s} {name.lower()}" for s in synonyms],
    }
    if cert == "certified":
        out["known_answer"] = {"where": "region = 'south' AND month = 2026-11", "value": 1}
    return out


def v(name, source, domain, measures):
    return metric_view(
        {
            "kind": "metric_view",
            "name": name,
            "domain": domain,
            "source": source,
            "joins": ["dim_d"],
            "dimensions": [{"name": "Month", "expr": "m"}, {"name": "Region", "expr": "dim_d.region"}],
            "measures": measures,
            "owners": {"dataset": "t@x"},
        },
        GOLD,
    )


VIEWS = [v("mv_o", "fact_o", "sales", [m("Rate"), m("Value", "draft")]), v("mv_s", "fact_s", "make", [m("Scrap")])]
PUB = [p for x in VIEWS for p in published(x)]


def test_one_dashboard_per_domain_from_certified_views_only():
    ds = dashboards(PUB)
    assert [(d.name, [p.view for p in d.pages]) for d in ds] == [
        ("make_metrics", ["mv_s"]),
        ("sales_metrics", ["mv_o"]),
    ]
    measures = {t.measure for d in ds for p in d.pages for t in p.tiles}
    assert measures == {"Rate", "Scrap"}  # the draft never reaches a dashboard


# kind -> how many tiles of that kind per certified measure (one counter, one trend, one per other dimension)
@pytest.mark.parametrize(("kind", "per_measure"), [("counter", 1), ("trend", 1), ("breakdown", 1)])
def test_tiles_per_measure(kind, per_measure):
    assert kind in TILE_KINDS
    tiles = [t for d in dashboards(PUB) for p in d.pages for t in p.tiles if t.kind == kind]
    assert len(tiles) == 2 * per_measure
    if kind == "trend":
        assert {t.dimension for t in tiles} == {"Month"}


SPACE = conversation_space("s", "S", PUB, "Asia/Kolkata", VIEWS)

# case -> check on the space
SPACE_CASES = {
    "attaches certified views only": lambda s: s.views == ("mv_o", "mv_s"),
    "declines the draft by name": lambda s: s.declined == ("Value",) and any("Value" in i for i in s.instructions),
    "names the time zone": lambda s: any("Asia/Kolkata" in i for i in s.instructions),
    "maps synonyms to measures": lambda s: any("Rate: w rate" in i for i in s.instructions),
    "a question per known answer": lambda s: "What was Rate for region south in 2026-11?" in s.sample_questions,
    "a question per measure in its synonym's words": lambda s: (
        "Show w scrap by region for each month." in s.sample_questions
    ),
    "no question about a draft": lambda s: not any("Value" in q for q in s.sample_questions),
}


@pytest.mark.parametrize("case", SPACE_CASES)
def test_conversation_space(case):
    assert SPACE_CASES[case](SPACE)


def test_nothing_to_decline_leaves_no_decline_instruction():
    s = conversation_space("s", "S", PUB, "UTC", [VIEWS[1]])
    assert s.declined == ()
    assert not any("not certified" in i for i in s.instructions)


def test_generation_is_deterministic():
    assert sample_questions(PUB) == sample_questions(list(PUB))
    assert dashboards(PUB) == dashboards(list(reversed(PUB)))
