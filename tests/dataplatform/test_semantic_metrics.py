"""The semantic layer (ADR-022, DPC-0073, SERV-04, SERV-06): metric files -> linted metric views -> certified views.

Table-driven. Rows marked "slice" read the tyre slice in ~/data-platform-catalog and skip without it. The known-answer
tests against gold metric views are written now and pending on W1d (gold): they skip until DP_GOLD_METRICS names a
JSON file of metric-view answers ({"<view>|<measure>|<address>": value}) produced by running known_answers.sql.
"""

import json
import os
from pathlib import Path

import pytest

from dataplatform.semantic.metrics import (
    LIFECYCLE,
    VIEW_NAME,
    KnownAnswerCase,
    MetricError,
    check_known_answers,
    known_answer_cases,
    lint,
    metric_view,
    oracle_value,
    parse_where,
    published,
)

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_slice = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre slice)")

GOLD = {
    "fact_x": {
        "kind": "gold_fact",
        "name": "fact_x",
        "domain": "sales",
        "dimensions": [{"name": "dim_a", "key": "a_sk"}],
        "inputs": [{"dataset": "src.x"}],
    },
    "dim_a": {"kind": "gold_dimension", "name": "dim_a", "domain": "core", "inputs": [{"dataset": "src.a"}]},
}


def measure(name="Rate %", cert="certified", ka=True, **over):
    m = {
        "name": name,
        "expr": "SUM(n) / COUNT(1)",
        "additive": False,
        "time_dimension": "Month",
        "certification": cert,
        "synonyms": [name.lower() + " word"],
    }
    if ka:
        m["known_answer"] = {"where": "region = 'south' AND month = 2026-11", "value": 0.5, "tolerance": 0.01}
    return {**m, **over}


def doc(name="mv_x", measures=None, **over):
    d = {
        "kind": "metric_view",
        "name": name,
        "domain": "sales",
        "source": "fact_x",
        "joins": ["dim_a"],
        "dimensions": [{"name": "Month", "expr": "DATE_TRUNC('MONTH', d)"}, {"name": "Region", "expr": "dim_a.region"}],
        "measures": measures if measures is not None else [measure()],
        "owners": {"dataset": "team@x.example"},
    }
    return {**d, **over}


def view(**over):
    return metric_view(doc(**over), GOLD)


# ---------------------------------------------------------------------------------------------------- reading

WHERE = {
    "text and month": (
        "region = 'south' AND month = 2026-11",
        [("Region", "south", "equals"), ("Month", "2026-11", "month")],
    ),
    "day": ("month = 2026-11-12", [("Month", "2026-11-12", "day")]),
    "case of the dimension ignored": ("REGION = 'n'", [("Region", "n", "equals")]),
    "lower-case and": ("region = 'a' and month = 2026-01", [("Region", "a", "equals"), ("Month", "2026-01", "month")]),
    "unknown dimension": ("plant = 'x'", MetricError),
    "not an equality": ("region > 'a'", MetricError),
    "unquoted text": ("region = south", MetricError),
}


@pytest.mark.parametrize("case", WHERE)
def test_parse_where(case):
    where, want = WHERE[case]
    dims = view().dimensions
    if isinstance(want, type):
        with pytest.raises(want):
            parse_where(where, dims)
        return
    assert [(f.dimension, f.value, f.grain) for f in parse_where(where, dims)] == want


def test_metric_view_reads_joins_and_conformed_dimensions():
    v = view()
    assert [(j.name, j.key, j.domain) for j in v.joins] == [("dim_a", "a_sk", "core")]
    assert {d.name: d.conformed_from for d in v.dimensions} == {"Month": "time", "Region": "dim_a"}
    assert v.source_domain == "sales"
    assert v.owner == "team@x.example"


READ_ERRORS = {
    "not a metric view": doc(kind="gold_fact"),
    "source is not gold": doc(source="fact_missing"),
    "join is not a dimension of the fact": doc(joins=["dim_b"]),
}


@pytest.mark.parametrize("case", READ_ERRORS)
def test_metric_view_refuses(case):
    with pytest.raises(MetricError):
        metric_view(READ_ERRORS[case], GOLD)


# ---------------------------------------------------------------------------------------------------- linting

# case -> (views, a fragment every expected error contains; [] = clean)
LINT = {
    "clean": ([view()], []),
    "name without mv_": ([view(name="metrics_x")], ["mv_ + lower snake"]),
    "name not snake case": ([view(name="mv_X")], ["mv_ + lower snake"]),
    "source not a fact_": (
        [metric_view(doc(source="dim_a", joins=[]), {**GOLD, "dim_a": {**GOLD["dim_a"], "dimensions": []}})],
        ["is not a fact_ table"],
    ),
    "certified without known answer": ([view(measures=[measure(ka=False)])], ["without a known-answer test"]),
    "draft without known answer is fine": ([view(measures=[measure(cert="draft", ka=False)])], []),
    "unknown lifecycle state": ([view(measures=[measure(cert="approved")])], ["not one of"]),
    "time dimension not in view": ([view(measures=[measure(time_dimension="Week")])], ["time dimension 'Week'"]),
    "measure defined twice": ([view(), view(name="mv_y")], ["also defined in mv_x"]),
    "synonym means two metrics": (
        [view(measures=[measure("A", synonyms=["sales"]), measure("B", synonyms=["sales"])])],
        ["already means"],
    ),
    "draft synonym may repeat a word": (
        [view(measures=[measure("A", synonyms=["s"]), measure("B", cert="draft", synonyms=["s"])])],
        [],
    ),
    "no owner": ([view(owners={})], ["no owner"]),
}


@pytest.mark.parametrize("case", LINT)
def test_lint(case):
    views, want = LINT[case]
    errors = lint(views)
    assert len(errors) == len(want), errors
    for e, w in zip(errors, want, strict=False):
        assert w in e


# ---------------------------------------------------------------------------------------------------- publishing

# case -> (measure states, published view names with their measure counts)
PUBLISH = {
    "all certified: one view, no preview": (["certified", "certified"], [("mv_x", 2)]),
    "one draft: certified view plus preview": (["certified", "draft"], [("mv_x", 1), ("mv_x_preview", 2)]),
    "reviewed counts as not certified": (["certified", "reviewed"], [("mv_x", 1), ("mv_x_preview", 2)]),
    "deprecated goes nowhere": (["certified", "deprecated"], [("mv_x", 1)]),
    "nothing certified: preview only": (["draft"], [("mv_x_preview", 1)]),
}


@pytest.mark.parametrize("case", PUBLISH)
def test_published(case):
    states, want = PUBLISH[case]
    ms = [measure(f"M{i}", cert=s, ka=s == "certified") for i, s in enumerate(states)]
    pub = published(view(measures=ms))
    assert [(p.name, len(p.measures)) for p in pub] == want
    certified_view = [p for p in pub if p.certified_only]
    assert all(m.certification == "certified" for p in certified_view for m in p.measures)
    preview = [p for p in pub if not p.certified_only]
    assert all(p.certification != "certified" for p in preview)


# ---------------------------------------------------------------------------------------------------- known answers

CASE = known_answer_cases([view()])[0]

ORACLE = {
    "number at the address": ({"Rate %": {"2026-11/south": 0.5}}, 0.5),
    "expected inside a cell": ({"Rate %": {"2026-11/south": {"expected": 0.505, "truth": 0.6}}}, 0.505),
    "address missing": ({"Rate %": {"2026-11/north": 0.5}}, None),
    "measure missing": ({}, None),
}


@pytest.mark.parametrize("case", ORACLE)
def test_oracle_value(case):
    metrics, want = ORACLE[case]
    assert CASE.oracle_key == "2026-11/south"  # time first, then the other filters
    assert oracle_value(metrics, CASE) == want


VERDICTS = {
    "exact": (0.5, True),
    "inside tolerance": (0.509, True),
    "outside tolerance": (0.52, False),
    "no value": (None, False),
}


@pytest.mark.parametrize("case", VERDICTS)
def test_check_known_answers(case):
    got, ok = VERDICTS[case]
    [v] = check_known_answers([CASE], lambda c: got)
    assert (v.got, v.ok) == (got, ok)


# ---------------------------------------------------------------------------------------------------- the tyre slice


@pytest.fixture(scope="module")
def tyre():
    yaml = pytest.importorskip("yaml")
    if not SLICE.exists():
        pytest.skip("needs ~/data-platform-catalog (the tyre slice)")
    gold = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((SLICE / "manifest/gold").glob("*.yaml"))}
    docs = [yaml.safe_load(p.read_text()) for p in sorted((SLICE / "manifest/metrics").glob("*.yaml"))]
    views = [metric_view(d, gold) for d in docs]
    return {"docs": docs, "views": views, "published": [p for v in views for p in published(v)]}


@needs_slice
def test_tyre_metric_files_lint_clean(tyre):
    assert lint(tyre["views"]) == []


@needs_slice
def test_every_certified_metric_gets_a_certified_view(tyre):
    certified = {
        (d["name"], m["name"]) for d in tyre["docs"] for m in d["measures"] if m["certification"] == "certified"
    }
    served = {(p.name, m.name) for p in tyre["published"] if p.certified_only for m in p.measures}
    assert served == certified
    assert {p.name for p in tyre["published"] if p.certified_only} == {"mv_otif", "mv_sales", "mv_scrap"}


@needs_slice
def test_tyre_names_follow_the_standard(tyre):
    assert all(VIEW_NAME.match(p.name) for p in tyre["published"])
    assert all(m.certification in LIFECYCLE for v in tyre["views"] for m in v.measures)


def _corrections_store():
    """The world's people record corrections in the metadata store (ADR-017, W1b), so a world run needs one bound."""
    from dataplatform.adapters.memory_meta import MemoryMetaStore
    from dataplatform.meta import loader

    store = MemoryMetaStore()
    loader.load(store, SLICE / "manifest")
    return store


@needs_slice
def test_known_answer_addresses_resolve_in_the_world_oracle(tyre, tmp_path):
    """Ready now: every certified known answer names an address the world's oracle fills (a small world run)."""
    from dataplatform.adapters.file_drop import LocalFileDrop
    from dataplatform.adapters.local_stream import LocalStream
    from dataplatform.adapters.memory_sources import MemoryRecords
    from dataplatform.adapters.sqlite_source import SqliteSource
    from dataplatform.world import dwd as dwd_loader
    from dataplatform.world.domains import model_for
    from dataplatform.world.engine import Sinks, World

    dwd = dwd_loader.load(SLICE)
    sinks = Sinks(
        SqliteSource(tmp_path / "db.sqlite"),
        LocalFileDrop(tmp_path / "drops"),
        MemoryRecords(),
        LocalStream(tmp_path / "stream"),
        _corrections_store(),
    )
    last = list(World(dwd, model_for(dwd, 0.02), sinks, tmp_path / "out").run(14))[-1]
    cases = known_answer_cases(tyre["views"])
    assert len(cases) == 5
    missing = [c.label for c in cases if oracle_value(last["metrics"], c) is None]
    assert missing == []


@needs_slice
@pytest.mark.skipif(
    not os.environ.get("DP_WORLD_FULL"), reason="full-scale world takes about 3 minutes: DP_WORLD_FULL=1"
)
def test_manifest_known_answers_equal_the_full_world_oracle(tyre, tmp_path):
    """The known answers in the metric files are the oracle's seed-11 numbers at scale 1.0 after 14 days (passed on
    4 Nov 2026 in 160 s). When gold exists, the metric views must give these same numbers (tests below)."""
    from dataplatform.adapters.file_drop import LocalFileDrop
    from dataplatform.adapters.local_stream import LocalStream
    from dataplatform.adapters.memory_sources import MemoryRecords
    from dataplatform.adapters.sqlite_source import SqliteSource
    from dataplatform.world import dwd as dwd_loader
    from dataplatform.world.domains import model_for
    from dataplatform.world.engine import Sinks, World

    dwd = dwd_loader.load(SLICE)
    sinks = Sinks(
        SqliteSource(tmp_path / "db.sqlite"),
        LocalFileDrop(tmp_path / "drops"),
        MemoryRecords(),
        LocalStream(tmp_path / "stream"),
        _corrections_store(),
    )
    last = list(World(dwd, model_for(dwd, 1.0), sinks, tmp_path / "out").run(14))[-1]
    verdicts = check_known_answers(known_answer_cases(tyre["views"]), lambda c: oracle_value(last["metrics"], c))
    assert [(v.case.label, v.got) for v in verdicts if not v.ok] == []


# ---------------------------------------------------------------------------------------------------- pending on gold


def _gold_answers():
    path = os.environ.get("DP_GOLD_METRICS")
    if not path:
        pytest.skip("pending W1d (gold): run known_answers.sql against the metric views and set DP_GOLD_METRICS")
    return json.loads(Path(path).read_text())


def _cases():
    if not SLICE.exists():
        return []
    import yaml

    gold = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((SLICE / "manifest/gold").glob("*.yaml"))}
    views = [
        metric_view(yaml.safe_load(p.read_text()), gold) for p in sorted((SLICE / "manifest/metrics").glob("*.yaml"))
    ]
    return known_answer_cases([p for v in views for p in published(v)])


@needs_slice
@pytest.mark.parametrize("case", _cases(), ids=lambda c: c.label)
def test_metric_view_matches_known_answer_pending_gold(case: KnownAnswerCase):
    """Pending W1d: the metric view's number for each certified known answer (certified view and preview alike)."""
    answers = _gold_answers()
    [verdict] = check_known_answers([case], lambda c: answers.get(f"{c.view}|{c.measure}|{c.oracle_key}"))
    assert verdict.ok, f"{case.label}: view says {verdict.got}, the manifest's known answer is {case.answer.value}"


@needs_slice
@pytest.mark.parametrize("case", _cases(), ids=lambda c: c.label)
def test_metric_view_matches_world_oracle_pending_gold(case: KnownAnswerCase):
    """Pending W1d: the metric view and the oracle of the same world run give the same number (one number
    everywhere). DP_GOLD_ORACLE names the oracle day file of the run that fed gold."""
    answers = _gold_answers()
    oracle_path = os.environ.get("DP_GOLD_ORACLE")
    if not oracle_path:
        pytest.skip("pending W1d (gold): set DP_GOLD_ORACLE to the oracle file of the run that fed gold")
    oracle = json.loads(Path(oracle_path).read_text())["metrics"]
    want = oracle_value(oracle, case)
    got = answers.get(f"{case.view}|{case.measure}|{case.oracle_key}")
    assert want is not None
    assert got is not None
    assert abs(got - want) <= case.answer.tolerance + 1e-9
