"""Property tests for bronze to silver history (SCD Type 2 and Type 1): random histories against a small reference model.

The table-driven cases in test_silver_stages.py are a hand-written list. A list cannot show it is complete, and the review
of 19 Nov 2026 found cases it had missed. Here the tests generate the histories: changes to a few keys, delivered out of
order, some twice, split into batches anywhere. The reference model says what silver must hold after all of them, from
the full set of changes alone. If the stages disagree with the model, or the history breaks an invariant (one current
version per key, no overlap), Hypothesis shrinks the history to the smallest one that still fails and prints it.

The model is the contract in silver/history_contract.py written as a rule, so a case there is a special case here:
  * Type 2: order a key's changes by sequence; a new version opens when the tracked value differs from the one before;
    a version starts at its first change; the last version is current; a version is deleted when its last change is a delete.
  * Type 1: one row per key, from its change with the highest sequence; deleted when that change is a delete.
A delete row carries the value its key had just before it (as in the contract's cases).

Rules proven here (DAC): SCD-01, SCD-02 (order of arrival does not matter), DUP-04 (a change sent twice changes nothing).
Runs on both engines; `HYPOTHESIS_MAX_EXAMPLES` sets the size (default 25; the DuckDB engine is fast enough for hundreds).
"""

import os
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass

import pytest

pytest.importorskip("pyspark")
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, Phase, given, settings
from hypothesis import strategies as st
from silver_helpers import assert_history_invariants, plan, rows
from test_silver_stages import SCD2, _apply, _feed

from dataplatform.silver.history_model import type1_rows, type2_versions

pytestmark = [pytest.mark.slow, pytest.mark.usefixtures("spark")]

KEYS = ("a", "b", "c")
VALUES = (1, 2, 3)
MAX_EXAMPLES = int(os.environ.get("HYPOTHESIS_MAX_EXAMPLES", "25"))
# HYPOTHESIS_SHRINK=0 skips the shrink phase: a long exploration run then reports the failing history as found
PHASES = [Phase.explicit, Phase.reuse, Phase.generate] + ([Phase.shrink] if os.environ.get("HYPOTHESIS_SHRINK", "1") != "0" else [])
SETTINGS = settings(max_examples=MAX_EXAMPLES, deadline=None, derandomize=True, phases=PHASES,
                    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])


@dataclass(frozen=True)
class Change:
    key: str
    value: int
    note: str
    seq: int
    op: str  # I, U or D


@dataclass(frozen=True)
class History:
    changes: tuple[Change, ...]            # every distinct change, in sequence order
    arrivals: tuple[tuple[Change, ...], ...]  # the batches, in arrival order; a change may appear more than once


@st.composite
def histories(draw, max_changes: int = 7) -> History:
    n = draw(st.integers(1, max_changes))
    seqs = sorted(draw(st.lists(st.integers(1, 40), min_size=n, max_size=n, unique=True)))
    last: dict[str, int] = {}
    changes = []
    for seq in seqs:
        key, op = draw(st.sampled_from(KEYS)), draw(st.sampled_from(("I", "U", "U", "D")))
        value = last[key] if op == "D" and key in last else draw(st.sampled_from(VALUES))
        last[key] = value
        changes.append(Change(key, value, draw(st.sampled_from(("x", "y"))), seq, op))
    deliveries = list(changes) + [changes[i] for i in draw(st.lists(st.integers(0, n - 1), max_size=3))]
    order = draw(st.permutations(range(len(deliveries))))
    arrived = [deliveries[i] for i in order]
    cuts = sorted(draw(st.lists(st.integers(1, max(1, len(arrived) - 1)), max_size=3, unique=True)))
    bounds = [0, *[c for c in cuts if c < len(arrived)], len(arrived)]
    return History(tuple(changes), tuple(tuple(arrived[a:b]) for a, b in zip(bounds, bounds[1:]) if b > a))


def batches_of(h: History):
    """The history as feed rows: committed minute and feed position follow arrival, as in a real change feed."""
    out, n = [], 0
    for batch in h.arrivals:
        rows_ = []
        for c in batch:
            n += 1
            rows_.append((c.key, c.value, c.note, c.seq, c.op, n, n))
        out.append(rows_)
    return _feed(out)


@contextmanager
def example_session(spark):
    """The shared session, cleaned after each generated history. On the DuckDB engine every checkpoint leaves a table
    `_hold_<uuid>` behind, and hundreds of histories in one session ran it out of memory (a fresh session per history is
    no use: stopping one closes the connection every other test shares)."""
    try:
        yield spark
    finally:
        if type(spark).__module__.startswith("sqlframe"):
            conn = spark._conn
            held = conn.execute(r"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '\_hold\_%' ESCAPE '\'").fetchall()
            for (name,) in held:
                # shiftleft: allow SL-SEC-01 -- the name was read from the catalog and matched _hold_<uuid>; no value from data
                conn.execute(f'DROP TABLE IF EXISTS "{name}"')


def difference(got, want) -> str:
    return f"\n  silver has, model does not: {[g for g in got if g not in want]}\n  model has, silver does not: {[w for w in want if w not in got]}"


def sort_like_rows(items):
    return sorted(items, key=lambda t: tuple((x is not None, str(x)) for x in t))


def _as_changes(h: History):
    return [(c.key, c.value, c.seq, c.op) for c in h.changes]


def model_type2(h: History):
    return sort_like_rows(type2_versions(_as_changes(h)))


def model_type1(h: History):
    return sort_like_rows(type1_rows(_as_changes(h)))


def lands_inside_a_run(h: History) -> bool:
    """The known gap of 19 Nov (test_silver_stages: late-after-flip-back): a change that arrives late, with a different
    value, between two changes of one version that were already stored. Silver keeps only a version's start, so the
    later of the two is forgotten. Examples with this shape are skipped until it is fixed; remove this when it is."""
    seen: list[Change] = []
    for batch in h.arrivals:
        for c in batch:
            same_key = sorted((s for s in seen if s.key == c.key), key=lambda s: s.seq)
            before = [s for s in same_key if s.seq < c.seq]
            after = [s for s in same_key if s.seq > c.seq]
            if before and after and before[-1].value == after[0].value != c.value:
                return True
            seen.append(c)
    return False


def late_update_after_delete(h: History) -> bool:
    """The known gap (23 Nov 2026): an update that arrives after a delete of the same key but has an earlier sequence than the
    delete. Silver stores a delete as a flag on a version, without the delete's sequence, so the delete cannot move to the
    version the late update opens. Skipped until the sequence is stored; test_silver_stages has the minimal case."""
    deleted_at: dict[str, int] = {}
    for batch in h.arrivals:
        for c in batch:
            if c.op != "D" and c.key in deleted_at and c.seq < deleted_at[c.key]:
                return True
            if c.op == "D":
                deleted_at[c.key] = max(deleted_at.get(c.key, c.seq), c.seq)
    return False


def known_gap(h: History) -> bool:
    return lands_inside_a_run(h) or late_update_after_delete(h)


def explain(h: History) -> str:
    return "arrivals (key, value, seq, op): " + " | ".join(
        ", ".join(f"({c.key},{c.value},{c.seq},{c.op})" for c in batch) for batch in h.arrivals)


@SETTINGS
@given(h=histories())
def test_scd2_matches_the_reference_model(spark, h):
    hypothesis.assume(not known_gap(h))
    with example_session(spark) as session:
        silver = _apply(session, plan(**SCD2), batches_of(h))
        got = rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted")
        assert got == model_type2(h), explain(h) + difference(got, model_type2(h))
        assert_history_invariants(silver)


TYPE1 = {"kind": "transaction", "columns": SCD2["columns"],
         "dedup": {"mode": "business_key_latest", "business_keys": ["id"], "order_by": "seq"},
         "load": {"deletes": {"detect": "change_feed", "apply": "mark_deleted"}}}


@SETTINGS
@given(h=histories())
def test_scd1_matches_the_reference_model(spark, h):
    with example_session(spark) as session:
        silver = _apply(session, plan(**TYPE1), batches_of(h))
        got = rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted")
        assert got == model_type1(h), explain(h) + difference(got, model_type1(h))
        assert_history_invariants(silver)


@SETTINGS
@given(h=histories(), data=st.data())
def test_scd2_does_not_depend_on_arrival_order_or_batching(spark, h, data):
    """Metamorphic: the same changes in another order and split into other batches give the same silver."""
    hypothesis.assume(not known_gap(h))
    flat = [c for batch in h.arrivals for c in batch]
    shuffled = data.draw(st.permutations(flat))
    other = History(h.changes, (tuple(shuffled),))
    hypothesis.assume(not known_gap(other))
    cols = ("id", "v", "_seq_from", "_is_current", "_is_deleted")
    with example_session(spark) as session:
        first = rows(_apply(session, plan(**SCD2), batches_of(h)), *cols)
        second = rows(_apply(session, plan(**SCD2), batches_of(other)), *cols)
    assert first == second, explain(h) + " ## vs one batch: " + explain(other) + difference(first, second)
