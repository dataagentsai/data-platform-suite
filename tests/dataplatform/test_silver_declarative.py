"""The declarative engine's recipe per dataset (silver/declarative.py, ADR-035): which stages run in the flow and the
managed change-capture settings, or the reason it is refused."""

import pytest

from dataplatform.silver.declarative import OUTSIDE_THE_FLOW, ChangeCapture, recipe
from dataplatform.silver.plan import PlanError, SilverPlan


def plan(**kw):
    base = {"source": "erp", "dataset": "x", "columns": (), "keys": ("order_id", "line_no"), "history": "scd2",
            "kind": "master", "ingest_read": "change_feed", "order_by": "change_seq", "delete_detect": "change_feed",
            "delete_apply": "mark_deleted"}
    return SilverPlan(**{**base, **kw})


@pytest.mark.parametrize(
    ("case", "kw", "want"),
    [
        ("SCD2 from a change feed with deletes", {},
         ChangeCapture(("order_id", "line_no"), "change_seq", 2, "_op = 'D'", (), ("_op",))),
        ("SCD1 transaction", {"history": "scd1", "kind": "transaction"},
         ChangeCapture(("order_id", "line_no"), "change_seq", 1, "_op = 'D'", (), ("_op",))),
        ("tracked columns carried", {"track": ("promised_date",)},
         ChangeCapture(("order_id", "line_no"), "change_seq", 2, "_op = 'D'", ("promised_date",), ("_op",))),
        ("no delete rule: deletes not applied", {"delete_detect": "none", "delete_apply": "none"},
         ChangeCapture(("order_id", "line_no"), "change_seq", 2, None, (), ("_op",))),
        ("an API source has no change-feed op column to exclude",
         {"ingest_read": "api", "delete_detect": "none", "delete_apply": "none"},
         ChangeCapture(("order_id", "line_no"), "change_seq", 2, None, (), ())),
    ],
)
def test_history_datasets_get_a_change_capture(case, kw, want):
    r = recipe(plan(**kw))
    assert r.capture == want
    assert r.dedupe_keys == ()


@pytest.mark.parametrize(
    ("case", "kw", "want"),
    [
        ("append with de-duplication on its keys", {"history": "append", "kind": "event", "dedup_mode": "full_row",
                                                     "dedup_keys": ("reading_id",)}, ("reading_id",)),
        ("append falls back to the business keys", {"history": "append", "kind": "event",
                                                    "dedup_mode": "business_key_first"}, ("order_id", "line_no")),
        ("append without de-duplication", {"history": "append", "kind": "event", "dedup_mode": "none"}, ()),
    ],
)
def test_append_datasets(case, kw, want):
    r = recipe(plan(**kw))
    assert r.capture is None
    assert r.dedupe_keys == want


@pytest.mark.parametrize(
    ("case", "kw", "within", "noted"),
    [
        ("event time and lookback bound the state", {"event_time": "event_ts", "lookback_days": 2},
         ("event_ts", 2), False),
        ("no event time: unbounded, and said so", {}, None, True),
        ("no lookback: unbounded, and said so", {"event_time": "event_ts"}, None, True),
    ],
)
def test_append_dedupe_is_bounded_by_event_time(case, kw, within, noted):
    r = recipe(plan(history="append", kind="event", dedup_mode="business_key_first", **kw))
    assert r.dedupe_within == within
    assert bool(r.notes) == noted


def test_the_flow_skips_only_the_two_whole_table_stages():
    r = recipe(plan(disabled=frozenset({"hygiene"})))
    assert r.flow_plan.disabled == frozenset({"hygiene"}) | OUTSIDE_THE_FLOW
    assert r.flow_plan.runs("quality")
    assert r.flow_plan.runs("provenance")
    assert not r.flow_plan.runs("history")
    assert not r.flow_plan.runs("duplicates")


@pytest.mark.parametrize(
    ("case", "kw", "says"),
    [
        ("snapshot history", {"history": "snapshot", "kind": "snapshot"}, "history 'snapshot'"),
        ("source-versioned history", {"history": "source_versioned"}, "history 'source_versioned'"),
        ("no change order", {"order_by": ""}, "needs a change order"),
        ("no business keys", {"keys": ()}, "needs business keys"),
        ("deletes by comparing full copies", {"delete_detect": "key_compare"}, "comparing full copies"),
    ],
)
def test_refused_with_the_reason(case, kw, says):
    with pytest.raises(PlanError, match=says):
        recipe(plan(**kw))
