"""Freshness against the manifest's targets (FRESH-01): table-driven over the pure functions, plus the targets the
tyre manifest gives when it is loaded into the store."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.meta import loader
from dataplatform.obs.freshness import (
    FreshnessTarget,
    check,
    next_due,
    state_of,
    target_from_view,
    targets_from_store,
    tier_for,
)
from dataplatform.obs.runlog import DatasetRunRow

NOW = datetime(2026, 11, 10, 0, 0, tzinfo=UTC)
TYRE = Path.home() / "data-platform-catalog/slices/02-tyre/manifest"
T = FreshnessTarget("erp", "orders", 1.0, 2.0, 1, "erp-rota", "sales@x")


def ran(hours_ago, status="succeeded", ended=True):
    at = NOW - timedelta(hours=hours_ago)
    return DatasetRunRow("r", "erp", "orders", status, "fp", None, None, {}, "", at - timedelta(minutes=1),
                         at if ended else None)


@pytest.mark.parametrize(
    ("age", "warn", "error", "state"),
    [(None, 1, 2, "never"), (0.5, 1, 2, "ok"), (1.0, 1, 2, "ok"), (1.5, 1, 2, "warn"), (2.5, 1, 2, "error")],
)
def test_state(age, warn, error, state):
    assert state_of(age, warn, error) == state


@pytest.mark.parametrize(
    ("error_hours", "override", "tier"), [(2, None, 1), (4, None, 1), (48, None, 2), (120, None, 3), (2, 3, 3)]
)
def test_tier(error_hours, override, tier):
    assert tier_for(error_hours, override) == tier


DS = [
    ("never loaded", [], {}, [("", "none", None, "never")]),
    ("fresh", [ran(0.5)], {}, [("", "last_success", 0.5, "ok")]),
    ("newest success counts, failures do not", [ran(5), ran(1.5), ran(0.1, "failed")], {},
     [("", "last_success", 1.5, "warn")]),
    ("a run still open does not count", [ran(3), ran(0.1, ended=False)], {}, [("", "last_success", 3.0, "error")]),
    ("a run after the check time does not count", [ran(-1), ran(3)], {}, [("", "last_success", 3.0, "error")]),
    ("one lagging bookmark is an error, its siblings ok",
     [ran(0.2)], {"D-1": "2026-10-31T23:59:59+05:30", "D-2": "2026-10-31T23:59:59+05:30",
                  "D-42": "2026-09-30T23:59:59+05:30"},
     [("", "last_success", 0.2, "ok"), ("D-1", "bookmark_lag", 0.0, "ok"), ("D-2", "bookmark_lag", 0.0, "ok"),
      ("D-42", "bookmark_lag", 744.0, "error")]),
    ("bookmarks that are not times are ignored", [ran(0.2)], {"shard-0": "1234", "shard-1": "1300"},
     [("", "last_success", 0.2, "ok")]),
    ("a single bookmark has nothing to lag behind", [ran(0.2)], {"D-1": "2026-10-01T00:00:00+00:00"},
     [("", "last_success", 0.2, "ok")]),
]


@pytest.mark.parametrize(("case", "runs", "marks", "expect"), DS, ids=[c[0] for c in DS])
def test_check(case, runs, marks, expect):
    got = check([T], runs, {("erp", "orders"): marks}, NOW)
    assert [(r.key, r.basis, r.age_hours, r.state) for r in got] == expect
    assert {r.tier for r in got} == {1}


@pytest.mark.parametrize(
    ("view", "expect"),
    [({"source": "s", "dataset": "d", "settings": {}}, None),
     ({"source": "s", "dataset": "d", "settings": {"sla_freshness_hours": "24"}, "manifest": {}},
      FreshnessTarget("s", "d", 24.0, 48.0, 2, "", "")),
     ({"source": "s", "dataset": "d", "settings": {"sla_freshness_hours": "1", "sla_freshness_error_hours": "2"},
       "manifest": {"owners": {"dataset": "o@x", "alerts": "rota"}}},
      FreshnessTarget("s", "d", 1.0, 2.0, 1, "rota", "o@x"))],
)
def test_target_from_view(view, expect):
    assert target_from_view(view) == expect


@pytest.mark.skipif(not TYRE.exists(), reason="spec catalog not beside the repo")
def test_tyre_targets_come_from_the_manifest():
    store = MemoryMetaStore()
    loader.load(store, TYRE)
    got = {t.name: (t.warn_hours, t.error_hours, t.tier, t.alert_to) for t in targets_from_store(store, "tyre")}
    assert got["erp.sales_order_line"] == (1.0, 2.0, 1, "erp-data-rota")
    assert got["partner.dealer_sellout"] == (72.0, 120.0, 3, "partner-data-rota")
    assert got["erp.material"][2] == 2
    assert len(got) == 9
    overridden = targets_from_store(store, "tyre", {"erp.material": 1})
    assert next(t.tier for t in overridden if t.name == "erp.material") == 1


T2 = FreshnessTarget("erp", "lines", 1.0, 6.0, 2, "erp-rota", "sales@x")


def ran_ds(dataset, hours_ago, status="succeeded"):
    at = NOW - timedelta(hours=hours_ago)
    return DatasetRunRow("r", "erp", dataset, status, "fp", None, None, {}, "", at - timedelta(minutes=1), at)


@pytest.mark.parametrize(
    ("case", "runs", "due_in_hours"),
    [
        ("nothing loaded yet: nothing to wake for", [], None),
        ("the earliest error target to pass", [ran_ds("orders", 0.5), ran_ds("lines", 0.5)], 1.5),
        ("already late: wait for the next one only", [ran_ds("orders", 3), ran_ds("lines", 1)], 5.0),
        ("everything already late: no wake-up", [ran_ds("orders", 3), ran_ds("lines", 7)], None),
        ("a failed run does not move the deadline", [ran_ds("orders", 0.5), ran_ds("orders", 0.1, "failed")], 1.5),
    ],
)
def test_next_due_wakes_only_when_lateness_can_change(case, runs, due_in_hours):
    due = next_due([T, T2], runs, NOW)
    assert (None if due is None else round((due - NOW).total_seconds() / 3600, 3)) == due_in_hours
