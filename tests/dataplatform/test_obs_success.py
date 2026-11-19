"""The success scorecard (obs/success.py, B20). Table-driven: each measured criterion, met, not met and not seen."""

import pytest

from dataplatform.obs.success import COLUMNS, CRITERIA, Evidence, rows, scorecard


def state(e, cid):
    return next(s for s in scorecard(e) if s.id == cid)


CASES = [
    ("C1 every layer equal every day", Evidence(layer_days={"bronze": [True, True], "gold": [True, True]}), "C1",
     "met", "2 of 2 (100%)"),
    ("C1 one gold day off", Evidence(layer_days={"bronze": [True, True], "gold": [True, False]}), "C1", "not_met",
     "1 of 2 (50%)"),
    ("C1 nothing checked is not met by default", Evidence(), "C1", "not_measured", ""),
    ("C3 a drop named counts as handled", Evidence(recon=[("erp.a", True, False), ("erp.b", False, True)]), "C3",
     "met", "2 of 2 (100%)"),
    ("C3 a silent difference", Evidence(recon=[("erp.b", False, False)]), "C3", "not_met", "0 of 1 (0%)"),
    ("C4 one ledger failed", Evidence(ledger=["succeeded", "failed"]), "C4", "not_met", "1 of 2 (50%)"),
    ("C5 deferred at bronze, handled at silver", Evidence(verdicts=[("x", "deferred"), ("x", "handled")]), "C5",
     "met", "1 of 1 (100%)"),
    ("C5 one missed", Evidence(verdicts=[("x", "handled"), ("y", "missed")]), "C5", "not_met", "1 of 2 (50%)"),
    ("C8 within target; commit->bronze shown, not judged", Evidence(hops={"commit->bronze": 589_000,
                                                                          "bronze->silver": 994}), "C8", "met",
     "bronze->silver 16.6 min"),
    ("C8 slow silver", Evidence(hops={"bronze->silver": 2400}), "C8", "not_met", "bronze->silver 40.0 min"),
    ("C8 only the context hop: not measured", Evidence(hops={"commit->bronze": 600}), "C8", "not_measured", ""),
    ("C9 fast enough", Evidence(seconds_per_day=348), "C9", "met", "5.8 min a day"),
    ("C9 too slow", Evidence(seconds_per_day=400), "C9", "not_met", "6.7 min a day"),
    ("C15 every restricted value a token", Evidence(privacy=[("silver", "a.email", 0), ("bronze", "a.email", 0)]),
     "C15", "met", "0 in clear over 2 restricted columns"),
    ("C15 a clear email in silver", Evidence(privacy=[("silver", "a.email", 3)]), "C15", "not_met",
     "3 in clear over 1 restricted columns"),
    ("C10 cost is measured elsewhere", Evidence(seconds_per_day=1), "C10", "not_measured", ""),
]


@pytest.mark.parametrize(("case", "e", "cid", "want", "value"), CASES, ids=[c[0] for c in CASES])
def test_scorecard(case, e, cid, want, value):
    s = state(e, cid)
    assert (s.state, s.value) == (want, value), case


def test_every_criterion_is_scored_once_and_rows_match_the_columns():
    scores = scorecard(Evidence())
    assert [s.id for s in scores] == [c.id for c in CRITERIA] == [f"C{i}" for i in range(1, 21)]
    assert all(s.state == "not_measured" for s in scores)  # nothing seen: nothing met
    assert list(rows("r1", scores)[0]) == [c for c, _ in COLUMNS]
