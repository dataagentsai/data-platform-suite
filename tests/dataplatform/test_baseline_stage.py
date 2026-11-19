"""The baseline batch (stages/baseline.py, B8, assumed decision A3): bronze compacted to the last row per key, and
the replay of that baseline through the processors compared with silver. Runs on Spark and on DuckDB."""

from functools import reduce

import pytest

pytest.importorskip("pyspark")
from silver_helpers import frame, hold, plan, rows, stage_hold, ts

from dataplatform.stages import run_stages
from dataplatform.stages.baseline import compact, compactable, replay_check

pytestmark = pytest.mark.usefixtures("spark")

COLS = [("id", "string"), ("v", "integer"), ("note", "string"), ("seq", "integer")]
FEED = "id string, v long, note string, seq long, _op string, _committed_at timestamp, _feed_pos long"
SCD2 = {"kind": "master", "columns": COLS, "history": {"track_columns": ["v"]},
        "load": {"history_order_by": "seq", "deletes": {"detect": "change_feed", "apply": "mark_deleted"}}}
SCD1 = {"kind": "transaction", "columns": COLS,
        "dedup": {"mode": "business_key_latest", "business_keys": ["id"], "order_by": "seq"},
        "load": {"deletes": {"detect": "change_feed", "apply": "mark_deleted"}}}


def batch(spark, n, data):
    return frame(spark, FEED, [(k, v, "x", seq, op, ts(seq), pos) for k, v, seq, op, pos in data], batch=f"b{n}",
                 window=f"2026-11-{8 + n:02d}")


def apply(spark, p, batches):
    """Silver as a frame after each batch (a merge on _row_key done in memory), as the job engine writes it."""
    current = None
    for n, df in enumerate(batches, 1):
        out, _, _ = run_stages(p, df, current, f"silver-{n}", f"2026-11-{8 + n:02d}", hold=stage_hold(df))
        out = hold(out)
        keep = [] if current is None else (
            current.join(out.select("_row_key"), "_row_key", "left_anti").select(*current.columns).collect())
        new = out.where("_action = 'upsert'").drop("_action")
        current = hold(new if not keep else new.unionByName(spark.createDataFrame(keep, current.schema)))
    return current


@pytest.mark.parametrize(("case", "kw", "why"), [
    ("SCD2 change feed", SCD2, ""),
    ("SCD1 change feed", SCD1, ""),
    ("a full copy is its own baseline", {**SCD2, "read": "full_copy"}, "full copy"),
    ("events keep their bronze", {"kind": "event", "columns": COLS}, "every row"),
])
def test_compactable(case, kw, why):
    got = compactable(plan(**kw))
    assert (why in got if why else got == ""), (case, got)


# (id, v, seq, op, feed position)
@pytest.mark.parametrize(("case", "data", "want"), [
    ("the highest change sequence wins", [("a", 1, 1, "I", 1), ("a", 3, 3, "U", 2), ("a", 2, 2, "U", 3)],
     [("a", 3, 3, "U")]),
    ("each key keeps one row", [("a", 1, 1, "I", 1), ("b", 5, 1, "I", 2)], [("a", 1, 1, "I"), ("b", 5, 1, "I")]),
    ("a delete as the last change stays, so a replay marks it deleted", [("a", 1, 1, "I", 1), ("a", 1, 2, "D", 2)],
     [("a", 1, 2, "D")]),
    ("same sequence: the later arrival wins", [("a", 1, 4, "U", 1), ("a", 9, 4, "U", 2)], [("a", 9, 4, "U")]),
])
def test_compact(spark, case, data, want):
    assert rows(compact(batch(spark, 1, data), plan(**SCD2)), "id", "v", "seq", "_op") == want, case


def test_compact_refuses_events(spark):
    with pytest.raises(ValueError, match="no baseline"):
        compact(batch(spark, 1, [("a", 1, 1, "I", 1)]), plan(kind="event", columns=COLS))


HISTORY = [[("a", 1, 1, "I", 1), ("b", 2, 1, "I", 2), ("c", 3, 1, "I", 3)],
           [("a", 5, 2, "U", 4), ("c", 3, 2, "D", 5)],
           [("b", 7, 3, "U", 6), ("d", 8, 3, "I", 7)]]


@pytest.mark.parametrize(("case", "kw", "cut", "silver_drop", "want"), [
    ("SCD2: baseline of days 1-2 replays to silver; b moved on day 3", SCD2, 2, None,
     {"baseline_keys": 1, "silver_keys": 1, "missing": 0, "extra": 0}),
    ("SCD1: same", SCD1, 2, None, {"baseline_keys": 1, "silver_keys": 1, "missing": 0, "extra": 0}),
    ("every batch in the baseline", SCD2, 3, None, {"baseline_keys": 3, "silver_keys": 3, "missing": 0, "extra": 0}),
    ("silver lost a key: the replay shows it as extra", SCD2, 3, "a",
     {"baseline_keys": 3, "silver_keys": 2, "missing": 0, "extra": 1}),
])
def test_replay_check(spark, case, kw, cut, silver_drop, want):  # noqa: PLR0917 -- one argument per table column
    p = plan(**kw)
    batches = [batch(spark, n, data) for n, data in enumerate(HISTORY, 1)]
    silver = apply(spark, p, batches)
    if silver_drop:
        silver = hold(silver.where(f"id != '{silver_drop}'"))
    base = compact(reduce(lambda x, y: x.unionByName(y), batches[:cut]), p)
    replayed, _, _ = run_stages(p, base, None, "hk", "2026-11-12", hold=stage_hold(base))
    later = None
    if cut < len(batches):
        rest = reduce(lambda x, y: x.unionByName(y), batches[cut:])
        later, _, _ = run_stages(p, rest, None, "hk", "2026-11-12", hold=stage_hold(rest))
    assert replay_check(hold(replayed), later, silver) == want, case
