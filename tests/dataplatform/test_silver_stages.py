"""The thirteen silver stages (ADR-007), one table of cases each, on a shared local Spark. Every stage is a pure
function: frames in, frames out (ADR-013); nothing here reads or writes a table.

Rules proven here (DAC): PROJ-01 (every contract column reaches silver unless a selection is declared), DUP-03
(duplicates inside the source), PURE-01 (every stage runs with no read or write).
"""

import hashlib
from datetime import UTC, date, datetime

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import Window
from pyspark.sql import functions as F
from silver_helpers import (
    TOKEN,
    assert_history_invariants,
    assert_nothing_dropped,
    frame,
    hold,
    plan,
    rows,
    stage_hold,
    ts,
    view,
)

from dataplatform.silver.history_contract import CASES as HISTORY_CONTRACT
from dataplatform.silver.plan import PlanError, Rule, plan_from_view
from dataplatform.silver.ports import StageError
from dataplatform.stages import FUNCTIONS, run_stages
from dataplatform.stages.common import StageCtx, text_parts

pytestmark = pytest.mark.usefixtures("spark")


def run(stage, df, p, current=None, as_of="2026-11-09"):
    return FUNCTIONS[stage](df, StageCtx(p, current, "silver-1", as_of))


def reasons(out):
    return sorted(r["_reason"] for r in out.quarantine.collect()) if out.quarantine is not None else []


# ---------------------------------------------------------------------------------------------------- 1 contract check
@pytest.mark.parametrize(
    ("case", "ddl", "data", "kw", "error"),
    [
        ("every column present", "id string, v long, seq long", [("a", 1, 1)], {}, None),
        ("an extra column is fine (all columns, ADR-008)", "id string, v long, seq long, w string",
         [("a", 1, 1, "x")], {}, None),
        ("a missing column stops the batch, named", "id string, seq long", [("a", 1)], {}, "contract column\\(s\\) v"),
        ("a renamed column is looked up by its source name", "id string, value long, seq long", [("a", 1, 1)],
         {"column_settings": [{"name": "v", "from": "value"}]}, None),
        ("an excluded column is not required", "id string, seq long", [("a", 1)],
         {"column_settings": [{"name": "v", "exclude": True}]}, None),
    ],
)
def test_contract_check(spark, case, ddl, data, kw, error):
    df = frame(spark, ddl, data)
    if error:
        with pytest.raises(StageError, match=error):
            run("contract_check", df, plan(**kw))
    else:
        assert rows(run("contract_check", df, plan(**kw)).df, "id") == [(r[0],) for r in data], case


# ---------------------------------------------------------------------------------------------------- 2 project
@pytest.mark.parametrize(
    ("case", "ddl", "data", "kw", "want_cols", "absent"),
    [
        ("all columns by default", "id string, v long, seq long, w string", [("a", 1, 1, "x")], {},
         ["id", "v", "seq", "w", "_batch_key", "_dq_flags"], []),
        ("rename from the source name", "id string, value long, seq long", [("a", 1, 1)],
         {"column_settings": [{"name": "v", "from": "value"}]}, ["v"], ["value"]),
        ("exclude a column", "id string, v long, seq long", [("a", 1, 1)],
         {"column_settings": [{"name": "seq", "exclude": True}]}, ["id", "v"], ["seq"]),
        ("extract from the held columns", "id string, seq long, _extra string", [("a", 1, '{"v": 7}')],
         {"column_settings": [{"name": "v", "path": "$.v"}]}, ["v"], []),
    ],
)
def test_project(spark, case, ddl, data, kw, want_cols, absent):
    out = run("project", frame(spark, ddl, data), plan(**kw)).df
    assert set(want_cols) <= set(out.columns) and not set(absent) & set(out.columns), case


def test_project_declared_keeps_only_the_contract(spark):
    p = plan_from_view(view(), TOKEN, projection="declared")
    out = run("project", frame(spark, "id string, v long, seq long, w string", [("a", 1, 1, "x")]), p).df
    assert "w" not in out.columns
    assert "id" in out.columns


def test_project_extracts_the_value(spark):
    out = run("project", frame(spark, "id string, seq long, _extra string", [("a", 1, '{"v": 7}')]),
              plan(column_settings=[{"name": "v", "path": "$.v"}])).df
    assert rows(out, "v") == [("7",)]


# ---------------------------------------------------------------------------------------------------- 3 hygiene
@pytest.mark.parametrize(
    ("case", "raw", "want", "kw"),
    [
        ("trimmed", "  a b  ", "a b", {}),
        ("runs of spaces collapsed", "a   b", "a b", {}),
        ("empty becomes null", "   ", None, {}),
        ("control characters removed", "a\x07b", "ab", {}),
        ("byte-order mark removed", "﻿ab", "ab", {}),
        ("line endings made \\n", "a\r\nb", "a\nb", {}),
        ("null stays null", None, None, {}),
        ("a column may opt out", "  a  ", "  a  ", {"column_settings": [{"name": "s", "hygiene": False}]}),
        ("the stage switched off leaves values", "  a  ", "  a  ", {"settings": {"processor_groups.hygiene": "disabled"}}),
    ],
)
def test_hygiene(spark, case, raw, want, kw):
    p = plan(columns=[("id", "string"), ("s", "string")], **kw)
    out, _, _ = run_stages(p, frame(spark, "id string, s string", [("a", raw)]), None, "r", "2026-11-09")
    assert rows(out, "s") == [(want,)], case


# ---------------------------------------------------------------------------------------------------- 4 parse and type
@pytest.mark.parametrize(
    ("case", "logical", "raw", "settings", "want", "quarantined", "emptied"),
    [
        ("text to whole number", "integer", "42", {}, 42, [], 0),
        ("a fraction is never truncated", "integer", "4.5", {}, None, ["x is not a integer"], 0),
        ("text to number", "number", "4.5", {}, 4.5, [], 0),
        ("text to date", "date", "2026-11-09", {}, date(2026, 11, 9), [], 0),
        ("a date with a declared pattern", "date", "09.11.2026", {"format": "dd.MM.yyyy"}, date(2026, 11, 9), [], 0),
        ("a bad date is quarantined (DQ-02)", "date", "31.02.2026", {"format": "dd.MM.yyyy"}, None,
         ["x is not a date"], 0),
        ("text to boolean", "boolean", "true", {}, True, [], 0),
        ("an unconvertible value emptied when the column says so, and counted", "integer", "n/a",
         {"on_cast_failure": "empty"}, None, [], 1),
        ("null stays null, no failure", "integer", None, {}, None, [], 0),
    ],
)
def test_parse_type(spark, case, logical, raw, settings, want, quarantined, emptied):
    p = plan(columns=[("id", "string"), ("x", logical)], column_settings=[{"name": "x", **settings}])
    df = run("project", frame(spark, "id string, x string", [("a", raw)]), p).df
    out = run("parse_type", df, p)
    assert reasons(out) == quarantined, case
    assert rows(out.df, "x") == ([] if quarantined else [(want,)])
    assert (out.tally["emptied"].count() if "emptied" in out.tally else 0) == emptied
    if emptied:
        assert rows(out.df, "_dq_flags") == [(["emptied:x"],)]


def test_parse_type_leaves_typed_columns_alone(spark):
    p = plan()
    df = run("project", frame(spark, "id string, v long, seq long", [("a", 1, 2)]), p).df
    out = run("parse_type", df, p)
    assert reasons(out) == []
    assert rows(out.df, "v") == [(1,)]


# ---------------------------------------------------------------------------------------------------- 5 time
@pytest.mark.parametrize(
    ("case", "settings", "want"),
    [
        ("UTC kept, nothing added", {}, None),
        ("local time beside it", {"local_tz": "Asia/Kolkata"}, "2026-11-09T05:30:00"),
        ("another zone", {"local_tz": "Europe/Budapest"}, "2026-11-09T01:00:00"),
    ],
)
def test_time(spark, case, settings, want):
    p = plan(columns=[("id", "string"), ("at", "timestamp")], column_settings=[{"name": "at", **settings}])
    df = run("project", frame(spark, "id string, at timestamp", [("a", ts(0))]), p).df
    out = run("time", df, p).df
    if want is None:
        assert "at_local" not in out.columns, case
    else:
        assert rows(out, "at_local") == [(want,)], case
    assert rows(out, "at")[0][0] is not None


# ---------------------------------------------------------------------------------------------------- 6 standardise
@pytest.mark.parametrize(
    ("case", "settings", "raw", "want"),
    [
        ("nothing declared", {}, "Ab", "Ab"),
        ("upper", {"case": "upper"}, "Ab", "AB"),
        ("lower", {"case": "lower"}, "Ab", "ab"),
        ("a value map", {"values": {"N": "north"}}, "N", "north"),
        ("a value not in the map is kept", {"values": {"N": "north"}}, "S", "S"),
        ("map then case", {"values": {"n": "North"}, "case": "upper"}, "n", "NORTH"),
    ],
)
def test_standardise(spark, case, settings, raw, want):
    p = plan(columns=[("id", "string"), ("r", "string")], column_settings=[{"name": "r", **settings}])
    df = run("project", frame(spark, "id string, r string", [("a", raw)]), p).df
    assert rows(run("standardise", df, p).df, "r") == [(want,)], case


# ---------------------------------------------------------------------------------------------------- 7 personal data
@pytest.mark.parametrize(
    ("case", "value", "held"),
    [
        ("a token passes", "tok_" + "a" * 24, []),
        ("a plain value is quarantined (DPC-0070)", "someone@example.com", ["email is not tokenised"]),
        ("an empty value passes", None, []),
    ],
)
def test_personal_data(spark, case, value, held):
    p = plan(columns=[("id", "string"), ("email", "string")], restricted=("email",))
    df = run("project", frame(spark, "id string, email string", [("a", value)]), p).df
    out = run("personal_data", df, p)
    assert reasons(out) == held, case
    assert out.df.count() == (0 if held else 1)


def test_personal_data_needs_a_token_format(spark):
    p = plan_from_view(view(columns=[("id", "string"), ("email", "string")], restricted=("email",)), "")
    with pytest.raises(StageError, match="no token format"):
        run("personal_data", frame(spark, "id string, email string", [("a", "x")]), p)


# ---------------------------------------------------------------------------------------------------- 8 row filter
@pytest.mark.parametrize(
    ("case", "filters", "kept", "dropped"),
    [
        ("no filter", (), ["a", "b", "c"], 0),
        ("one filter", (("positive", "v > 0"),), ["a", "b"], 1),
        ("two filters", (("positive", "v > 0"), ("small", "v < 5")), ["a"], 2),
        ("a null predicate drops", (("w_set", "w IS NOT NULL AND w = 'x'"),), ["a"], 2),
    ],
)
def test_row_filter(spark, case, filters, kept, dropped):
    p = plan_from_view(view(columns=[("id", "string"), ("v", "integer"), ("w", "string")]), TOKEN,
                       row_filters=tuple(Rule(n, pred, "", "drop") for n, pred in filters))
    df = frame(spark, "id string, v long, w string", [("a", 1, "x"), ("b", 9, None), ("c", -1, None)])
    out = run("row_filter", df, p)
    assert [r[0] for r in rows(out.df, "id")] == kept, case
    assert (out.tally["dropped"].count() if out.tally else 0) == dropped


# ---------------------------------------------------------------------------------------------------- 9 quality
@pytest.mark.parametrize(
    ("case", "quality", "v", "held", "flags"),
    [
        ("a good row passes", None, 1, [], []),
        ("a missing required value is quarantined (critical)", None, None, ["v_present"], []),
        ("flagged and kept when the dataset says flag", {"actions": {"critical": {"rows": "flag"}}}, None, [],
         ["v_present"]),
    ],
)
def test_quality(spark, case, quality, v, held, flags):
    p = plan(required=("v",), quality=quality)
    df = run("project", frame(spark, "id string, v long, seq long", [("a", v, 1)]), p).df
    out = run("quality", df, p)
    assert reasons(out) == held, case
    if not held:
        assert rows(out.df, "_dq_flags") == [(flags,)]


def test_one_poison_record_does_not_stop_the_load(spark):
    """DQ-03: the bad row is set aside with its reason; the good rows of the same batch go on."""
    p = plan(required=("v",))
    df = run("project", frame(spark, "id string, v long, seq long", [("a", 1, 1), ("b", None, 1), ("c", 3, 1)]), p).df
    out = run("quality", df, p)
    assert reasons(out) == ["v_present"]
    assert rows(out.df, "id") == [("a",), ("c",)]


# ---------------------------------------------------------------------------------------------------- 10 duplicates
def _prep(spark, data, p, ddl="id string, v long, seq long, _feed_pos long"):
    return run("project", frame(spark, ddl, data), p).df


@pytest.mark.parametrize(
    ("case", "kw", "data", "want"),
    [
        ("no dedup", {}, [("a", 1, 1, 1), ("a", 1, 1, 2)], [("a", 1), ("a", 1)]),
        ("first arrival per key (DUP-04)", {"dedup": {"mode": "business_key_first", "business_keys": ["id"]}},
         [("a", 2, 1, 2), ("a", 1, 1, 1), ("b", 3, 1, 3)], [("a", 1), ("b", 3)]),
        ("latest by the order column", {"kind": "transaction",
                                        "dedup": {"mode": "business_key_latest", "business_keys": ["id"],
                                                  "order_by": "seq"}},
         [("a", 1, 2, 1), ("a", 2, 1, 2)], [("a", 1)]),
        ("identical rows", {"dedup": {"mode": "full_row", "business_keys": ["id"]}},
         [("a", 1, 1, 1), ("a", 1, 1, 2), ("a", 2, 1, 3)], [("a", 1), ("a", 2)]),
        ("type 2 keeps every distinct change, drops a change sent twice",
         {"kind": "master", "load": {"history_order_by": "seq"},
          "dedup": {"mode": "business_key_latest", "business_keys": ["id"], "order_by": "seq"}},
         [("a", 1, 1, 1), ("a", 2, 2, 2), ("a", 2, 2, 3)], [("a", 1), ("a", 2)]),
    ],
)
def test_duplicates(spark, case, kw, data, want):
    p = plan(**kw)
    out = run("duplicates", _prep(spark, data, p), p)
    assert rows(out.df, "id", "v") == want, case
    assert out.tally["dropped"].count() == len(data) - len(want)


@pytest.mark.parametrize(
    ("case", "lookback", "silver_window", "replayed"),
    [
        ("a key already in silver is a replay (DUP-04)", None, "2026-11-01", 1),
        ("inside the late lookback", 2, "2026-11-08", 1),
        ("outside the late lookback it is not looked at", 2, "2026-11-01", 0),
    ],
)
def test_duplicates_against_silver(spark, case, lookback, silver_window, replayed):
    p = plan(dedup={"mode": "business_key_first", "business_keys": ["id"]},
             load={"late_lookback_days": lookback} if lookback is not None else None)
    current = spark.createDataFrame([("a", silver_window)], "id string, _window string")
    out = run("duplicates", _prep(spark, [("a", 1, 1, 1), ("b", 1, 1, 2)], p), p, current, "2026-11-09")
    assert out.tally["replayed"].count() == replayed, case
    assert rows(out.df, "id") == ([("b",)] if replayed else [("a",), ("b",)])


# ---------------------------------------------------------------------------------------------------- 11 keys
@pytest.mark.parametrize(
    ("case", "ids", "held"),
    [
        ("keys present", [("a", 1), ("b", 2)], []),
        ("an empty key part is quarantined", [("a", None), ("b", 2)], ["business key n is empty"]),
    ],
)
def test_keys(spark, case, ids, held):
    p = plan(columns=[("id", "string"), ("n", "integer"), ("seq", "integer")], keys=("id", "n"))
    df = run("project", frame(spark, "id string, n long, seq long", [(i, n, 1) for i, n in ids]), p).df
    out = run("keys", df, p)
    assert reasons(out) == held, case
    hashes = rows(out.df, "_key_hash")
    assert len(hashes) == len(ids) - len(held)
    assert all(len(h[0]) == 64 for h in hashes)


def test_keys_hash_is_stable_and_distinguishes_parts(spark):
    p = plan(columns=[("id", "string"), ("n", "string"), ("seq", "integer")], keys=("id", "n"))
    df = run("project", frame(spark, "id string, n string, seq long", [("ab", "c", 1), ("a", "bc", 1),
                                                                         ("ab", "c", 2)]), p).df
    h = [r[0] for r in rows(run("keys", df, p).df, "_key_hash")]
    assert len(set(h)) == 2


# ---------------------------------------------------------------------------------------------------- 12 history
# Each case: plan, the batches in arrival order, then what silver holds after all of them, as
# (id, v, seq_from, is_current, is_deleted) for type 2 and type 1, (id, v, is_current) for snapshot.
SCD2 = {"kind": "master", "history": {"track_columns": ["v"]}, "load": {"history_order_by": "seq",
                                                                       "deletes": {"detect": "change_feed",
                                                                                   "apply": "mark_deleted"}},
        "columns": [("id", "string"), ("v", "integer"), ("note", "string"), ("seq", "integer")]}
FEED = "id string, v long, note string, seq long, _op string, _committed_at timestamp, _feed_pos long"


def _apply(spark, p, batches, ddl=FEED):
    """Run every stage for each batch and keep silver as a frame (a merge on _row_key done in memory)."""
    current = None
    for n, data in enumerate(batches, 1):
        df = frame(spark, ddl, data, batch=f"b{n}", window=f"2026-11-{8 + n:02d}")
        out, _, _ = run_stages(p, df, current, f"silver-{n}", f"2026-11-{8 + n:02d}", hold=stage_hold(df))
        out = hold(out)
        keep = [] if current is None else (
            current.join(out.select("_row_key"), "_row_key", "left_anti").select(*current.columns).collect())
        new = out.where("_action = 'upsert'").drop("_action")
        current = new if not keep else new.unionByName(spark.createDataFrame(keep, current.schema))
        current = hold(current)
    return current


def _feed(batches):
    """The shared contract's change rows (committed minute as an int) as this test's feed rows."""
    return [[(k, v, note, seq, op, ts(minute), pos) for k, v, note, seq, op, minute, pos in batch] for batch in batches]


@pytest.mark.parametrize(("case", "batches", "want"), HISTORY_CONTRACT, ids=[c[0] for c in HISTORY_CONTRACT])
def test_history_type2(spark, case, batches, want):
    """The SCD Type 2 contract both silver engines pass (ADR-035; silver/history_contract.py)."""
    silver = _apply(spark, plan(**SCD2), _feed(batches))
    assert rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted") == want, case
    assert_history_invariants(silver)


def test_history_type2_validity_never_overlaps(spark):
    """The late change committed after the newer one: its validity collapses, the current version starts first."""
    silver = _apply(spark, plan(**SCD2), [[("a", 1, "x", 1, "I", ts(0), 1)], [("a", 3, "x", 5, "U", ts(5), 2)],
                                          [("a", 2, "x", 4, "U", ts(65), 3)]])
    got = {r["_seq_from"]: (r["_valid_from"], r["_valid_to"]) for r in silver.collect()}
    assert got[1][1] == got[4][0] == got[5][0]
    assert got[4][1] == got[5][0]
    assert got[5][1] is None


@pytest.mark.parametrize(
    ("case", "deletes", "copies", "want"),
    [
        ("a key missing from a full copy is marked deleted (key compare)", {"detect": "key_compare",
                                                                          "apply": "mark_deleted"},
         [[("a", 1, 1), ("b", 2, 1)], [("a", 1, 2)]], [("a", 1, True, False), ("b", 2, True, True)]),
        ("no delete rule: the key stays", {"detect": "none", "apply": "none"},
         [[("a", 1, 1), ("b", 2, 1)], [("a", 1, 2)]], [("a", 1, True, False), ("b", 2, True, False)]),
        ("a key that comes back is live again", {"detect": "key_compare", "apply": "mark_deleted"},
         [[("a", 1, 1), ("b", 2, 1)], [("a", 1, 2)], [("a", 1, 3), ("b", 2, 3)]],
         [("a", 1, True, False), ("b", 2, True, False)]),
    ],
)
def test_history_full_copy_deletes(spark, case, deletes, copies, want):
    p = plan(kind="master", read="full_copy", columns=[("id", "string"), ("v", "integer"), ("seq", "integer")],
             history={"track_columns": ["v"]}, load={"history_order_by": "seq", "deletes": deletes})
    silver = _apply(spark, p, copies, "id string, v long, seq long")
    assert rows(silver, "id", "v", "_is_current", "_is_deleted") == want, case


@pytest.mark.parametrize(
    ("case", "batches", "want"),
    [
        ("the latest change wins", [[("a", 1, "x", 1, "I", ts(0), 1)], [("a", 2, "x", 2, "U", ts(1), 2)]],
         [("a", 2, 2, True, False)]),
        ("an older change arriving late does not overwrite", [[("a", 2, "x", 2, "U", ts(1), 1)],
                                                             [("a", 1, "x", 1, "I", ts(0), 2)]],
         [("a", 2, 2, True, False)]),
        ("a delete is soft", [[("a", 1, "x", 1, "I", ts(0), 1)], [("a", 1, "x", 2, "D", ts(1), 2)]],
         [("a", 1, 2, True, True)]),
    ],
)
def test_history_type1(spark, case, batches, want):
    p = plan(kind="transaction", columns=SCD2["columns"],
             dedup={"mode": "business_key_latest", "business_keys": ["id"], "order_by": "seq"},
             load={"deletes": {"detect": "change_feed", "apply": "mark_deleted"}})
    silver = _apply(spark, p, batches)
    assert rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted") == want, case
    assert_history_invariants(silver)


@pytest.mark.parametrize(
    ("case", "batches", "want"),
    [
        ("a window's rows", [[("d1", "m1", 3, "f1"), ("d1", "m2", 4, "f1")]],
         [("d1", "m1", 3, True), ("d1", "m2", 4, True)]),
        ("a newer file replaces the window; the old rows stay, not current",
         [[("d1", "m1", 3, "f1"), ("d1", "m2", 4, "f1")], [("d1", "m1", 5, "f2")]],
         [("d1", "m1", 3, False), ("d1", "m1", 5, True), ("d1", "m2", 4, False)]),
        ("another window is untouched", [[("d1", "m1", 3, "f1")], [("d2", "m1", 9, "f2")]],
         [("d1", "m1", 3, True), ("d2", "m1", 9, True)]),
        ("an older file sent again does not win", [[("d1", "m1", 5, "f2")], [("d1", "m1", 3, "f1")]],
         [("d1", "m1", 5, True)]),
    ],
)
def test_history_snapshot(spark, case, batches, want):
    p = plan(kind="snapshot", keys=("dealer", "mat"), read="files",
             columns=[("dealer", "string"), ("mat", "string"), ("units", "integer"), ("file_name", "string")],
             dedup={"mode": "business_key_latest", "business_keys": ["dealer", "mat"], "order_by": "file_name"})
    silver = _apply(spark, p, batches, "dealer string, mat string, units long, file_name string")
    assert rows(silver, "dealer", "mat", "units", "_is_current") == want, case


@pytest.mark.parametrize(
    ("case", "batches", "want"),
    [
        ("new rows appended", [[("r1", 1, 1, 1)], [("r2", 1, 1, 2)]], ["r1", "r2"]),
        ("a replay across batches is dropped (DUP-04)", [[("r1", 1, 1, 1)], [("r1", 1, 1, 2), ("r2", 1, 1, 3)]],
         ["r1", "r2"]),
        ("a replay within a batch is dropped", [[("r1", 1, 1, 1), ("r1", 1, 1, 2)]], ["r1"]),
    ],
)
def test_history_append(spark, case, batches, want):
    p = plan(dedup={"mode": "business_key_first", "business_keys": ["id"]})
    silver = _apply(spark, p, batches, "id string, v long, seq long, _seq long")
    assert [r[0] for r in rows(silver, "id")] == want, case


def test_late_readings_keep_their_event_time(spark):
    """LATE-03: a reading that arrives late is appended with its own event time, so it counts in its event hour."""
    p = plan(columns=[("id", "string"), ("event_ts", "timestamp")], settings={"watermark_column": "event_ts"},
             dedup={"mode": "business_key_first", "business_keys": ["id"]})
    silver = _apply(spark, p, [[("r1", ts(0)), ("r2", ts(61))], [("r3", ts(1))]], "id string, event_ts timestamp")
    hours = silver.groupBy(F.date_trunc("hour", "_valid_from").alias("h")).count().collect()
    assert sorted(r["count"] for r in hours) == [1, 2]


# History gaps found by reading the code against the docs (19 Nov 2026). Each case is what the history MUST do; a case
# that fails today is marked xfail(strict) with the reason, so the suite stays green and the bug stays visible. Fixing
# the bug turns the case into an unexpected pass, which strict makes an error: the mark is then removed.
def _known_bug(reason):
    return pytest.mark.xfail(strict=True, reason=reason)


I, D, U = "I", "D", "U"
SCD2_MORE = [
    pytest.param(
        "a deleted key comes back with the same tracked value: live again, one version",
        [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)], [("a", 1, "x", 3, I, 9, 3)]],
        [("a", 1, 1, True, False)], id="return-same-value"),
    pytest.param(
        "a deleted key comes back with a new tracked value: the old version stays deleted, a new one is current",
        [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)], [("a", 2, "x", 3, I, 9, 3)]],
        [("a", 1, 1, False, True), ("a", 2, 3, True, False)], id="return-new-value"),
    pytest.param(
        "a delete on a key with several versions marks only the last one",
        [[("a", 1, "x", 1, I, 0, 1)], [("a", 2, "x", 2, U, 5, 2)], [("a", 2, "x", 3, D, 9, 3)]],
        [("a", 1, 1, False, False), ("a", 2, 2, True, True)], id="delete-last-version"),
    pytest.param(
        "a delete for a key silver never saw is kept as a deleted version",
        [[("b", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)]],
        [("a", 1, 2, True, True), ("b", 1, 1, True, False)], id="delete-unknown-key"),
    pytest.param(
        "the same delete sent twice changes nothing",
        [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)], [("a", 1, "x", 2, D, 5, 2)]],
        [("a", 1, 1, True, True)], id="delete-twice"),
    pytest.param(
        "a late change after the value flipped back: history is value 1, 2, 1 and the newest is current",
        [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "y", 3, U, 5, 2)], [("a", 2, "x", 2, U, 9, 3)]],
        [("a", 1, 1, False, False), ("a", 2, 2, False, False), ("a", 1, 3, True, False)],
        marks=_known_bug("a stored version keeps only its start, so a note-only change at seq 3 is forgotten when a "
                         "late seq 2 change arrives (found 19 Nov 2026)"), id="late-after-flip-back"),
]


@pytest.mark.parametrize(("case", "batches", "want"), SCD2_MORE)
def test_history_type2_more(spark, case, batches, want):
    silver = _apply(spark, plan(**SCD2), _feed(batches))
    assert rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted") == want, case
    assert_history_invariants(silver)


@pytest.mark.parametrize(
    ("case", "batches", "key", "want"),
    [
        pytest.param("the delete time is kept when a replay touches the key again",
                     [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)],
                      [("a", 1, "x", 2, D, 5, 2)]], "a", 5, id="replay"),
        pytest.param("the delete time is kept when a later change to another key arrives",
                     [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)],
                      [("b", 1, "x", 3, I, 9, 3)]], "a", 5, id="other-key"),
    ],
)
def test_history_type2_delete_time(spark, case, batches, key, want):
    silver = _apply(spark, plan(**SCD2), _feed(batches))
    got = [r["_deleted_at"] for r in silver.where(F.col("id") == key).collect() if r["_is_deleted"]]
    assert [g.replace(tzinfo=None) for g in got] == [ts(want).replace(tzinfo=None)], case  # DuckDB returns naive


@pytest.mark.parametrize(
    ("case", "apply", "batches", "want"),
    [
        pytest.param("remove: every version of a deleted key is taken out, other keys stay", "remove",
                     [[("a", 1, "x", 1, I, 0, 1), ("b", 1, "x", 2, I, 0, 2)],
                      [("a", 2, "x", 3, U, 5, 3)], [("a", 2, "x", 4, D, 9, 4)]],
                     [("b", 1, 2, True, False)],
                     id="remove"),
        pytest.param("mark_deleted: the same feed keeps every version", "mark_deleted",
                     [[("a", 1, "x", 1, I, 0, 1), ("b", 1, "x", 2, I, 0, 2)],
                      [("a", 2, "x", 3, U, 5, 3)], [("a", 2, "x", 4, D, 9, 4)]],
                     [("a", 1, 1, False, False), ("a", 2, 3, True, True), ("b", 1, 2, True, False)], id="mark"),
        pytest.param("none: a delete row is ignored", "none",
                     [[("a", 1, "x", 1, I, 0, 1)], [("a", 1, "x", 2, D, 5, 2)]],
                     [("a", 1, 1, True, False)], id="none"),
    ],
)
def test_history_type2_delete_apply(spark, case, apply, batches, want):
    cfg = {**SCD2, "load": {**SCD2["load"], "deletes": {"detect": "change_feed", "apply": apply}}}
    silver = _apply(spark, plan(**cfg), _feed(batches))
    assert rows(silver, "id", "v", "_seq_from", "_is_current", "_is_deleted") == want, case


@pytest.mark.parametrize(
    ("case", "apply", "batches", "want"),
    [
        pytest.param("a delete row in an event feed is not a reading when deletes are not applied", "none",
                     [[("r1", 1, 1, 1, I), ("r2", 1, 1, 2, D)]], ["r1"], id="append-delete-row"),
        pytest.param("plain event rows with no operation column are all kept", "none",
                     [[("r1", 1, 1, 1, None), ("r2", 1, 1, 2, None)]], ["r1", "r2"], id="append-no-op"),
    ],
)
def test_history_append_delete_rows(spark, case, apply, batches, want):
    p = plan(dedup={"mode": "business_key_first", "business_keys": ["id"]},
             load={"deletes": {"detect": "change_feed", "apply": apply}})
    silver = _apply(spark, p, batches, "id string, v long, seq long, _seq long, _op string")
    assert [r[0] for r in rows(silver, "id")] == want, case


# ---------------------------------------------------------------------------------------------------- 13 provenance
@pytest.mark.parametrize(
    ("case", "col", "check"),
    [
        ("the bronze run kept under its own name", "_bronze_run_id", lambda v: v == "ingest-1"),
        ("the silver run stamped", "_silver_run_id", lambda v: v == "silver-1"),
        ("the batch kept", "_batch_key", lambda v: v == "b1"),
        ("the file kept", "_source_file", lambda v: v == "f1"),
        ("a record hash over the business columns", "_record_hash", lambda v: len(v) == 64),
        ("a load time", "_silver_loaded_at", lambda v: v is not None),
    ],
)
def test_provenance(spark, case, col, check):
    p = plan()
    out, _, _ = run_stages(p, frame(spark, "id string, v long, seq long", [("a", 1, 1)]), None, "silver-1",
                           "2026-11-09", ts(0))
    assert check(out.collect()[0][col]), case
    assert out.columns[:3] == ["id", "v", "seq"]
    assert "_run_id" not in out.columns


def test_record_hash_changes_with_a_business_value(spark):
    p = plan()
    out, _, _ = run_stages(p, frame(spark, "id string, v long, seq long", [("a", 1, 1), ("b", 1, 1), ("c", 2, 1)]),
                           None, "s", "2026-11-09")
    h = {r["id"]: r["_record_hash"] for r in out.collect()}
    assert len({h["a"], h["b"], h["c"]}) == 3


# ---------------------------------------------------------------------------------------------------- one logic, two engines
# Exact values the stages compute, the same on every engine (docs/DUCKDB-SPIKE.md). Where an engine differs, the table
# says so per engine: the difference is known and kept in sight, never hidden.
def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def _key_of(spark, ddl, row, keys):
    p = plan(columns=[(c.split()[0], "string") for c in ddl.split(", ")], keys=keys)
    df = spark.createDataFrame([row], ddl)
    return run("keys", df, p).df.collect()[0]["_key_hash"]


def _record_of(spark, ddl, row):
    p = plan(columns=[("id", "string"), ("v", "integer"), ("seq", "integer")])
    out, _, _ = run_stages(p, frame(spark, ddl, [row]), None, "s", "2026-11-09")
    return out.collect()[0]["_record_hash"]


def _null_rank(spark, order):
    """The row number a null order value gets in a window over (null, 2, 1)."""
    df = spark.createDataFrame([("a", None), ("a", 2), ("a", 1)], "id string, p long")
    ranked = df.select("p", F.row_number().over(Window.partitionBy("id").orderBy(order)).alias("n"))
    return next(r["n"] for r in ranked.collect() if r["p"] is None)


def _row_key_of(spark):
    out = _apply(spark, plan(**SCD2), [[("a", 1, "x", 7, "I", ts(0), 1)]])
    return out.collect()[0]["_row_key"]


SEP_, NUL = "\u001f", "\u0000"
SAME = [
    ("key hash: text parts", lambda s: _key_of(s, "id string, n string", ("ab", "c"), ("id", "n")),
     _sha(f"ab{SEP_}c")),
    ("key hash: parts never run together", lambda s: _key_of(s, "id string, n string", ("a", "bc"), ("id", "n")),
     _sha(f"a{SEP_}bc")),
    ("key hash: a whole number part", lambda s: _key_of(s, "id string, n long", ("a", 10), ("id", "n")),
     _sha(f"a{SEP_}10")),
    ("key hash: a date part", lambda s: _key_of(s, "id string, n date", ("a", date(2026, 11, 9)), ("id", "n")),
     _sha(f"a{SEP_}2026-11-09")),
    ("type 2 row key: key hash and version start", _row_key_of, _sha(_sha("a") + "|7")),
    ("a window order with nulls first stated", lambda s: _null_rank(s, F.col("p").asc_nulls_first()), 1),
    ("a descending window order with nulls last stated", lambda s: _null_rank(s, F.col("p").desc_nulls_last()), 3),
    ("record hash: business values as JSON", lambda s: _record_of(s, "id string, v long, seq long", ("a", 1, 2)),
     _sha('{"id":"a","v":1,"seq":2}')),
]
DIFFERENT = [
    # SQLFrame's to_json keeps null fields (Spark leaves them out); a record hash is compared within one engine only
    ("record hash: a null business value", lambda s: _record_of(s, "id string, v long, seq long", ("a", None, 2)),
     {"spark": _sha('{"id":"a","seq":2}'), "duckdb": _sha('{"id":"a","v":null,"seq":2}')}),
    # a timestamp as text: Spark "2026-11-09 00:00:00", DuckDB's TIMESTAMPTZ adds the offset
    ("key hash: a timestamp part", lambda s: _key_of(s, "id string, t timestamp", ("a", ts(0)), ("id", "t")),
     {"spark": _sha(f"a{SEP_}2026-11-09 00:00:00"), "duckdb": _sha(f"a{SEP_}2026-11-09 00:00:00+00")}),
    # a window order that leaves nulls to the engine: Spark puts them first, DuckDB last (the stages state it)
    ("a window order with the engine's default null place", lambda s: _null_rank(s, F.col("p")),
     {"spark": 1, "duckdb": 3}),
    # a null part: one NUL character on both engines (null_mark); the same hash
    ("key hash: a null part (not a key: type 2 tracked columns)", lambda s: s.createDataFrame(
        [("a", None)], "id string, n string").select(F.sha2(text_parts(("id", "n")), 256)).collect()[0][0],
     {"spark": _sha(f"a{SEP_}{NUL}"), "duckdb": _sha(f"a{SEP_}{NUL}")}),
]


@pytest.mark.parametrize(("case", "compute", "want"), SAME, ids=[c[0] for c in SAME])
def test_same_values_on_every_engine(spark, case, compute, want):
    assert compute(spark) == want, case


@pytest.mark.parametrize(("case", "compute", "want"), DIFFERENT, ids=[c[0] for c in DIFFERENT])
def test_known_engine_differences(spark, engine_name, case, compute, want):
    assert compute(spark) == want[engine_name], case


# ---------------------------------------------------------------------------------------------------- source_versioned
SV_DDL = "id string, v long, vf timestamp, vt timestamp"


def _sv(gaps: bool = False):
    return plan(kind="master", read="files", columns=[("id", "string"), ("v", "integer"), ("vf", "timestamp"),
                                                      ("vt", "timestamp")],
                history={"type": "source_versioned", "valid_from_column": "vf", "valid_to_column": "vt",
                         "gaps_allowed": gaps})


SV_CASES = [
    ("two clean versions: the later one is current",
     False, [[("a", 1, ts(0), ts(60)), ("a", 2, ts(60), None)]],
     [("a", 1, False, False), ("a", 2, True, False)]),
    ("the source closed the last version: no current version, the key is deleted",
     False, [[("a", 1, ts(0), ts(60)), ("a", 2, ts(60), ts(120))]],
     [("a", 1, False, False), ("a", 2, True, True)]),
    ("an end at 9999 is open, not closed",
     False, [[("a", 1, ts(0), datetime(9999, 12, 31, tzinfo=UTC))]],
     [("a", 1, True, False)]),
    ("overlapping versions: the key is held, the other key loads",
     False, [[("a", 1, ts(0), ts(90)), ("a", 2, ts(60), None), ("b", 7, ts(0), None)]],
     [("b", 7, True, False)]),
    ("a gap is held when gaps are not allowed",
     False, [[("a", 1, ts(0), ts(30)), ("a", 2, ts(60), None)]], []),
    ("a gap loads when the dataset allows gaps",
     True, [[("a", 1, ts(0), ts(30)), ("a", 2, ts(60), None)]],
     [("a", 1, False, False), ("a", 2, True, False)]),
    ("the same batch twice changes nothing",
     False, [[("a", 1, ts(0), ts(60)), ("a", 2, ts(60), None)], [("a", 1, ts(0), ts(60)), ("a", 2, ts(60), None)]],
     [("a", 1, False, False), ("a", 2, True, False)]),
    ("a later delivery closes the current version and adds the next",
     False, [[("a", 1, ts(0), None)], [("a", 1, ts(0), ts(60)), ("a", 2, ts(60), None)]],
     [("a", 1, False, False), ("a", 2, True, False)]),
]


@pytest.mark.parametrize(("case", "gaps", "batches", "want"), SV_CASES, ids=[c[0] for c in SV_CASES])
def test_history_source_versioned(spark, case, gaps, batches, want):
    silver = _apply(spark, _sv(gaps), batches, SV_DDL)
    assert rows(silver, "id", "v", "_is_current", "_is_deleted") == want, case
    if want:
        assert_history_invariants(silver)


def test_history_source_versioned_drops_nothing(spark):
    df = frame(spark, SV_DDL, [("a", 1, ts(0), ts(90)), ("a", 2, ts(60), None), ("b", 7, ts(0), None)])
    out, bad, _ = run_stages(_sv(), df, None, "silver-1", "2026-11-09", hold=stage_hold(df))
    assert_nothing_dropped({"a", "b"}, out.where("_action = 'upsert'"), bad)


def test_history_source_versioned_names_the_reason(spark):
    df = frame(spark, SV_DDL, [("a", 1, ts(0), ts(90)), ("a", 2, ts(60), None)])
    _, bad, _ = run_stages(_sv(), df, None, "silver-1", "2026-11-09", hold=stage_hold(df))
    reasons = {r["_reason"] for r in bad.where("_stage = 'history'").collect()}
    assert reasons == {"src.things: versions overlap"}


def test_source_versioned_needs_its_columns():
    with pytest.raises(PlanError, match="source_versioned needs"):
        plan(kind="master", read="files", history={"type": "source_versioned", "valid_from_column": "vf"})
