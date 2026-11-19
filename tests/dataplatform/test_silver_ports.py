"""Contract tests (M5, ADR-020) for silver's ports, run against their adapter: the stage runner on PySpark and the
silver tables on Delta folders. Each case is a sequence of calls and what the port must answer."""

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
from silver_helpers import frame, plan, ts

from dataplatform.adapters.spark_silver import DeltaSilverTables, SparkStageRunner, _quoted
from dataplatform.silver.ports import StageError

FEED = "id string, v long, seq long, _op string, _committed_at timestamp"
SCD1 = {"kind": "transaction", "dedup": {"mode": "business_key_latest", "business_keys": ["id"], "order_by": "seq"},
        "load": {"deletes": {"detect": "change_feed", "apply": "mark_deleted"}},
        "reconciliation": {"measures": [{"name": "row_count"}, {"name": "v_total", "column": "v"}]}}
APPEND = {"dedup": {"mode": "business_key_first", "business_keys": ["id"]},
          "reconciliation": {"measures": [{"name": "row_count"}, {"name": "v_total", "column": "v"}]}}


@pytest.fixture
def rig(spark, tmp_path):
    tables = DeltaSilverTables(spark, tmp_path / "bronze", tmp_path / "silver")
    return spark, tables, SparkStageRunner()


def put(rig, p, data, txn, ddl=FEED, batch="b1"):
    """Bronze rows -> stages -> write edge, as the silver runner does."""
    spark, tables, runner = rig
    frame(spark, ddl, data, batch=batch).write.format("delta").mode("append").save(
        str(tables.bronze_root / "src__things"))
    result = runner.run(p, tables.bronze(p, [batch]), tables.current(p), f"s{txn}", "2026-11-09")
    result.txn = txn
    n = tables.write(p, result)
    runner.release(result)
    return n, result


# ---------------------------------------------------------------------------------------------------- silver tables
@pytest.mark.parametrize(
    ("case", "kw", "batches", "want"),
    [
        ("a table never written gives zeros", SCD1, [],
         {"row_count": 0, "versions": 0, "deleted_marked": 0, "v_total": 0}),
        ("upserts are written", SCD1, [[("a", 1, 1, "I", ts(0)), ("b", 2, 1, "I", ts(0))]],
         {"row_count": 2, "versions": 2, "v_total": 3}),
        ("the same rows written again change nothing", SCD1,
         [[("a", 1, 1, "I", ts(0))], [("a", 1, 1, "I", ts(0))]], {"row_count": 1, "versions": 1, "v_total": 1}),
        ("an update replaces the row by its row key", SCD1,
         [[("a", 1, 1, "I", ts(0))], [("a", 5, 2, "U", ts(1))]], {"row_count": 1, "versions": 1, "v_total": 5}),
        ("a soft delete keeps the row, counted apart", SCD1,
         [[("a", 1, 1, "I", ts(0)), ("b", 2, 1, "I", ts(0))], [("a", 1, 2, "D", ts(1))]],
         {"row_count": 1, "versions": 2, "deleted_marked": 1, "v_total": 2}),
        ("appends add rows", APPEND, [[("a", 1, 1, "I", ts(0))], [("b", 2, 1, "I", ts(0))]],
         {"row_count": 2, "v_total": 3}),
    ],
)
def test_silver_tables_write_and_measure(rig, case, kw, batches, want):
    p = plan(**kw)
    for n, data in enumerate(batches, 1):
        put(rig, p, data, n, batch=f"b{n}")
    got = rig[1].measures(p, p.measures)
    assert {k: got[k] for k in want} == want, case


def test_an_append_written_twice_with_the_same_mark_is_written_once(rig):
    spark, tables, runner = rig
    p = plan(**APPEND)
    frame(spark, FEED, [("a", 1, 1, "I", ts(0))]).write.format("delta").save(str(tables.bronze_root / "src__things"))
    result = runner.run(p, tables.bronze(p, ["b1"]), None, "s1", "2026-11-09")
    result.txn = 7
    tables.write(p, result)
    tables.write(p, result)
    assert tables.measures(p, p.measures)["versions"] == 1


def test_stale_rows_are_removed_by_the_merge(rig):
    """A late change equal to the version after it moves that version's start: the old row key is deleted."""
    p = plan(kind="master", history={"track_columns": ["v"]}, load={"history_order_by": "seq"})
    put(rig, p, [("a", 1, 1, "I", ts(0))], 1, batch="b1")
    put(rig, p, [("a", 3, 5, "U", ts(5))], 2, batch="b2")
    _, result = put(rig, p, [("a", 3, 4, "U", ts(4))], 3, batch="b3")
    assert result.counts["rows_removed"] == 1
    assert [(r["v"], r["_seq_from"]) for r in rig[1].versions(p, {"id": "a"})] == [(1, 1), (3, 4)]


def test_a_new_column_reaches_silver_without_a_release(rig):
    """ADR-008: all columns by default; the merge adds a column it has not seen."""
    p = plan(**SCD1)
    put(rig, p, [("a", 1, 1, "I", ts(0))], 1, batch="b1")
    rig[1].spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    put(rig, p, [("a", 2, 2, "U", ts(1), "x")], 2, FEED + ", w string", batch="b2")
    rig[1].spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
    assert rig[1].versions(p, {"id": "a"})[0]["w"] == "x"


def test_quarantined_rows_land_in_the_quarantine_table(rig):
    p = plan(required=("v",), **APPEND)
    n, result = put(rig, p, [("a", None, 1, "I", ts(0)), ("b", 2, 1, "I", ts(0))], 1)
    q = rig[1].frame("src", "things", "__quarantine")
    assert n == 1
    assert result.counts["quality.quarantined"] == 1
    assert [(r["_stage"], r["_reason"]) for r in q.collect()] == [("quality", "v_present")]


@pytest.mark.parametrize(
    ("case", "batches", "want"),
    [
        ("never written", [], None),
        ("live keys", [[("a", 1, 1, "I", ts(0)), ("b", 1, 1, "I", ts(0))]], {("a",), ("b",)}),
        ("a deleted key is not current", [[("a", 1, 1, "I", ts(0)), ("b", 1, 1, "I", ts(0))],
                                          [("a", 1, 2, "D", ts(1))]], {("b",)}),
    ],
)
def test_current_keys(rig, case, batches, want):
    p = plan(**SCD1)
    for n, data in enumerate(batches, 1):
        put(rig, p, data, n, batch=f"b{n}")
    assert rig[1].current_keys("src", "things", ("id",)) == want, case


def test_count_by_hour_counts_from_the_start_given(rig):
    """Hours are counted from `start`: a business day that starts on the half hour (IST midnight is 18:30 UTC)."""
    p = plan(columns=[("id", "string"), ("event_ts", "timestamp")], settings={"watermark_column": "event_ts"},
             **APPEND)
    put(rig, p, [("a", ts(29)), ("b", ts(31)), ("c", ts(89)), ("d", ts(95))], 1, "id string, event_ts timestamp")
    got = rig[1].count_by_hour(p, "event_ts", "2026-11-09T00:30:00+00:00", "2026-11-09T02:30:00+00:00")
    assert got == {"2026-11-09T00:30:00+00:00": 2, "2026-11-09T01:30:00+00:00": 1}


def test_bronze_reads_only_the_batches_asked(rig):
    spark, tables, _ = rig
    p = plan(**APPEND)
    for b in ("b1", "b2"):
        frame(spark, FEED, [(b, 1, 1, "I", ts(0))], batch=b).write.format("delta").mode("append").save(
            str(tables.bronze_root / "src__things"))
    assert [r["id"] for r in tables.bronze(p, ["b2"]).collect()] == ["b2"]


def test_a_missing_bronze_table_is_a_stage_error(rig):
    with pytest.raises(StageError, match="no bronze table"):
        rig[1].bronze(plan(**APPEND), ["b1"])


# ---------------------------------------------------------------------------------------------------- stage runner
@pytest.mark.parametrize(
    ("case", "kw", "data", "want"),
    [
        ("rows in and out", APPEND, [("a", 1, 1, "I", ts(0)), ("b", 1, 1, "I", ts(0))],
         {"rows_in": 2, "rows_out": 2, "rows_removed": 0}),
        ("duplicates counted", APPEND, [("a", 1, 1, "I", ts(0)), ("a", 1, 1, "I", ts(0))],
         {"rows_in": 2, "rows_out": 1, "duplicates.dropped": 1}),
        ("quarantine counted per stage", {"required": ("v",), **APPEND},
         [("a", None, 1, "I", ts(0)), ("b", 1, 1, "I", ts(0))], {"rows_out": 1, "quality.quarantined": 1}),
    ],
)
def test_stage_runner_counts(rig, case, kw, data, want):
    spark, _tables, runner = rig
    p = plan(**kw)
    result = runner.run(p, frame(spark, FEED, data), None, "s1", "2026-11-09")
    assert {k: result.counts.get(k) for k in want} == want, case
    runner.release(result)


def test_stage_runner_stops_a_batch_that_breaks_its_contract(rig):
    spark, _, runner = rig
    with pytest.raises(StageError, match="contract column"):
        runner.run(plan(**APPEND), frame(spark, "id string, seq long", [("a", 1)]), None, "s1", "2026-11-09")


@pytest.mark.parametrize(
    ("case", "name", "want"),
    [
        ("a plain column", "w", "`w`"),
        ("a space stays inside the quotes", "unit price", "`unit price`"),
        ("a backtick is doubled, so it cannot end the identifier", "a` int); DROP TABLE x; --",
         "`a`` int); DROP TABLE x; --`"),
        ("a path", "/tmp/silver/src__things", "`/tmp/silver/src__things`"),
    ],
)
def test_ddl_identifiers_are_quoted(case, name, want):
    """The merge's ALTER TABLE takes no bind parameters: every identifier goes through _quoted() (SL-SEC-01)."""
    assert _quoted(name) == want, case


@pytest.mark.parametrize("name", ["a\nb", "a\x00b"])
def test_an_identifier_with_a_newline_or_nul_is_refused(name):
    with pytest.raises(ValueError, match="not a usable identifier"):
        _quoted(name)
