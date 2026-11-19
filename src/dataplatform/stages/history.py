"""Stage 12: history by dataset kind (ADR-016), deletes soft. A pure function: new rows and silver as it stands in,
the rows to write out. Every output row has a `_row_key` and an `_action`: `upsert` (write it) or `delete` (a stale
row of a key whose history was rebuilt, or every row of a key whose delete rule is `remove`).

    scd2      type 2. Events (silver's versions and the new changes) are laid out per key in the source's order
              (load.history_order_by, e.g. change_seq), never in arrival order: a change that arrives after a newer
              one goes into its place in history and the newer one stays current (SCD-02). A change to a tracked
              column opens a version; any other change updates the version it falls in. A delete (op D, or a key
              missing from a full copy when deletes are detected by key compare) marks the version deleted (DEL-01).
              Validity runs on the effective time; a version never starts after a later-ordered one.
    scd1      type 1: the latest change by the order column wins, whatever order changes arrive in.
    append    new rows only; duplicates and replays were removed by stage 10.
    snapshot  each batch is the whole of its window (the business key minus its last column, e.g. a dealer's month);
              the newest batch for a window becomes current and the window's older rows stop being current.

Re-running a batch changes nothing: version and row keys are computed from the data, not from the run.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from dataplatform.silver.plan import SilverPlan
from dataplatform.stages.common import SEP, Out, StageCtx, arrival_desc, quarantine_rows, text_parts

SILVER_ONLY = ("_silver_run_id", "_silver_loaded_at", "_record_hash")
RECOMPUTED = ("_valid_from", "_valid_to", "_is_current", "_is_deleted", "_deleted_at", "_seq_from", "_row_key")
WORK = ("_pos", "_ts", "_dts", "_existing", "_t", "_new", "_ver", "_start", "_gts", "_rn")


def working(current: DataFrame) -> DataFrame:
    """Silver rows in the stages' working shape (bronze run under its bronze name, silver-only columns gone)."""
    # renamed only when present (snapshot passes rows already in the working shape; renaming a missing column is a
    # no-op on Spark and an error on SQLFrame, docs/DUCKDB-SPIKE.md)
    out = current.withColumnRenamed("_bronze_run_id", "_run_id") if "_bronze_run_id" in current.columns else current
    return out.drop(*[c for c in SILVER_ONLY if c in out.columns])


def _ts(df: DataFrame, plan: SilverPlan) -> Column:
    if plan.effective_time and plan.effective_time in df.columns:
        return F.col(plan.effective_time).cast("timestamp")
    return F.lit(None).cast("timestamp")


def _op(df: DataFrame) -> Column:
    return F.coalesce(F.col("_op"), F.lit("U")) if "_op" in df.columns else F.lit("U")


def _incoming(df: DataFrame, plan: SilverPlan) -> DataFrame:
    out = df.withColumn("_op", _op(df)).withColumn("_ts", _ts(df, plan)).withColumn("_existing", F.lit(False))
    out = out.withColumn("_dts", F.col("_ts"))  # when a delete took effect; a stored version brings its own (_existing)
    if plan.delete_apply == "none":
        out = out.where(F.col("_op") != "D")
    return out


def _existing(versions: DataFrame) -> DataFrame:
    op = F.when(F.col("_is_deleted"), F.lit("D")).otherwise(_op(versions))
    out = versions.withColumn("_op", op).withColumn("_ts", F.col("_valid_from")).withColumn("_existing", F.lit(True))
    # a deleted version keeps the time of its delete, not the start of its validity (the time must survive a rerun)
    out = out.withColumn("_dts", F.when(F.col("_is_deleted"), F.coalesce(F.col("_deleted_at"), F.col("_valid_from")))
                         .otherwise(F.col("_valid_from")))
    return out.withColumn("_pos", F.col("_seq_from")).drop(*[c for c in RECOMPUTED if c != "_seq_from"]).drop(
        "_seq_from"
    )


def _key_compare_deletes(inc: DataFrame, cur: DataFrame, plan: SilverPlan) -> DataFrame | None:
    """A full copy is the whole source: a key live in silver and missing from the copy was deleted at the source
    (DPC-0017). The delete takes the copy's order value and time. The mass-delete guard has already held any copy
    missing too many keys (DPC-0054)."""
    if plan.delete_detect != "key_compare" or plan.delete_apply == "none" or plan.ingest_read != "full_copy":
        return None
    live = cur.where(F.col("_is_current") & ~F.col("_is_deleted"))
    gone = live.join(inc.select("_key_hash").distinct(), "_key_hash", "left_anti")
    copy = inc.agg(
        F.max(plan.order_by).alias("_copy_pos"),
        F.max("_ts").alias("_copy_ts"),
        F.max("_batch_key").alias("_copy_batch"),
        F.max("_source_file").alias("_copy_file"),
        F.max("_window").alias("_copy_window"),
    ).where(F.col("_copy_pos").isNotNull())
    # `copy` is a global aggregate (agg with no grouping): one row or none, so the product has exactly as many rows as
    # `gone`; it stamps the copy's order value and time on each deleted key. Broadcast: no shuffle of `gone`.
    # shiftleft: allow SL-COST-CODE-14 -- intended: `copy` is a one-row global aggregate, so no row multiplication
    dels = _existing(gone).crossJoin(copy.hint("broadcast"))
    return dels.withColumns({
        plan.order_by: F.col("_copy_pos"),
        "_pos": F.col("_copy_pos"),
        "_ts": F.col("_copy_ts"),
        "_dts": F.col("_copy_ts"),
        "_op": F.lit("D"),
        "_existing": F.lit(False),
        "_batch_key": F.col("_copy_batch"),
        "_source_file": F.col("_copy_file"),
        "_window": F.col("_copy_window"),
    }).drop("_copy_pos", "_copy_ts", "_copy_batch", "_copy_file", "_copy_window")


def _events(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> tuple[DataFrame, DataFrame | None]:
    """(every event of the keys this batch touches, silver's rows of those keys)."""
    if cur is None:
        return inc, None
    dels = _key_compare_deletes(inc, cur, plan)
    events = inc if dels is None else inc.unionByName(dels, allowMissingColumns=True)
    touched = events.select("_key_hash").distinct()
    rows = cur.join(touched, "_key_hash", "left_semi")
    return events.unionByName(_existing(rows), allowMissingColumns=True), rows


def _stale(rows: DataFrame | None, out: DataFrame) -> DataFrame | None:
    """Silver rows of the touched keys whose row key the new history no longer has: removed."""
    if rows is None:
        return None
    gone = rows.select("_row_key").join(out.select("_row_key"), "_row_key", "left_anti")
    return gone.withColumn("_action", F.lit("delete"))


def _finish(out: DataFrame, stale: DataFrame | None, plan: SilverPlan) -> DataFrame:
    out = out.drop(*[c for c in WORK if c in out.columns]).withColumn("_action", F.lit("upsert"))
    if plan.delete_apply == "remove":  # a key whose current version is deleted loses every version it has
        live_delete = (F.col("_is_current") & F.col("_is_deleted")).cast("int")
        gone = F.max(live_delete).over(Window.partitionBy("_key_hash")) > 0
        out = out.withColumn("_action", F.when(gone, F.lit("delete")).otherwise(F.lit("upsert")))
    return out if stale is None else out.unionByName(stale, allowMissingColumns=True)


# ---------------------------------------------------------------------------------------------------- scd2
def _tracked(plan: SilverPlan, columns: list[str]) -> list[str]:
    if plan.track:
        return list(plan.track)
    skip = set(plan.keys) | {plan.order_by}
    return [c.name for c in plan.kept if c.name not in skip and c.name in columns]


def scd2(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> DataFrame:
    o = plan.order_by
    new = _incoming(inc, plan).withColumn("_pos", F.col(o))
    events, rows = _events(new, cur, plan)
    # nulls placed explicitly in every window order (Spark's default, NULLS FIRST ascending): SQLFrame's window
    # orders leave it to the engine, and DuckDB puts nulls last (docs/DUCKDB-SPIKE.md)
    by_pos = Window.partitionBy("_key_hash").orderBy(F.col("_pos").asc_nulls_first(), F.col(o).asc_nulls_first(),
                                                     F.col("_existing").desc_nulls_last())
    tracked = F.sha2(text_parts(_tracked(plan, events.columns)), 256)
    # last_value(..., ignoreNulls) rather than last(ignorenulls=True): the same window function, and the form every
    # engine the stages run on keeps the null skipping of (docs/DUCKDB-SPIKE.md)
    before = F.last_value("_t", F.lit(True)).over(by_pos.rowsBetween(Window.unboundedPreceding, -1))
    events = (
        # a delete event never opens a version (it marks the one it falls in); a stored version keeps its values
        events.withColumn("_t", F.when((F.col("_op") == "D") & ~F.col("_existing"), F.lit(None)).otherwise(tracked))
        .withColumn("_new", F.when(before.isNull(), True).when(F.col("_t").isNull(), False)
                    .otherwise(F.col("_t") != before))
        .withColumn("_ver", F.sum(F.col("_new").cast("int")).over(
            by_pos.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    )
    group = Window.partitionBy("_key_hash", "_ver")
    latest = group.orderBy(F.col(o).desc_nulls_last(), F.col("_existing").asc_nulls_first(), *arrival_desc(events))
    versions = events.withColumns({
        "_start": F.min("_pos").over(group),
        "_gts": F.min("_ts").over(group),
        "_rn": F.row_number().over(latest),
    }).where("_rn = 1")
    per_key = Window.partitionBy("_key_hash").orderBy(F.col("_start").asc_nulls_first())
    deleted = F.col("_op") == "D"
    out = versions.withColumns({
        "_valid_from": F.min("_gts").over(per_key.rowsBetween(Window.currentRow, Window.unboundedFollowing)),
        "_is_current": F.lead("_start").over(per_key).isNull(),
        "_is_deleted": deleted,
        "_deleted_at": F.when(deleted, F.col("_dts")),
        "_seq_from": F.col("_start"),
        "_row_key": F.sha2(F.concat_ws("|", F.col("_key_hash"), F.col("_start").cast("string")), 256),
    }).withColumn("_valid_to", F.lead("_valid_from").over(per_key))
    return _finish(out, _stale(rows, out), plan)


# ---------------------------------------------------------------------------------------------------- scd1
def scd1(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> DataFrame:
    o = plan.order_by
    events, rows = _events(_incoming(inc, plan).withColumn("_pos", F.col(o)), cur, plan)
    latest = Window.partitionBy("_key_hash").orderBy(F.col(o).desc_nulls_last(), F.col("_existing").asc_nulls_first(),
                                                     *arrival_desc(events))
    deleted = F.col("_op") == "D"
    out = events.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").withColumns({
        "_valid_from": F.col("_ts"),
        "_valid_to": F.lit(None).cast("timestamp"),
        "_is_current": F.lit(True),
        "_is_deleted": deleted,
        "_deleted_at": F.when(deleted, F.col("_ts")),
        "_seq_from": F.col(o),
        "_row_key": F.col("_key_hash"),
    })
    return _finish(out, _stale(rows, out), plan)


# ---------------------------------------------------------------------------------------------------- append
def _live() -> dict[str, Column]:
    """A row that is current, not deleted, with no version history of its own."""
    return {
        "_valid_from": F.lit(None).cast("timestamp"),
        "_valid_to": F.lit(None).cast("timestamp"),
        "_is_current": F.lit(True),
        "_is_deleted": F.lit(False),
        "_deleted_at": F.lit(None).cast("timestamp"),
        "_seq_from": F.lit(None).cast("string"),
    }


def append(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> DataFrame:
    if plan.delete_apply == "none" and "_op" in inc.columns:  # a delete row is not a new fact (as in scd1 and scd2)
        inc = inc.where(F.coalesce(F.col("_op"), F.lit("U")) != "D")
    has_event = bool(plan.event_time) and plan.event_time in inc.columns
    when = F.col(plan.event_time).cast("timestamp") if has_event else _ts(inc, plan)
    return _finish(inc.withColumns({**_live(), "_valid_from": when, "_row_key": F.col("_key_hash")}), None, plan)


# ---------------------------------------------------------------------------------------------------- snapshot
def snapshot(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> DataFrame:
    order = [F.col(plan.order_by)] if plan.order_by else []
    stamp = F.struct(*order, F.col("_batch_key"))
    win = F.sha2(F.concat_ws(SEP, *[F.col(c).cast("string") for c in plan.snapshot_by]), 256)
    by_win = Window.partitionBy("_win")
    new = (
        inc.withColumn("_win", win)
        .withColumn("_stamp", stamp)
        .withColumn("_newest", F.max("_stamp").over(by_win))
        .where(F.col("_stamp") == F.col("_newest"))
        .drop("_newest")
    )
    new_rows = new.withColumns({
        **_live(),
        "_row_key": F.sha2(F.concat_ws("|", F.col("_key_hash"), F.col("_batch_key")), 256),
    })
    if cur is None:
        return _finish(new_rows.drop("_win", "_stamp"), None, plan)
    old = working(cur).where(F.col("_is_current")).withColumn("_win", win).withColumn("_stamp", stamp)
    held = old.groupBy("_win").agg(F.max("_stamp").alias("_held"))
    incoming = new.groupBy("_win").agg(F.max("_stamp").alias("_in"))
    applies = incoming.join(held, "_win", "left").where(F.col("_held").isNull() | (F.col("_in") >= F.col("_held")))
    taken = new_rows.join(applies.select("_win"), "_win", "left_semi")
    superseded = (
        old.join(applies.select("_win"), "_win", "left_semi")
        .join(taken.select("_row_key"), "_row_key", "left_anti")
        .withColumn("_is_current", F.lit(False))
    )
    out = taken.unionByName(superseded, allowMissingColumns=True).drop("_win", "_stamp")
    return _finish(out, None, plan)


# ---------------------------------------------------------------------------------------------------- source_versioned
FAR = "9999-01-01"  # a validity end at or after this is "open" (sources write 9999-12-31 for "no end")
SV_WORK = ("_vf", "_vt", "_sv_rn", "_prev_vt", "_sv_reason", "_key_reason")


def source_versioned(inc: DataFrame, cur: DataFrame | None, plan: SilverPlan) -> tuple[DataFrame, DataFrame | None]:
    """History the source already keeps (ADR-032): every version as delivered, with the source's own validity dates,
    checked per key and never rebuilt. A key whose versions overlap, leave a gap (unless gaps are allowed) or end
    before they start is quarantined whole, with the reason; its stored history is left as it was. Good keys get the
    standard columns: the latest version is current; a source that closed it marks the key deleted."""
    def stamp(df: DataFrame) -> DataFrame:
        vt = F.col(plan.valid_to).cast("timestamp")
        return df.withColumns({"_vf": F.col(plan.valid_from).cast("timestamp"),
                               "_vt": F.when(vt >= F.lit(FAR).cast("timestamp"), F.lit(None)).otherwise(vt)})

    new = stamp(inc).withColumn("_existing", F.lit(False))
    rows = None
    events = new
    if cur is not None:
        rows = cur.join(new.select("_key_hash").distinct(), "_key_hash", "left_semi")
        old = stamp(rows.drop(*[c for c in RECOMPUTED if c in rows.columns])).withColumn("_existing", F.lit(True))
        events = new.unionByName(old, allowMissingColumns=True)
    # one version per (key, start): the newest delivery wins over what silver holds
    same = Window.partitionBy("_key_hash", "_vf").orderBy(F.col("_existing").asc_nulls_last(), *arrival_desc(events))
    events = events.withColumn("_sv_rn", F.row_number().over(same)).where("_sv_rn = 1")
    per_key = Window.partitionBy("_key_hash").orderBy(F.col("_vf").asc_nulls_first())
    prev_vt = F.lag("_vt").over(per_key)
    is_first = F.lag("_vf").over(per_key).isNull()
    reason = (
        F.when(F.col("_vf").isNull(), F.lit("a version has no start"))
        .when(F.col("_vt").isNotNull() & (F.col("_vt") < F.col("_vf")), F.lit("a version ends before it starts"))
        .when(~is_first & (prev_vt.isNull() | (prev_vt > F.col("_vf"))), F.lit("versions overlap"))
    )
    if not plan.gaps_allowed:
        reason = reason.when(~is_first & (prev_vt < F.col("_vf")), F.lit("a gap between versions"))
    events = events.withColumn("_sv_reason", reason)
    # max over the key ignores nulls on every engine the stages run on (one reason names the key's failure)
    events = events.withColumn("_key_reason", F.max("_sv_reason").over(Window.partitionBy("_key_hash")))
    bad_new = new.join(events.where(F.col("_key_reason").isNotNull()).select("_key_hash", "_key_reason").distinct(),
                       "_key_hash")
    record = [c for c in inc.columns if not c.startswith("_")]
    _, quarantine = quarantine_rows(bad_new, F.concat(F.lit(f"{plan.table}: "), F.col("_key_reason")), "history",
                                    record)
    good = events.where(F.col("_key_reason").isNull())
    last = F.lead("_vf").over(per_key).isNull()
    closed = last & F.col("_vt").isNotNull()
    out = good.withColumns({
        "_valid_from": F.col("_vf"),
        "_valid_to": F.col("_vt"),
        "_is_current": last,
        "_is_deleted": closed,
        "_deleted_at": F.when(closed, F.col("_vt")),
        "_seq_from": F.unix_timestamp(F.col("_vf")).cast("bigint"),
        "_row_key": F.sha2(F.concat_ws("|", F.col("_key_hash"), F.col("_vf").cast("string")), 256),
    }).drop(*SV_WORK, "_existing")
    good_keys = good.select("_key_hash").distinct()
    stale = None if rows is None else _stale(rows.join(good_keys, "_key_hash", "left_semi"), out)
    return _finish(out, stale, plan), quarantine


KINDS = {"scd2": scd2, "scd1": scd1, "append": append, "snapshot": snapshot}


def history(df: DataFrame, ctx: StageCtx) -> Out:
    cur = None if ctx.current is None else working(ctx.current)
    if ctx.plan.history == "source_versioned":
        rows, bad = source_versioned(df, cur, ctx.plan)
        return Out(rows, bad)
    return Out(KINDS[ctx.plan.history](df, cur, ctx.plan))
