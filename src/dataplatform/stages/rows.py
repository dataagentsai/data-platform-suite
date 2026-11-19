"""Stages 10, 11 and 13: duplicates, keys, provenance. Pure functions over DataFrames (ADR-013)."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dataplatform.silver.plan import LOAD_COLUMNS, SILVER_COLUMNS, TRANSPORT_COLUMNS, SilverPlan
from dataplatform.stages.common import (
    Out,
    StageCtx,
    arrival_desc,
    arrival_order,
    key_hash,
    quarantine_rows,
    reasons,
)


def recent(current: DataFrame, plan: SilverPlan, as_of: str) -> DataFrame:
    """Silver rows inside the dataset's late lookback (by bronze window), or all of silver when it has none."""
    if plan.lookback_days is None:
        return current
    since = (date.fromisoformat(as_of) - timedelta(days=plan.lookback_days)).isoformat()
    return current.where(F.col("_window") >= F.lit(since))


# ---------------------------------------------------------------------------------------------------- 10 duplicates
def duplicates(df: DataFrame, ctx: StageCtx) -> Out:
    """Within the batch: business_key_first keeps the first arrival per key, business_key_latest the latest by the
    order column, full_row drops identical rows. A type 2 history keeps every distinct change (the same key and order
    value twice is one change). Against silver: a key already loaded (within the late lookback) is a replay (DUP-04)
    for kinds that only append."""
    plan = ctx.plan
    keys = list(plan.dedup_keys)
    arrival = arrival_order(df)
    if plan.dedup_mode == "none":
        ranked = df.withColumn("_rn", F.lit(1))
    elif plan.dedup_mode == "full_row":
        cols = [c.name for c in plan.kept]
        ranked = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*cols).orderBy(*arrival)))
    elif plan.history == "scd2":
        part = Window.partitionBy(*keys, plan.order_by).orderBy(*arrival)
        ranked = df.withColumn("_rn", F.row_number().over(part))
    elif plan.dedup_mode == "business_key_latest":
        order = [F.col(plan.order_by).desc_nulls_last()] if plan.order_by else []
        latest = Window.partitionBy(*keys).orderBy(*order, *arrival_desc(df))
        ranked = df.withColumn("_rn", F.row_number().over(latest))
    else:  # business_key_first
        ranked = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*arrival)))
    kept, dropped = ranked.where("_rn = 1").drop("_rn"), ranked.where("_rn > 1").drop("_rn")
    tally = {"dropped": dropped}
    if plan.history == "append" and ctx.current is not None and plan.dedup_mode != "none":
        seen = recent(ctx.current, plan, ctx.as_of).select(*keys).distinct().withColumn("_seen", F.lit(True))
        marked = kept.join(seen, keys, "left")
        kept = marked.where(F.col("_seen").isNull()).drop("_seen")
        tally["replayed"] = marked.where(F.col("_seen").isNotNull()).drop("_seen")
    return Out(kept, tally=tally)


# ---------------------------------------------------------------------------------------------------- 11 keys
def keys(df: DataFrame, ctx: StageCtx) -> Out:
    """Every business key column present, else the row is quarantined; the key hash every later join uses."""
    checks = [(F.col(k).isNull(), f"business key {k} is empty") for k in ctx.plan.keys]
    good, bad = quarantine_rows(df, reasons(checks), "keys")
    return Out(good.withColumn("_key_hash", key_hash(ctx.plan.keys)), bad)


# ---------------------------------------------------------------------------------------------------- 13 provenance
def provenance(df: DataFrame, ctx: StageCtx) -> Out:
    """Where each row came from and when silver took it: batch, file, bronze window and run, silver run, load time,
    and a record hash over the business columns. Columns come out in a fixed order: contract, other source columns,
    then load, transport and silver columns."""
    plan = ctx.plan
    business = [c.name for c in plan.kept]
    out = df.withColumnRenamed("_run_id", "_bronze_run_id") if "_run_id" in df.columns else df
    out = (
        out.withColumn("_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in business])), 256))
        .withColumn("_silver_run_id", F.lit(ctx.run_id))
        .withColumn("_silver_loaded_at", F.lit(ctx.loaded_at).cast("timestamp"))
    )
    fixed = [*LOAD_COLUMNS, *TRANSPORT_COLUMNS, *SILVER_COLUMNS, "_action"]
    known = set(business) | set(fixed)
    extra = sorted(c for c in out.columns if c not in known)
    order = business + extra + [c for c in fixed if c in out.columns and c != "_run_id"]
    return Out(out.select(*order))
