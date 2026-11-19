"""The baseline batch (B8, ADR-029, assumed decision A3): a compaction of bronze, not a copy of silver.

Silver's processors cannot be undone (value maps, text date formats, JSON paths, local-time columns, dropped columns),
so a baseline written from silver could not be replayed through them. The baseline is bronze's own rows instead: the
last row per business key among the batches up to the cut-off, deletes kept (so a replay marks the key deleted
again). Pure PySpark: it runs on Spark, on DuckDB through SQLFrame and on Databricks.

    compactable     why a dataset cannot have a baseline ("" when it can)
    compact         the baseline rows: last row per key, raw bronze shape
    replay_check    the baseline replayed through the processors against silver, on the keys no later batch touched

The check needs no scratch table: the processors are pure (ADR-013), so the replay is a frame, compared with current
silver by key hash. A version history before the cut-off is not kept (ADR-029: the baseline replaces it).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dataplatform.silver.plan import SilverPlan
from dataplatform.stages.common import arrival_desc

KEYED = ("scd1", "scd2")


def compactable(plan: SilverPlan) -> str:
    """'' when the dataset can have a baseline, else the reason it keeps its bronze."""
    if plan.ingest_read == "full_copy":
        return "a full copy is its own baseline"
    if plan.history not in KEYED:
        return f"history {plan.history}: a baseline would hold every row, so bronze is kept"
    if not plan.keys:
        return "no business key to compact on"
    pathed = [k for k in plan.keys if plan.column(k).path]
    if pathed:
        return f"key column(s) {pathed} come from a JSON path, not a bronze column"
    return ""


def _bronze_name(plan: SilverPlan, name: str) -> str:
    return plan.column(name).source or name


def compact(bronze: DataFrame, plan: SilverPlan) -> DataFrame:
    """The last row per business key (history order first, then arrival), in bronze's own columns."""
    reason = compactable(plan)
    if reason:
        raise ValueError(f"{plan.table}: no baseline: {reason}")
    keys = [_bronze_name(plan, k) for k in plan.keys]
    order = []
    if plan.order_by and _bronze_name(plan, plan.order_by) in bronze.columns:
        order.append(F.col(_bronze_name(plan, plan.order_by)).desc_nulls_last())
    w = Window.partitionBy(*keys).orderBy(*order, *arrival_desc(bronze))
    return bronze.withColumn("_bl_n", F.row_number().over(w)).where(F.col("_bl_n") == 1).drop("_bl_n")


def _live(rows: DataFrame) -> DataFrame:
    out = rows
    if "_action" in out.columns:
        out = out.where(F.col("_action") != "delete")
    for col, want in (("_is_current", True), ("_is_deleted", False)):
        if col in out.columns:
            out = out.where(F.coalesce(F.col(col), F.lit(want)) == F.lit(want))
    return out.select("_key_hash").distinct()


def replay_check(replayed: DataFrame, later: DataFrame | None, silver: DataFrame | None) -> dict[str, int]:
    """Live keys of the replayed baseline against live keys of current silver, leaving out keys any later batch
    touched (their silver rows moved on after the cut-off). Every frame carries _key_hash (the keys processor).
    Returns baseline_keys, silver_keys, missing (in silver, not in the replay) and extra (the other way)."""
    mine = _live(replayed)
    theirs = _live(silver) if silver is not None else mine.limit(0)
    if later is not None:
        moved = later.select("_key_hash").distinct()
        mine = mine.join(moved, "_key_hash", "left_anti")
        theirs = theirs.join(moved, "_key_hash", "left_anti")
    return {
        "baseline_keys": mine.count(),
        "silver_keys": theirs.count(),
        "missing": theirs.join(mine, "_key_hash", "left_anti").count(),
        "extra": mine.join(theirs, "_key_hash", "left_anti").count(),
    }
