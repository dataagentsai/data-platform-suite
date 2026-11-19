"""The day's checks on Databricks. Every result carries the run id, so the evidence view joins them to the run log.

Judged from the world's own record (it is regenerated from its seed), never from the platform's logs, except the
detector checks, whose whole point is to ask whether the platform noticed.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from dataplatform import reconcile
from dataplatform.model import CheckResult
from dataplatform.world.oracle import expected_current, judge_silver
from dataplatform.world.shop import ShopWorld


def run_checks(spark: Any, A: Any, cfg: dict, meta: str, landing: str, metric_view_sql: str) -> list[CheckResult]:
    ds = cfg["dataset"]
    world_cfg = json.loads((Path(A.root) / f"realms/databricks/compiled/{A.slice}.json").read_text())["world"]
    world = ShopWorld(world_cfg)
    days = world.run(A.day)
    stopped = {name for d in days for inc in d.incidents if inc["kind"] == "renamed_column"
               for _, name, _ in d.deliveries}
    R: list[CheckResult] = []
    rid, day = A.run_id, A.day

    silver = spark.table(ds["silver_table"])
    current = [r.asDict() for r in silver.where("__END_AT IS NULL").select("order_id", "status", "amount").collect()]
    R += judge_silver(rid, day, expected_current(world.changes, stopped), current)

    # reconciliation, source -> bronze, for what this run loaded
    today = days[-1]
    accepted = [rows for _, name, rows in today.deliveries
                if name not in stopped and not name.endswith("_resent.jsonl")]
    src_rows = [r for rows in accepted for r in rows]
    b = spark.table(ds["bronze_table"]).where(F.col("_run_id") == rid) \
        .agg(F.count("*").alias("n"), F.round(F.sum("amount"), 2).alias("amt")).first()
    R += reconcile.compare(rid, day, {
        "source": {"row_count": len(src_rows), "amount_total": round(sum(r.get("amount", 0) for r in src_rows), 2)},
        "bronze": {"row_count": b["n"], "amount_total": float(b["amt"] or 0)},
    }, ds["reconciliation"].get("tolerance", {}))

    R += detectors(spark, rid, day, today, world, meta, silver)

    # contract table-level rules (duplicateValues on key + change sequence; rowCount > 0)
    dup = silver.groupBy("order_id", "__START_AT").count().where("count > 1").count()
    R.append(CheckResult(rid, day, "contract.duplicateValues", "DUP-02", dup == 0, 0, dup))
    n = silver.count()
    R.append(CheckResult(rid, day, "contract.rowCount", "GATE-01", n > 0, ">0", n))
    q = spark.table(ds["quarantine_table"]).count() if spark.catalog.tableExists(ds["quarantine_table"]) else None
    R.append(CheckResult(rid, day, "quality.quarantine_rows", "DQ-02", q == 0, 0, q,
                         "" if q == 0 else "the world plants no bad values, so nothing should be quarantined"))

    R.append(metric_view(spark, rid, day, ds, metric_view_sql))
    return R


def detectors(spark, rid, day, today, world, meta, silver) -> list[CheckResult]:
    """SIM-01: each planted incident is noticed on its day, and nothing fires on a clean day."""
    reg = spark.table(f"{meta}.batch_registry").where(F.col("run_id") == rid).collect()
    statuses = [r["status"] for r in reg]
    kinds = {i["kind"] for i in today.incidents}
    out = []

    fired_dup = "skipped_duplicate" in statuses
    out.append(CheckResult(rid, day, "detector.duplicate_delivery", "DUP-01", fired_dup == ("duplicate_delivery" in kinds),
                           "duplicate_delivery" in kinds, fired_dup))
    rej = [r for r in reg if r["status"] == "rejected_contract"]
    fired_rej = bool(rej)
    named = all("amount" in (r["reason"] or "") for r in rej)
    out.append(CheckResult(rid, day, "detector.renamed_column", "SCH-02",
                           fired_rej == ("renamed_column" in kinds) and named, "renamed_column" in kinds, fired_rej,
                           "; ".join(r["reason"] for r in rej)))
    if "source_delete" in kinds:
        gone = world.deleted_orders()
        still = silver.where(F.col("order_id").isin(gone) & F.col("__END_AT").isNull()).count()
        closed = silver.where(F.col("order_id").isin(gone)).select("order_id").distinct().count()
        out.append(CheckResult(rid, day, "detector.source_delete", "DEL-01", still == 0 and closed == len(gone),
                               {"current": 0, "with_history": len(gone)}, {"current": still, "with_history": closed}))
    if day >= 5 and world.late_order():
        oid = world.late_order()
        rows = silver.where(F.col("order_id") == oid).orderBy("__START_AT").collect()
        cur = [r for r in rows if r["__END_AT"] is None]
        overlap = any(a["__END_AT"] is None or a["__END_AT"] > b["__START_AT"] for a, b in zip(rows, rows[1:]))
        history = [r["status"] for r in rows]
        ok = len(cur) == 1 and cur[0]["status"] == "shipped" and not overlap and history == ["placed", "paid", "shipped"]
        out.append(CheckResult(rid, day, "detector.late_change", "SCD-02", ok,
                               {"current_rows": 1, "status": "shipped", "history": ["placed", "paid", "shipped"]},
                               {"current_rows": len(cur), "status": cur[0]["status"] if cur else None,
                                "history": history}))
    return out


def metric_view(spark, rid, day, ds, sql) -> CheckResult:
    """SERV-04: the business definition lives in one place, and it agrees with the gold table."""
    name = ds["gold_table"].rsplit(".", 1)[0] + ".orders_metrics"
    try:
        spark.sql(sql)
        mv = spark.sql(f"SELECT MEASURE(orders) AS o, ROUND(MEASURE(revenue), 2) AS r FROM {name}").first()
        g = spark.table(ds["gold_table"]).agg(F.sum("orders").alias("o"), F.round(F.sum("amount"), 2).alias("r")).first()
        ok = mv["o"] == g["o"] and abs(float(mv["r"]) - float(g["r"])) < 0.01
        return CheckResult(rid, day, "semantic.metric_view_agrees_with_gold", "SERV-04", ok,
                           {"orders": g["o"], "revenue": float(g["r"])}, {"orders": mv["o"], "revenue": float(mv["r"])})
    except Exception as e:  # noqa: BLE001 - recorded as a platform finding, not a crash
        return CheckResult(rid, day, "semantic.metric_view_agrees_with_gold", "SERV-04", False, "created",
                           "failed", str(e).splitlines()[0][:300])
