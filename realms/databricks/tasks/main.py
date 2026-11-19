"""Slice 1 job steps on Databricks. One entry file; the job calls it once per step.

    prepare  create schemas, volume and metadata tables; load the compiled manifest into the metadata DB;
             the world writes this day's deliveries into the landing volume (the world plays the mover)
    land     the lander (our code) reads landing, writes bronze, logs every batch            -> then the
             silver pipeline runs (Databricks AUTO CDC, SCD type 2, expectations from the metadata DB)
    finish   gold table + metric view; reconciliation, oracle and detector checks, all keyed by the run id

Wiring only: every rule lives in the dataplatform package or the metadata DB, never here.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--step", required=True, choices=["prepare", "land", "finish", "reset"])
    p.add_argument("--day", type=int, default=1)
    p.add_argument("--run-id", default="local")
    p.add_argument("--env", default="dev")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--root", required=True, help="the bundle's synced file root in the workspace")
    p.add_argument("--slice", default="01-orders")
    return p.parse_args()


A = args()
sys.path.insert(0, str(Path(A.root) / "src"))

from pyspark.sql import SparkSession  # noqa: E402

from dataplatform.adapters import spark_delta as sd  # noqa: E402
from dataplatform.adapters.files import FileLanding  # noqa: E402
from dataplatform.keys import config_fingerprint  # noqa: E402
from dataplatform.manifest import fill  # noqa: E402

spark = SparkSession.builder.getOrCreate()
P = f"{A.catalog}.dp_{A.env}"
SCHEMAS = {"meta": f"{P}_meta", "landing": f"{P}_landing", "bronze": f"{P}_bronze", "silver": f"{P}_silver",
           "gold": f"{P}_gold"}
LANDING = f"/Volumes/{A.catalog}/dp_{A.env}_landing/files"
META = SCHEMAS["meta"]


def step_reset() -> None:
    # Runs after the bundle is destroyed and redeployed: the old silver pipeline is gone, but its tables are still
    # registered to it, and a new pipeline may not write to another pipeline's tables. So silver is dropped too.
    for s in SCHEMAS.values():
        spark.sql(f"DROP SCHEMA IF EXISTS {sd.ident(s)} CASCADE")
    print("reset: dropped", list(SCHEMAS.values()))


def step_prepare() -> None:
    for s in SCHEMAS.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sd.ident(s)}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {sd.ident(SCHEMAS['landing'] + '.files')}")
    sd.create_meta(spark, META)
    if A.day == 1:
        probe_platform()

    compiled = fill(json.loads((Path(A.root) / f"realms/databricks/compiled/{A.slice}.json").read_text()), SCHEMAS)
    ds, contract = compiled["dataset"], compiled["contract"]
    fp = config_fingerprint(ds)
    known = {r["config_fingerprint"] for r in spark.table(f"{META}.dataset_config").collect()}
    if fp not in known:                     # register only when the manifest changed (idempotent)
        sd._append(spark, f"{META}.dataset_config", [(ds["source_name"], ds["dataset_name"], json.dumps(ds),
                   json.dumps(contract), fp, sd.now())], sd.DDL["dataset_config"])
        spark.sql(f"DELETE FROM {sd.ident(META + '.quality_rules')}")
        sd._append(spark, f"{META}.quality_rules", [(ds["source_name"], ds["dataset_name"], k, v,
                   ds["contract_version"]) for k, v in ds["quality_rules"].items()], sd.DDL["quality_rules"])
        print(f"registered {ds['dataset_name']} config {fp}")

    # The world delivers every file up to today that is not in landing yet, so a missed run loses nothing:
    # a real source's files would still be waiting too.
    from dataplatform.world.shop import ShopWorld
    landing = FileLanding(LANDING)
    present = {d.name for d in landing.deliveries(ds["source_name"], ds["dataset_name"])}
    for out in ShopWorld(compiled["world"]).run(A.day):
        for window, name, rows in out.deliveries:
            if name not in present:
                landing.write(ds["source_name"], ds["dataset_name"], window, name, rows)
                print(f"world day {out.day}: delivered {name} ({len(rows)} rows)")
        if out.day == A.day:
            for inc in out.incidents:
                print(f"world day {A.day}: planted {inc['id']} ({inc['kind']}), expect {inc['expect']['detector']}")


def probe_platform() -> None:
    """DE-057: which system tables this workspace exposes. Cost, audit and lineage depend on them."""
    from dataplatform.model import CheckResult
    probes = {"system.billing.usage": "COST-02", "system.access.audit": "SEC-04",
              "system.access.table_lineage": "LIN-01", "system.lakeflow.job_run_timeline": "OBS-01"}
    results = []
    for table, obligation in probes.items():
        try:
            spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect()
            results.append(CheckResult(A.run_id, A.day, f"probe.{table}", obligation, True, "readable", "readable"))
        except Exception as e:  # noqa: BLE001 - a probe reports whatever the platform says
            results.append(CheckResult(A.run_id, A.day, f"probe.{table}", obligation, False, "readable",
                                       "not readable", str(e).splitlines()[0][:300]))
    sd.SparkChecks(spark, META).write(results)
    for r in results:
        print(("OK  " if r.passed else "NO  ") + r.check, r.detail)


def step_land() -> None:
    from dataplatform.lander import land
    cfg = sd.SparkMeta(spark, META).dataset_config("shop_db", "orders")
    outcomes = land(cfg["dataset"], cfg["contract"], FileLanding(LANDING), sd.SparkMeta(spark, META),
                    sd.DeltaWriter(spark), sd.SparkRunLog(spark, META), A.run_id, A.day)
    for o in outcomes:
        print(f"{o.delivery.name}: {o.status} {o.rows} rows {o.reason}")
    if not spark.catalog.tableExists(cfg["dataset"]["bronze_table"]):
        raise RuntimeError("bronze does not exist after landing; nothing for silver to read")


def step_finish() -> None:
    from checks import run_checks
    cfg = sd.SparkMeta(spark, META).dataset_config("shop_db", "orders")
    ds = cfg["dataset"]
    tz = ds["config"].get("schedule_timezone", "UTC")
    silver, gold = sd.ident(ds["silver_table"]), sd.ident(ds["gold_table"])
    spark.sql(f"""CREATE OR REPLACE TABLE {gold} AS
        SELECT to_date(from_utc_timestamp(order_ts, '{tz}')) AS order_date, COUNT(*) AS orders,
               ROUND(SUM(amount), 2) AS amount
        FROM {silver} WHERE __END_AT IS NULL GROUP BY 1""")
    metric_view = metric_view_sql(ds, tz)
    results = run_checks(spark, A, cfg, META, LANDING, metric_view)
    sd.SparkChecks(spark, META).write(results)
    failed = [r for r in results if not r.passed]
    for r in results:
        print(("PASS " if r.passed else "FAIL ") + f"{r.check} [{r.obligation}] expected={r.expected} "
              f"observed={r.observed} {r.detail}")
    if any(r.check.startswith(("oracle.", "detector.", "reconcile.")) for r in failed):
        raise SystemExit(f"{len(failed)} check(s) failed on day {A.day}; see {META}.evidence")


def metric_view_sql(ds: dict, tz: str) -> str:
    name = ds["gold_table"].rsplit(".", 1)[0] + ".orders_metrics"
    return f"""CREATE OR REPLACE VIEW {sd.ident(name)} WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: Orders metrics for slice 1. Defined once here; tiles and questions read these, never their own SQL.
source: {ds['silver_table']}
filter: __END_AT IS NULL
dimensions:
  - name: order_date
    expr: to_date(from_utc_timestamp(order_ts, '{tz}'))
  - name: status
    expr: status
  - name: store_id
    expr: store_id
measures:
  - name: orders
    expr: COUNT(1)
  - name: revenue
    expr: SUM(amount)
$$"""


if __name__ == "__main__":
    sys.path.insert(0, str(Path(A.root) / "realms/databricks/tasks"))
    {"prepare": step_prepare, "land": step_land, "finish": step_finish, "reset": step_reset}[A.step]()
