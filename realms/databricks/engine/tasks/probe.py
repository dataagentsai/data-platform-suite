"""What Databricks serverless allows the engine (13 Nov probe). Each check prints one line: ok or FAIL with the reason.
Throwaway schema, dropped at the end."""
import argparse
import os
import sys
import traceback

p = argparse.ArgumentParser()
p.add_argument("--root", required=True)
p.add_argument("--meta-instance", required=True)
p.add_argument("--schema", required=True)
a, _ = p.parse_known_args()
sys.path.insert(0, os.path.join(a.root, "src"))  # the bundle syncs src/dataplatform under the bundle root
for c in (a.root, os.path.join(a.root, "..", "..", "..", "src")):
    if os.path.isdir(os.path.join(c, "dataplatform")):
        sys.path.insert(0, c)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

spark = SparkSession.builder.getOrCreate()
S, T = a.schema, f"{a.schema}.t"
results = []


def check(name, fn):
    try:
        out = fn()
        results.append(f"ok    {name}{': ' + str(out)[:300] if out is not None else ''}")
    except Exception as exc:  # noqa: BLE001 - a probe reports every failure and goes on
        results.append(f"FAIL  {name}: {type(exc).__name__}: {str(exc).splitlines()[0][:300]}")


check("spark version", lambda: spark.version)
check("sparkContext", lambda: spark.sparkContext.appName)
df = spark.createDataFrame([("a", "2026-01", 1.0), ("b", "2026-02", 2.0)], "id string, win string, v double")
check("cache + count", lambda: df.cache().count())
check("localCheckpoint", lambda: df.localCheckpoint(eager=True).count())
check("schema", lambda: spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}").collect() and None)
props = {"delta.enableDeletionVectors": "false", "delta.enableRowTracking": "false"}


def create():
    w = df.writeTo(T).using("iceberg")
    for k, v in props.items():
        w = w.tableProperty(k, v)
    w.createOrReplace()
    return spark.sql(f"DESCRIBE TABLE EXTENDED {T}").where("col_name = 'Provider'").first()["data_type"]


check("writeTo iceberg createOrReplace (provider)", create)
check("append with snapshot properties", lambda: df.writeTo(T).option("snapshot-property.dp-app", "x").option(
    "snapshot-property.dp-txn", "1").append() or spark.table(T).count())
check("snapshots metadata table", lambda: spark.sql(f"SELECT summary FROM {T}.snapshots").collect()[-1][0])
check("describe history", lambda: [r["operation"] for r in spark.sql(f"DESCRIBE HISTORY {T}").collect()][:4])
check("history userMetadata / operationParameters", lambda: spark.sql(f"DESCRIBE HISTORY {T}").select(
    "operationParameters").first()[0])
check("delta txnAppId append", lambda: df.write.mode("append").option("txnAppId", "probe").option(
    "txnVersion", "5").saveAsTable(T) or spark.table(T).count())
check("delta txnAppId repeat (should not add)", lambda: df.write.mode("append").option("txnAppId", "probe").option(
    "txnVersion", "5").saveAsTable(T) or spark.table(T).count())
df.createOrReplaceTempView("src_probe")
check("MERGE NOT MATCHED BY SOURCE via temp view", lambda: spark.sql(
    f"MERGE INTO {T} t USING src_probe s ON 1 = 0 WHEN NOT MATCHED THEN INSERT (id, win, v) VALUES (s.id, s.win, s.v) "
    "WHEN NOT MATCHED BY SOURCE AND t.win IN ('2026-02') THEN DELETE").collect() and spark.table(T).count())
check("writeTo overwrite by condition", lambda: df.writeTo(T).overwrite(F.col("win") == "2026-01") or
      spark.table(T).count())
check("ALTER ADD COLUMNS", lambda: spark.sql(f"ALTER TABLE {T} ADD COLUMNS (note STRING)").collect() and None)
check("CLUSTER BY AUTO", lambda: spark.sql(f"ALTER TABLE {T} CLUSTER BY AUTO").collect() and None)
check("OPTIMIZE", lambda: spark.sql(f"OPTIMIZE {T}").collect() and None)
check("tableExists", lambda: spark.catalog.tableExists(T))


def lakebase():
    os.environ["DP_META_INSTANCE"] = a.meta_instance
    from dataplatform.adapters.lakebase import meta_store_from_env

    store = meta_store_from_env()
    return f"{len(store.datasets('tyre'))} tyre datasets, rules for erp.sales_order_line: " \
           f"{len(store.dq_rules('erp', 'sales_order_line'))}"


check("Lakebase as the job's identity", lambda: lakebase())
check("drop schema", lambda: spark.sql(f"DROP SCHEMA {S} CASCADE").collect() and None)
print("\n".join(["PROBE RESULTS"] + results))
if any(r.startswith("FAIL") for r in results):
    print("PROBE: some checks failed (see above)")
