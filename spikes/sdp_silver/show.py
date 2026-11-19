"""Print the spike's SCD Type 2 table and check it against the expected history.

    python show.py <work folder> <iceberg jar>
"""

import sys

from pyspark.sql import SparkSession

work, jar = sys.argv[1], sys.argv[2]
spark = (SparkSession.builder.master("local[1]").config("spark.jars", jar)
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop").config("spark.sql.catalog.local.warehouse", f"{work}/warehouse")
         .config("spark.ui.enabled", "false").getOrCreate())
df = spark.table("local.silver.order_line")
df.printSchema()
start, end = [c for c in df.columns if c.startswith("__START")][0], [c for c in df.columns if c.startswith("__END")][0]
rows = df.orderBy("order_id", "line_no", start).collect()
for r in rows:
    print(r.asDict())

def versions(o, l):
    return [(r.promised_date, r.qty, r[start], r[end]) for r in rows if r.order_id == o and r.line_no == l]

checks = {
    "dealer codes cleaned by our stage function": all(r.dealer_code in ("D-0042", "D-0007") for r in rows),
    "O1/1: three versions in sequence order 10, 20, 30": [v[2] for v in versions("O1", 1)] == [10, 20, 30],
    "O1/1: current version holds the newest change (12 Nov), not the late older one": [v[0] for v in versions("O1", 1) if v[3] is None] == ["2026-11-12"],
    "O1/2: deleted, so no open version": all(v[3] is not None for v in versions("O1", 2)) and len(versions("O1", 2)) == 1,
    "O2/1: qty change opened a second version": [v[1] for v in versions("O2", 1)] == [8, 6],
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
sys.exit(0 if all(checks.values()) else 1)
