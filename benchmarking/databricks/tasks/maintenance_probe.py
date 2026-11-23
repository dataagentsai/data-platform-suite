"""Maintenance probe on Databricks (benchmarking/scenarios/maintenance.yaml): make a fragmented table in the chosen
format through the platform's table port, try every maintenance statement that applies, and write what happened to
<env>_ops.reports.bench/<run id>/maintenance-<format>.json. A refused statement is a result. Run it once per format.
"""
import argparse
import json
import sys
import re
import tempfile
import time
from pathlib import Path

def word(pattern: str):
    """An argparse type: the value must match `pattern` whole, so it can go into a name or a path."""
    def check(value: str) -> str:
        if not re.fullmatch(pattern, value):
            raise argparse.ArgumentTypeError(f"{value!r} does not match {pattern}")
        return value
    return check


p = argparse.ArgumentParser()
p.add_argument("--table-format", required=True, choices=["iceberg-uc", "delta-uc-uniform", "s3tables"])
p.add_argument("--env", default="dev", type=word(r"[a-z][a-z0-9]{0,15}"))
p.add_argument("--commits", type=int, default=200, help="small appends: each leaves at least one small file")
p.add_argument("--rows", type=int, default=500, help="rows per append")
p.add_argument("--run-id", default="manual", type=word(r"[A-Za-z0-9_-]{1,64}"))
p.add_argument("--root", required=True, help="${workspace.file_path}: serverless gives a job file no __file__")
a, _ = p.parse_known_args()

base = Path(a.root)
for d in [base, *base.parents]:
    if (d / "src/dataplatform").exists():
        sys.path.insert(0, str(d / "src"))
    if (d / "bench").exists():
        sys.path.insert(0, str(d))
from pyspark.sql import SparkSession  # noqa: E402

from bench import maintenance  # noqa: E402
from dataplatform.adapters.table_store import make_store, store_catalog  # noqa: E402

spark = SparkSession.builder.getOrCreate()
store = make_store(spark, a.table_format, tempfile.mkdtemp(prefix="bench-"), store_catalog(a.table_format, a.env))
TABLE = "bench__frag"
store.drop("silver", TABLE)
t0 = time.monotonic()
for i in range(a.commits):
    df = spark.range(a.rows).selectExpr(f"id + {i * a.rows} AS k", "cast(id AS double) AS v")
    store.append("silver", TABLE, df, "bench", str(i))
build_s = round(time.monotonic() - t0, 1)

plain = store._plain("silver", TABLE)  # catalog.namespace.table
catalog, ident = plain.split(".", 1)[0], plain.split(".", 1)[1]
results = maintenance.run_probes(spark, a.table_format, store.name("silver", TABLE), ident, catalog)
report = {"format": a.table_format, "commits": a.commits, "rows_per_commit": a.rows, "build_seconds": build_s,
          "verdicts": maintenance.verdicts(results), "results": results}
store.drop("silver", TABLE)

# shiftleft: allow SL-SEC-01 -- env matched [a-z][a-z0-9]{0,15} when the arguments were read: identifier characters only
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.env}_ops.reports")
# shiftleft: allow SL-SEC-01 -- env matched [a-z][a-z0-9]{0,15} when the arguments were read: identifier characters only
spark.sql(f"CREATE VOLUME IF NOT EXISTS {a.env}_ops.reports.bench")
dest = Path(f"/Volumes/{a.env}_ops/reports/bench/{a.run_id}")
dest.mkdir(parents=True, exist_ok=True)
(dest / f"maintenance-{a.table_format}.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report["verdicts"], indent=2))
