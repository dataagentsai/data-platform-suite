"""The proof driver: the simulated tyre world, N days, against the DEPLOYED platform job. Each day it writes the day
into the mock sources, plays the client's movers into the landing volume, triggers the platform job (ingestion,
silver, gold as three tasks) and waits, then judges every layer against the world's known answers from the reports
the tasks saved (scripts/run_platform.py --hops job). The world stays in this one process; the platform never sees
it."""
import argparse
import os
import runpy
import shutil
import sys
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--days", default="3")
p.add_argument("--scale", default="0.1")
p.add_argument("--calendar", default="compressed")
p.add_argument("--housekeeping-days", default="")
p.add_argument("--env", default="v1")
p.add_argument("--meta-instance", required=True)
p.add_argument("--meta-database", required=True)
p.add_argument("--platform-job-id", required=True)
p.add_argument("--catalog-dir", default="", help="where the spec catalog slice sits (default: beside these tasks)")
p.add_argument("--run-id", default="manual")
p.add_argument("--root", required=True, help="${workspace.file_path}: serverless gives a job file no __file__")
a, _ = p.parse_known_args()

base = Path(a.root)
root = next(d for d in [base, *base.parents] if (d / "scripts/run_platform.py").exists())
here = root / "realms/databricks/v1"
sys.path.insert(0, str(here / "tasks"))
import paths  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
for statement in paths.SETUP:
    spark.sql(statement.format(env=a.env))

os.environ.update(DP_CATALOG=a.catalog_dir or str(here / "_catalog"), DP_META_INSTANCE=a.meta_instance,
                  DP_META_DATABASE=a.meta_database)
out = Path(f"/tmp/dp-proof-{a.run_id}")
sys.argv = ["run_platform.py", "--days", a.days, "--scale", a.scale, "--db", "sqlite", "--meta", "lakebase",
            "--spark", "existing", "--table-format", "iceberg-uc", "--env", a.env, "--upkeep-every", "7",
            "--reset-tables", "--out", str(out), "--hops", "job", "--platform-job-id", a.platform_job_id,
            "--landing", paths.landing(a.env), "--hops-dir", paths.hops(a.env), "--sources-dir", paths.sources(a.env),
            "--reset-shared", *(["--calendar", a.calendar] if a.calendar else []),
            *(["--housekeeping-days", a.housekeeping_days, "--prove-rebuild"] if a.housekeeping_days else [])]
code = 0
try:
    runpy.run_path(str(root / "scripts/run_platform.py"), run_name="__main__")
except SystemExit as exc:
    code = int(exc.code or 0) if not isinstance(exc.code, str) else 1
finally:
    dest = Path(paths.reports(a.env, a.run_id))
    dest.mkdir(parents=True, exist_ok=True)
    for name in ("ops.html", "report.json", "ops_tables.json"):
        if (out / name).exists():
            shutil.copyfile(out / name, dest / name)
            print(f"copied {name} to {dest / name}")
if code:
    raise SystemExit(f"the proof run failed (exit {code}); see report.json under {paths.reports(a.env, a.run_id)}")
