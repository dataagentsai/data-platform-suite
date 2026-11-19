"""One hop of the platform as a job task: ingestion, silver or gold for one business date (scripts/run_hop.py on the
job's own Spark session, Unity Catalog managed Iceberg tables, the metadata store on Lakebase as the job's identity).
Deliveries are read from the landing volume; the hop's report is saved on the ops volume for the proof driver."""
import argparse
import os
import runpy
import sys
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--hop", required=True, choices=["ingest", "silver", "gold"])
p.add_argument("--window", default="", help="the business date to load; empty = today in --business-tz")
p.add_argument("--business-tz", default="", help="the domain's business timezone, for a run with no window")
p.add_argument("--catalog-dir", default="", help="where the spec catalog slice sits (default: beside these tasks)")
p.add_argument("--calendar", default="compressed")
p.add_argument("--env", default="v1")
p.add_argument("--meta-instance", required=True)
p.add_argument("--meta-database", required=True)
p.add_argument("--secret-scope", required=True)
p.add_argument("--root", required=True, help="${workspace.file_path}: serverless gives a job file no __file__")
a, _ = p.parse_known_args()
if not a.window:  # a scheduled run names no date: it loads today's business date
    if not a.business_tz:
        raise SystemExit("the platform job needs a `window` (the business date to load) or --business-tz")
    from datetime import datetime
    from zoneinfo import ZoneInfo

    a.window = datetime.now(ZoneInfo(a.business_tz)).date().isoformat()

base = Path(a.root)
root = next(d for d in [base, *base.parents] if (d / "scripts/run_hop.py").exists())
here = root / "realms/databricks/v1"
sys.path.insert(0, str(here / "tasks"))
import paths  # noqa: E402

os.environ.update(DP_CATALOG=a.catalog_dir or str(here / "_catalog"), DP_META_INSTANCE=a.meta_instance,
                  DP_META_DATABASE=a.meta_database)
extra: list[str] = []
if a.hop == "ingest":  # only ingestion tokenises personal values; the key never appears in a job parameter
    from databricks.sdk import WorkspaceClient

    os.environ["DP_TOKEN_KEY"] = WorkspaceClient().dbutils.secrets.get(scope=a.secret_scope, key="token-key")
    extra = ["--sources-dir", paths.sources(a.env)]
prefix = {"ingest": "tyre", "silver": "silver", "gold": "gold"}[a.hop]
sys.argv = ["run_hop.py", "--hop", a.hop, "--window", a.window, "--run-id", f"{prefix}-{a.window}",
            "--out", f"/tmp/dp-hop-{a.hop}-{a.window}", "--landing", paths.landing(a.env),
            "--hops-dir", paths.hops(a.env),
            "--db", "sqlite", "--meta", "lakebase", "--spark", "existing", "--table-format", "iceberg-uc",
            "--env", a.env, "--calendar", a.calendar, *extra]
try:
    runpy.run_path(str(root / "scripts/run_hop.py"), run_name="__main__")
except SystemExit as exc:
    if exc.code not in (0, None):
        raise
