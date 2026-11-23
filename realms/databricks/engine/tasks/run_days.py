"""The chosen slice's world (--slice, default the tyre company), N days, on Databricks: the same day loop as the laptop (scripts/run_platform.py), on the job's own
Spark session, Unity Catalog managed Iceberg tables (<env>_<layer>.<schema>.<table>) and the metadata store on
Lakebase as the job's identity. The simulated world (sources, landing) lives on the driver's local disk; the ops page
and the report are copied to the volume <env>_ops.reports.runs/<run id>/ to open afterwards."""
import argparse
import os
import runpy
import secrets
import shutil
import sys
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--slice", default="02-tyre", help="the catalog slice to run (a folder of _catalog/slices/)")
p.add_argument("--days", default="3")
p.add_argument("--scale", default="0.1")
p.add_argument("--calendar", default="", help="a named world calendar; empty = the world's own")
p.add_argument("--housekeeping-days", default="", help="bronze housekeeping's correction window; empty = off")
p.add_argument("--table-format", default="iceberg-uc", choices=["iceberg-uc", "delta-uc-uniform", "s3tables"],
               help="the table format of every layer (the table port, ADR-027)")
p.add_argument("--env", default="dev")
p.add_argument("--meta-instance", default="dp-meta-dev")
p.add_argument("--run-id", default="manual")
p.add_argument("--root", required=True, help="${workspace.file_path}: serverless gives a job file no __file__")
a, _ = p.parse_known_args()

base = Path(a.root)
root = next(d for d in [base, *base.parents] if (d / "scripts/run_platform.py").exists())
catalog = next(d for d in (root / "realms/databricks/engine/_catalog", base / "_catalog") if d.exists())
os.environ.update(DP_CATALOG=str(catalog), DP_META_INSTANCE=a.meta_instance, DP_META_DATABASE="dp_meta_proof",
                  DP_TOKEN_KEY=secrets.token_hex(32))  # a fresh key per proof run, never stored
out = Path(f"/tmp/dp-proof-{a.run_id}")
sys.argv = ["run_platform.py", "--slice", a.slice, "--days", a.days, "--scale", a.scale, "--db", "sqlite", "--meta", "lakebase",
            "--spark", "existing", "--table-format", a.table_format, "--env", a.env, "--upkeep-every", "7", "--reset-tables",
            "--out", str(out), *(["--calendar", a.calendar] if a.calendar else []),
            *(["--housekeeping-days", a.housekeeping_days, "--prove-rebuild"] if a.housekeeping_days else [])]
code = 0
try:
    runpy.run_path(str(root / "scripts/run_platform.py"), run_name="__main__")
except SystemExit as exc:
    code = int(exc.code or 0) if not isinstance(exc.code, str) else 1
finally:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.env}_ops.reports")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {a.env}_ops.reports.runs")
    dest = Path(f"/Volumes/{a.env}_ops/reports/runs/{a.run_id}")
    dest.mkdir(parents=True, exist_ok=True)
    for name in ("ops.html", "report.json", "ops_tables.json"):
        if (out / name).exists():
            shutil.copyfile(out / name, dest / name)
            print(f"copied {name} to {dest / name}")
    if (out / "ops_tables.json").exists():  # the ops dashboards read these (B13)
        import json

        sys.path.insert(0, str(root / "src"))
        from dataplatform.adapters.spark_ops_tables import publish

        for note in publish(spark, json.loads((out / "ops_tables.json").read_text()), f"{a.env}_ops", "ops"):
            print(f"ops tables: {note}")
    semantic_failed = 0
    import yaml  # the job's environment lists pyyaml

    platform = yaml.safe_load((catalog / "slices" / a.slice / "manifest/platform.yaml").read_text())
    domain = str(platform["domain"])  # generated/<domain>/<env>: the business metric views of this domain, if any
    generated = root / "realms/databricks/semantic_governance/generated" / domain / a.env
    if not code and (out / "report.json").exists() and generated.exists():  # the business metric views (B12)
        import json

        sys.path.insert(0, str(root / "realms/databricks/engine/tasks"))
        from semantic_layer import check, create_views

        for note in create_views(spark, generated):
            print(f"semantic: {note}")
        rows = check(spark, generated, json.loads((out / "report.json").read_text()))
        for r in rows:
            print(f"semantic: {'ok  ' if r['ok'] else 'DIFF'} {r['view']} {r['measure']} {r['address']}: view "
                  f"{r['view_value']}, gold {r['gold_value']}, world {r['world_value']} (spec at full scale "
                  f"{r['spec_value']})")
        (dest / "semantic_check.json").write_text(json.dumps(rows, indent=1))
        semantic_failed = sum(not r["ok"] for r in rows)
        # B20 C2: the certified answers join the run's scorecard (ops_success), measured here after the views exist
        from dataplatform.obs import success
        from dataplatform.obs.ops_export import export

        c2 = next(x for x in success.scorecard(success.Evidence(
            known_answers={f"{r['view']}.{r['measure']}.{r['address']}": bool(r["ok"]) for r in rows})) if x.id == "C2")
        print(f"success criteria (B20): C2 {c2.state}: {c2.value}")
        doc = json.loads((out / "ops_tables.json").read_text()) if (out / "ops_tables.json").exists() else {}
        if "ops_success" in doc:
            keep = [r for r in doc["ops_success"]["rows"] if r[1] != "C2"]
            part = export({"ops_success": success.rows(out.name, [c2])}, {"ops_success": success.COLUMNS})
            doc["ops_success"]["rows"] = sorted(keep + part["ops_success"]["rows"], key=lambda r: int(r[1][1:]))
            (out / "ops_tables.json").write_text(json.dumps(doc))
            shutil.copyfile(out / "ops_tables.json", dest / "ops_tables.json")
            for note in publish(spark, {"ops_success": doc["ops_success"]}, f"{a.env}_ops", "ops"):
                print(f"ops tables: {note}")
if code:
    raise SystemExit(f"run_platform exited {code}: a layer did not equal the oracle (see the log above)")
if semantic_failed:
    raise SystemExit(f"{semantic_failed} metric view answer(s) differ from gold or the world (see 'semantic:' lines)")
