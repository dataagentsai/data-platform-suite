"""One hop of the platform as its own process: ingestion, silver or gold for one business date.

    python scripts/run_hop.py --hop ingest --window 2026-11-09 --run-id tyre-2026-11-09 --out build/run --meta postgres
    python scripts/run_hop.py --hop silver --window 2026-11-09 --run-id silver-2026-11-09 --out build/run --meta postgres
    python scripts/run_hop.py --hop gold   --window 2026-11-09 --run-id gold-2026-11-09   --out build/run --meta postgres

This is the shape a deployed job has: it opens the metadata store, the sources and the tables that are already
there, runs the hop, and leaves its report at <out>/hops/<run id>.json (dataplatform.hop_reports). It creates and
empties nothing. The day loop (run_platform.py --hops process) starts one of these per hop per day and judges the day
from the saved reports; on Databricks the same entry point is a job task.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from platform_binding import attach_meta, build_engine  # noqa: E402
from run_world import DEFAULT_CATALOG, sinks  # noqa: E402

from dataplatform.hop_reports import KINDS, dump  # noqa: E402
from dataplatform.world import dwd as dwd_loader  # noqa: E402


def run_ingest(a: Any, store: Any, dwd: Any, tz: str, eng: Any) -> Any:
    from dataplatform.adapters.delta_bronze import DeltaBronze
    from dataplatform.adapters.files import FileLanding
    from dataplatform.adapters.http_api import HttpRecordClient, serve
    from dataplatform.adapters.keyed_tokens import KeyedTokenizer
    from dataplatform.ingest.pipelines import Plan, Sources
    from dataplatform.ingest.runner import ingest
    from dataplatform.resilience import SourceGuard

    if a.sources_dir is not None:  # a job on its own machine: the feeder's copy of the day's mock sources
        import shutil

        shutil.rmtree(a.out / "world" / "sources", ignore_errors=True)
        shutil.copytree(a.sources_dir, a.out / "world" / "sources")
    s = sinks(a.out / "world", a.db, attach=True)
    server = None
    if a.spark == "existing":  # Databricks serverless refuses the loopback HTTP call: the record store directly
        api: Any = s.api
    else:
        server = serve(s.api)  # type: ignore[arg-type]
        api = HttpRecordClient(f"http://127.0.0.1:{server.server_address[1]}")
    systems = {n.split(".")[0]: d.system for n, d in dwd.datasets.items()}
    sources = Sources(FileLanding(a.landing or a.out / "landing"), s.db, api, s.stream, systems, tz,
                      SourceGuard(time.monotonic, time.sleep))
    bronze = eng.bronze_writer if eng.bronze_writer is not None else DeltaBronze(a.out / "bronze")
    try:
        return ingest(store, bronze, KeyedTokenizer.from_env_or_file(ROOT / "build/.token_key"), sources,
                      dwd.platform["domain"], "dev", a.run_id, Plan("normal", a.window), silver=eng.tables)
    finally:
        if server is not None:
            server.shutdown()


def run_silver(a: Any, store: Any, dwd: Any, tz: str, eng: Any) -> Any:
    from dataplatform.adapters.keyed_tokens import KeyedTokenizer
    from dataplatform.silver.runner import build_silver

    return build_silver(store, eng.tables, eng.stages, dwd.platform["domain"], "dev", a.run_id, a.window,
                        KeyedTokenizer.PATTERN)


def run_gold(a: Any, store: Any, dwd: Any, tz: str, eng: Any) -> Any:
    from dataplatform.gold.harness import run_gold as gold_run

    return gold_run(store, eng.gold[0], eng.gold[1], dwd.platform["domain"], "dev", a.run_id, a.window, tz)


HOPS = {"ingest": run_ingest, "silver": run_silver, "gold": run_gold}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hop", choices=KINDS, required=True)
    ap.add_argument("--window", required=True, help="the business date of the run, as the day loop names it")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out", type=Path, default=ROOT / "build/run")
    ap.add_argument("--landing", type=Path, default=None, help="the landing area (default <out>/landing)")
    ap.add_argument("--hops-dir", type=Path, default=None, help="where the report is saved (default <out>/hops)")
    ap.add_argument("--sources-dir", type=Path, default=None,
                    help="ingest only: a copy of the mock sources to open (a job on its own machine)")
    ap.add_argument("--db", choices=["postgres", "sqlite"], default=os.environ.get("WORLD_DB", "postgres"))
    ap.add_argument("--meta", choices=["postgres", "lakebase"], default=os.environ.get("DP_RUN_META", "postgres"))
    ap.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    ap.add_argument("--calendar", default=None)
    ap.add_argument("--no-silver", dest="silver", action="store_false")
    ap.add_argument("--no-gold", dest="gold", action="store_false")
    ap.add_argument("--engine", choices=["spark", "duckdb"], default=os.environ.get("ENGINE", "spark"))
    ap.add_argument("--spark", choices=["local", "existing"], default="local")
    ap.add_argument("--env", default="dev")
    ap.add_argument("--table-format", choices=["legacy", "delta", "iceberg", "iceberg-uc"],
                    default=os.environ.get("DP_TABLE_FORMAT", "legacy"))
    a = ap.parse_args()
    if a.hop != "ingest" and not a.silver:
        ap.error(f"--hop {a.hop} needs silver")
    if a.hop == "gold" and not a.gold:
        ap.error("--hop gold needs gold")
    a.gold = a.gold and a.hop == "gold"  # only the gold hop builds the gold adapters

    dwd = dwd_loader.load(a.catalog, a.calendar)
    tz = dwd.world["clock"]["business_timezone"]
    store = attach_meta(a.meta)
    eng = build_engine(a, tz)
    try:
        report = HOPS[a.hop](a, store, dwd, tz, eng)
    finally:
        if eng.spark is not None and a.spark == "local":  # the job's own session (Databricks) is not ours to stop
            eng.spark.stop()
    path = (a.hops_dir or a.out / "hops") / f"{a.run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dump(report), indent=1, default=str))
    print(f"{a.hop} {a.run_id}: {report.status}; report at {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
