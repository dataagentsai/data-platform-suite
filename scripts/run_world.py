"""Run the tyre world locally: N simulated days into the four mock sources, with the oracle's totals per day.

    python scripts/run_world.py --days 7                      # local Postgres (WORLD_PG_DSN, default dbname=tyre_world)
    python scripts/run_world.py --days 7 --db sqlite          # no Postgres needed
    python scripts/run_world.py --serve-api                   # serve the dealer interface of the last run
    python scripts/run_world.py --days 7 --db sqlite --binding aws --aws-prefix acme-dp --env dev
                                                              # file drop on S3, press readings on Kinesis
                                                              # (needs boto3 and AWS credentials; see
                                                              # realms/aws-databricks/README.md for cost)

This file is the binding for the local stack: the only place that picks adapters. A cloud binding swaps the database,
file drop and stream adapters here and nothing else.

Corrections the world's people record (day 10) go to a metadata store's corrections table (ADR-017): a PostgreSQL
store recreated each run (DP_WORLD_META_DSN, default dbname=tyre_world_meta) with --db postgres, in memory otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dataplatform.adapters.file_drop import LocalFileDrop  # noqa: E402
from dataplatform.adapters.http_api import HttpRecordClient, LocalRecordStore, serve  # noqa: E402
from dataplatform.adapters.local_stream import LocalStream  # noqa: E402
from dataplatform.world import dwd as dwd_loader  # noqa: E402
from dataplatform.world.domains import model_for  # noqa: E402
from dataplatform.world.engine import Sinks, World  # noqa: E402

DEFAULT_CATALOG = Path(os.environ.get("DP_CATALOG", Path.home() / "data-platform-catalog")) / "slices/02-tyre"


def aws_sinks(prefix: str, env: str) -> tuple[Any, Any]:
    """The aws binding: file drop and stream on the buckets and streams Terraform made (realms/aws-databricks/infra).
    The database and the dealer interface stay local until their cloud adapters exist."""
    import boto3

    from dataplatform.adapters.kinesis import KinesisStream
    from dataplatform.adapters.s3 import S3FileDrop, bucket_name

    drop = S3FileDrop(boto3.client("s3"), bucket_name(prefix, env, "landing"), prefix="drops/")
    return drop, KinesisStream(boto3.client("kinesis"), name_prefix=f"{prefix}-{env}-")


def sinks(out: Path, db: str, binding: str = "local", aws_prefix: str = "", env: str = "dev", *,
          attach: bool = False) -> Sinks:
    """The world's sources. `attach` opens the sources another process already holds (a hop running as its own
    process reads them); without it every source starts empty."""
    src = out / "sources"
    for sub in ("drops", "stream") if not attach else ():
        shutil.rmtree(src / sub, ignore_errors=True)
    src.mkdir(parents=True, exist_ok=True)
    store = LocalRecordStore(src / "dealer_api.sqlite")
    if not attach:
        store.reset()
    if db == "postgres":
        from dataplatform.adapters.postgres_source import PostgresSource

        database: Any = PostgresSource.connect()
    else:
        from dataplatform.adapters.sqlite_source import SqliteSource

        if not attach:
            (src / "source_db.sqlite").unlink(missing_ok=True)
        database = SqliteSource(src / "source_db.sqlite")
    if binding == "aws":
        drop, stream = aws_sinks(aws_prefix, env)
        return Sinks(db=database, drop=drop, api=store, stream=stream)
    return Sinks(db=database, drop=LocalFileDrop(src / "drops"), api=store, stream=LocalStream(src / "stream"))


def corrections_store(db: str, catalog: Path) -> Any:
    """A metadata store loaded with the slice's manifest, fresh each run, for the corrections the world records."""
    from dataplatform.meta import loader

    if db == "postgres":
        from dataplatform.adapters.postgres_meta import PostgresMetaStore, create_database, install

        dsn = os.environ.get("DP_WORLD_META_DSN", "dbname=tyre_world_meta")
        create_database(dsn)
        store: Any = PostgresMetaStore.connect(dsn)
        store.conn.execute("DROP SCHEMA IF EXISTS config, runtime CASCADE")
        store.conn.autocommit = False
        install(store.conn)
        store.conn.autocommit = True
    else:
        from dataplatform.adapters.memory_meta import MemoryMetaStore

        store = MemoryMetaStore()
    loader.load(store, catalog / "manifest")
    return store


def read_back(world: World, s: Sinks) -> dict[str, Any]:
    """Read every source through its port, the way an ingestion job would, and count what is there."""
    out: dict[str, Any] = {}
    for name, ds in world.dwd.datasets.items():
        kind = world.dwd.kind_of(ds.system)
        if kind == "database":
            t = ds.table
            out[name] = {"live_rows": len(s.db.rows(ds.system, t))}
            if t.change_feed:
                out[name]["feed_rows"] = len(s.db.feed(ds.system, t))
        elif kind == "file_drop":
            files = s.drop.list(ds.system, name)
            out[name] = {"files": len(files), "bytes": sum(f.size for f in files)}
        elif kind == "rest_api":
            server = serve(s.api)  # type: ignore[arg-type]
            client = HttpRecordClient(f"http://127.0.0.1:{server.server_address[1]}")
            cursor, total, pages = "", 0, 0
            while True:
                items, cursor = client.page(ds.system, ds.table_name, cursor, 200)
                if not items:
                    break
                total, pages = total + len(items), pages + 1
            server.shutdown()
            out[name] = {"records_over_http": total, "pages": pages}
        elif kind == "stream":
            n = 0
            for shard in s.stream.shards(ds.table_name):
                after = 0
                while batch := s.stream.read(ds.table_name, shard, after, 100_000):
                    n, after = n + len(batch), batch[-1].seq
            out[name] = {"records": n, "shards": len(s.stream.shards(ds.table_name))}
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG, help="the slice folder holding world.yaml")
    ap.add_argument("--out", type=Path, default=ROOT / "build/world")
    ap.add_argument("--db", choices=["postgres", "sqlite"], default=os.environ.get("WORLD_DB", "postgres"))
    ap.add_argument("--scale", type=float, default=1.0, help="1.0 = the DWD's volumes; smaller for a quick look")
    ap.add_argument("--serve-api", action="store_true", help="serve the dealer interface of the last run and wait")
    ap.add_argument("--calendar", default=None, help="a named calendar from world.yaml (e.g. compressed)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--binding", choices=["local", "aws"], default=os.environ.get("WORLD_BINDING", "local"),
                    help="aws: file drop on S3, press readings on Kinesis; database and dealer interface stay local")
    ap.add_argument("--aws-prefix", default=os.environ.get("DP_AWS_PREFIX", ""), help="Terraform name_prefix")
    ap.add_argument("--env", default=os.environ.get("DP_ENV", "dev"), choices=["dev", "prod"])
    a = ap.parse_args()
    if a.binding == "aws" and not a.aws_prefix:
        ap.error("--binding aws needs --aws-prefix (the Terraform name_prefix, e.g. acme-dp)")

    if a.serve_api:
        server = serve(LocalRecordStore(a.out / "sources/dealer_api.sqlite"), a.port)
        print(f"dealer interface on http://127.0.0.1:{a.port}/dealer_api/dealer?cursor=&limit=200  (Ctrl-C to stop)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            return 0

    dwd = dwd_loader.load(a.catalog, a.calendar)
    shutil.rmtree(a.out / "oracle", ignore_errors=True)
    s = sinks(a.out, a.db, a.binding, a.aws_prefix, a.env)
    s.corrections = corrections_store(a.db, a.catalog)
    world = World(dwd, model_for(dwd, a.scale), s, a.out)
    t0 = time.time()
    reports = world.run(a.days)
    took = time.time() - t0
    back = read_back(world, s)
    summary = {
        "days": a.days,
        "scale": a.scale,
        "db": a.db,
        "seconds": round(took, 1),
        "sources": back,
        "incidents": [f"day {r['day']}: {f['incident']} -> {f['detector']}" for r in reports for f in r["findings"]],
    }
    plans = s.corrections.backfill_plans("erp", "sales_order_line")
    summary["corrections"] = {p.plan_id: {"rows": len([c for c in s.corrections.corrections("erp", "sales_order_line")
                                                       if c.plan_id == p.plan_id]), "activate_on": p.activate_on,
                                          "windows": list(p.windows)} for p in plans}
    summary["known_answers"] = reports[-1]["known_answers"] if reports else {}
    (a.out / "summary.json").write_text(json.dumps(summary, indent=1))
    print(f"tyre world: {a.days} days at scale {a.scale} in {took:.1f}s  ->  {a.out}")
    print(f"{'day':>3}  {'date':10}  {'order lines':>11}  {'deliveries':>10}  {'readings':>9}  incidents")
    for r in reports:
        src = r["sources"]
        names = ", ".join(f"{f['incident']} ({f['detector']})" for f in r["findings"]) or "clean"
        print(
            f"{r['day']:>3}  {r['date']}  {src.get('erp.sales_order_line', 0):>11}  {src.get('erp.delivery', 0):>10}  "
            f"{src.get('press_stream.press_reading', 0):>9}  {names}"
        )
    for p, c in summary["corrections"].items():
        print(f"corrections in the metadata store: plan {p}, {c['rows']} rows, active from {c['activate_on']}")
    for k, v in summary["known_answers"].items():
        print(f"known answer {'match' if v['match'] else 'DIFF '}  {k}: spec {v['spec']}, world {v['world']}")
    print("read back through the ports:")
    for name, v in back.items():
        print(f"  {name:28} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
