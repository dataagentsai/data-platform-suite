"""Run the tyre world and the platform together, day by day, locally: world -> sources -> landing -> bronze -> silver.

    python scripts/run_platform.py --days 14                 # local PostgreSQL for the sources and the metadata store
    python scripts/run_platform.py --days 3 --scale 0.05     # small and quick
    python scripts/run_platform.py --days 7 --db sqlite --meta memory   # nothing installed but Python packages
    python scripts/run_platform.py --days 3 --no-silver      # ingestion only (no Spark)

Each day: the world writes the day into the mock sources (scripts/run_world.py's local binding), the push movers
land the change feeds and drop-zone files, one ingestion run takes everything new to bronze (Delta tables under
build/run/bronze/), and bronze rows per dataset are compared with the oracle's bronze totals for the day. Each
planted incident is judged by the detector the world names. The metadata store is the run's only config source:
the manifest is loaded into it first (PostgreSQL database DP_RUN_META_DSN, default dbname=dp_meta_run, recreated
each run; or in memory). Silver (W1c): after each day's ingestion, one silver run on a local Spark (local[2], 1 GB
driver) takes the new bronze batches through the thirteen stages into Delta tables under build/run/silver/, and
silver is compared with the oracle's silver totals for the day; DEL-01, SCD-02, LATE-03 and DUP-04 are judged on what
silver holds. The mass-delete guard compares each full copy with silver's current keys. Gold (W1d): after silver,
one gold run for the day's business date builds the dimensions and the facts (models/tyre/gold/) into Delta tables
under build/run/gold/, and gold is compared with the oracle's gold for the day; LATE-01, LATE-02, HIST-05, DEL-01 and
DUP-01 are judged on gold. After the last day the metric views' known answers are asked of gold
(build/run/gold_metrics.json, the DP_GOLD_METRICS file W5's tests read). Report: build/run/report.json.

This file is the local binding: the only place that picks adapters for a run.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from platform_binding import build_engine  # noqa: E402
from run_world import DEFAULT_CATALOG, sinks  # noqa: E402

from dataplatform.adapters.delta_bronze import DeltaBronze  # noqa: E402
from dataplatform.adapters.files import FileLanding  # noqa: E402
from dataplatform.adapters.http_api import HttpRecordClient, serve  # noqa: E402
from dataplatform.adapters.keyed_tokens import KeyedTokenizer  # noqa: E402
from dataplatform.adapters.local_movers import ChangeFeedMover, DmsMover, DropMover  # noqa: E402
from dataplatform.adapters.memory_meta import MemoryMetaStore  # noqa: E402
from dataplatform.ingest.daybyday import Day, run_days  # noqa: E402
from dataplatform.ingest.pipelines import Sources  # noqa: E402
from dataplatform.meta import loader  # noqa: E402
from dataplatform.world import dwd as dwd_loader  # noqa: E402
from dataplatform.world.domains import model_for  # noqa: E402
from dataplatform.world.dwd import Dwd  # noqa: E402
from dataplatform.world.engine import World  # noqa: E402

RUN_META_DSN = os.environ.get("DP_RUN_META_DSN", "dbname=dp_meta_run")


def meta_store(kind: str) -> Any:
    if kind == "memory":
        return MemoryMetaStore()
    if kind == "lakebase":  # on Databricks: a fresh proof database on the Lakebase instance, as the job's identity
        return lakebase_fresh_store()
    from dataplatform.adapters.postgres_meta import PostgresMetaStore, create_database, install

    create_database(RUN_META_DSN)
    store = PostgresMetaStore.connect(RUN_META_DSN)
    store.conn.execute("DROP SCHEMA IF EXISTS config, runtime CASCADE")  # a fresh run starts from an empty store
    store.conn.autocommit = False
    install(store.conn)
    store.conn.autocommit = True
    return store


def lakebase_fresh_store() -> Any:
    """DP_META_INSTANCE (and DP_META_DATABASE, default dp_meta_proof): create the database if missing, empty it, install
    the schema. The manifest's own store (dp_meta, loaded by meta_setup.py) is never touched by a proof run."""
    from dataplatform.adapters.lakebase import connect_lakebase
    from dataplatform.adapters.postgres_meta import _ident, install

    instance = os.environ["DP_META_INSTANCE"]
    database = os.environ.get("DP_META_DATABASE", "dp_meta_proof")
    admin = connect_lakebase(instance, "postgres")
    if not admin.conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone():
        admin.conn.execute(f"CREATE DATABASE {_ident(database)}")  # nosec B608 - identifier checked by _ident()
    admin.conn.close()
    store = connect_lakebase(instance, database)
    store.conn.execute("DROP SCHEMA IF EXISTS config, runtime CASCADE")  # a proof run starts from an empty store
    store.conn.autocommit = False
    install(store.conn)
    store.conn.autocommit = True
    return store


class WallClock:
    """Real time. A run whose hops are separate processes cannot share a simulated clock with them."""

    def set(self, instant: datetime) -> None:
        """Nothing to set: the hops stamp their own real times."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class SimClock:
    """The clock port for a local run: the simulated day's time, plus the real seconds spent since it was set, so
    the run log has true step durations inside a simulated day (ManualClock alone stood still: every duration 0)."""

    def __init__(self, start: datetime) -> None:
        self._base, self._t0 = start, time.monotonic()

    def set(self, instant: datetime) -> None:
        self._base, self._t0 = max(instant, self.now()), time.monotonic()  # never backwards

    def now(self) -> datetime:
        return self._base + timedelta(seconds=time.monotonic() - self._t0)


def write_ops_page(a: Any, days: list[Any], silver_days: list[Any], gold_days: list[Any], monitor: Any, clock: Any,
                   tables: Any, store: Any) -> Path:
    """The local ops page (B10): the ops tables the dashboards read, plus every layer against the oracle, the
    planted incidents' verdicts and the delay per hop, in one HTML file beside the run."""
    from dataplatform.obs.page import render

    if days:
        clock.set(clock.now() + timedelta(hours=20))
        monitor.wake(clock.now())
    ops = monitor.tables(clock.now())
    checks = [{"day": d.day, "layer": "bronze", "equals_oracle": d.bronze_ok} for d in days]
    checks += [{"day": s.day, "layer": "silver", "equals_oracle": s.ok} for s, _ in silver_days]
    checks += [{"day": g.day, "layer": "gold", "equals_oracle": g.ok} for g, _ in gold_days]
    incidents = [{"layer": layer, **v.__dict__} for layer, items in
                 (("bronze", [v for d in days for v in d.verdicts]),
                  ("silver", [v for s, _ in silver_days for v in s.verdicts]),
                  ("gold", [v for g, _ in gold_days for v in g.verdicts])) for v in items]
    hops = hop_delays(a, tables, store, monitor.domain)
    html = render(ops, "Tyre platform: local run", f"{len(days)} day(s), scale {a.scale}, "
                  f"engine {a.engine}, table format {a.table_format}, change capture {a.cdc_shape}. This local run "
                  "loads once per simulated day, so datasets promised hourly freshness show breaches between days: "
                  "that is the schedule, not a fault.", checks, incidents, hops)
    path = a.out / "ops.html"
    path.write_text(html)
    from dataplatform.obs.ops import COLUMNS
    from dataplatform.obs.ops_export import export

    (a.out / "ops_tables.json").write_text(json.dumps(export(ops, COLUMNS)))  # a realm publishes these (dashboards)
    return path


def hop_delays(a: Any, tables: Any, store: Any, domain: str) -> list[dict[str, Any]]:
    """Delay per hop per dataset from silver's stamps (Spark engine only)."""
    hops: list[dict[str, Any]] = []
    if tables is not None and hasattr(tables, "frame") and a.engine == "spark":
        from dataplatform.adapters.spark_silver import hop_delays_frame

        for ref in store.datasets(domain):
            df = tables.frame(ref.source, ref.dataset) if ref.kind == "dataset" else None
            if df is not None:
                hops += [{"dataset": f"{ref.source}.{ref.dataset}", **d.__dict__} for d in hop_delays_frame(df)]
    return hops


def success_scorecard(a: Any, days: list[Any], silver_days: list[Any], gold_days: list[Any], store: Any,
                      hops: list[dict[str, Any]], seconds: float, *, tables: Any = None,
                      domain: str = "") -> list[Any]:
    """B20: the success criteria this run can measure (obs/success.py), from its own evidence."""
    from dataplatform.obs.success import Evidence, scorecard

    e = Evidence(seconds_per_day=seconds / max(len(days), 1))
    e.layer_days["bronze"] = [d.bronze_ok for d in days]
    if silver_days:
        e.layer_days["silver"] = [x.ok for x, _ in silver_days]
    if gold_days:
        e.layer_days["gold"] = [g.ok for g, _ in gold_days]
    e.verdicts = [(v.incident, v.verdict) for d in days for v in d.verdicts]
    e.verdicts += [(v.incident, v.verdict) for x, _ in silver_days for v in x.verdicts]
    e.verdicts += [(v.incident, v.verdict) for g, _ in gold_days for v in g.verdicts]
    e.recon = [(ds, r.ok, bool(r.missing or r.extra)) for x, _ in silver_days for ds, r in x.recon.items()]
    for g, _ in gold_days:
        e.ledger += [st["status"] for st in store.run_report(g.report.run_id).get("steps", [])
                     if st["step"] == "gold.ledger"]
    for h in hops:
        e.hops[h["hop"]] = max(e.hops.get(h["hop"], 0.0), float(h["p95"]))
    if tables is not None:
        from dataplatform.silver.privacy_scan import scan

        e.privacy = scan(store, tables, domain, KeyedTokenizer.PATTERN)
    return scorecard(e)


def write_calendar(spark: Any, table_store: Any, dwd: Dwd) -> None:
    """The shared, generated date dimension (B15): five years either side of the world's start, fiscal year from April
    (the Indian financial year), written whole each run (idempotent)."""
    from pyspark.sql import types as T

    from dataplatform.gold.calendar import COLUMNS, TABLE, calendar_rows

    start = datetime.fromisoformat(str(dwd.world["clock"]["start"])).date()
    rows = calendar_rows(start.replace(year=start.year - 5, month=1, day=1),
                         start.replace(year=start.year + 5, month=12, day=31), fiscal_start_month=4)
    kinds = {"integer": T.IntegerType(), "date": T.DateType(), "string": T.StringType(), "boolean": T.BooleanType()}
    schema = T.StructType([T.StructField(c, kinds[k], False) for c, k in COLUMNS])
    df = spark.createDataFrame([tuple(r[c] for c, _ in COLUMNS) for r in rows], schema)
    table_store.overwrite("gold", TABLE, df)
    print(f"gold: {TABLE} written ({len(rows)} days)")


def upkeep_all(store: Any, keep_versions: int = 2) -> dict[str, int]:
    """Upkeep on every table of every layer: on Iceberg, compaction plus snapshot expiry (a 14-day run without it
    reached 1,483 snapshots and 4,191 data files and ran a 1 GB driver out of heap on day 14, 13 Nov)."""
    out = {"tables": 0, "rewritten_files": 0, "expired_files": 0}
    for layer in ("bronze", "silver", "gold"):
        for table in store.tables(layer):
            got = store.upkeep(layer, table, keep_versions)
            out["tables"] += 1
            out["rewritten_files"] += int(got.get("rewritten_files", 0))
            out["expired_files"] += int(got.get("expired_files", 0))
    return out


def change_feeds(dwd: Dwd) -> list[str]:
    """The datasets a change-capture mover delivers (database sources read as a change feed)."""
    return [n for n, ds in dwd.datasets.items()
            if dwd.kind_of(ds.system) == "database" and ds.manifest["ingest"]["read"] == "change_feed"]


def mover(dwd: Dwd, landing: FileLanding, s: Any, tz: str, cdc_shape: str = "native", clock: Any = None) -> Any:
    feeds = DmsMover(s.db, landing) if cdc_shape == "dms" else ChangeFeedMover(s.db, landing)
    drops = DropMover(s.drop, landing, tz)

    def move(day: int, window: str) -> None:
        if clock is not None:
            clock.set(datetime.fromisoformat(window[:10]).replace(tzinfo=UTC) + timedelta(hours=1))
        for name, ds in dwd.datasets.items():
            source, dataset = name.split(".", 1)
            kind, read = dwd.kind_of(ds.system), ds.manifest["ingest"]["read"]
            if kind == "database" and read == "change_feed":
                feeds.move(ds.system, source, ds.table, window)
            elif kind == "file_drop":
                drops.move(ds.system, name, source, dataset)

    return move


def show_silver(s: Any) -> None:
    diff = s.differences
    rows = sum(int(m.get("row_count", (0, 0))[0]) for m in s.measures.values())
    print(f"        silver run {s.report.status:9} {rows:>8} current rows  "
          f"{'= oracle' if not diff else 'DIFFERS ' + json.dumps(diff)}")
    for d in s.report.datasets:
        if d.status == "failed":
            print(f"        silver {d.name:28} FAILED {d.reason[:140]}")
    for v in s.verdicts:
        print(f"        incident {v.incident} -> {v.detector} (silver): {v.verdict.upper()}  ({v.evidence[:110]})")
    checked = len(getattr(s, "recon", {}))
    failing = {n: r for n, r in getattr(s, "recon", {}).items() if not r.ok}
    if checked:
        print(f"        live source vs silver: {checked - len(failing)} of {checked} database datasets agree"
              + "".join(f"; {n}: {r.message()[:120]}" for n, r in failing.items()))


def show_gold(g: Any) -> None:
    diff = g.differences
    print(f"        gold run   {g.report.status:9} "
          + "  ".join(f"{t.name}:{t.counts.get('windows_replaced', t.counts.get('members', 0))}"
                      f"{'/' + str(t.counts.get('windows_incomplete')) + ' incomplete' if 'windows_incomplete' in t.counts else ''}"
                      for t in g.report.tables)
          + f"  {'= oracle' if not diff else 'DIFFERS ' + json.dumps(diff)}")
    for t in g.report.tables:
        if t.status == "failed":
            print(f"        gold {t.name:28} FAILED {t.reason[:200]}")
        for r in t.restated:
            print(f"        gold {t.name} restated {r.window} ({r.detector}): {r.before} -> {r.after}")
    for v in g.verdicts:
        print(f"        incident {v.incident} -> {v.detector} (gold): {v.verdict.upper()}  ({v.evidence[:140]})")


def gold_metrics(a: Any, reader: Any, tz: str) -> dict[str, Any]:
    """Every certified known answer asked of gold the way the metric view defines it (W5's DP_GOLD_METRICS file)."""
    import yaml

    from dataplatform.semantic.answers import answers
    from dataplatform.semantic.metrics import known_answer_cases, metric_view, published

    m = a.catalog / "manifest"
    gold_docs = {p.stem: yaml.safe_load(p.read_text()) for p in sorted((m / "gold").glob("*.yaml"))}
    views = [v for p in sorted((m / "metrics").glob("*.yaml"))
             for v in published(metric_view(yaml.safe_load(p.read_text()), gold_docs))]
    got = answers(reader, views, known_answer_cases(views), tz)
    (a.out / "gold_metrics.json").write_text(json.dumps(got, indent=1))
    return got


def show(day: Day) -> None:
    bad = {ds: v for ds, v in day.bronze.items() if v[0] != v[1]}
    total = sum(g for g, _ in day.bronze.values())
    print(
        f"day {day.day:>2} {day.date}  run {day.run.status:9}  bronze {total:>8} rows  "
        f"{'= oracle' if not bad else 'DIFFERS ' + json.dumps(bad)}"
    )
    for d in day.run.datasets:
        if d.status != "succeeded" or d.noticed:
            print(
                f"        {d.source}.{d.dataset:20} {d.status:9} {json.dumps(d.noticed) if d.noticed else ''} "
                f"{'; '.join(r[:110] for r in d.reasons[:1])}"
            )
    for v in day.verdicts:
        print(f"        incident {v.incident} -> {v.detector}: {v.verdict.upper()}  ({v.evidence[:110]})")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=3)  # dev loop; 14-day proof on Databricks
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--db", choices=["postgres", "sqlite"], default=os.environ.get("WORLD_DB", "postgres"))
    ap.add_argument("--meta", choices=["postgres", "memory", "lakebase"], default=os.environ.get("DP_RUN_META", "postgres"))
    ap.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    ap.add_argument("--calendar", default=None, help="a named calendar from world.yaml, e.g. compressed: every "
                    "incident on days 2-3, for the 3-day loop (default: the world's own calendar)")
    ap.add_argument("--out", type=Path, default=ROOT / "build/run")
    ap.add_argument("--no-silver", dest="silver", action="store_false", help="ingestion only, no Spark")
    ap.add_argument("--no-gold", dest="gold", action="store_false", help="stop at silver")
    ap.add_argument("--engine", choices=["spark", "duckdb"], default=os.environ.get("ENGINE", "spark"),
                    help="silver and gold frames on local Spark (default) or on DuckDB through SQLFrame, Delta by "
                         "delta-rs (laptop and CI only; docs/DUCKDB-SPIKE.md)")
    ap.add_argument("--spark", choices=["local", "existing"], default="local",
                    help="local: start a small local Spark; existing: the session the job runs in (Databricks)")
    ap.add_argument("--env", default="dev", help="environment: catalogs <env>_<layer> for --table-format iceberg-uc")
    ap.add_argument("--upkeep-every", type=int, default=1, help="table upkeep every N days, and after the last day")
    ap.add_argument("--reset-tables", action="store_true",
                    help="iceberg-uc: drop every table in <env>_bronze/silver/gold first (a proof run's own catalogs)")
    ap.add_argument("--table-format", choices=["legacy", "delta", "iceberg", "iceberg-uc"],
                    default=os.environ.get("DP_TABLE_FORMAT", "legacy"),
                    help="spark engine only: legacy = W1's Delta folders (bronze by delta-rs); delta or iceberg = every "
                         "layer through the table port (ADR-027), bronze written by Spark")
    ap.add_argument("--cdc-shape", choices=["native", "dms"], default=os.environ.get("DP_CDC_SHAPE", "native"),
                    help="how change-capture deliveries look: native = JSON lines with an integer feed position; "
                         "dms = parquet shaped like a managed migration service (B4)")
    ap.add_argument("--keep-spark", dest="fresh_spark", action="store_false",
                    help="one Spark session for every day (default: a fresh one each day, for a small laptop)")
    ap.add_argument("--housekeeping-days", type=int, default=None,
                    help="run bronze housekeeping (B8) after each day with this correction window in days; off when "
                         "unset (the proof job uses 1 on the compressed calendar)")
    ap.add_argument("--landing", type=Path, default=None, help="the landing area (default <out>/landing); a volume "
                    "path when hops run as jobs, so the feeder and the ingestion job see the same deliveries")
    ap.add_argument("--hops-dir", type=Path, default=None, help="where hops save their reports (default <out>/hops)")
    ap.add_argument("--sources-dir", type=Path, default=None,
                    help="--hops job: where the day's mock sources are copied for the ingestion job to open")
    ap.add_argument("--reset-shared", action="store_true",
                    help="empty --landing, --hops-dir and --sources-dir first (a proof run's own folders only)")
    ap.add_argument("--platform-job-id", default=None, help="--hops job: the deployed platform job to trigger each day")
    ap.add_argument("--hops", choices=["inline", "process", "job"], default="inline",
                    help="inline: every hop in this process (default); process: ingestion, silver and gold each run "
                         "as their own process (scripts/run_hop.py), as deployed jobs do, and the day is judged from "
                         "the reports they save. Needs a metadata store other processes can open (not memory)")
    ap.add_argument("--prove-rebuild", action="store_true",
                    help="after the last day, rebuild silver from bronze for every dataset that got a baseline and "
                         "check its current rows did not change (B8: a trimmed bronze still rebuilds silver)")
    a = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):  # a long run shows each day as it ends (not on a Databricks job stream)
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

    if a.hops != "inline" and a.meta == "memory":
        ap.error(f"--hops {a.hops} needs --meta postgres or lakebase: the in-memory store lives in one process")
    if a.hops == "job" and not (a.platform_job_id and a.landing and a.hops_dir and a.sources_dir):
        ap.error("--hops job needs --platform-job-id, --landing, --hops-dir and --sources-dir (paths the job can read)")
    shutil.rmtree(a.out, ignore_errors=True)
    a.landing = a.landing or a.out / "landing"
    a.hops_dir = a.hops_dir or a.out / "hops"
    for shared in (a.landing, a.hops_dir, a.sources_dir):  # folders outside <out>: a proof run starts them empty
        if shared is None or a.out in shared.parents:
            continue
        if a.reset_shared:
            shutil.rmtree(shared, ignore_errors=True)
        shared.mkdir(parents=True, exist_ok=True)
        if any(shared.iterdir()):
            ap.error(f"{shared} is not empty; a proof run starts from empty shared folders (--reset-shared empties "
                     "them: never point it at a real landing area)")
    dwd = dwd_loader.load(a.catalog, a.calendar)
    from dataplatform.obs.trace import LocalTracer
    from dataplatform.obs.traced_store import TracedStore

    from dataplatform.adapters.memory_runlog import StampingRunLog

    tracer = LocalTracer()
    # Hops in their own processes write the run log with the store's own times, so the monitor reads real time there.
    clock: Any = WallClock() if a.hops != "inline" else SimClock(datetime(2026, 1, 1, tzinfo=UTC))
    raw_store = meta_store(a.meta)
    stamped = StampingRunLog(raw_store, clock)  # the run log with times, for the ops tables (B10)
    run_log: Any = stamped
    if a.hops != "inline":
        from dataplatform.adapters import postgres_runlog

        postgres_runlog.install(raw_store.conn)
        run_log = postgres_runlog.PostgresRunLogReader(raw_store.conn)
    monitor_box: list[Any] = []  # the monitor exists once the manifest is loaded; runs push to it as they end

    def pushed(run_id: str) -> None:
        if monitor_box:
            monitor_box[0].run_finished(run_id, clock.now())

    store = TracedStore(stamped, tracer, on_finish=(pushed,))  # every run traced and pushed (B10, ADR-024)
    extra: dict[str, dict[str, str]] = {}
    if a.cdc_shape == "dms":
        from dataplatform.adapters.local_movers import DMS_SETTINGS

        extra = {n: dict(DMS_SETTINGS) for n in change_feeds(dwd)}
    report = loader.load(store, a.catalog / "manifest", extra_settings=extra)
    from dataplatform.adapters.memory_system import MemorySystemTables
    from dataplatform.obs.alerts import load_config
    from dataplatform.obs.monitor import MemoryNotifier, Monitor

    rules = Path(__file__).resolve().parents[1] / "src/dataplatform/obs/rules/alerts.yaml"
    since = datetime(2000, 1, 1, tzinfo=UTC)
    monitor_box.append(Monitor(run_log, MemorySystemTables(), store, dwd.platform["domain"],
                               load_config(rules, since), MemoryNotifier(), since=since))
    gold_tables = loader.load_gold(store, a.catalog / "manifest")
    print(f"metadata store: {len(report.loaded)} datasets and {len(gold_tables)} gold tables loaded, "
          f"{len(report.warnings)} warning(s)")
    s = sinks(a.out / "world", a.db)
    s.corrections = store  # the sales team records corrections in the store's corrections table (ADR-017)
    tz = dwd.world["clock"]["business_timezone"]
    landing = FileLanding(a.landing)
    server = None
    if a.spark == "existing":  # Databricks serverless refused the loopback HTTP call (13 Nov): the record store
        api = s.api  # through the same port directly; the port contract tests prove the wire and the store agree
    else:
        server = serve(s.api)  # type: ignore[arg-type]
        api = HttpRecordClient(f"http://127.0.0.1:{server.server_address[1]}")
    from dataplatform.resilience import SourceGuard

    guard = SourceGuard(time.monotonic, time.sleep)  # retry, circuit breaker, connection cap for pulls (B7)
    sources = Sources(landing, s.db, api, s.stream, {n.split(".")[0]: d.system for n, d in dwd.datasets.items()}, tz,
                      guard)
    world = World(dwd, model_for(dwd, a.scale), s, a.out / "world")
    silver_days: list[Any] = []
    gold_days: list[Any] = []
    swept_days: list[dict[str, Any]] = []

    def reset_tables(ts: Any) -> None:
        if a.table_format == "iceberg-uc" and a.reset_tables:  # a proof run starts from empty tables, as a local
            dropped = 0  # run starts from an empty folder; only this domain's run owns these catalogs' schemas
            for layer in ("bronze", "silver", "gold"):
                for name in ts.tables(layer):
                    ts.drop(layer, name)
                    dropped += 1
            print(f"reset: {dropped} tables dropped in {a.env}_bronze, {a.env}_silver, {a.env}_gold")

    eng = build_engine(a, tz, on_store=reset_tables)
    spark, tables, stages, gold = eng.spark, eng.tables, eng.stages, eng.gold
    table_store, bronze_writer, local_session = eng.table_store, eng.bronze_writer, eng.local_session
    if table_store is not None and gold is not None:
        write_calendar(spark, table_store, dwd)

    def fresh_session() -> None:
        """A new session for the next day: every table is on disk, so nothing is lost, and a 1 GB driver never
        carries fourteen days of plans, caches and Delta log snapshots (Spark ran out of heap on day 12); on DuckDB a
        new in-memory connection drops the day's tables."""
        nonlocal spark
        if local_session is None:
            return
        spark.stop()
        spark = local_session()
        holders = [tables, stages, *(gold or ())[:3], table_store]
        for h in holders:
            if hasattr(h, "spark"):
                h.spark = spark

    def live_rows(name: str) -> list[dict[str, Any]] | None:
        """The live table of a database source now (REC-05: live source against silver); None for other sources."""
        ds = dwd.datasets.get(name)
        if ds is None or dwd.kind_of(ds.system) != "database":
            return None
        return list(s.db.rows(ds.system, ds.table))

    bronze = bronze_writer if bronze_writer is not None else DeltaBronze(a.out / "bronze")

    def housekeeping(day: Day) -> None:
        """Bronze housekeeping (B8) with the run's correction window: old batches go once a checked baseline holds."""
        from dataplatform.housekeeping import run_housekeeping
        from dataplatform.silver.baseline_writer import SilverBaseliner

        t = time.time()
        baseliner = SilverBaseliner(tables, stages, bronze, KeyedTokenizer.PATTERN) if tables is not None else None
        swept = run_housekeeping(store, bronze, dwd.platform["domain"], env="dev", run_id=f"housekeeping-{day.day:02d}",
                                 today=date.fromisoformat(day.date[:10]), correction_days=a.housekeeping_days,
                                 baseliner=baseliner)
        gone = [x for x in swept if x.plan.remove]
        waiting = [x.dataset for x in swept if x.plan.needs_baseline]
        print(f"        housekeeping  {sum(len(x.plan.remove) for x in gone)} batches, "
              f"{sum(x.rows_removed for x in gone)} rows removed from {len(gone)} dataset(s); "
              f"{len(waiting)} wait for a baseline ({time.time() - t:.0f}s)")
        for x in swept:
            if x.baseline is not None:
                print(f"          baseline {x.dataset:<28} {x.baseline.status:10} {x.baseline.message[:100]}")
        swept_days.append({"day": day.day, "date": day.date, "datasets": [
            {"dataset": x.dataset, "removed": list(x.plan.remove), "rows_removed": x.rows_removed,
             "keep_from": x.plan.keep_from, "needs_baseline": x.plan.needs_baseline, "reason": x.plan.reason,
             "baseline": x.baseline.__dict__ if x.baseline else None}
            for x in swept]})

    def prove_rebuild_after(days: list[Any]) -> dict[str, Any]:
        from dataplatform.silver.baseline_writer import prove_rebuild

        names = {x["dataset"] for d in swept_days for x in d["datasets"]
                 if x.get("baseline") and x["baseline"]["status"] == "written"}
        if not names:
            print("rebuild proof: no dataset got a baseline")
            return {}
        diff = prove_rebuild(store, tables, stages, dwd.platform["domain"], names, run_id="rebuild-proof",
                             as_of=days[-1].date[:10], token_pattern=KeyedTokenizer.PATTERN)
        for n in sorted(names):
            print(f"rebuild proof: {n:<28} {'same current rows and measures' if not diff[n] else diff[n]}")
        return diff

    job_days: set[str] = set()

    def run_platform_job(window: str) -> None:
        """--hops job: the day's mock sources copied where the job can open them, then the deployed platform job
        (ingestion, silver, gold as three tasks) triggered for the window and waited for."""
        from datetime import timedelta as wait

        from databricks.sdk import WorkspaceClient  # only on Databricks or with the SDK installed

        shutil.rmtree(a.sources_dir, ignore_errors=True)
        shutil.copytree(a.out / "world" / "sources", a.sources_dir)
        t = time.time()
        WorkspaceClient().jobs.run_now(job_id=int(a.platform_job_id), job_parameters={"window": window}).result(
            timeout=wait(minutes=90))
        print(f"        platform job for {window}: finished ({time.time() - t:.0f}s)")

    def run_hop(hop: str, run_id: str, window: str) -> Any:
        """One hop outside this process, then its saved report read back and the monitor told. process: the hop as
        its own process (scripts/run_hop.py). job: the deployed platform job runs all three hops once per day; the
        first hop asked for triggers it."""
        from dataplatform.hop_reports import load

        if a.hops == "job":
            if window not in job_days:
                job_days.add(window)
                run_platform_job(window)
            report = load(hop, json.loads((a.hops_dir / f"{run_id}.json").read_text()))
            pushed(run_id)
            return report
        import subprocess  # nosec B404 - starts this repo's own script with a fixed argument list

        cmd = [sys.executable, str(ROOT / "scripts/run_hop.py"), "--hop", hop, "--run-id", run_id, "--window", window,
               "--out", str(a.out), "--db", a.db, "--meta", a.meta, "--catalog", str(a.catalog), "--engine", a.engine,
               "--table-format", a.table_format, "--spark", a.spark, "--env", a.env,
               "--landing", str(a.landing), "--hops-dir", str(a.hops_dir),
               *(["--calendar", a.calendar] if a.calendar else []), *([] if a.silver else ["--no-silver"]),
               *([] if a.gold else ["--no-gold"])]
        subprocess.run(cmd, check=True)  # nosec B603 - no shell; arguments are this run's own options
        report = load(hop, json.loads((a.hops_dir / f"{run_id}.json").read_text()))
        pushed(run_id)
        return report

    def on_day(day: Day) -> None:
        show(day)
        clock.set(datetime.fromisoformat(day.date).replace(tzinfo=UTC) + timedelta(hours=2))
        if tables is not None:
            t = time.time()
            from dataplatform.silver.daybyday import judge_silver, silver_day

            if a.hops != "inline":
                s = judge_silver(day.day, day.oracle, run_hop("silver", f"silver-{day.date}", day.date), store,
                                 tables, dwd.platform["domain"], KeyedTokenizer.PATTERN, live_rows)
            else:
                s = silver_day(day.day, day.oracle, store, tables, stages, dwd.platform["domain"], "dev",
                               KeyedTokenizer.PATTERN, live=live_rows)
            silver_days.append((s, round(time.time() - t, 1)))
            show_silver(s)
        if gold is not None:
            from dataplatform.gold.daybyday import gold_day, judge_gold

            clock.set(datetime.fromisoformat(day.date).replace(tzinfo=UTC) + timedelta(hours=3))
            t = time.time()
            if a.hops != "inline":
                g = judge_gold(day.day, day.oracle, run_hop("gold", f"gold-{day.date}", day.date), gold[2], gold[3],
                               [x for x, _ in gold_days])
            else:
                g = gold_day(day.day, day.oracle, store, gold[0], gold[1], gold[2], gold[3], dwd.platform["domain"],
                             "dev", tz, [x for x, _ in gold_days])
            gold_days.append((g, round(time.time() - t, 1)))
            show_gold(g)
        if a.housekeeping_days is not None:
            housekeeping(day)
        if table_store is not None and (day.day % a.upkeep_every == 0 or day.day == a.days):  # the port's upkeep
            # (ADR-027): compaction and version expiry
            t = time.time()
            done = upkeep_all(table_store)
            print(f"        upkeep    {done['tables']} tables, {done['rewritten_files']} files compacted, "
                  f"{done['expired_files']} old files expired ({time.time() - t:.0f}s)")
        if spark is not None and a.fresh_spark and a.spark == "local" and day.day % 5 == 0:  # a fresh session every 5th day only: the
            fresh_session()  # restart guards long runs on a small driver; a 3-day dev loop never pays for it

    t0 = time.time()
    try:
        days = run_days(
            world,
            a.days,
            store,
            bronze,
            KeyedTokenizer.from_env_or_file(ROOT / "build/.token_key"),
            sources,
            mover(dwd, landing, s, tz, a.cdc_shape, clock),
            dwd.platform["domain"],
            "dev",
            "tyre",
            on_day,
            silver=tables,
            run_ingest=(lambda run_id, window: run_hop("ingest", run_id, window)) if a.hops != "inline" else None,
        )
        rebuilt = prove_rebuild_after(days) if a.prove_rebuild and tables is not None else None
        metrics = gold_metrics(a, gold[2], tz) if gold is not None else {}
        page = write_ops_page(a, days, silver_days, gold_days, monitor_box[0], clock, tables, store)  # Spark still up
        scores = success_scorecard(a, days, silver_days, gold_days, store,
                                   hop_delays(a, tables, store, dwd.platform["domain"]), time.time() - t0,
                                   tables=tables, domain=dwd.platform["domain"])
    finally:
        if server is not None:
            server.shutdown()
        if spark is not None and a.spark == "local":  # the job's own session (Databricks) is not ours to stop
            spark.stop()
    last = json.loads((a.out / "world/oracle" / f"day-{a.days:02d}.json").read_text())
    out = {
        "days": a.days,
        "scale": a.scale,
        "engine": a.engine if a.silver else None,
        "seconds": round(time.time() - t0, 1),
        "bronze_equals_oracle": all(d.bronze_ok for d in days),
        "per_day": [
            {
                "day": d.day,
                "date": d.date,
                "run": d.run.status,
                "bronze": {k: {"bronze": g, "oracle": w} for k, (g, w) in d.bronze.items()},
                "datasets": [
                    {
                        "dataset": f"{x.source}.{x.dataset}",
                        "status": x.status,
                        "counts": x.counts,
                        "noticed": x.noticed,
                        "reasons": x.reasons,
                    }
                    for x in d.run.datasets
                ],
                "incidents": [v.__dict__ for v in d.verdicts],
            }
            for d in days
        ],
        "known_answers": last["known_answers"],
        "corrections": len(store.corrections("erp", "sales_order_line")),
        "silver_equals_oracle": all(s.ok for s, _ in silver_days) if a.silver else None,
        "gold_equals_oracle": all(g.ok for g, _ in gold_days) if gold_days else None,
        "gold": [
            {
                "day": g.day,
                "date": g.date,
                "run": g.report.status,
                "seconds": secs,
                "measures": {t: {k: {"gold": x, "oracle": w} for k, (x, w) in m.items()} for t, m in g.measures.items()},
                "tables": {t.name: {"status": t.status, "counts": t.counts, "replaced": t.replaced,
                                    "incomplete": t.incomplete[:40], "reason": t.reason,
                                    "restated": [r.__dict__ for r in t.restated], "plans_applied": t.plans_applied}
                           for t in g.report.tables},
                "differing": {k: v for k, v in g.notes.items() if k.endswith(("differing", "explained_by_late_dimension"))},
                "incidents": [v.__dict__ for v in g.verdicts],
            }
            for g, secs in gold_days
        ],
        "gold_known_answers": metrics,
        "housekeeping": swept_days,
        "rebuild_proof": rebuilt,
        "success": [x.__dict__ for x in scores],
        "silver": [
            {
                "day": s.day,
                "date": s.date,
                "run": s.report.status,
                "seconds": secs,
                "measures": {ds: {k: {"silver": g, "oracle": w} for k, (g, w) in m.items()} for ds, m in s.measures.items()},
                "datasets": [d.__dict__ for d in s.report.datasets],
                "incidents": [v.__dict__ for v in s.verdicts],
            }
            for s, secs in silver_days
        ],
    }
    (a.out / "report.json").write_text(json.dumps(out, indent=1, default=str))
    print(f"\n{a.days} days in {out['seconds']}s; bronze equals the oracle every day: {out['bronze_equals_oracle']}")
    print(f"known answers (day {a.days}):")
    for k, v in last["known_answers"].items():
        print(f"  {'match' if v['match'] else 'DIFF '}  {k}: spec {v['spec']}, world {v['world']}")
    if a.silver:
        print(f"silver equals the oracle every day: {out['silver_equals_oracle']}; silver time "
              f"{sum(x for _, x in silver_days):.0f}s of {out['seconds']}s")
    if gold_days:
        print(f"gold equals the oracle every day: {out['gold_equals_oracle']}; gold time "
              f"{sum(x for _, x in gold_days):.0f}s")
        print(f"known answers asked of gold (scale {a.scale}; the spec's values hold at scale 1.0 after 14 days):")
        for k, v in metrics.items():
            print(f"  {k}: {v}")
        print(f"  DP_GOLD_METRICS={a.out / 'gold_metrics.json'} DP_GOLD_ORACLE={a.out / 'world/oracle'}"
              f"/day-{a.days:02d}.json  (make gold-known-answers)")
    print(f"corrections in the store: {out['corrections']}; report: {a.out / 'report.json'}")
    runs = sum(1 for sp in tracer.finished if sp.name.startswith("run."))
    print(f"traced: {runs} runs, {len(tracer.finished)} spans (one trace per run, dp_run_id on every span)")
    print(f"ops page: {page}")
    from dataplatform.obs import success
    from dataplatform.obs.ops_export import export

    tally = {st: sum(1 for x in scores if x.state == st) for st in success.STATES}
    print(f"success criteria (B20): {tally['met']} met, {tally['not_met']} not met, "
          f"{tally['not_measured']} measured outside this run")
    for x in scores:
        if x.state != "not_measured":
            print(f"  {x.id:<4} {x.state:<8} {x.metric}: {x.value} (target {x.target})")
    doc_path = a.out / "ops_tables.json"
    if doc_path.exists():  # the scorecard travels with the ops tables, so a realm publishes it beside them
        doc = json.loads(doc_path.read_text())
        doc.update(export({"ops_success": success.rows(a.out.name, scores)},
                          {"ops_success": success.COLUMNS}))
        doc_path.write_text(json.dumps(doc))
    ok = out["bronze_equals_oracle"] and out["silver_equals_oracle"] is not False and not any((rebuilt or {}).values())
    return 0 if ok and out["gold_equals_oracle"] is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
