"""DataTwin scenarios with the engine attached (G10, TEST-05): the `ingestion`, `silver` and `gold` checks run for real.

The engine plays the same seeded tyre world day by day at a small scale (local adapters: SQLite sources, the
in-memory metadata store, bronze by delta-rs; silver on the shared local Spark), and its verdicts, daily totals and
bookmarks are attached to the scenario harness (twin/engine.py). Gold runs after silver on the same Spark (W1d);
DES (W1e) checks stay pending. Ingestion needs no Spark; silver and gold do. All are slow.
"""

from pathlib import Path

import pytest

pytest.importorskip("deltalake")
from dataplatform.twin import engine
from dataplatform.twin.runner import Harness, run
from dataplatform.twin.scenario import load_all

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
DAYS, SCALE = 14, 0.02
pytestmark = [pytest.mark.slow, pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog")]
SCENARIOS = load_all()


def play(tmp, with_silver=None):
    """Ingestion (and silver, given a Spark session) over the world; returns the layer runs."""
    from dataplatform.adapters.delta_bronze import DeltaBronze
    from dataplatform.adapters.file_drop import LocalFileDrop
    from dataplatform.adapters.files import FileLanding
    from dataplatform.adapters.keyed_tokens import KeyedTokenizer
    from dataplatform.adapters.local_movers import ChangeFeedMover, DropMover
    from dataplatform.adapters.local_stream import LocalStream
    from dataplatform.adapters.memory_meta import MemoryMetaStore
    from dataplatform.adapters.memory_sources import MemoryRecords
    from dataplatform.adapters.sqlite_source import SqliteSource
    from dataplatform.ingest.daybyday import run_days
    from dataplatform.ingest.pipelines import Sources
    from dataplatform.meta import loader
    from dataplatform.world import dwd as dwd_loader
    from dataplatform.world.domains import model_for
    from dataplatform.world.engine import Sinks, World

    dwd = dwd_loader.load(SLICE)
    store = MemoryMetaStore()
    loader.load(store, SLICE / "manifest")
    s = Sinks(SqliteSource(tmp / "src.sqlite"), LocalFileDrop(tmp / "drops"), MemoryRecords(),
              LocalStream(tmp / "stream"), store)
    tz = dwd.world["clock"]["business_timezone"]
    landing = FileLanding(tmp / "landing")
    sources = Sources(landing, s.db, s.api, s.stream, {n.split(".")[0]: d.system for n, d in dwd.datasets.items()}, tz)
    feeds, drops = ChangeFeedMover(s.db, landing), DropMover(s.drop, landing, tz)
    names = sorted(dwd.datasets)

    def move(day, window):
        for name, ds in dwd.datasets.items():
            source, dataset = name.split(".", 1)
            if dwd.kind_of(ds.system) == "database" and ds.manifest["ingest"]["read"] == "change_feed":
                feeds.move(ds.system, source, ds.table, window)
            elif dwd.kind_of(ds.system) == "file_drop":
                drops.move(ds.system, name, source, dataset)

    marks, silver_days, gold_days, tables = {}, [], [], None
    if with_silver is not None:
        from dataplatform.adapters.keyed_tokens import KeyedTokenizer as K
        from dataplatform.adapters.spark_gold import DeltaGoldTables, SparkGoldEngine, SparkGoldReader
        from dataplatform.adapters.spark_silver import DeltaSilverTables, SparkStageRunner
        from dataplatform.gold.daybyday import gold_day
        from dataplatform.meta.loader import load_gold
        from dataplatform.silver.daybyday import silver_day
        from dataplatform.world.domains.tyre_gold import TyreGoldJudge

        tables, stages = DeltaSilverTables(with_silver, tmp / "bronze", tmp / "silver"), SparkStageRunner()
        load_gold(store, SLICE / "manifest")
        gold = (DeltaGoldTables(with_silver, tmp / "silver", tmp / "gold"),
                SparkGoldEngine(with_silver, Path(__file__).resolve().parents[2] / "models/tyre"),
                SparkGoldReader(with_silver, tmp / "gold"), TyreGoldJudge(tz))

    def on_day(day):
        marks[day.day] = {n: dict(store.bookmarks(*n.split(".", 1))) for n in names}
        if tables is not None:
            silver_days.append(silver_day(day.day, day.oracle, store, tables, stages, "tyre", "dev", K.PATTERN))
            gold_days.append(gold_day(day.day, day.oracle, store, *gold, "tyre", "dev", tz, list(gold_days)))

    days = run_days(World(dwd, model_for(dwd, SCALE), s, tmp / "world"), DAYS, store, DeltaBronze(tmp / "bronze"),
                    KeyedTokenizer(b"t" * 32), sources, move, "tyre", prefix="twin", on_day=on_day, silver=tables)
    runs = [engine.from_ingestion(days, marks)]
    if tables is not None:
        runs += [engine.from_silver(silver_days), engine.from_gold(gold_days)]
    return runs


@pytest.fixture(scope="module")
def harness():
    return Harness.build(SLICE, days=DAYS)


@pytest.fixture(scope="module")
def ingestion(harness, tmp_path_factory):
    (layer,) = play(tmp_path_factory.mktemp("twin-ingest"))
    harness.attach(layer)
    return harness


INGESTION = [s for s in SCENARIOS if "ingestion" in s.layers]


@pytest.mark.parametrize("scenario", INGESTION, ids=[s.id for s in INGESTION])
def test_ingestion_checks_run_on_the_engine(ingestion, scenario):
    result = run(scenario, ingestion)
    owed = {c.id for c in scenario.checks if "pending" in c.args}  # the layer owes what these need (said in the file)
    checks = {c.id: c for c in result.checks if c.layer == "ingestion" and c.id not in owed}
    assert {k: (c.status, c.detail) for k, c in checks.items() if c.status != "passed"} == {}
    assert result.status != "failed"


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    h = Harness.build(SLICE, days=DAYS)
    for layer in play(tmp_path_factory.mktemp("twin-silver"), spark):
        h.attach(layer)
    return h


ENGINE = [s for s in SCENARIOS if {"silver", "gold"} & s.layers]


@pytest.mark.parametrize("scenario", ENGINE, ids=[s.id for s in ENGINE])
def test_silver_and_gold_checks_run_on_the_engine(silver, scenario):
    result = run(scenario, silver)
    owed = {c.id for c in scenario.checks if "pending" in c.args}
    checks = {c.id: c for c in result.checks if c.layer in ("ingestion", "silver", "gold") and c.id not in owed}
    assert {k: (c.status, c.detail) for k, c in checks.items() if c.status != "passed"} == {}
    assert result.status != "failed"
    assert all(c.status == "pending" for c in result.checks if c.layer == "des")
