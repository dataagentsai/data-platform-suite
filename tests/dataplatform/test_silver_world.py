"""Integration: the tyre world's 14 days through ingestion and silver, day by day (W1c). Small scale, local adapters:
SQLite sources, the in-memory metadata store, bronze by delta-rs, silver on the shared local Spark. Several minutes;
marked slow.

Table-driven: silver per day must equal the oracle's silver totals, and each incident silver owns (DEL-01, SCD-02,
LATE-03, DUP-04) must be handled on what silver holds. The mass-delete guard compares with silver (DEL-04 held).
"""

from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("deltalake")
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

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
DAYS, SCALE = 14, 0.05
pytestmark = [pytest.mark.slow, pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog")]


@pytest.fixture(scope="module")
def platform(spark, tmp_path_factory):
    from dataplatform.adapters.delta_bronze import DeltaBronze
    from dataplatform.adapters.spark_silver import DeltaSilverTables, SparkStageRunner
    from dataplatform.silver.daybyday import silver_day

    tmp = tmp_path_factory.mktemp("silver-world")
    dwd = dwd_loader.load(SLICE)
    store = MemoryMetaStore()
    loader.load(store, SLICE / "manifest")
    s = Sinks(SqliteSource(tmp / "src.sqlite"), LocalFileDrop(tmp / "drops"), MemoryRecords(),
              LocalStream(tmp / "stream"), store)
    tz = dwd.world["clock"]["business_timezone"]
    landing = FileLanding(tmp / "landing")
    sources = Sources(landing, s.db, s.api, s.stream, {n.split(".")[0]: d.system for n, d in dwd.datasets.items()}, tz)
    feeds, drops = ChangeFeedMover(s.db, landing), DropMover(s.drop, landing, tz)
    tables, stages = DeltaSilverTables(spark, tmp / "bronze", tmp / "silver"), SparkStageRunner()
    silver = []

    def move(day, window):
        for name, ds in dwd.datasets.items():
            source, dataset = name.split(".", 1)
            if dwd.kind_of(ds.system) == "database" and ds.manifest["ingest"]["read"] == "change_feed":
                feeds.move(ds.system, source, ds.table, window)
            elif dwd.kind_of(ds.system) == "file_drop":
                drops.move(ds.system, name, source, dataset)

    def after(day):
        silver.append(silver_day(day.day, day.oracle, store, tables, stages, "tyre", "dev", KeyedTokenizer.PATTERN))

    days = run_days(World(dwd, model_for(dwd, SCALE), s, tmp / "world"), DAYS, store, DeltaBronze(tmp / "bronze"),
                    KeyedTokenizer(b"t" * 32), sources, move, "tyre", prefix="it", on_day=after, silver=tables)
    return {"days": days, "silver": silver}


@pytest.mark.parametrize("day", range(1, DAYS + 1))
def test_silver_equals_the_oracle(platform, day):
    s = platform["silver"][day - 1]
    assert s.report.status == "succeeded", [d.reason for d in s.report.datasets if d.status == "failed"]
    assert s.differences == {}


@pytest.mark.parametrize(
    ("detector", "verdict"),
    [("DEL-01", "handled"), ("SCD-02", "handled"), ("LATE-03", "handled"), ("DUP-04", "handled")],
)
def test_silver_handles_its_incidents(platform, detector, verdict):
    got = [v for s in platform["silver"] for v in s.verdicts if v.detector == detector]
    assert got and all(v.verdict == verdict for v in got), [v.evidence for v in got]


def test_the_mass_delete_guard_still_holds_the_short_copy(platform):
    got = [v for d in platform["days"] for v in d.verdicts if v.detector == "DEL-04"]
    assert got
    assert all(v.verdict == "handled" for v in got)
