"""Integration: the tyre world's 14 days through ingestion, day by day (W1b). Small scale, local adapters only:
SQLite sources, local drops and stream, the in-memory metadata store loaded from the tyre manifest, landing and bronze
(Delta via delta-rs) in a temporary folder. About two minutes; marked slow.

Table-driven: bronze per day must equal the oracle's bronze totals, and each planted incident must get the verdict
ingestion owes it (handled, flagged, or deferred to the layer that owns its detector).

Rules proven here (DAC): REC-01 (counts and totals reconciled with the truth, layer by layer).
"""

from pathlib import Path

import pytest

from dataplatform.adapters.file_drop import LocalFileDrop
from dataplatform.adapters.files import FileLanding
from dataplatform.adapters.keyed_tokens import KeyedTokenizer
from dataplatform.adapters.local_movers import ChangeFeedMover, DropMover
from dataplatform.adapters.local_stream import LocalStream
from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_sources import MemoryRecords
from dataplatform.adapters.sqlite_source import SqliteSource
from dataplatform.ingest.daybyday import run_days
from dataplatform.ingest.pipelines import Plan, Sources
from dataplatform.ingest.runner import ingest
from dataplatform.meta import loader
from dataplatform.world import dwd as dwd_loader
from dataplatform.world.domains import model_for
from dataplatform.world.engine import Sinks, World

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
DAYS, SCALE = 14, 0.05
pytestmark = [pytest.mark.slow, pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog")]
deltalake = pytest.importorskip("deltalake")


@pytest.fixture(scope="module")
def platform(tmp_path_factory):
    from dataplatform.adapters.delta_bronze import DeltaBronze

    tmp = tmp_path_factory.mktemp("platform")
    dwd = dwd_loader.load(SLICE)
    store = MemoryMetaStore()
    loader.load(store, SLICE / "manifest")
    s = Sinks(
        SqliteSource(tmp / "src.sqlite"),
        LocalFileDrop(tmp / "drops"),
        MemoryRecords(),
        LocalStream(tmp / "stream"),
        store,
    )
    tz = dwd.world["clock"]["business_timezone"]
    landing, bronze = FileLanding(tmp / "landing"), DeltaBronze(tmp / "bronze")
    tokens = KeyedTokenizer(b"t" * 32)
    sources = Sources(landing, s.db, s.api, s.stream, {n.split(".")[0]: d.system for n, d in dwd.datasets.items()}, tz)
    feeds, drops = ChangeFeedMover(s.db, landing), DropMover(s.drop, landing, tz)

    def move(day, window):
        for name, ds in dwd.datasets.items():
            source, dataset = name.split(".", 1)
            if dwd.kind_of(ds.system) == "database" and ds.manifest["ingest"]["read"] == "change_feed":
                feeds.move(ds.system, source, ds.table, window)
            elif dwd.kind_of(ds.system) == "file_drop":
                drops.move(ds.system, name, source, dataset)

    days = run_days(
        World(dwd, model_for(dwd, SCALE), s, tmp / "world"),
        DAYS,
        store,
        bronze,
        tokens,
        sources,
        move,
        "tyre",
        prefix="it",
    )
    return {"days": days, "store": store, "bronze": bronze, "sources": sources, "tokens": tokens}


@pytest.mark.parametrize("day", range(1, DAYS + 1))
def test_bronze_equals_the_oracle(platform, day):
    d = platform["days"][day - 1]
    assert d.bronze_ok, {k: v for k, v in d.bronze.items() if v[0] != v[1]}


INCIDENTS = [
    ("readings-out-of-order", 3, "LATE-03", "flagged"),
    ("order-lines-deleted", 4, "DEL-01", "flagged"),
    ("sellout-file-late", 5, "LATE-01", "deferred"),
    ("sellout-file-resent", 6, "DUP-01", "handled"),
    ("lab-column-renamed", 7, "SCH-02", "handled"),
    ("promised-date-out-of-order", 8, "SCD-02", "flagged"),
    ("material-copy-short", 9, "DEL-04", "handled"),
    ("promised-date-corrected", 10, "HIST-05", "deferred"),
    ("new-dealer-late", 11, "LATE-02", "deferred"),
    ("change-feed-drops-rows", 12, "REC-05", "deferred"),
    ("stream-replayed", 13, "DUP-04", "flagged"),
]


@pytest.mark.parametrize(("incident", "day", "detector", "verdict"), INCIDENTS, ids=[i[0] for i in INCIDENTS])
def test_incident_verdict(platform, incident, day, detector, verdict):
    (v,) = [v for v in platform["days"][day - 1].verdicts if v.incident == incident]
    assert (v.detector, v.verdict) == (detector, verdict), v.evidence


@pytest.mark.parametrize(
    ("case", "check"),
    [
        (
            "the renamed column is named and P-CHN keeps loading",
            lambda days: (
                any("defect_code" in r for r in days[6].run.by_name()["lims.inspection_result"].reasons)
                and days[6].bronze["lims.inspection_result"][0] > 0
            ),
        ),
        ("the held copy blocks only its dataset", lambda days: days[8].run.status == "partial"),
        (
            "day 1 has no deliveries yet: skipped, not succeeded",
            lambda days: days[0].run.by_name()["erp.delivery"].status == "skipped",
        ),
        ("clean days succeed", lambda days: days[13].run.status == "succeeded"),
    ],
)
def test_run_statuses(platform, case, check):
    assert check(platform["days"])


def test_corrections_reach_the_store_not_a_file(platform):
    assert len(platform["store"].corrections("erp", "sales_order_line")) == 12


def test_dealer_contacts_are_tokenised(platform):
    keys = platform["bronze"].keys("dealer_api.dealer", ("contact_email",), {})
    assert keys and all(str(k[0]).startswith("tok_") for k in keys)


def test_reload_of_a_past_day_changes_nothing(platform):
    """HIST-04, DUP-02: reloading day 3 rewrites its batches by key; every bronze table keeps its fingerprint."""
    tables = sorted({f"{r.source}.{r.dataset}" for r in platform["days"][0].run.datasets})
    before = {t: platform["bronze"].fingerprint(t) for t in tables}
    rep = ingest(
        platform["store"],
        platform["bronze"],
        platform["tokens"],
        platform["sources"],
        "tyre",
        "dev",
        "it-reload-d3",
        Plan("reload", "2026-11-23", (platform["days"][2].date,)),
    )
    assert {t: platform["bronze"].fingerprint(t) for t in tables} == before
    assert sum(d.counts.get("loaded", 0) for d in rep.datasets) > 0
