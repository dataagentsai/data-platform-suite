"""The tyre world (G4): 14 simulated days into the mock sources, checked against the DWD's calendar and the oracle.

One run at a small scale is shared by the module. Table-driven: each incident row names its day, its detector and a
check on what the sources hold, so a broken incident fails by name.
"""

import csv
import io
import json
from datetime import datetime
from pathlib import Path

import pytest

from dataplatform.adapters.file_drop import LocalFileDrop
from dataplatform.adapters.local_stream import LocalStream
from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_sources import MemoryRecords, MemorySourceDb
from dataplatform.adapters.sqlite_source import SqliteSource
from dataplatform.meta import loader
from dataplatform.world import dwd as dwd_loader
from dataplatform.world.clock import iso
from dataplatform.world.domains import model_for
from dataplatform.world.domains.tyre import DLV, INSP, MAT, PROD, READ, SELL, SOL
from dataplatform.world.engine import Emission, Sinks, World, WorldError

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
SCALE, DAYS = 0.05, 14
pytestmark = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre DWD)")


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("world")
    dwd = dwd_loader.load(SLICE)
    model = model_for(dwd, SCALE)
    store = MemoryMetaStore()  # where the sales team records corrections (ADR-017)
    loader.load(store, SLICE / "manifest")
    sinks = Sinks(
        SqliteSource(tmp / "db.sqlite"),
        LocalFileDrop(tmp / "drops"),
        MemoryRecords(),
        LocalStream(tmp / "stream"),
        store,
    )
    world = World(dwd, model, sinks, tmp / "out")
    reports = {r["day"]: r for r in world.run(DAYS)}
    return {"dwd": dwd, "model": model, "s": sinks, "reports": reports, "out": tmp / "out", "store": store}


def table(run, ds):
    d = run["dwd"].datasets[ds]
    return d.system, d.table


def files(run, ds):
    system = run["dwd"].datasets[ds].system
    return system, run["s"].drop.list(system, ds)


def header(run, ds, name):
    d = run["dwd"].datasets[ds]
    delimiter = d.manifest["config"]["csv_delimiter"]  # the world writes the delimiter the manifest declares
    return next(csv.reader(io.StringIO(run["s"].drop.get(d.system, ds, name).decode()), delimiter=delimiter))


def day_of(run, ts):
    return run["model"].clock.day_of(datetime.fromisoformat(ts))


def REPLAYED(run):  # noqa: N802 - reads like the set it is
    """Reading ids replayed on day 13 (they arrive behind newer ones by design, and are DUP-04's, not LATE-03's)."""
    if "replayed" not in run:
        m, day = run["model"], 13
        restart = m.clock.at(day, m.incident["replay"]["restart_hour"]).timestamp()
        lo = restart - 60 * m.incident["replay"]["minutes"]
        run["replayed"] = {f"{p}-{t}" for p in m.presses for t in range(int(lo), int(restart))}
    return run["replayed"]


# ---------------------------------------------------------------- each incident, by name
def check_out_of_order_readings(run, f):
    assert 0 < f["detail"]["late_readings"] and 10 <= f["detail"]["max_delay_s"] <= 90
    s, behind = run["s"].stream, set()
    for shard in s.shards("press_reading"):
        newest = 0.0
        for r in s.read("press_reading", shard, 0, 10**7):
            ts = datetime.fromisoformat(r.data["event_ts"]).timestamp()
            if ts < newest - 5 and r.data["reading_id"] not in REPLAYED(run):  # normal jitter is under 2 s
                behind.add(day_of(run, r.arrived_at))
            newest = max(newest, ts)
    assert behind == {3}, "readings arrive behind newer ones on day 3 and on no other day"


def check_deletes(run, f):
    keys = {tuple(k) for k in f["detail"]["keys"]}
    system, t = table(run, SOL)
    assert len(keys) == 5
    assert not keys & {(r["order_id"], r["line_no"]) for r in run["s"].db.rows(system, t)}
    deletes = {(r["order_id"], r["line_no"]) for r in run["s"].db.feed(system, t) if r["op"] == "D"}
    assert deletes == keys
    assert run["reports"][4]["silver"][SOL]["deleted_marked"] == 5


def check_late_file(run, f):
    _, fs = files(run, SELL)
    mine = [x for x in fs if x.name == "sellout_D-0042_2026-10.csv"]
    assert len(mine) == 1 and day_of(run, mine[0].arrived_at) == 5
    comp = {d: run["reports"][d]["gold"]["fact_sales"]["sellout_completeness"]["2026-10"]["complete"] for d in (4, 5)}
    assert comp == {4: False, 5: True}
    assert run["reports"][5]["gold"]["fact_sales"]["restatements_today"] == ["2026-10"]


def check_resend(run, f):
    system, fs = files(run, SELL)
    by = {x.name: x for x in fs}
    a, b = by["sellout_D-0042_2026-10.csv"], by[f["detail"]["file"]]
    assert a.checksum == b.checksum and a.name != b.name
    r5, r6 = run["reports"][5], run["reports"][6]
    assert r6["bronze"][SELL] == 0 < r6["sources"][SELL]
    assert r6["silver"][SELL] == r5["silver"][SELL]
    assert r6["gold"]["fact_sales"]["restatements_today"] == []


def check_rename(run, f):
    _, fs = files(run, INSP)
    hun7 = [x for x in fs if "P-HUN" in x.name and day_of(run, x.arrived_at) == 7]
    renamed = [x for x in hun7 if "defect" in header(run, INSP, x.name)]
    assert renamed and all("defect_code" not in header(run, INSP, x.name) for x in renamed)
    assert all(int(x.name.split("T")[1][:2]) >= 10 for x in renamed)  # from 10:00 local
    assert all("defect_code" in header(run, INSP, x.name) for x in fs if "P-CHN" in x.name)
    assert all("defect_code" in header(run, INSP, x.name) for x in fs if day_of(run, x.arrived_at) == 8)
    held = run["reports"][7]["silver"][INSP]["held_back_rows"]
    assert held > 0 and run["reports"][7]["sources"][INSP] - run["reports"][7]["bronze"][INSP] == held


def check_late_change(run, f):
    system, t = table(run, SOL)
    key = tuple(f["detail"]["key"])
    feed = [r for r in run["s"].db.feed(system, t) if (r["order_id"], r["line_no"]) == key]
    assert [r["change_seq"] for r in feed][-2:] == [f["detail"]["newer_seq"], f["detail"]["older_seq"]]
    live = next(r for r in run["s"].db.rows(system, t) if (r["order_id"], r["line_no"]) == key)
    assert live["change_seq"] == f["detail"]["newer_seq"]
    assert live["promised_date"] == f["detail"]["current_promised_date"]


def check_short_copy(run, f):
    system, t = table(run, MAT)
    at = iso(run["model"].clock.at(9, 1))
    assert len(run["s"].db.copy(system, t, at)) == f["detail"]["rows_delivered"] == 1080
    r9 = run["reports"][9]
    assert r9["bronze"][MAT] == 0 and r9["silver"][MAT]["row_count"] == 1800


def check_correction(run, f):
    store = run["store"]  # the 12 rows are in the metadata store's corrections table, not in a file
    assert not (run["out"] / "world/corrections.json").exists()
    (plan,) = store.backfill_plans("erp", "sales_order_line")
    assert plan.plan_id == f["detail"]["plan_id"] and list(plan.windows) == f["detail"]["windows"]
    assert plan.activate_on == str(run["model"].clock.date(11))
    corrections = store.corrections("erp", "sales_order_line", as_of=str(run["model"].clock.date(10)))
    assert len(corrections) == 12 and {c.status for c in corrections} == {"recorded"}
    assert {c.status for c in store.corrections("erp", "sales_order_line", plan.activate_on)} == {"active"}
    m, c0 = run["model"], corrections[0]
    line = m.lines[(c0.key["order_id"], c0.key["line_no"])]
    assert str(m.promise(line, 10)) == c0.was and str(m.promise(line, 11)) == c0.value
    system, t = table(run, SOL)  # a correction is not a source change: silver stays as it was
    keys = {(c.key["order_id"], c.key["line_no"]) for c in corrections}
    assert not [
        r
        for r in run["s"].db.feed(system, t)
        if (r["order_id"], r["line_no"]) in keys and day_of(run, r["committed_at"]) in (10, 11)
    ]


def check_late_dimension(run, f):
    orphans = {d: run["reports"][d]["gold"]["fact_otif"]["orphan_lines"] for d in (10, 11, 12, 13)}
    assert orphans[10] == 0 and orphans[11] > 0 and orphans[12] > orphans[11] and orphans[13] == 0
    seen, cursor = {}, ""
    while True:
        items, cursor = run["s"].api.page("dealer_api", "dealer", cursor, 200)
        if not items:
            break
        seen.update({r["dealer_code"]: r for r in items})
    assert day_of(run, seen["D-9001"]["updated_at"]) == 13
    assert run["reports"][12]["silver"]["dealer_api.dealer"]["row_count"] == 600
    assert run["reports"][13]["silver"]["dealer_api.dealer"]["row_count"] == 601


def check_silent_loss(run, f):
    keys = f["detail"]["keys"]
    system, t = table(run, DLV)
    live = {r["delivery_id"] for r in run["s"].db.rows(system, t)}
    fed = {r["delivery_id"] for r in run["s"].db.feed(system, t)}
    assert len(keys) == 7 and sorted(live - fed) == keys
    s = run["reports"][12]["silver"][DLV]
    assert s["truth"]["row_count"] - s["row_count"] == 7 and s["missing_keys"] == keys


def check_replay(run, f):
    r = run["model"].readings[13]
    every = round(10 / SCALE)
    assert f["detail"]["replayed_readings"] == r["replayed"] == 72 * (45 * 60 // every)
    assert r["arrived"] == r["unique"] + r["replayed"]
    assert run["reports"][13]["bronze"][READ] == r["arrived"]


INCIDENTS = [
    ("readings-out-of-order", 3, "LATE-03", check_out_of_order_readings),
    ("order-lines-deleted", 4, "DEL-01", check_deletes),
    ("sellout-file-late", 5, "LATE-01", check_late_file),
    ("sellout-file-resent", 6, "DUP-01", check_resend),
    ("lab-column-renamed", 7, "SCH-02", check_rename),
    ("promised-date-out-of-order", 8, "SCD-02", check_late_change),
    ("material-copy-short", 9, None, check_short_copy),
    ("promised-date-corrected", 10, None, check_correction),
    ("new-dealer-late", 11, "LATE-02", check_late_dimension),
    ("change-feed-drops-rows", 12, "REC-05", check_silent_loss),
    ("stream-replayed", 13, "DUP-04", check_replay),
]


@pytest.mark.parametrize("incident,day,detector,check", INCIDENTS, ids=[i[0] for i in INCIDENTS])
def test_incident_planted(run, incident, day, detector, check):
    found = [f for f in run["reports"][day]["findings"] if f["incident"] == incident]
    assert len(found) == 1, f"{incident} not planted on day {day}"
    spec = next(e for e in run["dwd"].world["calendar"] if e["id"] == incident)
    assert found[0]["detector"] == spec["expect"]["detector"] == (detector or spec["expect"]["detector"])
    check(run, found[0])


def test_detectors_fire_only_on_their_days(run):
    """SIM-01: each incident shows up on its calendar day (a late dimension also on the day after) and no other."""
    allowed = {e["id"]: {e["day"]} for e in run["dwd"].world["calendar"]}
    allowed["new-dealer-late"] = {11, 12}
    for day, r in run["reports"].items():
        for f in r["findings"]:
            assert day in allowed[f["incident"]], f"{f['incident']} fired on day {day}"
    assert [d for d, r in run["reports"].items() if r["clean_day"]] == [1, 2, 14]


# ---------------------------------------------------------------- the oracle agrees with what the sources hold
@pytest.mark.parametrize(
    "ds,measure,source",
    [
        (SOL, "row_count", lambda run, s, t: len(run["s"].db.rows(s, t))),
        (SOL, "net_value_total", lambda run, s, t: round(sum(r["net_value"] for r in run["s"].db.rows(s, t)), 2)),
        (DLV, "row_count", lambda run, s, t: len(run["s"].db.feed(s, t))),  # what can reach silver
        (PROD, "row_count", lambda run, s, t: len(run["s"].db.rows(s, t))),
        (PROD, "tyres_cured_total", lambda run, s, t: float(sum(r["tyres_cured"] for r in run["s"].db.rows(s, t)))),
    ],
)
def test_silver_oracle_matches_sources(run, ds, measure, source):
    system, t = table(run, ds)
    assert run["reports"][DAYS]["silver"][ds][measure] == source(run, system, t)


def test_stream_oracle_matches_stream(run):
    s, total = run["s"].stream, 0
    ids = set()
    for shard in s.shards("press_reading"):
        recs = s.read("press_reading", shard, 0, 10**7)
        total += len(recs)
        ids |= {r.data["reading_id"] for r in recs}
    silver = run["reports"][DAYS]["silver"][READ]
    assert (silver["arrived_total"], silver["row_count"]) == (total, len(ids))


@pytest.mark.parametrize("day", range(1, DAYS + 1))
def test_bronze_never_exceeds_what_sources_sent(run, day):
    r = run["reports"][day]
    assert set(r["bronze"]) == set(r["sources"])
    assert all(0 <= r["bronze"][ds] <= r["sources"][ds] for ds in r["bronze"])


def test_otif_and_scrap_are_plausible(run):
    """Volumes and shares stay near the DWD's population (on time 93%, in full 96%, scrap 1.8%)."""
    otif = run["reports"][DAYS]["metrics"]["OTIF %"]["2026-11/all"]
    assert 0.8 <= otif["truth"] <= 0.95 and otif["lines"] > 1000
    scrap = run["reports"][DAYS]["metrics"]["Scrap rate %"]["2026-11-12/P-CHN"]
    assert 0.01 <= scrap["truth"] <= 0.03


def test_same_seed_same_world(tmp_path):
    def three_days(sub):
        dwd = dwd_loader.load(SLICE)
        sinks = Sinks(
            MemorySourceDb(), LocalFileDrop(tmp_path / sub / "d"), MemoryRecords(), LocalStream(tmp_path / sub / "s")
        )
        return World(dwd, model_for(dwd, 0.02), sinks, tmp_path / sub).run(3)

    assert three_days("a") == three_days("b")


# ---------------------------------------------------------------- the engine refuses what the DWD does not declare
@pytest.mark.parametrize(
    "emission,error",
    [
        (Emission(files=[("erp_db", SELL, "x.csv", b"a\n", "2026-11-09T00:00:00+00:00")]), "not a file_drop system"),
        (Emission(changes=[("mes_db", SOL, [])]), "not held by mes_db"),
        (Emission(records=[("nowhere", "dealer_api.dealer", "k", {}, "t")]), "not a rest_api system"),
    ],
)
def test_engine_refuses_undeclared_writes(tmp_path, emission, error):
    dwd = dwd_loader.load(SLICE)
    world = World(
        dwd,
        model_for(dwd, 0.02),
        Sinks(MemorySourceDb(), LocalFileDrop(tmp_path), MemoryRecords(), LocalStream(tmp_path)),
        tmp_path,
    )
    world.prepare()
    with pytest.raises(WorldError, match=error):
        world.write(emission)


def test_engine_refuses_an_incident_kind_the_model_cannot_plant(tmp_path):
    dwd = dwd_loader.load(SLICE)
    dwd.world["calendar"].append({"id": "x", "day": 2, "kind": "meteor_strike", "dataset": SOL})
    with pytest.raises(WorldError, match="meteor_strike"):
        World(
            dwd,
            model_for(dwd, 0.02),
            Sinks(MemorySourceDb(), LocalFileDrop(tmp_path), MemoryRecords(), LocalStream(tmp_path)),
            tmp_path,
        )


# ---------------------------------------------------------------- order lines carry a status (contract 0.2.0)
@pytest.mark.parametrize(
    "case,check",
    [
        ("every line has a status from the contract",
         lambda rows, feed: {r["line_status"] for r in rows} <= {"open", "cancelled", "rejected"}),
        ("cancelled lines keep their quantity and value",
         lambda rows, feed: all(r["ordered_qty"] > 0 and r["net_value"] > 0 for r in rows
                                if r["line_status"] == "cancelled")),
        ("some lines are cancelled", lambda rows, feed: any(r["line_status"] == "cancelled" for r in rows)),
        ("new lines are open", lambda rows, feed: {r["line_status"] for r in feed if r["op"] == "I"} == {"open"}),
        ("a reason only on rejected lines",
         lambda rows, feed: all((r["line_status"] == "rejected") == (r["rejection_reason"] is not None) for r in rows)),
    ],
)
def test_order_line_status(run, case, check):
    system, t = table(run, SOL)
    assert check(run["s"].db.rows(system, t), run["s"].db.feed(system, t)), case


def test_known_answers_come_from_the_metric_views(run):
    """The oracle asks every known-answer question the metric view files carry (no values restated in code)."""
    answers = run["reports"][DAYS]["known_answers"]
    spec = {f"{v} {m['name']} {m['known_answer']['where']}" for v, mv in run["dwd"].metrics.items()
            for m in mv["measures"] if m.get("known_answer")}
    assert set(answers) == spec and len(spec) == 5
    assert all(a["world"] is not None for a in answers.values())


def test_no_local_params_table():
    """Incident values come only from the DWD's params blocks (catalog 544406d)."""
    import dataplatform.world.domains.tyre as tyre

    assert not hasattr(tyre, "PARAMS")
