"""The clinical trial world: the compressed calendar over 3 days at a small scale, written into the mock sources and
checked against the DWD's calendar and the oracle. Table-driven: each incident row names its day, its detector and a
check on what the sources hold, so a broken incident fails by name. The oracle and the gold judge are also checked on
small hand-made tables."""

import csv
import io
import json
import os
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from dataplatform.adapters.file_drop import LocalFileDrop
from dataplatform.adapters.local_stream import LocalStream
from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.adapters.memory_sources import MemoryRecords
from dataplatform.adapters.sqlite_source import SqliteSource
from dataplatform.gold.daybyday import GoldDay
from dataplatform.meta import loader
from dataplatform.world import dwd as dwd_loader
from dataplatform.world.domains import gold_judge_for, model_for, models_folder
from dataplatform.world.domains.clinical import AE, DISP, KIT, LAB, LOG, SITE, SUBJ, TEMP, VISIT
from dataplatform.world.domains.clinical_gold import ENROL, LABS, TEMP as TEMP_FACT, VISITS, ClinicalGoldJudge, _windows
from dataplatform.world.domains.clinical_oracle import _pct, _where, known_answers
from dataplatform.world.engine import Sinks, World, WorldError

CATALOG = Path(os.environ.get("DP_CATALOG", Path.home() / "data-platform-catalog"))
SLICE = CATALOG / "slices/03-clinical"
SCALE, DAYS = 0.1, 3
pytestmark = pytest.mark.skipif(not SLICE.exists(), reason="needs the catalog's clinical slice (DP_CATALOG)")


def build(tmp: Path, scale: float = SCALE, days: int = DAYS, calendar: str | None = "compressed"):
    tmp.mkdir(parents=True, exist_ok=True)
    dwd = dwd_loader.load(SLICE, calendar)
    model = model_for(dwd, scale)
    store = MemoryMetaStore()  # where the study team records corrections (ADR-017)
    loader.load(store, SLICE / "manifest")
    sinks = Sinks(SqliteSource(tmp / "db.sqlite"), LocalFileDrop(tmp / "drops"), MemoryRecords(),
                  LocalStream(tmp / "stream"), store)
    world = World(dwd, model, sinks, tmp / "out")
    reports = {r["day"]: r for r in world.run(days)}
    return {"dwd": dwd, "model": model, "s": sinks, "reports": reports, "store": store, "out": tmp / "out"}


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    return build(tmp_path_factory.mktemp("clinical"))


def table(run, ds):
    d = run["dwd"].datasets[ds]
    return d.system, d.table


def files(run, ds):
    system = run["dwd"].datasets[ds].system
    return run["s"].drop.list(system, ds)


def header(run, ds, name):
    d = run["dwd"].datasets[ds]
    delimiter = d.manifest["config"]["csv_delimiter"]  # the world writes the delimiter the manifest declares
    return next(csv.reader(io.StringIO(run["s"].drop.get(d.system, ds, name).decode()), delimiter=delimiter))


def day_of(run, ts):
    return run["model"].clock.day_of(datetime.fromisoformat(ts))


# ---------------------------------------------------------------- each incident, by name
def check_late_lab_file(run, f):
    name = f["detail"]["file"]
    mine = [x for x in files(run, LAB) if x.name == name]
    assert len(mine) == 1 and day_of(run, mine[0].arrived_at) == 2  # collected on day 1, arrives on day 2
    assert run["reports"][1]["gold"]["fact_lab_out_of_range"]["by_collection_date"]["2026-11-09"]["complete"] is False
    assert run["reports"][2]["gold"]["fact_lab_out_of_range"]["by_collection_date"]["2026-11-09"]["complete"] is True


def check_rename(run, f):
    renamed = [x for x in files(run, LAB) if "flag" in header(run, LAB, x.name) and "range_flag" not in header(run, LAB, x.name)]
    assert [x.name for x in renamed] == [f["detail"]["first_file"]]
    assert all("range_flag" in header(run, LAB, x.name) for x in files(run, LAB) if x not in renamed)
    held = run["reports"][3]["silver"][LAB]["held_back_rows"]
    assert held > 0 and run["reports"][3]["sources"][LAB] - run["reports"][3]["bronze"][LAB] == held


def check_resend(run, f):
    by = {x.name: x for x in files(run, LOG)}
    a, b = by[f["detail"]["same_as"]], by[f["detail"]["file"]]
    assert a.checksum == b.checksum and a.name != b.name
    r2, r3 = run["reports"][2], run["reports"][3]
    assert r3["bronze"][LOG] == 0 < r3["sources"][LOG]
    assert r3["silver"][LOG] == r2["silver"][LOG]


def check_deleted_subject(run, f):
    system, t = table(run, SUBJ)
    keys = {k[0] for k in f["detail"]["keys"]}
    assert len(keys) == 1 and f["detail"]["via"] == "full_copy"
    assert not keys & {r["subject_id"] for r in run["s"].db.rows(system, t)}
    copy_at = next(c for c in reversed(run["s"].db.copies(system, t)))
    assert not keys & {r["subject_id"] for r in run["s"].db.copy(system, t, copy_at)}
    assert run["reports"][2]["silver"][SUBJ]["deleted_marked"] == 1


def check_late_change(run, f):
    system, t = table(run, AE)
    key = f["detail"]["key"][0]
    feed = [r for r in run["s"].db.feed(system, t) if r["ae_id"] == key]
    assert [r["change_seq"] for r in feed][-2:] == [f["detail"]["newer_seq"], f["detail"]["older_seq"]]
    live = next(r for r in run["s"].db.rows(system, t) if r["ae_id"] == key)
    assert live["change_seq"] == f["detail"]["newer_seq"] and live["severity"] == f["detail"]["current_severity"]


def check_short_copy(run, f):
    system, t = table(run, KIT)
    at = run["s"].db.copies(system, t)[-1]
    assert len(run["s"].db.copy(system, t, at)) == f["detail"]["rows_delivered"] < f["detail"]["rows_expected"]
    assert 0.39 <= f["detail"]["missing_share"] <= 0.41


def check_correction(run, f):
    rows = run["store"].corrections("edc", "visit")
    assert len(rows) == f["detail"]["visits"] == 6
    assert {c.field for c in rows} == {"planned_date"}
    live = {r["visit_id"]: r["planned_date"] for r in run["s"].db.rows(*table(run, VISIT))}
    assert all(live[c.key["visit_id"]] == c.was for c in rows)  # the data capture system was not changed


def check_new_site(run, f):
    d = f["detail"]
    assert d["orphan_visits"] == 12 and d["appears_day"] == 3
    site_ids = {s for s in run["model"].sites}
    assert d["site"] in site_ids and run["model"].sites_seen[d["site"]] == 3
    assert {r["site_id"] for r in run["s"].db.rows(*table(run, SUBJ))} >= {d["site"]}  # its subjects are entered


def check_silent_loss(run, f):
    system, t = table(run, DISP)
    ids = set(f["detail"]["keys"])
    live = {r["dispensing_id"] for r in run["s"].db.rows(system, t)}
    fed = {r["dispensing_id"] for r in run["s"].db.feed(system, t)}
    assert len(ids) == 3 and ids <= live and not ids & fed


def check_site_status(run, f):
    d = f["detail"]
    assert (d["was"], d["now"]) == ("active", "on_hold") and run["model"].sites[d["key"][0]]["site_status"] == "on_hold"


def check_late_readings(run, f):
    assert f["detail"]["late_readings"] > 0 and 10 <= f["detail"]["max_delay_s"] <= 90


def check_replay(run, f):
    n = f["detail"]["replayed_readings"]
    assert n > 0 and run["reports"][3]["sources"][TEMP] - run["reports"][3]["silver"][TEMP]["row_count"] + \
        run["reports"][2]["silver"][TEMP]["row_count"] >= n


INCIDENTS = [
    ("lab-file-late", 2, "LATE-01", check_late_lab_file),
    ("lab-column-renamed", 3, "SCH-02", check_rename),
    ("enrolment-log-resent", 3, "DUP-01", check_resend),
    ("subject-withdrawn", 2, "DEL-01", check_deleted_subject),
    ("adverse-event-out-of-order", 3, "SCD-02", check_late_change),
    ("kit-copy-short", 3, "DEL-04", check_short_copy),
    ("visits-planned-date-corrected", 2, "HIST-05", check_correction),
    ("new-site-late", 2, "LATE-02", check_new_site),
    ("dispensing-feed-drops-rows", 3, "REC-05", check_silent_loss),
    ("site-status-changed", 2, "SCD-02", check_site_status),
    ("temperature-readings-out-of-order", 2, "LATE-03", check_late_readings),
    ("temperature-stream-replayed", 3, "DUP-04", check_replay),
]


@pytest.mark.parametrize("incident,day,detector,check", INCIDENTS, ids=[i[0] for i in INCIDENTS])
def test_incident_planted(run, incident, day, detector, check):
    found = [f for d in run["model"].findings.values() for f in d if f["incident"] == incident]
    assert found, f"{incident} was never planted"
    first = next(d for d, fs in run["model"].findings.items() if any(f["incident"] == incident for f in fs))
    assert first == day and found[0]["detector"] == detector
    check(run, found[0])


def test_every_calendar_incident_has_a_check(run):
    assert {i[0] for i in INCIDENTS} == {e["id"] for e in run["dwd"].world["calendar"]}


def test_the_oracle_reports_every_finding_on_its_day(run):
    oracle = {d: json.loads((run["out"] / "oracle" / f"day-{d:02d}.json").read_text()) for d in range(1, DAYS + 1)}
    seen = {(d, f["incident"]) for d, o in oracle.items() for f in o["findings"]}
    assert {i for _, i in seen} == {i[0] for i in INCIDENTS}
    assert {d for d, i in seen if i == "kit-copy-short"} == {3}


@pytest.mark.parametrize("day", range(1, DAYS + 1))
def test_bronze_never_exceeds_what_sources_sent(run, day):
    r = run["reports"][day]
    assert set(r["bronze"]) == set(r["sources"])
    assert all(0 <= r["bronze"][ds] <= r["sources"][ds] for ds in r["bronze"])


@pytest.mark.parametrize("ds,measure,source", [
    (SUBJ, "row_count", lambda run: len(run["s"].db.rows(*table(run, SUBJ)))),
    (VISIT, "row_count", lambda run: len(run["s"].db.rows(*table(run, VISIT)))),
    (AE, "row_count", lambda run: len(run["s"].db.rows(*table(run, AE)))),
    (KIT, "row_count", lambda run: len(run["s"].db.rows(*table(run, KIT)))),
    (DISP, "truth_row_count", lambda run: len(run["s"].db.rows(*table(run, DISP)))),
])
def test_silver_oracle_matches_sources(run, ds, measure, source):
    silver = run["reports"][DAYS]["silver"][ds]
    want = silver["truth"]["row_count"] if measure.startswith("truth") else silver["row_count"]
    assert want == source(run)


def test_privacy_subject_ids_are_clear_only_in_sources(run):
    """The world writes clear subject identifiers (the tokenising is the platform's); the contracts mark them."""
    contract = run["dwd"].datasets[SUBJ].contract["schema"][0]["properties"]
    assert next(p for p in contract if p["name"] == "subject_id")["classification"] == "restricted"


# ---------------------------------------------------------------- determinism and scale
def snapshot(tmp: Path, scale: float, calendar: str | None = "compressed"):
    r = build(tmp, scale, 3, calendar)
    return {"oracle": {d: v for d, v in r["reports"].items()}, "findings": r["model"].incidents()}


def test_same_seed_same_world(tmp_path):
    assert snapshot(tmp_path / "a", 0.05) == snapshot(tmp_path / "b", 0.05)


SCALE_CASES = [
    # (what is counted, a function of the model, how the bigger scale must compare)
    ("subjects", lambda m: len(m.subjects), lambda small, big: small < big),
    ("visits", lambda m: len(m.visits), lambda small, big: small < big),
    ("kits", lambda m: len(m.kits), lambda small, big: small < big),
    ("subject floor holds", lambda m: len([s for s in m.subjects if not s.new_site]), lambda small, big: small >= 110),
]


@pytest.mark.parametrize("name,count,holds", SCALE_CASES, ids=[c[0] for c in SCALE_CASES])
def test_scale_shrinks_the_population(name, count, holds):
    dwd = dwd_loader.load(SLICE, "compressed")
    assert holds(count(model_for(dwd, 0.05)), count(model_for(dwd, 0.5)))


def test_a_different_seed_gives_a_different_world():
    dwd = dwd_loader.load(SLICE, "compressed")
    other = dwd_loader.load(SLICE, "compressed")
    other.world["seed"] = 24
    a, b = model_for(dwd, 0.05), model_for(other, 0.05)
    assert [s.screening for s in a.subjects] != [s.screening for s in b.subjects]


def test_a_calendar_that_breaks_the_order_is_refused():
    dwd = dwd_loader.load(SLICE)
    broken = {**dwd.world, "calendars": {"x": {"moves": {e["id"]: {"day": 1} for e in dwd.world["calendar"]}}}}
    with pytest.raises(dwd_loader.DwdError, match="breaks the order"):
        dwd_loader.apply_calendar(broken, "x")


def test_the_engine_refuses_an_unknown_system(run):
    from dataplatform.world.engine import Emission

    with pytest.raises(WorldError, match="not a file_drop system"):
        World(run["dwd"], run["model"], run["s"], run["out"]).write(
            Emission(files=[("edc_db", LAB, "x.csv", b"a\n", "2026-11-09T00:00:00+00:00")]))


# ---------------------------------------------------------------- the domain is chosen from the world
@pytest.mark.parametrize("world,judge,folder", [("03-clinical", "ClinicalGoldJudge", "models/clinical")])
def test_binding_picks_judge_and_models_from_the_world(world, judge, folder):
    dwd = dwd_loader.load(CATALOG / "slices" / world, "compressed")
    assert type(gold_judge_for(dwd, "Asia/Kolkata")).__name__ == judge
    assert models_folder(dwd) == folder
    assert (Path(__file__).resolve().parents[2] / folder / "gold").is_dir()


def test_every_gold_build_of_the_clinical_manifests_exists():
    root = Path(__file__).resolve().parents[2] / "models/clinical"
    refs = []
    for p in sorted((SLICE / "manifest/gold").glob("fact_*.yaml")):
        text = p.read_text()
        ref = next(line.split("ref:")[1] for line in text.splitlines() if line.startswith("build:")).strip(' "}')
        refs.append(ref)
    paths = [root / (r.split(":")[0].replace(".", "/") + ".py" if ":" in r else r) for r in refs]
    assert len(paths) == 4 and all(p.exists() for p in paths), paths


# ---------------------------------------------------------------- the oracle on small hand-made tables
WHERE_CASES = [
    ("one text filter", "country = 'IN'", {"country": "IN"}),
    ("a month and a country", "Country = 'GB' AND planned month = 2026-11", {"country": "GB", "planned month": "2026-11"}),
    ("a month alone", "reading month = 2026-11", {"reading month": "2026-11"}),
]


@pytest.mark.parametrize("name,text,want", WHERE_CASES, ids=[c[0] for c in WHERE_CASES])
def test_where_parses_a_known_answer_question(name, text, want):
    assert _where(text) == want


PCT_CASES = [("a share", 3, 4, 0.75), ("rounded to four places", 1, 3, 0.3333), ("nothing to divide", 0, 0, None)]


@pytest.mark.parametrize("name,n,total,want", PCT_CASES, ids=[c[0] for c in PCT_CASES])
def test_pct(name, n, total, want):
    assert _pct(n, total) == want


KNOWN_CASES = [
    ("matches inside the tolerance", {"M": {"2026-11/IN": 0.8176}}, 0.8175, 0.0005, True),
    ("differs outside the tolerance", {"M": {"2026-11/IN": 0.70}}, 0.8175, 0.0005, False),
    ("the question was never answered", {"M": {"2026-11/GB": 0.8}}, 0.8175, 0.0005, False),
]


@pytest.mark.parametrize("name,metrics,spec,tol,match", KNOWN_CASES, ids=[c[0] for c in KNOWN_CASES])
def test_known_answers_compare_the_spec_with_the_world(name, metrics, spec, tol, match):
    model = SimpleNamespace(dwd=SimpleNamespace(metrics={"mv": {"measures": [
        {"name": "M", "known_answer": {"where": "country = 'IN' AND planned month = 2026-11", "value": spec,
                                       "tolerance": tol}}]}}))
    (got,) = known_answers(model, metrics).values()
    assert got["match"] is match and got["spec"] == spec


# ---------------------------------------------------------------- the gold judge on small hand-made tables
class FakeReader:
    """A GoldReader over canned rows: the query's table name picks the rows."""

    def __init__(self, tables):
        self.tables = tables

    def rows(self, sql, params=None, **_):
        name = sql.split("FROM")[1].split()[0]
        if name not in self.tables:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        return self.tables[name]


WINDOW_CASES = [
    ("equal", {"w1": {"n": 2, "complete": 1}}, {"w1": {"n": 2, "complete": True}}, []),
    ("a number differs", {"w1": {"n": 3, "complete": 1}}, {"w1": {"n": 2, "complete": True}}, ["w1"]),
    ("complete flag differs", {"w1": {"n": 2, "complete": 0}}, {"w1": {"n": 2, "complete": True}}, ["w1"]),
    ("a window gold lacks", {}, {"w1": {"n": 2, "complete": True}}, ["w1"]),
    ("a window the oracle lacks", {"w1": {"n": 2, "complete": 1}}, {}, ["w1"]),
]


@pytest.mark.parametrize("name,got,want,bad", WINDOW_CASES, ids=[c[0] for c in WINDOW_CASES])
def test_windows_differing(name, got, want, bad):
    assert _windows(got, want, {"n": "n"}) == bad


def window(n, forms, ok, kits, orphans, complete=True):
    return {"rows": n, "forms_expected_total": forms, "compliant": ok, "kits_dispensed": kits, "orphan_rows": orphans,
            "complete": complete}


VISIT_GOLD = [{"w": "2026-11-09", "n": 2, "forms": 10, "ok": 1, "kits": 1, "orphans": 0, "complete": 1}]
COMPARE_CASES = [
    ("gold equals the oracle", VISIT_GOLD, {"2026-11-09": window(2, 10, 1, 1, 0)}, 0.0),
    ("a compliant count differs", VISIT_GOLD, {"2026-11-09": window(2, 10, 2, 1, 0)}, 1.0),
    ("gold has not published", [], {"2026-11-09": window(2, 10, 1, 1, 0)}, 1.0),
]


@pytest.mark.parametrize("name,rows,want,differing", COMPARE_CASES, ids=[c[0] for c in COMPARE_CASES])
def test_judge_compares_visit_windows(name, rows, want, differing):
    oracle = {"gold": {VISITS: {"rows": sum(w["rows"] for w in want.values()), "by_planned_date": want}}}
    measures, notes = ClinicalGoldJudge("Asia/Kolkata").compare(
        oracle, FakeReader({"fact_visit_compliance": rows}), SimpleNamespace(by_name=dict))
    assert measures[VISITS]["windows_differing"][0] == differing and len(notes["visits_differing"]) == differing


def day(n, notes=None, measures=None, restated=()):
    table_report = SimpleNamespace(restated=[SimpleNamespace(window=w, detector=d) for w, d in restated],
                                   plans_applied=["bp"])
    report = SimpleNamespace(by_name=lambda: {VISITS: table_report, ENROL: table_report})
    return GoldDay(n, f"2026-11-{8 + n:02d}", report, measures or {}, notes=notes or {})


LATE_FINDING = {"incident": "lab-file-late", "detector": "LATE-01", "dataset": LAB,
                "detail": {"collection_date": "2026-11-09", "due_day": 1}}
VERDICT_CASES = [
    ("late file: flagged, then complete", LATE_FINDING,
     day(2, {"lab_complete": {"2026-11-09": True}, "lab_differing": []}),
     [day(1, {"lab_complete": {"2026-11-09": False}})], "handled"),
    ("late file: never flagged", LATE_FINDING,
     day(2, {"lab_complete": {"2026-11-09": True}, "lab_differing": []}),
     [day(1, {"lab_complete": {"2026-11-09": True}})], "missed"),
    ("late file: still incomplete", LATE_FINDING,
     day(2, {"lab_complete": {"2026-11-09": False}, "lab_differing": []}),
     [day(1, {"lab_complete": {"2026-11-09": False}})], "missed"),
    ("resend: nothing changed", {"incident": "r", "detector": "DUP-01", "dataset": LOG, "detail": {}},
     day(3, {"enrolment_screened": {"2026-10-01": 5.0}, "enrolment_restated": []}),
     [day(2, {"enrolment_screened": {"2026-10-01": 5.0}})], "handled"),
    ("resend: a month restated", {"incident": "r", "detector": "DUP-01", "dataset": LOG, "detail": {}},
     day(3, {"enrolment_screened": {"2026-10-01": 5.0}, "enrolment_restated": ["2026-10-01"]}),
     [day(2, {"enrolment_screened": {"2026-10-01": 5.0}})], "missed"),
    ("new site: orphans equal the oracle", {"incident": "n", "detector": "LATE-02", "dataset": SITE,
                                            "detail": {"site": "S-0099"}},
     day(2, measures={VISITS: {"orphan_rows": (12.0, 12.0), "rows": (700.0, 700.0)}}), [], "flagged"),
    ("new site: orphans differ", {"incident": "n", "detector": "LATE-02", "dataset": SITE, "detail": {"site": "S-0099"}},
     day(2, measures={VISITS: {"orphan_rows": (0.0, 12.0), "rows": (700.0, 700.0)}}), [], "missed"),
    ("replay: counted once", {"incident": "p", "detector": "DUP-04", "dataset": "coldchain_stream.temperature_reading",
                              "detail": {}},
     day(3, {"temperature_differing": []}, {TEMP_FACT: {"readings_total": (10.0, 10.0),
                                                        "excursion_readings_total": (4.0, 4.0)}}), [], "handled"),
    ("replay: counted twice", {"incident": "p", "detector": "DUP-04", "dataset": "coldchain_stream.temperature_reading",
                               "detail": {}},
     day(3, {"temperature_differing": ["2026-11-11"]}, {TEMP_FACT: {"readings_total": (12.0, 10.0),
                                                                    "excursion_readings_total": (8.0, 4.0)}}), [], "missed"),
]


@pytest.mark.parametrize("name,finding,today,before,verdict", VERDICT_CASES, ids=[c[0] for c in VERDICT_CASES])
def test_judge_verdicts(name, finding, today, before, verdict):
    got = ClinicalGoldJudge("Asia/Kolkata")._today(today.day, finding, today, before)
    assert got is not None and got.verdict == verdict


LATER_CASES = [
    ("plan applied, only its windows", day(11, {"visits_differing": []}, restated=[("2026-11-09", "HIST-05")]), "handled"),
    ("a window outside the plan restated", day(11, {"visits_differing": []},
                                               restated=[("2026-11-12", "HIST-05")]), "missed"),
    ("late-data restatement beside the plan", day(11, {"visits_differing": []},
                                                  restated=[("2026-11-09", "HIST-05"), ("2026-11-09", "LATE-01")]), "missed"),
    ("numbers differ from the corrected truth", day(11, {"visits_differing": ["2026-11-09"]},
                                                    restated=[("2026-11-09", "HIST-05")]), "missed"),
]


@pytest.mark.parametrize("name,today,verdict", LATER_CASES, ids=[c[0] for c in LATER_CASES])
def test_judge_correction_plan(name, today, verdict):
    finding = {"incident": "c", "detector": "HIST-05", "dataset": VISIT,
               "detail": {"activate_day": 11, "windows": ["2026-11-09", "2026-11-10"], "plan_id": "bp"}}
    got = ClinicalGoldJudge("Asia/Kolkata")._later(11, finding, today, [])
    assert got is not None and got.verdict == verdict


def test_a_table_gold_has_not_published_reads_as_no_rows():
    measures, _ = ClinicalGoldJudge("Asia/Kolkata").compare(
        {"gold": {LABS: {"rows": 2}}}, FakeReader({}), SimpleNamespace(by_name=dict))
    assert measures[LABS]["rows"] == (0.0, 2.0)


# ---------------------------------------------------------------- the generic day-by-day judges the clinical world needs
def _plan(restricted: bool):
    from dataplatform.silver.plan import ColumnPlan

    col = ColumnPlan("subject_id", "string", True, "restricted" if restricted else "")
    return SimpleNamespace(keys=("subject_id",), columns=(col,))


STORED_KEY_CASES = [
    ("a restricted key is looked up as its token", True, lambda cls, v: f"tok_{v}", {"subject_id": "tok_A"}),
    ("a plain key is looked up as it is", False, lambda cls, v: f"tok_{v}", {"subject_id": "A"}),
    ("no tokenizer given", True, None, {"subject_id": "A"}),
]


@pytest.mark.parametrize("name,restricted,tokenize,want", STORED_KEY_CASES, ids=[c[0] for c in STORED_KEY_CASES])
def test_silver_judge_looks_keys_up_as_stored(name, restricted, tokenize, want):
    from dataplatform.silver.daybyday import _stored_key

    assert _stored_key(_plan(restricted), ["A"], tokenize) == want


JUDGE_CASES = [
    ("a delete seen through a full copy is silver's", "DEL-01", {"via": "full_copy"}, "deferred"),
    ("a delete in a change feed not counted is missed", "DEL-01", {}, "missed"),
    ("a master change is silver's", "SCD-02", {"field": "site_status"}, "deferred"),
    ("a late change not counted is missed", "SCD-02", {}, "missed"),
]


@pytest.mark.parametrize("name,detector,detail,verdict", JUDGE_CASES, ids=[c[0] for c in JUDGE_CASES])
def test_ingestion_judge_defers_what_a_later_layer_applies(name, detector, detail, verdict):
    from dataplatform.ingest.daybyday import judge

    finding = {"incident": "i", "detector": detector, "dataset": "x.y", "detail": detail}
    assert judge(1, finding, None).verdict == verdict
