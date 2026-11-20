"""The clinical trial (data-platform-catalog slices/03-clinical/world.yaml): six source systems, day by day.

Deterministic: the same seed and scale give the same data on any machine. Each concern draws from its own random
stream, so changing how readings are made never changes a visit. The model keeps the world's own truth beside
everything it emits; clinical_oracle.py turns that truth into the expected totals (R9: never from the platform's logs).

Volumes follow the DWD's population; `scale` shrinks subjects and reading frequency for tests (1.0 = as specified).
Every number an incident needs (a site, a share, an hour) is read from the DWD's `params:` block, never from here.

The truth the model keeps, in the order the oracle uses it:
    Subject / Visit   the people and their planned visits; a visit's outcome (completed, cancelled or missed) is decided
                      when the visit is made and happens on `res_day`, the day it is recorded (or on day 1 when it
                      lies before the clock)
    lab               the results of each site and collection day, the files they travel in and what became of them
    readings          temperature readings per shipment and day (unique, whatever arrives late or twice)
"""

from __future__ import annotations

import csv
import io
import math
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator

from dataplatform.meta.port import BackfillPlan, Correction
from dataplatform.source_ports import Row, SourceChange
from dataplatform.world.clock import SimClock, iso
from dataplatform.world.dwd import Dwd
from dataplatform.world.engine import Chunk, Emission

UTC = timezone.utc
SUBJ, VISIT, AE = "edc.subject", "edc.visit", "edc.adverse_event"
KIT, DISP = "supply.kit", "supply.dispensing"
LAB, LOG, SITE, TEMP = (
    "lab.lab_result",
    "site.site_enrolment_log",
    "ctms_api.site",
    "coldchain_stream.temperature_reading",
)

COUNTRIES = ["IN", "HU", "GB", "AU", "DE", "ZA"]
ANALYTES = {  # code: (unit, reference low, reference high)
    "ALT": ("U/L", 7.0, 56.0),
    "AST": ("U/L", 10.0, 40.0),
    "CREA": ("mg/dL", 0.6, 1.3),
    "GLUC": ("mg/dL", 70.0, 100.0),
    "HGB": ("g/dL", 12.0, 17.5),
    "K": ("mmol/L", 3.5, 5.1),
    "PLT": ("10^9/L", 150.0, 450.0),
    "WBC": ("10^9/L", 4.0, 11.0),
}
AE_TERMS = ["headache", "nausea", "fatigue", "dizziness", "rash", "insomnia", "cough", "back pain"]
SEVERITIES = ["mild", "moderate", "severe"]
OUTCOMES = ["not_recovered", "recovering", "recovered"]
LOG_MONTH_DEFAULT = "2026-10"


class Subject:
    __slots__ = ("id", "idx", "site", "cohort", "screening", "entered_day", "deleted_day", "visits", "new_site")

    def __init__(self, idx: int, site: str, cohort: str, screening: date, entered_day: int, new_site: bool = False):
        self.id, self.idx, self.site, self.cohort = f"SUBJ-{idx:05d}", idx, site, cohort
        self.screening, self.entered_day, self.new_site = screening, entered_day, new_site
        self.deleted_day: int | None = None
        self.visits: list[Visit] = []


class Visit:
    """One planned visit: what the source holds on each day follows from `outcome` and `res_day`."""

    __slots__ = (
        "id",
        "subj",
        "code",
        "planned",
        "first_planned",
        "window",
        "forms_exp",
        "kits_exp",
        "analytes",
        "created_day",
        "scheduled_ts",
        "outcome",
        "res_day",
        "actual",
        "forms_rec",
        "disp",
        "seq",
        "versions",
    )

    def __init__(self, vid: str, subj: Subject, code: str, planned: date, spec: dict[str, Any], created_day: int):
        self.id, self.subj, self.code, self.planned, self.first_planned = vid, subj, code, planned, planned
        self.window, self.forms_exp = int(spec["window_days"]), int(spec["forms_expected"])
        self.kits_exp, self.analytes = int(spec["kits_expected"]), int(spec["analytes_ordered"])
        self.created_day = created_day
        self.scheduled_ts = ""
        self.outcome = "missed"  # completed | cancelled | missed (stays scheduled)
        self.res_day = 10**6
        self.actual: date | None = None
        self.forms_rec = 0
        self.disp: dict[str, Any] | None = None  # the dispensing record this visit gets, when it gets one
        self.seq = 0
        self.versions = 1

    def status(self, d: int) -> str:
        return self.outcome if self.outcome != "missed" and self.res_day <= d else "scheduled"


class ClinicalTrial:
    handles = frozenset(
        {
            "late_delivery",
            "renamed_column",
            "duplicate_delivery",
            "source_delete",
            "late_change",
            "short_full_copy",
            "correction",
            "late_dimension",
            "silent_loss",
            "master_change",
            "late_events",
            "replay",
        }
    )

    def __init__(self, dwd: Dwd, scale: float = 1.0):
        w = dwd.world
        self.dwd, self.scale, self.pop = dwd, scale, w["population"]
        self.sys = {name: ds.system for name, ds in dwd.datasets.items()}
        self.clock = SimClock(str(w["clock"]["start"]), w["clock"]["business_timezone"])
        self.days = int(w["clock"]["days"])
        # Each incident's exact values come from its `params:` block in the DWD (and `pick`, how many); no local table.
        self.incident = {e["kind"]: {**e, **(e.get("params") or {})} for e in w["calendar"]}
        seed = w["seed"]
        self.rng = {
            k: random.Random(f"{seed}:{k}")
            for k in ("master", "subjects", "visits", "lab", "ae", "supply", "stream", "sites", "log")
        }
        self.edc_seq = self.supply_seq = self.ae_seq = 0
        self.findings: dict[int, list[dict[str, Any]]] = {}
        self.ledger: dict[int, dict[str, dict[str, int]]] = {}
        self.corrections: list[dict[str, Any]] = []  # the world's truth: which visits, which dates, when active
        self.pending_corrections: list[tuple[BackfillPlan, list[Correction]]] = []
        self.sites: dict[str, Row] = {}
        self.sites_seen: dict[str, int] = {}  # site -> the day the interface first lists it
        self.site_versions = 0
        self.subjects: list[Subject] = []
        self.visits: list[Visit] = []
        self.kits: dict[str, Row] = {}
        self.kit_versions = 0
        self._last_copy: dict[str, dict[str, tuple[Any, ...]]] = {KIT: {}, SUBJ: {}}
        self.subject_versions = 0
        self.dispensings: dict[str, dict[str, Any]] = {}  # dispensing_id -> row + day + lost
        self.aes: dict[str, dict[str, Any]] = {}
        self.lab: dict[tuple[str, int], dict[str, Any]] = {}  # (site, collection day) -> the day's results and their file
        self.log_files: dict[str, dict[str, Any]] = {}
        self.log_plan: dict[str, int] = {}
        self.readings: dict[int, dict[str, Any]] = {}
        self.by_ship_day: dict[tuple[str, str], list[float]] = {}  # (shipment, date) -> [readings, excursions, max C]
        self.status_change: dict[str, Any] | None = None
        self.late_change_key: str | None = None
        self.new_subject_ids: list[str] = []
        self._make_population()

    # ------------------------------------------------------------------ population
    def _make_population(self) -> None:
        r, p = self.rng["master"], self.pop
        n_sites = int(p["sites"])
        start = self.clock.date(1)
        for i in range(1, n_sites + 1):
            code = f"S-{i:04d}"
            self.sites[code] = self._site_row(code, COUNTRIES[(i - 1) % len(COUNTRIES)], r, start - timedelta(days=60))
            self.sites_seen[code] = 0
        codes = sorted(self.sites)
        n = max(110, round(int(p["subjects"]) * self.scale))
        cohorts, weights = list(p["cohorts"]), [float(x) for x in p["cohorts"].values()]
        rs = self.rng["subjects"]
        back = int(p["screening_days_back"])
        for i in range(1, n + 1):
            site = codes[(i - 1) % n_sites]
            # the first subject of every site was screened in October, so every site has an October enrolment log
            screening = start - timedelta(days=20 if i <= n_sites else rs.randint(0, back))
            self.subjects.append(Subject(i, site, rs.choices(cohorts, weights)[0], screening, 1))
        for s in self.subjects:
            self._make_visits(s, 1)
        late = self.incident.get("late_dimension")
        if late:
            for k in range(int(late["subjects"])):
                s = Subject(n + 1 + k, late["site"], cohorts[k % len(cohorts)], self.clock.date(late["day"]),
                            int(late["day"]), new_site=True)
                self.subjects.append(s)
                self.new_subject_ids.append(s.id)
                self._make_visits(s, int(late["day"]))
        self._make_kits(r, n)

    def _site_row(self, code: str, country: str, r: random.Random, updated: date) -> Row:
        n = int(code[2:])
        return {
            "site_id": code,
            "site_name": f"Site {n:04d} Clinic",
            "country": country,
            "site_status": "active",
            "investigator_name": f"Investigator {n:04d}",
            "contact_phone": f"+44-20-{r.randint(10000000, 99999999)}",
            "updated_at": iso(datetime(updated.year, updated.month, updated.day, 9, tzinfo=self.clock.tz)),
        }

    def _make_visits(self, s: Subject, created_day: int) -> None:
        """The subject's visits and what becomes of each. Decided here, once, from the visits stream."""
        r, p = self.rng["visits"], self.pop
        for code, spec in p["visit_schedule"].items():
            planned = s.screening + timedelta(days=int(spec["offset_days"]))
            v = Visit(f"V-{s.idx:05d}-{code}", s, code, planned, spec, created_day)
            created = self.clock.date(created_day) if s.new_site else s.screening
            hour = 9 if s.new_site else 8
            v.scheduled_ts = iso(datetime(created.year, created.month, created.day, hour, tzinfo=self.clock.tz))
            roll = r.random()
            planned_day = (planned - self.clock.date(1)).days + 1
            floor = created_day if s.new_site else -10**6  # a new subject's visits are recorded from the day it is entered
            if roll < float(p["visit_cancel_share"]):
                v.outcome, v.res_day = "cancelled", max(planned_day, floor)
            elif roll < float(p["visit_cancel_share"]) + float(p["visit_done_share"]):
                early = r.random() < float(p["visit_early_share"])
                v.actual = planned - timedelta(days=1 if early else 0)
                v.outcome = "completed"
                v.res_day = (v.actual - self.clock.date(1)).days + 1
                if v.res_day < floor:  # keep the recorded date a real day of the clock
                    v.actual, v.res_day = planned, max(planned_day, floor)
                short = r.random() < float(p["forms_short_share"])
                v.forms_rec = max(0, v.forms_exp - (1 if short else 0))
            if v.outcome == "completed" and v.kits_exp and r.random() >= float(p["kit_missing_share"]):
                v.disp = {}  # filled when the visit is recorded
            if v.res_day < 1 or s.new_site:  # before the clock, or a site with no laboratory arrangement yet
                v.analytes = 0
            s.visits.append(v)
            self.visits.append(v)

    def _make_kits(self, r: random.Random, n_subjects: int) -> None:
        total = int(self.pop["kits_per_subject"]) * n_subjects + 16
        for i in range(1, total + 1):
            code = f"K-{i:06d}"
            self.kits[code] = {
                "kit_id": code,
                "kit_type": r.choice(["bottle", "blister_pack"]),
                "lot_number": f"LOT-{r.randint(1, 30):03d}",
                "expiry_date": (self.clock.date(1) + timedelta(days=r.randint(200, 700))).isoformat(),
                "depot_code": f"DEP-{r.randint(1, 6):02d}",
                "treatment_arm": r.choice(["arm_1", "arm_2"]),
            }
        self.kit_order = sorted(self.kits)
        self.next_kit = 0

    # ------------------------------------------------------------------ one day
    def day(self, d: int) -> Emission:
        e = Emission()
        self.ledger[d] = {"source": {}, "bronze": {}}
        self.findings[d] = []
        self._subjects(d, e)
        self._kits(d, e)
        self._edc(d, e)
        self._labs(d, e)
        self._logs(d, e)
        self._sites(d, e)
        e.events[(self.sys[TEMP], TEMP)] = self._readings(d)
        e.corrections = self.pending_corrections
        self.pending_corrections = []
        return e

    def _count(self, d: int, ds: str, source: int, bronze: int) -> None:
        led = self.ledger[d]
        led["source"][ds] = led["source"].get(ds, 0) + source
        led["bronze"][ds] = led["bronze"].get(ds, 0) + bronze

    def _find(self, d: int, kind: str, **detail: Any) -> None:
        inc = self.incident[kind]
        self.findings[d].append(
            {
                "incident": inc["id"],
                "story": inc.get("story", ""),
                "dataset": inc["dataset"],
                "detector": inc["expect"]["detector"],
                "handled_by": inc["expect"]["handled_by"],
                "detail": detail,
            }
        )

    def _on(self, kind: str, d: int) -> dict[str, Any] | None:
        inc = self.incident.get(kind)
        return inc if inc and inc["day"] == d else None

    # ------------------------------------------------------------------ subjects (full copy, nightly)
    def _status_on(self, s: Subject, d: int) -> str:
        """The subject's status at the start of day d: what the visits recorded before today say."""
        done = {v.code: v for v in s.visits if v.outcome == "completed" and v.res_day < d}
        if "W12" in done:
            return "completed"
        return "randomised" if "BL" in done else "screening"

    def _subject_row(self, s: Subject, d: int, at: datetime) -> Row:
        return {
            "subject_id": s.id,
            "site_id": s.site,
            "cohort": s.cohort,
            "subject_status": self._status_on(s, d),
            "screening_date": s.screening.isoformat(),
            "extracted_at": iso(at),
        }

    def present(self, d: int) -> list[Subject]:
        """Subjects in the source at the end of day d (entered, and not deleted)."""
        return [s for s in self.subjects if s.entered_day <= d and (s.deleted_day is None or s.deleted_day > d)]

    def _subjects(self, d: int, e: Emission) -> None:
        r = self.rng["subjects"]
        change_at = self.clock.at(d, 0, 20)
        changes: list[SourceChange] = []
        before = {s.id for s in self.present(d - 1)} if d > 1 else set()
        gone = self._on("source_delete", d)
        if gone:
            pool = [s for s in self.present(d - 1) if s.entered_day <= gone["from_day"] and not s.new_site]
            pool = sorted(pool, key=lambda s: s.id)
            victims = r.sample(pool, int(gone["pick"]))
            for s in victims:
                s.deleted_day = d
                changes.append(SourceChange("D", self._subject_row(s, d, change_at), 0, iso(self.clock.at(d, 0, 30))))
            self._find(d, "source_delete", keys=[[s.id] for s in victims], via="full_copy")
        now = self.present(d)
        for s in now:
            if s.id not in before and not (s.new_site and s.entered_day != d):
                at = self.clock.at(d, 0, 30) if s.new_site else change_at
                changes.append(SourceChange("I", self._subject_row(s, d, at), 0, iso(at)))
            elif s.id in before and s.entered_day < d:
                row = self._subject_row(s, d, change_at)
                if self._last_copy[SUBJ].get(s.id) is not None and self._last_copy[SUBJ][s.id][2] != row["subject_status"]:
                    changes.append(SourceChange("U", row, 0, iso(change_at)))
        e.changes.append((self.sys[SUBJ], SUBJ, changes))
        copy_at = self.clock.at(d, 1)
        copy = [self._subject_row(s, d, copy_at) for s in sorted(now, key=lambda x: x.id)]
        self._accept_copy(SUBJ, copy)
        self._count(d, SUBJ, len(copy), len(copy))
        e.copies.append((self.sys[SUBJ], SUBJ, iso(copy_at), copy))

    def _accept_copy(self, ds: str, copy: list[Row]) -> None:
        """A copy the platform accepts: a new silver version for each row whose tracked columns changed."""
        last = self._last_copy[ds]
        for row in copy:
            if ds == SUBJ:
                key, t = row["subject_id"], (row["site_id"], row["cohort"], row["subject_status"])
            else:
                key, t = row["kit_id"], (row["lot_number"], row["expiry_date"], row["depot_code"])
            if last.get(key) != t:
                last[key] = t
                if ds == SUBJ:
                    self.subject_versions += 1
                else:
                    self.kit_versions += 1
        if ds == SUBJ:
            keep = {r["subject_id"] for r in copy}
            for k in [k for k in last if k not in keep]:
                del last[k]  # a deleted key leaves silver's current rows (it is marked deleted)

    # ------------------------------------------------------------------ kits (full copy, nightly)
    def _kits(self, d: int, e: Emission) -> None:
        r = self.rng["master"]
        change_at = self.clock.at(d, 0, 20)
        if d == 1:
            changes = [
                SourceChange("I", {**k, "extracted_at": iso(change_at)}, 0, iso(change_at)) for k in self.kits.values()
            ]
        else:
            changes = []
            for code in r.sample(sorted(self.kits), 2):
                self.kits[code]["depot_code"] = f"DEP-{r.randint(1, 6):02d}"
                changes.append(SourceChange("U", {**self.kits[code], "extracted_at": iso(change_at)}, 0, iso(change_at)))
        e.changes.append((self.sys[KIT], KIT, changes))
        copy_at = iso(self.clock.at(d, 1))
        copy = [dict(v) for _, v in sorted(self.kits.items())]
        short = self._on("short_full_copy", d)
        if short:
            drop = set(r.sample([c["kit_id"] for c in copy], round(len(copy) * short["missing_share"])))
            copy = [c for c in copy if c["kit_id"] not in drop]
            self._find(
                d,
                "short_full_copy",
                rows_expected=len(self.kits),
                rows_delivered=len(copy),
                missing_share=round(1 - len(copy) / len(self.kits), 3),
            )
            self._count(d, KIT, len(copy), 0)  # blocked: nothing appended to bronze
        else:
            self._accept_copy(KIT, copy)
            self._count(d, KIT, len(copy), len(copy))
        e.copies.append((self.sys[KIT], KIT, copy_at, copy))

    # ------------------------------------------------------------------ the data capture and supply databases
    def _seq(self) -> int:
        self.edc_seq += 1
        return self.edc_seq

    def _visit_row(self, v: Visit, d: int) -> Row:
        st = v.status(d)
        done = st == "completed"
        return {
            "visit_id": v.id,
            "subject_id": v.subj.id,
            "site_id": v.subj.site,
            "visit_code": v.code,
            "planned_date": v.planned.isoformat(),
            "window_days": v.window,
            "scheduled_ts": v.scheduled_ts,
            "visit_status": st,
            "actual_date": v.actual.isoformat() if done and v.actual else None,
            "forms_expected": v.forms_exp,
            "forms_recorded": v.forms_rec if done else 0,
            "kits_expected": v.kits_exp,
            "analytes_ordered": v.analytes,
            "change_seq": v.seq,
        }

    def _edc(self, d: int, e: Emission) -> None:
        r = self.rng["visits"]
        visit_ch: list[SourceChange] = []
        disp_ch: list[SourceChange] = []
        new_visits = [v for v in self.visits if v.created_day == d]
        for v in new_visits:  # the visits exist from the day their subject is entered
            at = self.clock.at(d, 0, 30 if v.subj.new_site else 10)
            v.seq = self._seq()
            # resolved before today: the insert already holds the final state; otherwise it is recorded later today
            visit_ch.append(SourceChange("I", self._visit_row(v, d - 1), v.seq, iso(at)))
            if v.res_day < 1:
                self._dispense(v, d, at, disp_ch)
        due = [v for v in self.visits if v.res_day == d and v.created_day <= d and v.outcome != "missed"]
        for v in sorted(due, key=lambda x: x.id):
            if v.res_day < 1:
                continue
            at = self.clock.at(d, 8) + timedelta(seconds=r.randint(0, 9 * 3600))
            v.seq = self._seq()
            v.versions += 1
            visit_ch.append(SourceChange("U", self._visit_row(v, d), v.seq, iso(at)))
            self._dispense(v, d, at, disp_ch)
        visit_ch.sort(key=lambda c: (c.committed_at, c.change_seq))
        e.changes.append((self.sys[VISIT], VISIT, visit_ch))
        self._count(d, VISIT, len(visit_ch), len(visit_ch))
        self._silent_loss(d, disp_ch)
        disp_ch.sort(key=lambda c: (c.committed_at, c.change_seq))
        e.changes.append((self.sys[DISP], DISP, disp_ch))
        self._count(d, DISP, len(disp_ch), sum(c.in_feed for c in disp_ch))
        corr = self._on("correction", d)
        if corr:
            self._correct(d, corr)
        ae = self._adverse_events(d)
        e.changes.append((self.sys[AE], AE, ae))
        self._count(d, AE, len(ae), len(ae))

    def _dispense(self, v: Visit, d: int, at: datetime, out: list[SourceChange]) -> None:
        if v.disp is None or v.outcome != "completed":
            return
        kit = self.kit_order[self.next_kit]
        self.next_kit += 1
        self.supply_seq += 1
        did = f"DP-{self.supply_seq:06d}"
        row = {
            "dispensing_id": did,
            "visit_id": v.id,
            "subject_id": v.subj.id,
            "site_id": v.subj.site,
            "kit_id": kit,
            "dispensed_ts": iso(at + timedelta(minutes=5)),
            "kits_dispensed": 1,
            "change_seq": self.supply_seq,
        }
        v.disp = {"id": did, "day": d, "lost": False}
        self.dispensings[did] = {**row, "day": d, "lost": False, "visit": v.id}
        out.append(SourceChange("I", row, self.supply_seq, iso(at + timedelta(minutes=5))))

    def _silent_loss(self, d: int, dispensings: list[SourceChange]) -> None:
        inc = self._on("silent_loss", d)
        if not inc:
            return
        lost = sorted(dispensings, key=lambda c: (c.committed_at, c.change_seq))[: inc["pick"]]
        ids = {c.row["dispensing_id"] for c in lost}
        for i, c in enumerate(dispensings):
            if c.row["dispensing_id"] in ids:
                dispensings[i] = SourceChange(c.op, c.row, c.change_seq, c.committed_at, in_feed=False)
                self.dispensings[c.row["dispensing_id"]]["lost"] = True
        self._find(
            d, "silent_loss", keys=sorted(ids), window=[lost[0].committed_at, lost[-1].committed_at] if lost else []
        )

    # ------------------------------------------------------------------ planned-date corrections (ADR-017)
    def _correct(self, d: int, inc: dict[str, Any]) -> None:
        """The study team records corrections as rows in the metadata store's corrections table, with a backfill plan
        that activates them later and names the windows to rebuild. The data capture system is not changed."""
        picks = [
            v
            for v in self.visits
            if (v.planned - self.clock.date(1)).days + 1 == inc["from_day"]
            and v.outcome == "completed"
            and v.res_day <= d
            and not v.subj.new_site
            and (v.subj.deleted_day is None or v.subj.deleted_day > d)
        ]
        picks = sorted(picks, key=lambda v: v.id)[: inc["pick"]]
        rows = []
        for v in picks:
            corrected = (v.first_planned + timedelta(days=int(inc["shift_days"]))).isoformat()
            self.corrections.append(
                {
                    "visit_id": v.id,
                    "column": "planned_date",
                    "was": v.first_planned.isoformat(),
                    "corrected": corrected,
                    "recorded_day": d,
                    "activate_day": inc["activate_day"],
                }
            )
            rows.append(
                Correction(key={"visit_id": v.id}, field="planned_date", value=corrected, was=v.first_planned.isoformat())
            )
        windows = sorted({c["was"] for c in self.corrections} | {c["corrected"] for c in self.corrections})
        source, dataset = VISIT.split(".")
        plan = BackfillPlan(
            plan_id=f"bp-{inc['id']}",
            source=source,
            dataset=dataset,
            windows=tuple(windows),
            activate_on=self.clock.date(inc["activate_day"]).isoformat(),
            reason=" ".join(str(inc["does"]).split()),
            recorded_by="study-team",
        )
        self.pending_corrections.append((plan, rows))
        self._find(d, "correction", visits=len(picks), activate_day=inc["activate_day"], windows=windows,
                   plan_id=plan.plan_id)

    def planned(self, v: Visit, d: int) -> date:
        """The planned date as the world knows it on day d (a correction counts once its plan is active)."""
        for c in self.corrections:
            if c["visit_id"] == v.id and d >= c["activate_day"]:
                return date.fromisoformat(c["corrected"])
        return v.first_planned

    # ------------------------------------------------------------------ adverse events
    def _adverse_events(self, d: int) -> list[SourceChange]:
        r = self.rng["ae"]
        out: list[SourceChange] = []
        people = [s for s in self.present(d) if not s.new_site]
        n = max(2, round(len(self.subjects) * float(self.pop["adverse_events_per_subject_day"])))
        if d == 1:
            n += round(len(self.subjects) * float(self.pop["adverse_events_per_subject_day"]) * 8)
        late = self._on("late_change", d)
        for i in range(n):
            s = r.choice(people)
            self.ae_seq += 1
            aid = f"AE-{self.ae_seq:06d}"
            back = r.randint(1, 8) if d == 1 and i < n - 2 else 0  # day 1 also holds the events of the days before
            reported = self.clock.at(d, 8) + timedelta(seconds=r.randint(0, 9 * 3600)) - timedelta(days=back)
            row: Row = {
                "ae_id": aid,
                "subject_id": s.id,
                "site_id": s.site,
                "ae_term": r.choice(AE_TERMS),
                "onset_date": reported.date().isoformat(),
                "severity": r.choice(SEVERITIES),
                "is_serious": 1 if r.random() < 0.15 else 0,
                "outcome": "not_recovered",
                "ae_status": "open",
                "reported_ts": iso(reported),
                "change_seq": self._seq(),
            }
            self.aes[aid] = {"row": row, "day": d, "versions": 1, "fixed": False}
            out.append(SourceChange("I", dict(row), row["change_seq"], iso(reported)))
        fix = self.incident.get("late_change")
        if fix and d == fix["from_day"]:  # the event the late change will act on is set aside now
            mine = [a for a in self.aes.values() if a["day"] == d and not a["fixed"]]
            if mine:
                mine[-1]["fixed"] = True
                self.late_change_key = mine[-1]["row"]["ae_id"]
        if d > 1:
            for aid in sorted(self.aes):
                a = self.aes[aid]
                if a["row"]["ae_status"] == "closed" or a["fixed"] or a["day"] == d:
                    continue
                if r.random() < 0.20:
                    out.append(self._ae_update(a, d, r))
        if late:
            out += self._act_late_change(d, late)
        return out

    def _ae_update(self, a: dict[str, Any], d: int, r: random.Random) -> SourceChange:
        row, at = a["row"], self.clock.at(d, 9) + timedelta(seconds=r.randint(0, 8 * 3600))
        before = (row["severity"], row["outcome"], row["ae_status"])
        step = OUTCOMES.index(row["outcome"])
        if r.random() < 0.5 and step < 2:
            row["outcome"] = OUTCOMES[step + 1]
            row["ae_status"] = "closed" if row["outcome"] == "recovered" else "open"
        else:
            row["severity"] = r.choice([x for x in SEVERITIES if x != row["severity"]])
        row["change_seq"] = self._seq()
        if (row["severity"], row["outcome"], row["ae_status"]) != before:
            a["versions"] += 1
        return SourceChange("U", dict(row), row["change_seq"], iso(at))

    def _act_late_change(self, d: int, inc: dict[str, Any]) -> list[SourceChange]:
        """Two severity changes: the older one commits six hours after the newer one."""
        if self.late_change_key is None:
            raise ValueError(f"no adverse event from day {inc['from_day']} to plant {inc['id']} on; raise the scale")
        a = self.aes[self.late_change_key]
        row, at = a["row"], self.clock.at(d, 9)
        original = row["severity"]
        older_sev = next(x for x in SEVERITIES if x != original)
        newer_sev = next(x for x in SEVERITIES if x not in (original, older_sev))
        row["severity"] = older_sev
        row["change_seq"] = self._seq()
        older = SourceChange("U", dict(row), row["change_seq"], iso(at))
        row["severity"] = newer_sev
        row["change_seq"] = self._seq()
        newer = SourceChange("U", dict(row), row["change_seq"], iso(at))
        a["versions"] += 2
        held = SourceChange("U", older.row, older.change_seq, iso(at + timedelta(hours=6)))
        self._find(d, "late_change", key=[row["ae_id"]], older_seq=older.change_seq, newer_seq=newer.change_seq,
                   current_severity=newer_sev)
        return [newer, held]

    # ------------------------------------------------------------------ the central laboratory
    def _results_of(self, v: Visit) -> list[Row]:
        r = self.rng["lab"]
        codes = sorted(r.sample(sorted(ANALYTES), v.analytes))
        out = []
        for code in codes:
            unit, lo, hi = ANALYTES[code]
            if r.random() < float(self.pop["out_of_range_share"]):
                value = round(lo - r.uniform(0.02, 0.3) * lo, 2) if r.random() < 0.5 else round(hi + r.uniform(0.02, 0.3) * hi, 2)
            else:
                value = round(r.uniform(lo, hi), 2)
            flag = "low" if value < lo else "high" if value > hi else "normal"
            at = datetime(v.actual.year, v.actual.month, v.actual.day, 9, tzinfo=self.clock.tz) + timedelta(  # type: ignore[union-attr]
                minutes=r.randint(0, 120)
            )
            out.append(
                {
                    "result_id": f"LR-{v.id}-{code}",
                    "visit_id": v.id,
                    "subject_id": v.subj.id,
                    "site_id": v.subj.site,
                    "analyte_code": code,
                    "collected_ts": iso(at),
                    "result_value": value,
                    "unit": unit,
                    "reference_low": lo,
                    "reference_high": hi,
                    "range_flag": flag,
                    "out_of_range": 0 if flag == "normal" else 1,
                }
            )
        return out

    def _labs(self, d: int, e: Emission) -> None:
        """One file per site per collection day, in the evening of that day; the incidents move or change a file."""
        late, rename = self.incident.get("late_delivery"), self.incident.get("renamed_column")
        by_site: dict[str, list[Visit]] = {}
        for v in self.visits:
            if v.res_day == d and v.outcome == "completed" and v.analytes and v.created_day <= d:
                by_site.setdefault(v.subj.site, []).append(v)
        for site, vs in sorted(by_site.items()):
            rows = [x for v in sorted(vs, key=lambda y: y.id) for x in self._results_of(v)]
            self.lab[(site, d)] = {"rows": rows, "expected": sum(v.analytes for v in vs), "arrive_day": d,
                                   "accepted": True, "file": ""}
        if late and d == late["due_day"]:
            busiest = max(sorted(site for site, k in self.lab if k == d), key=lambda s: len(self.lab[(s, d)]["rows"]))
            self.lab[(busiest, d)]["arrive_day"] = late["day"]
            self.late_site = busiest
        arriving = sorted(k for k, g in self.lab.items() if g["arrive_day"] == d)
        renamed_site = None
        if rename and rename["day"] <= d <= rename.get("until_day", rename["day"]):
            today = [k for k in arriving if k[1] == d]
            if today:
                renamed_site = max(sorted(k[0] for k in today), key=lambda s: len(self.lab[(s, d)]["rows"]))
        cols = [c for c, _ in self.dwd.datasets[LAB].table.columns]
        delimiter = str(self.dwd.datasets[LAB].manifest["config"].get("csv_delimiter", ","))
        for site, day in arriving:
            g = self.lab[(site, day)]
            day_date = self.clock.date(day)
            name = f"labresults_{site}_{day_date:%Y%m%d}.csv"  # matches the manifest's file_pattern
            is_renamed = renamed_site == site and day == d
            header = [rename["renamed_to"] if c == rename["column"] else c for c in cols] if is_renamed and rename else None
            arrived = iso(self.clock.at(d, 20) + timedelta(minutes=len(site) + int(site[2:])))
            e.files.append((self.sys[LAB], LAB, name, _csv(g["rows"], cols, header, delimiter), arrived))
            g["file"], g["accepted"] = name, not is_renamed
            n = len(g["rows"])
            self._count(d, LAB, n, 0 if is_renamed else n)
            if is_renamed and rename:
                self._find(d, "renamed_column", site=site, first_file=name, missing_column=rename["column"],
                           new_column=rename["renamed_to"])
            if late and site == getattr(self, "late_site", None) and day == late["due_day"]:
                self._find(d, "late_delivery", site=site, due_day=late["due_day"], arrived_day=d,
                           collection_date=self.clock.date(late["due_day"]).isoformat(), file=name)

    # ------------------------------------------------------------------ site enrolment logs
    def _logs(self, d: int, e: Emission) -> None:
        resend = self.incident.get("duplicate_delivery")
        month = (resend or {}).get("month", LOG_MONTH_DEFAULT)
        r = self.rng["log"]
        if d == 1:
            window = int(self.dwd.systems[self.sys[LOG]].get("file_window_days", 5))
            self.log_plan = {code: 1 + (int(code[2:]) % window) for code in self.sites}
            self.log_due = len(self.log_plan)
        for code in sorted(c for c, day in self.log_plan.items() if day == d):
            rows = self._log_rows(code, month)
            name = f"enrolment_{code}_{month}.csv"
            data = _csv(rows, ["site_id", "enrolment_month", "cohort", "subjects_screened", "subjects_randomised"],
                        delimiter=str(self.dwd.datasets[LOG].manifest["config"].get("csv_delimiter", ",")))
            arrived = self.clock.at(d, 9) + timedelta(seconds=r.randint(0, 8 * 3600))
            e.files.append((self.sys[LOG], LOG, name, data, iso(arrived)))
            self.log_files[name] = {"site": code, "month": month, "day": d, "rows": rows, "data": data}
            self._count(d, LOG, len(rows), len(rows))
        if resend and d == resend["day"]:
            original = self.log_files[f"enrolment_{resend['site']}_{resend['month']}.csv"]
            name = f"enrolment_{resend['site']}_{resend['month']}_resent.csv"
            e.files.append((self.sys[LOG], LOG, name, original["data"], iso(self.clock.at(d, 10))))
            self._count(d, LOG, len(original["rows"]), 0)  # same bytes: skipped as a duplicate
            self._find(d, "duplicate_delivery", file=name, same_as=f"enrolment_{resend['site']}_{resend['month']}.csv")

    def _log_rows(self, site: str, month: str) -> list[Row]:
        """The site's report for the month: subjects screened and randomised in it, per cohort."""
        first = date.fromisoformat(f"{month}-01")
        end = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
        out = []
        for cohort in sorted(self.pop["cohorts"]):
            mine = [s for s in self.subjects if s.site == site and s.cohort == cohort and not s.new_site
                    and first <= s.screening < end]
            if not mine:
                continue
            randomised = sum(
                1 for s in mine
                if any(v.code == "BL" and v.outcome == "completed" and v.actual is not None and v.actual < end
                       for v in s.visits)
            )
            out.append({"site_id": site, "enrolment_month": first.isoformat(), "cohort": cohort,
                        "subjects_screened": len(mine), "subjects_randomised": randomised})
        return out

    # ------------------------------------------------------------------ the trial management interface
    def _sites(self, d: int, e: Emission) -> None:
        r = self.rng["sites"]
        late, change = self.incident.get("late_dimension"), self.incident.get("master_change")
        changed: list[tuple[str, datetime]] = []
        if d == 1:
            changed = [(code, self.clock.day_start(1) - timedelta(days=60) + timedelta(hours=9)) for code in sorted(self.sites)]
            self.site_versions += len(self.sites)
        if change and d == change["day"]:
            row = self.sites[change["site"]]
            row["site_status"] = change["to_status"]
            at = self.clock.at(d, 10)
            row["updated_at"] = iso(at)
            self.site_versions += 1
            changed.append((change["site"], at))
            self._find(d, "master_change", key=[change["site"]], field="site_status", was=change["from_status"],
                       now=change["to_status"])
        if late and d == late["appears_day"]:
            at = self.clock.at(d, 9)
            self.sites[late["site"]] = {**self._site_row(late["site"], late["country"], r, self.clock.date(d)),
                                        "site_name": "Site 0099 Clinic", "updated_at": iso(at)}
            self.sites_seen[late["site"]] = d
            self.site_versions += 1
            changed.append((late["site"], at))
        for code, at in changed:
            e.records.append((self.sys[SITE], SITE, code, dict(self.sites[code]), iso(at)))
        self._count(d, SITE, len(changed), len(changed))
        if late and late["day"] <= d < late["appears_day"]:
            orphans = [v.id for v in self.visits if v.subj.site == late["site"] and v.status(d) != "cancelled"]
            self._find(d, "late_dimension", site=late["site"], orphan_visits=len(orphans),
                       appears_day=late["appears_day"])

    # ------------------------------------------------------------------ shipment temperature stream
    def _readings(self, d: int) -> Iterator[Chunk]:
        """Hour by hour so a day of readings never sits in memory at once. Late readings carry into the next hour."""
        r, p = self.rng["stream"], self.pop
        every = max(60, round(int(p["reading_every_s"]) / math.sqrt(self.scale))) if self.scale < 1 else int(p["reading_every_s"])
        lo, hi = float(p["temperature_range_c"]["low"]), float(p["temperature_range_c"]["high"])
        late: Any = self._on("late_events", d)
        replay: Any = self._on("replay", d)
        exc: Any = p["excursion"] if int(p["excursion"]["day"]) == d else None
        ships = [f"SH-{i:04d}" for i in range(1, int(p["shipments"]) + 1)]
        start = int(self.clock.day_start(d).timestamp())
        late_from = int(self.clock.at(d, late["from_hour"]).timestamp()) if late else 0
        late_to = late_from + 3600 * late["hours"] if late else 0
        late_ships = set(r.sample(ships, late["shipments"])) if late else set()
        exc_ships = set(sorted(ships)[: int(exc["shipments"])]) if exc else set()
        exc_from = int(self.clock.at(d, exc["from_hour"]).timestamp()) if exc else 0
        exc_to = exc_from + max(60 * int(exc["minutes"]), every) if exc else 0
        restart = int(self.clock.at(d, replay["restart_hour"]).timestamp()) if replay else 0
        stats = self.readings.setdefault(
            d, {"arrived": 0, "unique": 0, "late": 0, "max_delay_s": 0, "replayed": 0, "by_hour": {},
                "excursion_readings": 0}
        )
        carry: list[tuple[float, str, Row]] = []
        replay_buf: list[tuple[str, Row]] = []
        forced = set()
        for h in range(24):
            h0, h1 = start + h * 3600, start + (h + 1) * 3600
            batch = carry
            for i, sid in enumerate(ships):
                for t in range(h0 + (i % every), h1, every):
                    hot = sid in exc_ships and exc_from <= t < exc_to
                    temp = round(r.uniform(hi + 2.5, float(exc["peak_c"]) + 0.4) if hot else min(hi - 0.5, max(lo + 0.4, r.gauss(5.0, 0.7))), 1)
                    row = {
                        "reading_id": f"{sid}-{t}",
                        "shipment_id": sid,
                        "event_ts": _ts(t),
                        "temperature_c": temp,
                        "humidity_pct": round(r.uniform(35, 60), 1),
                        "logger_state": ("in_transit", "at_depot", "at_site")[((t // 3600) + i) % 3],
                    }
                    day = self.clock.date(d).isoformat()
                    cell = self.by_ship_day.setdefault((sid, day), [0, 0, 0.0])
                    cell[0] += 1
                    cell[1] += int(temp < lo or temp > hi)
                    cell[2] = max(cell[2], temp)
                    delay = r.uniform(0.2, 2.0)
                    in_window = sid in late_ships and late_from <= t < late_to
                    if in_window and (r.random() < late["share"] or sid not in forced):
                        forced.add(sid)  # at least one late reading per chosen shipment, so the incident is never empty
                        delay = r.uniform(10, late["max_delay_s"])
                        stats["late"] += 1
                        stats["max_delay_s"] = max(stats["max_delay_s"], round(delay))
                    batch.append((t + delay, sid, row))
                    if replay and restart - 60 * replay["minutes"] <= t < restart:
                        replay_buf.append((sid, row))
            batch.sort(key=lambda x: x[0])
            ready = [(sid, _ts(at), row) for at, sid, row in batch if at < h1]
            carry = [x for x in batch if x[0] >= h1]
            if replay and h0 <= restart < h1:
                ready = (
                    [x for x in ready if x[1] < _ts(restart)]
                    + [(sid, _ts(restart + 5 + k // 200), row) for k, (sid, row) in enumerate(replay_buf)]
                    + [x for x in ready if x[1] >= _ts(restart)]
                )
                stats["replayed"] = len(replay_buf)
            stats["arrived"] += len(ready)
            yield ready
        if carry:
            stats["arrived"] += len(carry)
            yield [(sid, _ts(at), row) for at, sid, row in carry]
        self._reading_truth(d, every, ships)

    def _reading_truth(self, d: int, every: int, ships: list[str]) -> None:
        """Readings per event-time hour: every shipment makes one per `every` seconds, whatever arrives late or twice."""
        stats = self.readings[d]
        start = int(self.clock.day_start(d).timestamp())
        by_hour = {}
        for h in range(24):
            h0 = start + h * 3600
            by_hour[_ts(h0)] = sum(len(range(h0 + (i % every), h0 + 3600, every)) for i in range(len(ships)))
        stats["by_hour"] = by_hour
        stats["unique"] = sum(by_hour.values())
        day = self.clock.date(d).isoformat()
        stats["excursion_readings"] = sum(c[1] for (s, dd), c in self.by_ship_day.items() if dd == day)
        self._count(d, TEMP, stats["arrived"], stats["arrived"])
        if self._on("late_events", d):
            self._find(d, "late_events", late_readings=stats["late"], max_delay_s=stats["max_delay_s"])
        if self._on("replay", d):
            self._find(d, "replay", replayed_readings=stats["replayed"], minutes=self.incident["replay"]["minutes"])

    # ------------------------------------------------------------------ oracle
    def oracle(self, d: int) -> dict[str, Any]:
        from dataplatform.world.domains.clinical_oracle import day_oracle

        return day_oracle(self, d)

    def incidents(self) -> list[dict[str, Any]]:
        return [f for d in sorted(self.findings) for f in [{**x, "day": d} for x in self.findings[d]]]


def _ts(epoch: float) -> str:
    return datetime.fromtimestamp(int(epoch), UTC).isoformat()


def _csv(rows: list[Row], columns: list[str], header: list[str] | None = None, delimiter: str = ",") -> bytes:
    """CSV with a header line, in the delimiter the dataset's manifest declares. `header` renames columns on the way
    out (the laboratory's renamed column)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", delimiter=delimiter)
    w.writerow(header or columns)
    for row in rows:
        w.writerow([row.get(c, "") for c in columns])
    return buf.getvalue().encode()
