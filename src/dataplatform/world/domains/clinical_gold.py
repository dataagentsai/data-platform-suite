"""The clinical trial's gold judge: published gold against the oracle's gold for the day, and the incidents gold owns.

Reads gold only through the GoldReader port (SQL over the gold tables by name) and the oracle's day file; never the
platform's own logs for the numbers (R9). The run report is used only for what gold says it did (restatements, the
correction plans it applied), which is what the incidents judge.

    fact_visit_compliance       rows, forms expected, compliant visits, kits dispensed, rows on the unknown site; per
                                planned date: the same and complete
    fact_lab_out_of_range       per collection date: site-days, results expected, reported and out of range, complete
    fact_enrolment              per month: rows, subjects screened and randomised, complete
    fact_temperature_excursion  per reading date: shipment-days, readings, excursion readings, complete

Incidents: LATE-01 (the late laboratory file's site-day flagged incomplete until the file, then complete with the true
counts), DUP-01 (a re-sent enrolment log changes no gold number and restates nothing), HIST-05 (on the plan's day only its
windows restate and visit compliance equals the corrected truth), LATE-02 (no row dropped; the new site's visits on -1
while the site is missing, re-keyed the day it arrives), LATE-03 and DUP-04 (every reading and excursion counted once).
"""

from __future__ import annotations

from typing import Any

from dataplatform.gold.daybyday import GoldDay
from dataplatform.gold.harness import GoldReport
from dataplatform.gold.ports import GoldReader
from dataplatform.ingest.daybyday import Verdict

VISITS, LABS, ENROL, TEMP = "fact_visit_compliance", "fact_lab_out_of_range", "fact_enrolment", "fact_temperature_excursion"

Q_VISITS = """
SELECT date_format(planned_date, 'yyyy-MM-dd') AS w, COUNT(1) AS n, SUM(forms_expected) AS forms, SUM(compliant) AS ok,
       SUM(kits_dispensed) AS kits, SUM(CASE WHEN site_sk = -1 THEN 1 ELSE 0 END) AS orphans,
       MIN(CAST(_is_complete AS INT)) AS complete
FROM fact_visit_compliance GROUP BY 1
"""
Q_LABS = """
SELECT date_format(collection_date, 'yyyy-MM-dd') AS w, COUNT(1) AS n, SUM(results_expected) AS expected,
       SUM(results_reported) AS reported, SUM(results_out_of_range) AS oor, MIN(CAST(_is_complete AS INT)) AS complete
FROM fact_lab_out_of_range GROUP BY 1
"""
Q_ENROL = """
SELECT date_format(enrolment_month, 'yyyy-MM-dd') AS w, COUNT(1) AS n, SUM(subjects_screened) AS screened,
       SUM(subjects_randomised) AS randomised, MIN(CAST(_is_complete AS INT)) AS complete
FROM fact_enrolment GROUP BY 1
"""
Q_TEMP = """
SELECT date_format(reading_date, 'yyyy-MM-dd') AS w, COUNT(1) AS n, SUM(readings) AS readings,
       SUM(excursion_readings) AS exc, MIN(CAST(_is_complete AS INT)) AS complete
FROM fact_temperature_excursion GROUP BY 1
"""


def _rows(reader: GoldReader, sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """A table gold has not published yet reads as no rows (the comparison then shows what is missing)."""
    try:
        return reader.rows(sql, params)
    except Exception as exc:  # the engine's "no such table": gold failed or has not run yet
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return []
        raise


def _f(x: Any) -> float:
    return float(x or 0)


def _windows(got: dict[str, dict[str, Any]], want: dict[str, dict[str, Any]],
             pairs: dict[str, str]) -> list[str]:
    """The windows where any named number (gold column -> oracle key) or the complete flag differs."""
    bad = []
    for w in sorted(set(got) | set(want)):
        g, e = got.get(w) or {}, want.get(w) or {}
        same = all(_f(g.get(a)) == _f(e.get(b)) for a, b in pairs.items())
        if not same or _f(g.get("complete")) != float(bool(e.get("complete"))):
            bad.append(w)
    return bad


class ClinicalGoldJudge:
    def __init__(self, tz: str):
        self.tz = tz

    # ------------------------------------------------------------------------------------------------ compare
    def compare(self, oracle: dict[str, Any], reader: GoldReader, report: GoldReport) -> tuple[
            dict[str, dict[str, tuple[float, float]]], dict[str, Any]]:
        want = oracle.get("gold") or {}
        notes: dict[str, Any] = {"findings": oracle.get("findings") or []}
        out = {VISITS: self._visits(want.get(VISITS) or {}, reader, notes),
               LABS: self._labs(want.get(LABS) or {}, reader, notes),
               ENROL: self._enrolment(want.get(ENROL) or {}, reader, report, notes),
               TEMP: self._temperature(want.get(TEMP) or {}, reader, notes)}
        return out, notes

    def _visits(self, want: dict[str, Any], reader: GoldReader, notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {r["w"]: r for r in _rows(reader, Q_VISITS)}
        exp = {w: {**e, "forms": e["forms_expected_total"], "ok": e["compliant"], "kits": e["kits_dispensed"],
                   "orphans": e["orphan_rows"], "n": e["rows"]} for w, e in (want.get("by_planned_date") or {}).items()}
        bad = _windows(got, exp, {"n": "n", "forms": "forms", "ok": "ok", "kits": "kits", "orphans": "orphans"})
        notes["visits_differing"] = bad
        orphans = sum(_f(r["orphans"]) for r in got.values())
        notes["visit_orphans"] = orphans
        return {"rows": (sum(_f(r["n"]) for r in got.values()), _f(want.get("rows"))),
                "forms_expected_total": (sum(_f(r["forms"]) for r in got.values()), _f(want.get("forms_expected_total"))),
                "compliant_total": (sum(_f(r["ok"]) for r in got.values()), _f(want.get("compliant_total"))),
                "kits_dispensed_total": (sum(_f(r["kits"]) for r in got.values()), _f(want.get("kits_dispensed_total"))),
                "orphan_rows": (orphans, _f(want.get("orphan_rows"))),
                "windows_differing": (float(len(bad)), 0.0)}

    def _labs(self, want: dict[str, Any], reader: GoldReader, notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {r["w"]: r for r in _rows(reader, Q_LABS)}
        exp = {w: {**e, "n": e["rows"], "expected": e["results_expected"], "reported": e["results_reported"],
                   "oor": e["results_out_of_range"]} for w, e in (want.get("by_collection_date") or {}).items()}
        bad = _windows(got, exp, {"n": "n", "expected": "expected", "reported": "reported", "oor": "oor"})
        notes["lab_differing"] = bad
        notes["lab_complete"] = {w: bool(_f(r["complete"])) for w, r in got.items()}
        return {"rows": (sum(_f(r["n"]) for r in got.values()), _f(want.get("rows"))),
                "results_reported": (sum(_f(r["reported"]) for r in got.values()), _f(want.get("results_reported"))),
                "results_out_of_range": (sum(_f(r["oor"]) for r in got.values()),
                                         _f(want.get("results_out_of_range"))),
                "windows_differing": (float(len(bad)), 0.0)}

    def _enrolment(self, want: dict[str, Any], reader: GoldReader, report: GoldReport,
                   notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {r["w"]: r for r in _rows(reader, Q_ENROL)}
        exp = {w: {**e, "n": e["rows"], "screened": e["subjects_screened"], "randomised": e["subjects_randomised"]}
               for w, e in (want.get("by_month") or {}).items()}
        bad = _windows(got, exp, {"n": "n", "screened": "screened", "randomised": "randomised"})
        notes["enrolment_differing"] = bad
        notes["enrolment_screened"] = {w: _f(r["screened"]) for w, r in got.items()}
        t = report.by_name().get(ENROL)
        notes["enrolment_restated"] = sorted({r.window for r in (t.restated if t else [])})
        return {"rows": (sum(_f(r["n"]) for r in got.values()), _f(want.get("rows"))),
                "subjects_screened_total": (sum(_f(r["screened"]) for r in got.values()),
                                            _f(want.get("subjects_screened_total"))),
                "windows_differing": (float(len(bad)), 0.0)}

    def _temperature(self, want: dict[str, Any], reader: GoldReader,
                     notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {r["w"]: r for r in _rows(reader, Q_TEMP)}
        exp = {w: {**e, "n": e["rows"], "readings": e["readings"], "exc": e["excursion_readings"]}
               for w, e in (want.get("by_reading_date") or {}).items()}
        bad = _windows(got, exp, {"n": "n", "readings": "readings", "exc": "exc"})
        notes["temperature_differing"] = bad
        return {"readings_total": (sum(_f(r["readings"]) for r in got.values()), _f(want.get("readings_total"))),
                "excursion_readings_total": (sum(_f(r["exc"]) for r in got.values()),
                                             _f(want.get("excursion_readings_total"))),
                "windows_differing": (float(len(bad)), 0.0)}

    # ------------------------------------------------------------------------------------------------ judge
    def judge(self, n: int, oracle: dict[str, Any], today: GoldDay, before: list[GoldDay],
              reader: GoldReader) -> list[Verdict]:
        out: list[Verdict] = []
        for f in oracle.get("findings") or []:
            v = self._today(n, f, today, before)
            if v is not None:
                out.append(v)
        for day in before:  # incidents planted earlier and closed today
            for f in day.notes.get("findings") or []:
                v = self._later(n, f, today, before)
                if v is not None:
                    out.append(v)
        return out

    @staticmethod
    def _v(n: int, f: dict[str, Any], dataset: str, ok: bool, evidence: str, verdict: str = "") -> Verdict:
        return Verdict(n, f["incident"], f["detector"], dataset, verdict or ("handled" if ok else "missed"), evidence)

    def _today(self, n: int, f: dict[str, Any], today: GoldDay, before: list[GoldDay]) -> Verdict | None:
        det, d = f["detector"], f["detail"]
        if det == "LATE-01":
            date = str(d["collection_date"])
            earlier = [b.notes.get("lab_complete", {}).get(date) for b in before if b.day >= int(d["due_day"])]
            flagged = bool(earlier) and all(x is False for x in earlier)
            done = today.notes.get("lab_complete", {}).get(date) is True
            equal = not today.notes.get("lab_differing")
            return self._v(n, f, LABS, flagged and done and equal,
                           f"{date} flagged incomplete on day(s) {int(d['due_day'])}-{n - 1}: {flagged}; complete today: "
                           f"{done}; laboratory windows equal the oracle: {equal}")
        if det == "DUP-01":
            prev = before[-1].notes.get("enrolment_screened", {}) if before else {}
            now = today.notes.get("enrolment_screened", {})
            same = all(now.get(m) == v for m, v in prev.items())
            none = not today.notes.get("enrolment_restated")
            return self._v(n, f, ENROL, same and none,
                           f"subjects screened per month unchanged since yesterday: {same}; restatements today: "
                           f"{today.notes.get('enrolment_restated')}")
        if det == "LATE-02":
            orphans, rows = today.measures[VISITS]["orphan_rows"], today.measures[VISITS]["rows"]
            ok = orphans[0] == orphans[1] > 0 and rows[0] == rows[1]
            return self._v(n, f, VISITS, ok, f"{int(orphans[0])} visits of {d['site']} on the unknown member "
                                             f"(oracle {int(orphans[1])}); rows {int(rows[0])} = oracle {int(rows[1])}",
                           "flagged" if ok else "missed")
        if det in ("LATE-03", "DUP-04") and f["dataset"].startswith("coldchain"):
            t = today.measures[TEMP]
            ok = not today.notes.get("temperature_differing") and t["excursion_readings_total"][0] == \
                t["excursion_readings_total"][1] and t["readings_total"][0] == t["readings_total"][1]
            return self._v(n, f, TEMP, ok, f"readings {int(t['readings_total'][0])} and excursion readings "
                                           f"{int(t['excursion_readings_total'][0])} equal the oracle's "
                                           f"({int(t['readings_total'][1])}, {int(t['excursion_readings_total'][1])}); "
                                           f"windows differing: {len(today.notes.get('temperature_differing') or [])}")
        return None

    def _later(self, n: int, f: dict[str, Any], today: GoldDay, before: list[GoldDay]) -> Verdict | None:
        det, d = f["detector"], f["detail"]
        if det == "HIST-05" and int(d["activate_day"]) == n:
            t = today.report.by_name().get(VISITS)
            applied = bool(t) and d["plan_id"] in t.plans_applied  # type: ignore[union-attr]
            restated = sorted(r.window for r in (t.restated if t else []))
            outside = sorted(set(restated) - set(d["windows"]))
            late01 = [r.window for r in (t.restated if t else []) if r.detector != "HIST-05"]
            equal = not today.notes.get("visits_differing")
            ok = applied and not outside and not late01 and equal
            return self._v(n, f, VISITS, ok, f"plan {d['plan_id']} applied: {applied}; windows restated {restated} "
                                             f"(outside the plan: {outside}); visit compliance windows equal the "
                                             f"corrected truth: {equal}")
        if det == "LATE-02" and int(d["appears_day"]) == n and not any(
                x["detector"] == "LATE-02" for x in today.notes.get("findings") or []):
            if today.notes.get("late02_closed"):  # planted on several days, closed once
                return None
            orphans, rows = today.measures[VISITS]["orphan_rows"], today.measures[VISITS]["rows"]
            seen = [b.notes.get("visit_orphans", 0) for b in before
                    if any(x["incident"] == f["incident"] for x in b.notes.get("findings") or [])]
            ok = orphans[0] == 0 and rows[0] == rows[1] and all(s > 0 for s in seen)
            today.notes["late02_closed"] = True
            return self._v(n, f, VISITS, ok, f"{d['site']} arrived: visits on the unknown member now "
                                             f"{int(orphans[0])} (were {seen}); rows {int(rows[0])} = oracle "
                                             f"{int(rows[1])}")
        return None
