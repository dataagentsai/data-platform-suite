"""The clinical trial's oracle: per simulated day, what a correct platform holds at bronze, silver and gold, and what each
certified metric must say. Computed only from the world's own truth (R9, SIM-01).

Two numbers where they can differ:
    expected   what a correct platform shows, given what reached it (the 3 dispensing rows the change feed skipped never
               do; the laboratory file with the renamed column is rightly stopped)
    truth      what really happened at the source. The gap is what the reconciliation detectors must find.
"""

from __future__ import annotations

import re
from datetime import date
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from dataplatform.world.domains.clinical import ClinicalTrial, Visit

from dataplatform.world.domains.clinical import AE, DISP, KIT, LAB, LOG, SITE, SUBJ, TEMP, VISIT

TIME_KEY = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")
UNKNOWN_COUNTRY = "unknown"


def day_oracle(m: "ClinicalTrial", d: int) -> dict[str, Any]:
    today = m.clock.date(d)
    visits, lab = _visits(m, d, today), _lab(m, d, today)
    enrol, temp = _enrolment(m, d), _temperature(m, d, today)
    metrics = {**visits["metrics"], **lab["metrics"], **enrol["metrics"], **temp["metrics"]}
    return {
        "day": d,
        "date": today.isoformat(),
        "sources": m.ledger[d]["source"],
        "bronze": m.ledger[d]["bronze"],
        "silver": _silver(m, d),
        "gold": {"fact_visit_compliance": visits["fact"], "fact_lab_out_of_range": lab["fact"],
                 "fact_enrolment": enrol["fact"], "fact_temperature_excursion": temp["fact"]},
        "metrics": metrics,
        "known_answers": known_answers(m, metrics),
        "findings": m.findings[d],
        "clean_day": not m.findings[d],
    }


# ---------------------------------------------------------------- silver
def measures(m: "ClinicalTrial", ds: str, rows: Iterable[Any]) -> dict[str, float]:
    """The dataset manifest's own reconciliation measures, so the oracle and the platform count the same things."""
    rows = list(rows)  # dict rows, or the model's own subject objects (counted, never summed)
    out: dict[str, float] = {}
    for meas in m.dwd.datasets[ds].manifest["reconciliation"]["measures"]:
        col = meas.get("column")
        out[meas["name"]] = len(rows) if not col else round(sum(float(r[col] or 0) for r in rows), 2)
    return out


def _silver(m: "ClinicalTrial", d: int) -> dict[str, Any]:
    visits = [v for v in m.visits if v.created_day <= d]
    aes = [a["row"] for a in m.aes.values() if a["day"] <= d]
    lab_rows = [r for g in m.lab.values() if g["arrive_day"] <= d and g["accepted"] for r in g["rows"]]
    lab_sent = [r for g in m.lab.values() if g["arrive_day"] <= d for r in g["rows"]]
    logs = {(r["site_id"], r["enrolment_month"], r["cohort"]): r for f in m.log_files.values() if f["day"] <= d
            for r in f["rows"]}
    disp_all = [x for x in m.dispensings.values() if x["day"] <= d]
    disp_seen = [x for x in disp_all if not x["lost"]]
    reads = [m.readings[k] for k in sorted(m.readings) if k <= d]
    return {
        SUBJ: {**measures(m, SUBJ, m.present(d)), "versions": m.subject_versions,
               "deleted_marked": sum(s.deleted_day is not None and s.deleted_day <= d for s in m.subjects)},
        VISIT: {**measures(m, VISIT, [m._visit_row(v, d) for v in visits]), "versions": len(visits)},
        AE: {**measures(m, AE, aes), "versions": sum(a["versions"] for a in m.aes.values() if a["day"] <= d)},
        LAB: {**measures(m, LAB, lab_rows), "truth": measures(m, LAB, lab_sent),
              "held_back_rows": len(lab_sent) - len(lab_rows)},
        LOG: measures(m, LOG, logs.values()),
        KIT: {"row_count": len(m.kits), "versions": m.kit_versions},
        DISP: {**measures(m, DISP, disp_seen), "truth": measures(m, DISP, disp_all),
               "missing_keys": sorted(x["dispensing_id"] for x in disp_all if x["lost"])},
        SITE: {"row_count": sum(1 for s in m.sites if m.sites_seen.get(s, 10**6) <= d), "versions": m.site_versions},
        TEMP: {
            "row_count": sum(r["unique"] for r in reads),
            "arrived_total": sum(r["arrived"] for r in reads),
            "today_by_event_hour": m.readings[d]["by_hour"],
        },
    }


# ---------------------------------------------------------------- gold: visit compliance
def _country(m: "ClinicalTrial", site: str, d: int) -> str:
    return m.sites[site]["country"] if m.sites_seen.get(site, 10**6) <= d else UNKNOWN_COUNTRY


def _compliant(m: "ClinicalTrial", v: "Visit", planned: date, d: int) -> tuple[bool, int]:
    kits = 1 if v.disp and v.disp.get("id") and v.disp["day"] <= d and not m.dispensings[v.disp["id"]]["lost"] else 0
    done = v.status(d) == "completed"
    in_window = bool(done and v.actual is not None and abs((v.actual - planned).days) <= v.window)
    ok = in_window and v.forms_rec >= v.forms_exp and kits >= v.kits_exp
    return ok, kits


def _visits(m: "ClinicalTrial", d: int, today: date) -> dict[str, Any]:
    windows: dict[str, dict[str, Any]] = {}
    acc: dict[tuple[str, str], list[int]] = {}  # (planned month, country) -> [rows, compliant, complete rows, compliant complete]
    for v in m.visits:
        if v.created_day > d or v.status(d) == "cancelled":
            continue
        p = m.planned(v, d)
        ok, kits = _compliant(m, v, p, d)
        w = windows.setdefault(p.isoformat(), {"rows": 0, "forms_expected_total": 0, "compliant": 0,
                                               "kits_dispensed": 0, "orphan_rows": 0, "complete": p <= today})
        w["rows"] += 1
        w["forms_expected_total"] += v.forms_exp
        w["compliant"] += ok
        w["kits_dispensed"] += kits
        country = _country(m, v.subj.site, d)
        w["orphan_rows"] += country == UNKNOWN_COUNTRY
        for c in ("all", country):
            a = acc.setdefault((p.strftime("%Y-%m"), c), [0, 0, 0, 0])
            a[0] += 1
            a[1] += ok
            if p <= today:
                a[2] += 1
                a[3] += ok
    rows = sum(w["rows"] for w in windows.values())
    orphans = sum(w["orphan_rows"] for w in windows.values())
    fact = {"rows": rows, "forms_expected_total": sum(w["forms_expected_total"] for w in windows.values()),
            "compliant_total": sum(w["compliant"] for w in windows.values()),
            "kits_dispensed_total": sum(w["kits_dispensed"] for w in windows.values()),
            "orphan_rows": orphans, "by_planned_date": dict(sorted(windows.items()))}
    metrics = {
        "Visit compliance %": {f"{mo}/{c}": _pct(a[3], a[2]) for (mo, c), a in sorted(acc.items())},
        "Planned visits": {f"{mo}/{c}": a[0] for (mo, c), a in sorted(acc.items())},
    }
    return {"fact": fact, "metrics": metrics}


# ---------------------------------------------------------------- gold: laboratory
def _lab(m: "ClinicalTrial", d: int, today: date) -> dict[str, Any]:
    windows: dict[str, dict[str, Any]] = {}
    acc: dict[tuple[str, str], list[int]] = {}  # (collection month, country) -> [reported, out of range, complete ones]
    for (site, day), g in sorted(m.lab.items()):
        if day > d:
            continue
        got = g["arrive_day"] <= d and g["accepted"]
        reported = len(g["rows"]) if got else 0
        oor = sum(r["out_of_range"] for r in g["rows"]) if got else 0
        w = windows.setdefault(m.clock.date(day).isoformat(),
                               {"rows": 0, "results_expected": 0, "results_reported": 0, "results_out_of_range": 0,
                                "complete": True})
        w["rows"] += 1
        w["results_expected"] += g["expected"]
        w["results_reported"] += reported
        w["results_out_of_range"] += oor
        w["complete"] = w["complete"] and reported >= g["expected"] and m.clock.date(day) <= today
        row_complete = reported >= g["expected"] and m.clock.date(day) <= today  # a row waits on its own file
        for c in ("all", _country(m, site, d)):
            a = acc.setdefault((m.clock.date(day).strftime("%Y-%m"), c), [0, 0, 0, 0])
            a[0] += reported
            a[1] += oor
            a[2] += reported if row_complete else 0
            a[3] += oor if row_complete else 0
    fact = {k: sum(w[k] for w in windows.values())
            for k in ("rows", "results_expected", "results_reported", "results_out_of_range")}
    fact["by_collection_date"] = dict(sorted(windows.items()))
    metrics = {
        "Out of range share": {f"{mo}/{c}": _pct(a[3], a[2]) for (mo, c), a in sorted(acc.items())},
        "Results reported": {f"{mo}/{c}": a[0] for (mo, c), a in sorted(acc.items())},
    }
    return {"fact": fact, "metrics": metrics}


# ---------------------------------------------------------------- gold: enrolment
def _enrolment(m: "ClinicalTrial", d: int) -> dict[str, Any]:
    sent = {(r["site_id"], r["enrolment_month"], r["cohort"]): (r, f["day"]) for f in m.log_files.values()
            if f["day"] <= d for r in f["rows"]}
    months: dict[str, dict[str, Any]] = {}
    acc: dict[tuple[str, str], list[int]] = {}
    sites_by_month: dict[str, set[str]] = {}
    for (site, month, _), (r, _day) in sorted(sent.items()):
        w = months.setdefault(month, {"rows": 0, "subjects_screened": 0, "subjects_randomised": 0})
        w["rows"] += 1
        w["subjects_screened"] += r["subjects_screened"]
        w["subjects_randomised"] += r["subjects_randomised"]
        sites_by_month.setdefault(month, set()).add(site)
    for month, w in months.items():
        w["complete"] = len(sites_by_month[month]) >= m.log_due
    for (site, month, _), (r, _day) in sorted(sent.items()):
        if not months[month]["complete"]:
            continue
        for c in ("all", _country(m, site, d)):
            a = acc.setdefault((month[:7], c), [0, 0])
            a[0] += r["subjects_screened"]
            a[1] += r["subjects_randomised"]
    fact = {"by_month": dict(sorted(months.items())),
            "rows": sum(w["rows"] for w in months.values()),
            "subjects_screened_total": sum(w["subjects_screened"] for w in months.values())}
    metrics = {
        "Subjects screened": {f"{mo}/{c}": a[0] for (mo, c), a in sorted(acc.items())},
        "Subjects randomised": {f"{mo}/{c}": a[1] for (mo, c), a in sorted(acc.items())},
    }
    return {"fact": fact, "metrics": metrics}


# ---------------------------------------------------------------- gold: temperature excursions
def _temperature(m: "ClinicalTrial", d: int, today: date) -> dict[str, Any]:
    windows: dict[str, dict[str, Any]] = {}
    for (_ship, day), (n, exc, top) in sorted(m.by_ship_day.items()):
        w = windows.setdefault(day, {"rows": 0, "readings": 0, "excursion_readings": 0, "max_temperature_c": 0.0,
                                     "complete": date.fromisoformat(day) <= today})
        w["rows"] += 1
        w["readings"] += n
        w["excursion_readings"] += exc
        w["max_temperature_c"] = max(w["max_temperature_c"], top)
    fact = {"rows": sum(w["rows"] for w in windows.values()),
            "readings_total": sum(w["readings"] for w in windows.values()),
            "excursion_readings_total": sum(w["excursion_readings"] for w in windows.values()),
            "by_reading_date": dict(sorted(windows.items()))}
    by_month: dict[str, int] = {}
    for day, w in windows.items():
        by_month[day[:7]] = by_month.get(day[:7], 0) + w["excursion_readings"]
    return {"fact": fact, "metrics": {"Excursion readings": {f"{mo}/all": v for mo, v in sorted(by_month.items())}}}


# ---------------------------------------------------------------- the metric views' known answers
def _where(text: str) -> dict[str, str]:
    """`country = 'IN' AND planned month = 2026-11` -> {"country": "IN", "planned month": "2026-11"}."""
    out = {}
    for part in re.split(r"\s+AND\s+", text.strip(), flags=re.IGNORECASE):
        name, _, value = part.partition("=")
        out[name.strip().lower()] = value.strip().strip("'\"")
    return out


def known_answers(m: "ClinicalTrial", metrics: dict[str, Any]) -> dict[str, Any]:
    """Every known answer the metric views carry (ADR-022), beside what the world produced for the same question.
    The questions and values are read from the metric view files, so the oracle never restates them."""
    out: dict[str, Any] = {}
    for view, spec in sorted(m.dwd.metrics.items()):
        for measure in spec.get("measures") or []:
            ka = measure.get("known_answer")
            if not ka or measure["name"] not in metrics:
                continue
            where = _where(ka["where"])
            t = next(v for v in where.values() if TIME_KEY.match(v))
            g = next((v for v in where.values() if not TIME_KEY.match(v)), "all")
            value = metrics[measure["name"]].get(f"{t}/{g}")
            tol = float(ka.get("tolerance", 0))
            out[f"{view} {measure['name']} {ka['where']}"] = {
                "spec": ka["value"],
                "world": value,
                "tolerance": tol,
                "match": value is not None and abs(float(value) - float(ka["value"])) <= tol,
            }
    return out


def _pct(n: float, total: float) -> float | None:
    return round(n / total, 4) if total else None
