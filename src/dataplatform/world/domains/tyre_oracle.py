"""The tyre world's oracle: per simulated day, what a correct platform holds at bronze, silver and gold, and what
each certified metric must say. Computed only from the world's own truth (R9, SIM-01).

Two numbers where they can differ:
    expected   what a correct platform shows, given what reached it (the 7 rows the change feed skipped never do;
               the lab files with the renamed column are rightly stopped)
    truth      what really happened at the source. The gap is what the reconciliation detectors must find.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from dataplatform.world.domains.tyre import Line, TyreCompany

from dataplatform.world.domains.tyre import DEALER, DLV, INSP, MAT, PRESS, PROD, READ, SELL, SOL


def day_oracle(m: "TyreCompany", d: int) -> dict[str, Any]:
    today = m.clock.date(d)
    otif = _otif(m, d, today)
    scrap = _scrap(m, d)
    sales = _sales(m, d)
    return {
        "day": d,
        "date": today.isoformat(),
        "sources": m.ledger[d]["source"],
        "bronze": m.ledger[d]["bronze"],
        "silver": _silver(m, d),
        "gold": {"fact_otif": otif["fact"], "fact_scrap": scrap["fact"], "fact_sales": sales["fact"]},
        "metrics": {**otif["metrics"], **scrap["metrics"], **sales["metrics"]},
        "known_answers": known_answers(m, {**otif["metrics"], **scrap["metrics"], **sales["metrics"]}),
        "findings": m.findings[d],
        "clean_day": not m.findings[d],
    }


# ---------------------------------------------------------------- silver
def measures(m: "TyreCompany", ds: str, rows: Iterable[dict[str, Any]]) -> dict[str, float]:
    """The dataset manifest's own reconciliation measures, so the oracle and the platform count the same things."""
    rows = list(rows)
    out: dict[str, float] = {}
    for meas in m.dwd.datasets[ds].manifest["reconciliation"]["measures"]:
        col = meas.get("column")
        out[meas["name"]] = len(rows) if not col else round(sum(float(r[col] or 0) for r in rows), 2)
    return out


def _silver(m: "TyreCompany", d: int) -> dict[str, Any]:
    lines = list(m.lines.values())
    current = [ln.row for ln in lines if not ln.deleted]
    dl_all = list(m.deliveries.values())
    dl_seen = [x for x in dl_all if not x["lost"]]
    insp_sent = [x for x in m.inspections.values() if x["filed_day"] is not None]
    insp_ok = [x for x in insp_sent if not x["rejected"]]
    reads = [m.readings[k] for k in sorted(m.readings) if k <= d]
    return {
        SOL: {
            **measures(m, SOL, current),
            "qty_total": sum(r["ordered_qty"] for r in current),
            "deleted_marked": sum(ln.deleted for ln in lines),
            "versions": sum(ln.versions for ln in lines),
        },
        DLV: {
            **measures(m, DLV, dl_seen),
            "truth": measures(m, DLV, dl_all),
            "missing_keys": sorted(x["delivery_id"] for x in dl_all if x["lost"]),
        },
        MAT: {"row_count": len(m._last_copy[MAT]), "versions": m.material_versions},
        PRESS: {"row_count": len(m._last_copy[PRESS]), "versions": m.press_versions},
        PROD: measures(m, PROD, m.production),
        INSP: {
            **measures(m, INSP, insp_ok),
            "truth": measures(m, INSP, insp_sent),
            "held_back_rows": len(insp_sent) - len(insp_ok),
        },
        SELL: {"row_count": len(m.sellout), "units_total": float(sum(m.sellout.values()))},
        DEALER: {"row_count": len(m.dealer_seen), "versions": m.dealer_versions},
        READ: {
            "row_count": sum(r["unique"] for r in reads),
            "arrived_total": sum(r["arrived"] for r in reads),
            "today_by_event_hour": m.readings[d]["by_hour"],
        },
    }


# ---------------------------------------------------------------- gold: OTIF
def _local_date(m: "TyreCompany", ts: str) -> date:
    return datetime.fromisoformat(ts).astimezone(m.clock.tz).date()


def _otif(m: "TyreCompany", d: int, today: date) -> dict[str, Any]:
    delivered = {x["line"]: x for x in m.deliveries.values()}
    windows: dict[str, dict[str, Any]] = {}
    month: dict[tuple[str, str], list[int]] = {}  # (promised month, region) -> [lines, otif, otif_truth]
    for ln in m.lines.values():
        if ln.deleted or ln.row["line_status"] != "open":  # OTIF measures open lines only (fact_otif contract)
            continue
        p = m.promise(ln, d)
        x = delivered.get(ln.key)
        hit_truth = _hit(m, ln, p, x)
        hit = hit_truth and not x["lost"]  # type: ignore[index]
        w = windows.setdefault(
            p.isoformat(),
            {
                "lines": 0,
                "ordered_qty_total": 0,
                "otif_lines": 0,
                "otif_lines_truth": 0,
                "orphan_lines": 0,
                "complete": p <= today,
            },
        )
        w["lines"] += 1
        w["ordered_qty_total"] += ln.row["ordered_qty"]
        w["otif_lines"] += hit
        w["otif_lines_truth"] += hit_truth
        w["orphan_lines"] += ln.row["dealer_code"] not in m.dealer_seen
        if p <= today:
            for region in ("all", ln.region):
                acc = month.setdefault((p.strftime("%Y-%m"), region), [0, 0, 0])
                acc[0], acc[1], acc[2] = acc[0] + 1, acc[1] + hit, acc[2] + hit_truth
    for w in windows.values():
        w["otif_pct"] = _pct(w["otif_lines"], w["lines"])
    rows = sum(w["lines"] for w in windows.values())
    orphans = sum(w["orphan_lines"] for w in windows.values())
    fact = {
        "rows": rows,
        "ordered_qty_total": sum(w["ordered_qty_total"] for w in windows.values()),
        "orphan_lines": orphans,
        "orphan_rate": _pct(orphans, rows),
        "by_promised_date": dict(sorted(windows.items())),
    }
    otif_pct = {
        f"{mo}/{rg}": {"expected": _pct(a[1], a[0]), "truth": _pct(a[2], a[0]), "lines": a[0]}
        for (mo, rg), a in sorted(month.items())
    }
    lines_by_month: dict[str, int] = {}
    for p, w in windows.items():
        lines_by_month[p[:7]] = lines_by_month.get(p[:7], 0) + w["lines"]
    return {"fact": fact, "metrics": {"OTIF %": otif_pct, "Order lines": lines_by_month}}


def _hit(m: "TyreCompany", ln: "Line", promised: date, x: dict[str, Any] | None) -> bool:
    return bool(x) and _local_date(m, x["delivered_ts"]) <= promised and x["delivered_qty"] >= ln.row["ordered_qty"]  # type: ignore[index]


# ---------------------------------------------------------------- gold: scrap
def _scrap(m: "TyreCompany", d: int) -> dict[str, Any]:
    days: dict[tuple[str, str], dict[str, Any]] = {}
    for p in m.production:
        insp = m.inspections[f"IN-{p['production_id'][3:]}"]
        k = (m.clock.date(p["day"]).isoformat(), p["plant"])
        w = days.setdefault(
            k, {"grain_rows": 0, "tyres_cured": 0, "tyres_scrapped": 0, "tyres_scrapped_truth": 0, "complete": True}
        )
        seen = insp["filed_day"] is not None and not insp["rejected"]
        w["grain_rows"] += 1
        w["tyres_cured"] += p["tyres_cured"]
        w["tyres_scrapped"] += insp["tyres_scrapped"] if seen else 0
        w["tyres_scrapped_truth"] += insp["tyres_scrapped"]
        w["complete"] = w["complete"] and seen
    by_day = {
        f"{day}/{plant}": {
            **w,
            "scrap_rate": _pct(w["tyres_scrapped"], w["tyres_cured"]),
            "scrap_rate_truth": _pct(w["tyres_scrapped_truth"], w["tyres_cured"]),
        }
        for (day, plant), w in sorted(days.items())
    }
    fact = {
        "rows": sum(w["grain_rows"] for w in days.values()),
        "tyres_cured_total": sum(w["tyres_cured"] for w in days.values()),
        "tyres_scrapped_total": sum(w["tyres_scrapped"] for w in days.values()),
        "by_day_plant": by_day,
    }
    metrics = {
        "Scrap rate %": {
            k: {"expected": v["scrap_rate"], "truth": v["scrap_rate_truth"], "complete": v["complete"]}
            for k, v in by_day.items()
        },
        "Tyres cured": {k: v["tyres_cured"] for k, v in by_day.items()},
    }
    return {"fact": fact, "metrics": metrics}


# ---------------------------------------------------------------- gold: sales
def _sales(m: "TyreCompany", d: int) -> dict[str, Any]:
    region = {code: row["region"] for code, row in m.dealers.items()}
    late = m.incident.get("late_dimension")
    if late:
        region.setdefault(late["dealer"], late["region"])
    acc: dict[tuple[str, str], dict[str, float]] = {}

    def cell(month: str, rg: str) -> dict[str, float]:
        return acc.setdefault((month, rg), {"units_sold_in": 0, "units_sold_out": 0, "net_value": 0.0})

    for ln in m.lines.values():
        if ln.deleted or ln.row["line_status"] != "open":  # a cancelled or rejected line is not sold in
            continue
        mo = _local_date(m, ln.row["order_ts"]).strftime("%Y-%m")
        for rg in ("all", ln.region):
            c = cell(mo, rg)
            c["units_sold_in"] += ln.row["ordered_qty"]
            c["net_value"] = round(c["net_value"] + ln.row["net_value"], 2)
    for (dealer, sale_month, _), units in m.sellout.items():
        for rg in ("all", region[dealer]):
            cell(sale_month[:7], rg)["units_sold_out"] += units
    sent = {f["dealer"] for f in m.sellout_files.values()}
    completeness = {}  # only months whose partner files are due inside the world's clock
    for mo, due in sorted(m.sellout_due.items()):
        reported = len({f["dealer"] for f in m.sellout_files.values() if f["month"] == mo})
        completeness[mo] = {"dealers_reported": reported, "dealers_expected": due, "complete": reported >= due}
    # A late file restates its month on the day the month first becomes complete (every owed file in), which is the
    # file's own day only when it is the last one owed; gold holds an incomplete month until then (ADR-018).
    late = {f["detail"]["month"] for day in range(1, d + 1) for f in m.findings.get(day, [])
            if f["detector"] == "LATE-01"}

    def reported_by(mo: str, day: int) -> int:
        return len({f["dealer"] for f in m.sellout_files.values() if f["month"] == mo and f["day"] <= day})

    restated = sorted(mo for mo, due in m.sellout_due.items()
                      if mo in late and reported_by(mo, d) >= due > reported_by(mo, d - 1))
    fact = {
        "by_month_region": {f"{mo}/{rg}": v for (mo, rg), v in sorted(acc.items())},
        "sellout_completeness": completeness,
        "restatements_today": restated,
        "units_out_total": float(sum(m.sellout.values())),
        "dealers_with_sellout": len(sent),
    }
    metrics = {
        "Sell-in units": {f"{mo}/{rg}": v["units_sold_in"] for (mo, rg), v in sorted(acc.items())},
        "Sell-out units": {f"{mo}/{rg}": v["units_sold_out"] for (mo, rg), v in sorted(acc.items())},
    }
    return {"fact": fact, "metrics": metrics}


# ---------------------------------------------------------------- the metric views' known answers
_TIME = ("promised month", "month", "day")
_GROUP = ("region", "plant")


def _where(text: str) -> dict[str, str]:
    """`region = 'south' AND promised month = 2026-11` -> {"region": "south", "promised month": "2026-11"}."""
    out = {}
    for part in re.split(r"\s+AND\s+", text.strip(), flags=re.IGNORECASE):
        name, _, value = part.partition("=")
        out[name.strip().lower()] = value.strip().strip("'\"")
    return out


def known_answers(m: "TyreCompany", metrics: dict[str, Any]) -> dict[str, Any]:
    """Every known answer the metric views carry (ADR-022), beside what the world produced for the same question.
    The questions and values are read from the metric view files, so the oracle never restates them."""
    out: dict[str, Any] = {}
    for view, spec in sorted(m.dwd.metrics.items()):
        for measure in spec.get("measures") or []:
            ka = measure.get("known_answer")
            if not ka or measure["name"] not in metrics:
                continue
            where = _where(ka["where"])
            t = next(where[k] for k in _TIME if k in where)
            g = next((where[k] for k in _GROUP if k in where), None)
            got = metrics[measure["name"]]
            value = next((got[k] for k in ([f"{t}/{g}"] if g else [t, f"{t}/all"]) if k in got), None)
            if isinstance(value, dict):
                value = value.get("expected")
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
