"""The tyre world's gold judge: published gold against the oracle's gold for the day, and the incidents gold owns.

Reads gold only through the GoldReader port (SQL over the gold tables by name) and the oracle's day file; never the
platform's own logs for the numbers (R9). The run report is used only for what gold says it did (restatements, the
correction plans it applied), which is what the incidents judge.

    fact_otif   rows, ordered quantity, lines on the unknown member; per promised date: lines, quantity, OTIF lines,
                orphans and complete
    fact_scrap  rows, tyres cured and scrapped; per business day and plant: rows, cured, scrapped and complete
    fact_sales  sell-in, sell-out and net value per month and region (cells holding rows on the unknown member are
                explained by LATE-02 while the dealer is missing); October's completeness; restatements today

Incidents: DEL-01 (the deleted lines are gone from gold), LATE-01 (the late month flagged until its file, then
restated and complete), DUP-01 (a resent file changes no gold number and restates nothing), HIST-05 (on the plan's
day only its windows restate and OTIF equals the corrected truth), LATE-02 (no row dropped; the new dealer's lines on
-1 while it is missing, re-keyed the day it arrives).
"""

from __future__ import annotations

from typing import Any

from dataplatform.gold.daybyday import GoldDay
from dataplatform.gold.harness import GoldReport
from dataplatform.gold.ports import GoldReader
from dataplatform.ingest.daybyday import Verdict

OTIF, SALES, SCRAP = "fact_otif", "fact_sales", "fact_scrap"
NET_TOLERANCE = 0.05

Q_OTIF = """
SELECT date_format(promised_date, 'yyyy-MM-dd') AS w, COUNT(1) AS lines, SUM(ordered_qty) AS qty,
       SUM(on_time_in_full) AS otif, SUM(CASE WHEN dealer_sk = -1 THEN 1 ELSE 0 END) AS orphans,
       MIN(CAST(_is_complete AS INT)) AS complete
FROM fact_otif GROUP BY 1
"""
Q_SCRAP = """
SELECT date_format(from_utc_timestamp(f.cure_hour, :tz), 'yyyy-MM-dd') AS d, p.plant_code AS plant, COUNT(1) AS n,
       SUM(f.tyres_cured) AS cured, SUM(f.tyres_scrapped) AS scrapped, MIN(CAST(f._is_complete AS INT)) AS complete
FROM fact_scrap AS f JOIN dim_press AS p ON f.press_sk = p.press_sk GROUP BY 1, 2
"""
Q_SALES = """
SELECT date_format(f.sale_month, 'yyyy-MM') AS m, d.region AS region, SUM(f.units_sold_in) AS sold_in,
       SUM(f.units_sold_out) AS sold_out, SUM(f.net_value) AS net, SUM(CASE WHEN f.dealer_sk = -1 THEN 1 ELSE 0 END)
       AS orphans, MIN(CAST(f._is_complete AS INT)) AS complete, COUNT(DISTINCT CASE WHEN f.units_sold_out > 0 THEN
       f.dealer_code END) AS dealers_out
FROM fact_sales AS f JOIN dim_dealer AS d ON f.dealer_sk = d.dealer_sk GROUP BY 1, 2
"""


def _rows(reader: GoldReader, sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """A table gold has not published yet reads as no rows (the comparison then shows what is missing)."""
    try:
        return reader.rows(sql, params)
    except Exception as exc:  # the engine's "no such table": gold failed or has not run yet
        if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
            return []
        raise


def _f(x: Any) -> float:
    return float(x or 0)


class TyreGoldJudge:
    def __init__(self, tz: str):
        self.tz = tz

    # ------------------------------------------------------------------------------------------------ compare
    def compare(self, oracle: dict[str, Any], reader: GoldReader, report: GoldReport) -> tuple[
            dict[str, dict[str, tuple[float, float]]], dict[str, Any]]:
        want = oracle.get("gold") or {}
        notes: dict[str, Any] = {"findings": oracle.get("findings") or []}
        out = {OTIF: self._otif(want.get(OTIF) or {}, reader, notes),
               SCRAP: self._scrap(want.get(SCRAP) or {}, reader, notes),
               SALES: self._sales(want.get(SALES) or {}, reader, report, notes)}
        return out, notes

    def _otif(self, want: dict[str, Any], reader: GoldReader, notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {r["w"]: r for r in _rows(reader, Q_OTIF)}
        exp = want.get("by_promised_date") or {}
        bad = []
        for w in sorted(set(got) | set(exp)):
            g, e = got.get(w) or {}, exp.get(w) or {}
            pairs = [(_f(g.get("lines")), _f(e.get("lines"))), (_f(g.get("qty")), _f(e.get("ordered_qty_total"))),
                     (_f(g.get("otif")), _f(e.get("otif_lines"))), (_f(g.get("orphans")), _f(e.get("orphan_lines"))),
                     (_f(g.get("complete")), float(bool(e.get("complete"))))]
            if any(a != b for a, b in pairs):
                bad.append(w)
        notes["otif_differing"] = bad
        notes["otif_windows"] = {w: [_f(r["lines"]), _f(r["qty"]), _f(r["otif"])] for w, r in got.items()}
        orphans = sum(_f(r["orphans"]) for r in got.values())
        notes["otif_orphans"] = orphans
        return {"rows": (sum(_f(r["lines"]) for r in got.values()), _f(want.get("rows"))),
                "ordered_qty_total": (sum(_f(r["qty"]) for r in got.values()), _f(want.get("ordered_qty_total"))),
                "orphan_lines": (orphans, _f(want.get("orphan_lines"))),
                "windows_differing": (float(len(bad)), 0.0)}

    def _scrap(self, want: dict[str, Any], reader: GoldReader, notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        got = {f"{r['d']}/{r['plant']}": r for r in _rows(reader, Q_SCRAP, {"tz": self.tz})}
        exp = want.get("by_day_plant") or {}
        bad = []
        for k in sorted(set(got) | set(exp)):
            g, e = got.get(k) or {}, exp.get(k) or {}
            pairs = [(_f(g.get("n")), _f(e.get("grain_rows"))), (_f(g.get("cured")), _f(e.get("tyres_cured"))),
                     (_f(g.get("scrapped")), _f(e.get("tyres_scrapped"))),
                     (_f(g.get("complete")), float(bool(e.get("complete"))))]
            if any(a != b for a, b in pairs):
                bad.append(k)
        notes["scrap_differing"] = bad
        return {"rows": (sum(_f(r["n"]) for r in got.values()), _f(want.get("rows"))),
                "tyres_cured_total": (sum(_f(r["cured"]) for r in got.values()), _f(want.get("tyres_cured_total"))),
                "tyres_scrapped_total": (sum(_f(r["scrapped"]) for r in got.values()),
                                         _f(want.get("tyres_scrapped_total"))),
                "day_plants_differing": (float(len(bad)), 0.0)}

    def _sales(self, want: dict[str, Any], reader: GoldReader, report: GoldReport,
               notes: dict[str, Any]) -> dict[str, tuple[float, float]]:
        rows = _rows(reader, Q_SALES)
        cells: dict[str, list[float]] = {}
        orphan_cells = set()
        for r in rows:
            for rg in ("all", r["region"]):
                c = cells.setdefault(f"{r['m']}/{rg}", [0.0, 0.0, 0.0])
                c[0], c[1], c[2] = c[0] + _f(r["sold_in"]), c[1] + _f(r["sold_out"]), c[2] + _f(r["net"])
            if _f(r["orphans"]):
                orphan_cells.add(r["m"])
        exp = want.get("by_month_region") or {}
        bad: list[str] = []
        explained: list[str] = []
        for k in sorted(set(cells) | set(exp)):
            g, e = cells.get(k, [0.0, 0.0, 0.0]), exp.get(k) or {}
            same = (g[0] == _f(e.get("units_sold_in")) and g[1] == _f(e.get("units_sold_out"))
                    and abs(g[2] - _f(e.get("net_value"))) <= NET_TOLERANCE)
            if not same:
                (explained if k.split("/")[0] in orphan_cells and not k.endswith("/all") else bad).append(k)
        month_done: dict[str, bool] = {}
        for r in rows:
            month_done[r["m"]] = month_done.get(r["m"], True) and bool(r["complete"])
        notes.update(sales_differing=bad, sales_explained_by_late_dimension=explained, sales_complete=month_done,
                     sales_out_by_month={m: cells[f"{m}/all"][1] for m in month_done if f"{m}/all" in cells})
        comp_bad = [m for m, c in (want.get("sellout_completeness") or {}).items()
                    if month_done.get(m, False) != bool(c.get("complete"))]
        notes["oracle_month_complete"] = {m: bool(c.get("complete")) for m, c in
                                          (want.get("sellout_completeness") or {}).items()}
        t = report.by_name().get(SALES)
        restated = sorted({r.window[:7] for r in (t.restated if t else []) if r.detector == "LATE-01"})
        notes["sales_restated"] = restated
        dealers = int((_rows(reader, "SELECT COUNT(DISTINCT dealer_code) AS n FROM fact_sales WHERE units_sold_out > 0")
                       or [{"n": 0}])[0]["n"])
        return {"cells_differing": (float(len(bad)), 0.0),
                "completeness_differing": (float(len(comp_bad)), 0.0),
                "restatements_differing": (float(len(set(restated) ^ set(want.get("restatements_today") or []))),
                                           0.0),
                "units_out_total": (sum(_f(r["sold_out"]) for r in rows), _f(want.get("units_out_total"))),
                "dealers_with_sellout": (float(dealers), _f(want.get("dealers_with_sellout")))}

    # ------------------------------------------------------------------------------------------------ judge
    def judge(self, n: int, oracle: dict[str, Any], today: GoldDay, before: list[GoldDay],
              reader: GoldReader) -> list[Verdict]:
        out: list[Verdict] = []
        for f in oracle.get("findings") or []:
            v = self._today(n, f, today, before, reader)
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

    def _today(self, n: int, f: dict[str, Any], today: GoldDay, before: list[GoldDay],
               reader: GoldReader) -> Verdict | None:
        det, d = f["detector"], f["detail"]
        if det == "DEL-01":
            keys = ", ".join(f"'{str(k[0]).replace(chr(39), '')}/{int(k[1])}'" for k in d["keys"]) or "''"
            # shiftleft: allow SL-SEC-01 -- the world's own keys, quotes stripped and line numbers cast to int (a test oracle)
            left = int((_rows(reader, "SELECT COUNT(1) AS n FROM fact_otif WHERE "
                              f"concat(order_id, '/', CAST(line_no AS STRING)) IN ({keys})") or [{"n": -1}])[0]["n"])
            rows = today.measures[OTIF]["rows"]
            return self._v(n, f, OTIF, left == 0 and rows[0] == rows[1],
                           f"{left} of {len(d['keys'])} deleted lines in fact_otif; fact_otif rows {int(rows[0])}, "
                           f"oracle {int(rows[1])}")
        if det == "LATE-01":
            month = str(d["month"])
            earlier = [b.notes.get("sales_complete", {}).get(month) for b in before]
            flagged = bool(earlier) and all(x is False for x in earlier)
            done = today.notes.get("sales_complete", {}).get(month) is True
            restated = month in today.notes.get("sales_restated", [])
            if not today.notes.get("oracle_month_complete", {}).get(month, True):  # other owed files still to come
                held = not done and not restated
                return self._v(n, f, SALES, held,
                               f"{month}: file loaded, other dealers' files still owed; gold holds the month "
                               f"incomplete: {not done}; restated early: {restated}", "flagged" if held else "missed")
            return self._v(n, f, SALES, flagged and done and restated,
                           f"{month} flagged incomplete on days {before[0].day}-{before[-1].day if before else '?'}: "
                           f"{flagged}; complete today: {done}; restated (LATE-01) today: {restated}")
        if det == "DUP-01":
            month = next((m for b in before for m in b.notes.get("sales_restated", [])), "")
            prev = before[-1].notes.get("sales_out_by_month", {}) if before else {}
            now = today.notes.get("sales_out_by_month", {})
            same = all(now.get(m) == v for m, v in prev.items())
            none = not today.notes.get("sales_restated")
            return self._v(n, f, SALES, same and none, f"sell-out per month unchanged since yesterday: {same} "
                                                       f"({month or 'October'} {now.get(month)}); restatements "
                                                       f"today: {today.notes.get('sales_restated')}")
        if det == "LATE-02":
            orphans, rows = today.measures[OTIF]["orphan_lines"], today.measures[OTIF]["rows"]
            ok = orphans[0] == orphans[1] > 0 and rows[0] == rows[1]
            return self._v(n, f, OTIF, ok, f"{int(orphans[0])} lines of {d['dealer']} on the unknown member "
                                           f"(oracle {int(orphans[1])}); rows {int(rows[0])} = oracle {int(rows[1])}",
                           "flagged" if ok else "missed")
        return None

    def _later(self, n: int, f: dict[str, Any], today: GoldDay, before: list[GoldDay]) -> Verdict | None:
        det, d = f["detector"], f["detail"]
        if det == "HIST-05" and int(d["activate_day"]) == n:
            t = today.report.by_name().get(OTIF)
            applied = bool(t) and d["plan_id"] in t.plans_applied  # type: ignore[union-attr]
            restated = sorted(r.window for r in (t.restated if t else []))
            outside = sorted(set(restated) - set(d["windows"]))
            late01 = [r.window for r in (t.restated if t else []) if r.detector != "HIST-05"]
            equal = not today.notes.get("otif_differing")
            ok = applied and not outside and not late01 and equal
            return self._v(n, f, OTIF, ok, f"plan {d['plan_id']} applied: {applied}; windows restated {restated} "
                                           f"(outside the plan: {outside}); OTIF windows equal the corrected truth: "
                                           f"{equal}")
        if det == "LATE-02" and int(d["appears_day"]) == n and not any(
                x["detector"] == "LATE-02" for x in today.notes.get("findings") or []):
            if today.notes.get("late02_closed"):  # planted on several days, closed once
                return None
            orphans, rows = today.measures[OTIF]["orphan_lines"], today.measures[OTIF]["rows"]
            seen = [b.notes.get("otif_orphans", 0) for b in before
                    if any(x["incident"] == f["incident"] for x in b.notes.get("findings") or [])]
            ok = orphans[0] == 0 and rows[0] == rows[1] and all(s > 0 for s in seen)
            today.notes["late02_closed"] = True
            return self._v(n, f, OTIF, ok, f"{f['detail']['dealer']} arrived: lines on the unknown member now "
                                           f"{int(orphans[0])} (were {seen}); rows {int(rows[0])} = oracle "
                                           f"{int(rows[1])}")
        return None
