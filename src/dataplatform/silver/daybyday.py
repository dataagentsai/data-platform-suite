"""Silver per world day: after the day's ingestion, one silver run takes the new bronze batches, and silver is compared
with the oracle's silver totals for the day (current rows, versions, deletes marked, and the manifest's reconciliation
measures). The incidents silver must apply are judged on what silver holds:

    DEL-01   the deleted keys are in silver, marked deleted (soft delete), not removed
    SCD-02   the change that arrived late sits in its place in history; the newer one stays current
    LATE-03  late readings counted in their event hour: silver per event hour equals the oracle's
    DUP-04   replayed readings dropped: silver holds each reading once

Domain-free: it knows DAC ids and the oracle's shape, not tyres.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from dataplatform.ingest.daybyday import Verdict
from dataplatform.meta.port import GOLD_KINDS, MetadataStore, StepRecord
from dataplatform.silver.live_recon import STEP as RECON_STEP
from dataplatform.silver.live_recon import LiveRecon, reconcile_live
from dataplatform.silver.plan import SilverPlan, plan_from_view
from dataplatform.silver.ports import SilverTables, StageRunner
from dataplatform.silver.runner import SilverReport, build_silver

TOLERANCE = 0.01  # sums are rounded to cents by both sides
BY_HOUR = "today_by_event_hour"
SILVER_DETECTORS = ("DEL-01", "SCD-02", "LATE-03", "DUP-04", "REC-05")


@dataclass
class SilverDay:
    day: int
    date: str
    report: SilverReport
    measures: dict[str, dict[str, tuple[float, float]]]  # dataset -> measure -> (silver, oracle)
    verdicts: list[Verdict] = field(default_factory=list)
    recon: dict[str, LiveRecon] = field(default_factory=dict)  # live source against silver, per dataset (REC-05)

    @property
    def differences(self) -> dict[str, dict[str, tuple[float, float]]]:
        return {
            ds: bad
            for ds, ms in self.measures.items()
            if (bad := {k: v for k, v in ms.items() if abs(v[0] - v[1]) > TOLERANCE})
        }

    @property
    def ok(self) -> bool:
        return not self.differences and self.report.status == "succeeded"


def plans(store: MetadataStore, domain: str, token_pattern: str) -> dict[str, SilverPlan]:
    return {
        f"{r.source}.{r.dataset}": plan_from_view(store.dataset_view(r.source, r.dataset), token_pattern)
        for r in store.datasets(domain)
        if r.is_enabled and r.kind not in GOLD_KINDS
    }


def _hours(by_hour: dict[str, Any]) -> tuple[str, str]:
    first = min(datetime.fromisoformat(h) for h in by_hour)
    last = max(datetime.fromisoformat(h) for h in by_hour) + timedelta(hours=1)
    return first.isoformat(), last.isoformat()


def hourly(tables: SilverTables, plan: SilverPlan, want: dict[str, Any]) -> dict[str, tuple[int, int]]:
    """Silver rows against the oracle's, per event hour (the oracle's hours only)."""
    start, end = _hours(want)
    got = tables.count_by_hour(plan, plan.event_time, start, end)
    norm = {datetime.fromisoformat(h): n for h, n in got.items()}
    return {h: (norm.get(datetime.fromisoformat(h), 0), int(n)) for h, n in want.items()}


def compare(oracle_silver: dict[str, Any], tables: SilverTables, ps: dict[str, SilverPlan]) -> dict[str, Any]:
    """Every number the oracle gives for silver that silver can count, per dataset, as (silver, oracle)."""
    out: dict[str, dict[str, tuple[float, float]]] = {}
    for name, want in sorted(oracle_silver.items()):
        plan = ps.get(name)
        if plan is None:
            continue
        got = tables.measures(plan, plan.measures)
        row = {k: (got[k], float(v)) for k, v in want.items() if k in got and isinstance(v, int | float)}
        if BY_HOUR in want and plan.event_time:
            per_hour = hourly(tables, plan, want[BY_HOUR])
            row["event_hours_differing"] = (float(sum(g != w for g, w in per_hour.values())), 0.0)
        out[name] = row
    return out


def judge(
    n: int, finding: dict[str, Any], tables: SilverTables, ps: dict[str, SilverPlan], day: SilverDay
) -> Verdict | None:
    det, ds = finding["detector"], finding["dataset"]
    if det not in SILVER_DETECTORS or ds not in ps:
        return None
    plan, detail = ps[ds], finding["detail"]

    def v(ok: bool, evidence: str) -> Verdict:
        return Verdict(n, finding["incident"], det, ds, "handled" if ok else "missed", evidence)

    if det == "DEL-01":
        rows = [tables.versions(plan, dict(zip(plan.keys, k, strict=True))) for k in detail["keys"]]
        marked = sum(1 for r in rows if r and any(x["_is_current"] and x["_is_deleted"] for x in r))
        return v(marked == len(rows), f"{marked} of {len(rows)} deleted keys kept in silver and marked deleted")
    if det == "SCD-02":
        history = tables.versions(plan, dict(zip(plan.keys, detail["key"], strict=True)))
        current = [r for r in history if r["_is_current"]]
        placed = any(not r["_is_current"] and r["_seq_from"] == detail["older_seq"] for r in history)
        now = str(current[0][_tracked_date(detail)]) if current else ""
        ok = placed and now == str(detail["current_promised_date"])
        return v(ok, f"{len(history)} versions; the late change (seq {detail['older_seq']}) "
                     f"{'is in history, not current' if placed else 'is missing or current'}; current value {now}")
    if det == "REC-05":
        r = day.recon.get(ds)
        want = {str(k) for k in detail.get("keys") or []}
        got = {"/".join(k) for k in r.missing} if r else set()
        ok = r is not None and not r.ok and got == want and not r.extra
        return v(ok, r.message() if r else "no live-source reconciliation ran for this dataset")
    if det == "LATE-03":
        bad = day.measures.get(ds, {}).get("event_hours_differing", (1.0, 0.0))[0]
        return v(bad == 0, f"{detail.get('late_readings', '?')} late readings; event hours differing from the "
                           f"oracle: {int(bad)}")
    counts = day.report.by_name().get(ds)
    c = counts.counts if counts else {}
    dropped = c.get("duplicates.replayed", 0) + c.get("duplicates.dropped", 0)
    same = day.measures.get(ds, {}).get("row_count", (0.0, 1.0))
    return v(dropped >= int(detail.get("replayed_readings", 0)) and same[0] == same[1],
             f"{dropped} duplicate or replayed rows dropped; silver rows {int(same[0])}, oracle {int(same[1])}")


def _tracked_date(detail: dict[str, Any]) -> str:
    """The column the finding names the current value of (current_<column>)."""
    return next(k.removeprefix("current_") for k in detail if k.startswith("current_"))


def silver_day(
    n: int,
    oracle: dict[str, Any],
    store: MetadataStore,
    tables: SilverTables,
    stages: StageRunner,
    domain: str,
    env: str,
    token_pattern: str,
    prefix: str = "silver",
    live: Callable[[str], list[Mapping[str, Any]] | None] | None = None,
) -> SilverDay:
    """One silver run for the day, then the live source against silver (REC-05, for datasets `live` can read), silver
    against the oracle and the day's silver incidents."""
    window = str(oracle["date"])
    report = build_silver(store, tables, stages, domain, env, f"{prefix}-{window}", window, token_pattern)
    return judge_silver(n, oracle, report, store, tables, domain, token_pattern, live)


def judge_silver(
    n: int,
    oracle: dict[str, Any],
    report: SilverReport,
    store: MetadataStore,
    tables: SilverTables,
    domain: str,
    token_pattern: str,
    live: Callable[[str], list[Mapping[str, Any]] | None] | None = None,
) -> SilverDay:
    """The day's silver run judged from its report: the live source against silver, silver against the oracle and
    the day's silver incidents. The run itself may have happened in another process (a deployed silver job)."""
    window = str(oracle["date"])
    ps = plans(store, domain, token_pattern)
    day = SilverDay(n, window, report, compare(oracle.get("silver") or {}, tables, ps))
    for name, plan in sorted(ps.items()) if live else []:
        rows = live(name)
        if rows is None:
            continue
        r = reconcile_live(name, rows, plan, tables)
        day.recon[name] = r
        store.log_step(StepRecord(report.run_id, plan.source, plan.dataset, RECON_STEP,
                                  "succeeded" if r.ok else "failed",
                                  {"source_rows": r.source_rows, "silver_rows": r.silver_rows,
                                   "missing": len(r.missing), "extra": len(r.extra)}, r.message()))
    for f in oracle.get("findings") or []:
        verdict = judge(n, f, tables, ps, day)
        if verdict is not None:
            day.verdicts.append(verdict)
    return day
