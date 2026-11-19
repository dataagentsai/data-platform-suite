"""The ops builder (G8, DPC-0057): run log + platform telemetry + freshness + alerts -> the compact ops tables. Pure.

Every table is a list of plain rows, joined by run id (the trace id), with `day` as the business day the row belongs
to. The ops metric views (obs/views.py) are defined over these tables and the dashboards read those views, so the
same numbers appear wherever they are shown (ADR-024 rule 7).

    ops_runs               one row per run: status, duration, datasets
    ops_dataset_runs       one row per dataset run: status, duration, rows, retries and slow-downs
    ops_run_status_daily   dataset runs per day per status
    ops_errors_daily       errors per day per class
    ops_steps              every owner's step row in the one step format (obs/steps.py): severity, owner, screen
    ops_step_durations     time per step of each dataset run (where a run spends its time)
    ops_ingestion_daily    files per day per dataset per outcome, rows loaded
    ops_files_held         every file not loaded (duplicate, rejected) with its reason
    ops_restatements       every published gold window that changed: old and new numbers, reason, run (RSTM-01)
    ops_freshness          every freshness check result
    ops_slo_daily          per day per dataset: was the freshness target met at every check
    ops_usage_daily        reads per day per table, distinct readers
    ops_lineage            table-to-table edges with the run that wrote them
    ops_alerts             every alert event (fired, grouped, snoozed, resolved)
    ops_alert_counts       alerts sent per rule per day (OPS-06 evidence)

Three customer levels, the same tables (ADR-024 rule 6). Each run and dataset-run row says where it came from in
`origin`: `run_log` (ours, or the customer's through the run-log port), `pipeline_events` (a declarative pipeline's
own event log, rule 5), or `telemetry` (a platform job run with no run-log record). With no run log connected,
the telemetry rows fill the job-level columns and every column that needs a log stays empty (None).

Cost is out of scope (ADR-024 rule 8): no billing is read and no cost column is built; job tags carry domain, data
product and run id so the separate cost product can attribute spend.

`run_signals` and `freshness_signals` turn the same inputs into alert signals for obs/alerts.py.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from statistics import median
from typing import Any

from dataplatform.obs.alerts import Event, Signal, business_day, counts_per_rule_per_day
from dataplatform.obs.freshness import FAILING, FreshnessResult, FreshnessTarget
from dataplatform.obs.pipeline_events import TableUpdateRow
from dataplatform.obs.runlog import RunLogSnapshot
from dataplatform.obs.steps import DETECT, SCREEN, kind_of, owner_of, severity
from dataplatform.obs.system import SystemSnapshot

Row = dict[str, Any]
Tables = dict[str, list[Row]]
SLOW_RATIO = 2.0  # a dataset run at least twice its own median time is a slow-down
TABLES = ("ops_runs", "ops_dataset_runs", "ops_run_status_daily", "ops_errors_daily", "ops_steps",
          "ops_step_durations", "ops_ingestion_daily", "ops_files_held", "ops_restatements", "ops_freshness", "ops_slo_daily",
          "ops_usage_daily", "ops_lineage", "ops_alerts", "ops_alert_counts")
ORIGINS = ("run_log", "pipeline_events", "telemetry")
# Every ops table's columns and types (obs/ops_export.py types), declared so a table with no rows today (no errors,
# no files held back) is still published with its columns, and its view and screen still work. A test checks the
# builder's rows against this on the 14-day fixture, where every table has rows.
COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "ops_runs": (("run_id", "text"), ("day", "text"), ("env", "text"), ("trigger", "text"), ("run_type", "text"),
                 ("status", "text"), ("started_at", "text"), ("duration_s", "decimal"), ("datasets", "integer"),
                 ("failed_datasets", "integer"), ("job_run_ids", "text"), ("origin", "text")),
    "ops_dataset_runs": (("run_id", "text"), ("day", "text"), ("source", "text"), ("dataset", "text"),
                         ("status", "text"), ("started_at", "text"), ("duration_s", "decimal"),
                         ("rows_read", "integer"), ("rows_written", "integer"), ("message", "text"),
                         ("watermark_after", "text"), ("origin", "text"), ("attempt", "integer"),
                         ("retry", "integer"), ("duration_ratio", "decimal"), ("slow", "flag")),
    "ops_run_status_daily": (("day", "text"), ("status", "text"), ("n", "decimal")),
    "ops_errors_daily": (("day", "text"), ("error_class", "text"), ("n", "decimal")),
    "ops_steps": (("run_id", "text"), ("day", "text"), ("source", "text"), ("dataset", "text"), ("kind", "text"),
                  ("owner", "text"), ("step", "text"), ("status", "text"), ("severity", "text"), ("passed", "flag"),
                  ("count", "integer"), ("message", "text"), ("screen", "text")),
    "ops_step_durations": (("run_id", "text"), ("day", "text"), ("dataset", "text"), ("step", "text"),
                           ("kind", "text"), ("duration_s", "decimal")),
    "ops_ingestion_daily": (("day", "text"), ("source", "text"), ("dataset", "text"), ("files_loaded", "integer"),
                            ("files_duplicate", "integer"), ("files_rejected", "integer"), ("rows_loaded", "integer")),
    "ops_files_held": (("run_id", "text"), ("day", "text"), ("source", "text"), ("dataset", "text"),
                       ("file_name", "text"), ("status", "text"), ("rows", "integer"), ("reason", "text")),
    "ops_restatements": (("run_id", "text"), ("day", "text"), ("table", "text"), ("window", "text"),
                         ("reason", "text"), ("measure", "text"), ("before", "decimal"), ("after", "decimal"),
                         ("change", "decimal")),
    "ops_freshness": (("checked_at", "text"), ("day", "text"), ("dataset", "text"), ("key", "text"),
                      ("subject", "text"), ("basis", "text"), ("age_hours", "decimal"), ("warn_hours", "decimal"),
                      ("error_hours", "decimal"), ("state", "text"), ("tier", "integer"), ("breach", "flag")),
    "ops_slo_daily": (("day", "text"), ("dataset", "text"), ("met", "flag"), ("met_pct", "decimal")),
    "ops_usage_daily": (("day", "text"), ("table", "text"), ("reads", "integer"), ("readers", "integer")),
    "ops_lineage": (("day", "text"), ("run_id", "text"), ("source_table", "text"), ("target_table", "text")),
    "ops_alerts": (("at", "text"), ("day", "text"), ("rule", "text"), ("subject", "text"), ("kind", "text"),
                   ("channel", "text"), ("to", "text"), ("run_id", "text")),
    "ops_alert_counts": (("rule", "text"), ("day", "text"), ("alerts", "integer")),
}


def _secs(a: datetime | None, b: datetime) -> float | None:
    return round((a - b).total_seconds(), 3) if a else None


def check_kind(step: str) -> str | None:
    return kind_of(step)


def _telemetry_jobs(log: RunLogSnapshot, system: SystemSnapshot) -> list[Any]:
    """Platform job runs no run-log run accounts for (all of them when no run log is connected)."""
    known = {r.run_id for r in log.runs}
    return [j for j in system.jobs if not (j.run_id and j.run_id in known)]


def runs_table(log: RunLogSnapshot, system: SystemSnapshot, tz: str) -> list[Row]:
    per_run: dict[str, list[str]] = defaultdict(list)
    for d in log.datasets:
        per_run[d.run_id].append(d.status)
    jobs: dict[str, list[str]] = defaultdict(list)
    for j in system.jobs:
        if j.run_id:
            jobs[j.run_id].append(j.job_run_id)
    out = [{"run_id": r.run_id, "day": business_day(r.started_at, tz), "env": r.env, "trigger": r.trigger,
            "run_type": r.run_type, "status": r.status, "started_at": r.started_at.isoformat(),
            "duration_s": _secs(r.ended_at, r.started_at), "datasets": len(per_run[r.run_id]),
            "failed_datasets": per_run[r.run_id].count("failed"), "job_run_ids": ",".join(sorted(jobs[r.run_id])),
            "origin": "run_log"} for r in log.runs]
    by_run: dict[str, list[Any]] = defaultdict(list)
    for j in _telemetry_jobs(log, system):
        by_run[j.run_id or j.job_run_id].append(j)
    for run_id, js in sorted(by_run.items()):
        first = min(j.started_at for j in js)
        ends = [j.ended_at for j in js]
        ended = None if None in ends else max(e for e in ends if e is not None)
        results = [j.result for j in js]
        status = "failed" if "failed" in results else ("running" if "running" in results else "succeeded")
        out.append({"run_id": run_id, "day": business_day(first, tz), "env": None, "trigger": None,
                    "run_type": None, "status": status, "started_at": first.isoformat(),
                    "duration_s": _secs(ended, first), "datasets": None, "failed_datasets": None,
                    "job_run_ids": ",".join(sorted(j.job_run_id for j in js)), "origin": "telemetry"})
    return out


def _dataset_row(run_id: str, day: str, source: str, dataset: str, status: str, started: datetime,
                 ended: datetime | None, origin: str, rows_read: int | None = None,
                 rows_written: int | None = None, message: str = "", watermark: str = "") -> Row:
    return {"run_id": run_id, "day": day, "source": source, "dataset": dataset, "status": status,
            "started_at": started.isoformat(), "duration_s": _secs(ended, started), "rows_read": rows_read,
            "rows_written": rows_written, "message": message, "watermark_after": watermark, "origin": origin}


def dataset_runs_table(log: RunLogSnapshot, system: SystemSnapshot, tz: str,
                       pipeline: list[TableUpdateRow] | None = None) -> list[Row]:
    out = [_dataset_row(d.run_id, business_day(d.started_at, tz), d.source, f"{d.source}.{d.dataset}", d.status,
                        d.started_at, d.ended_at, "run_log", int(d.counts.get("rows_read", 0)),
                        int(d.counts.get("rows_written", 0)), d.message, d.watermark_after or "")
           for d in log.datasets]
    out += [_dataset_row(u.run_id or u.update_id, business_day(u.started_at, tz), u.pipeline, u.table, u.status,
                         u.started_at, u.ended_at, "pipeline_events", None, u.rows_written)
            for u in pipeline or []]
    out += [_dataset_row(j.run_id or j.job_run_id, business_day(j.started_at, tz), j.job_name.split(".", 1)[0],
                         j.job_name, j.result, j.started_at, j.ended_at, "telemetry")
            for j in _telemetry_jobs(log, system)]
    return _performance(out)


def _performance(rows: list[Row]) -> list[Row]:
    """Attempts per dataset per day (a retry is any attempt after the first) and the time against the dataset's own
    median so far (a slow-down is SLOW_RATIO or more). Only what needs the run log: per dataset, not per job."""
    seen: dict[str, list[float]] = defaultdict(list)
    attempts: dict[tuple[str, str], int] = defaultdict(int)
    for r in sorted(rows, key=lambda x: (x["started_at"], x["dataset"])):
        key = (r["day"], r["dataset"])
        attempts[key] += 1
        r["attempt"], r["retry"] = attempts[key], int(attempts[key] > 1)
        past, took = seen[r["dataset"]], r["duration_s"]
        ratio = round(took / median(past), 3) if took is not None and past and median(past) > 0 else None
        r["duration_ratio"], r["slow"] = ratio, ratio is not None and ratio >= SLOW_RATIO
        if took is not None and r["status"] == "succeeded":
            past.append(took)
    return rows


def _count_by(rows: list[Row], keys: tuple[str, ...], measure: str | None = None) -> list[Row]:
    acc: dict[tuple[Any, ...], float] = defaultdict(float)
    for r in rows:
        acc[tuple(r[k] for k in keys)] += r[measure] if measure else 1
    name = measure or "n"
    return [{**dict(zip(keys, k, strict=True)), name: v} for k, v in sorted(acc.items())]


def steps_table(log: RunLogSnapshot, tz: str) -> list[Row]:
    """Every owner's step row (the one step format), with the owner and the owner's screen to go to."""
    out = []
    for s in log.steps:
        kind = kind_of(s.step)
        if kind:
            owner = owner_of(s.step)
            out.append({"run_id": s.run_id, "day": business_day(s.at, tz), "source": s.source,
                        "dataset": f"{s.source}.{s.dataset}", "kind": kind, "owner": owner, "step": s.step,
                        "status": s.status, "severity": severity(s.status), "passed": s.status == "succeeded",
                        "count": int(sum(s.counts.values())), "message": s.message, "screen": SCREEN[owner]})
    return out


RESTATEMENT_STEP = "gold.restatement"  # gold/harness.py writes one per changed window


def _fields(message: str) -> dict[str, str]:
    return dict(part.split("=", 1) for part in message.split("; ") if "=" in part)


def restatements_table(log: RunLogSnapshot, tz: str) -> list[Row]:
    """One row per restated window and measure (rows included): what finance asks when last month's number moved."""
    out = []
    for s in log.steps:
        if s.step != RESTATEMENT_STEP:
            continue
        f = _fields(s.message)
        for m in sorted({k.rsplit(".", 1)[0] for k in s.counts}):
            before, after = float(s.counts.get(f"{m}.before", 0)), float(s.counts.get(f"{m}.after", 0))
            out.append({"run_id": s.run_id, "day": business_day(s.at, tz), "table": s.dataset,
                        "window": f.get("window", ""), "reason": f.get("reason", ""), "measure": m,
                        "before": before, "after": after, "change": round(after - before, 6)})
    return out


def step_durations_table(log: RunLogSnapshot, tz: str) -> list[Row]:
    """Time per step: from the dataset run's start (or the step before) to the step's record."""
    started = {(d.run_id, d.source, d.dataset): d.started_at for d in log.datasets}
    last: dict[tuple[str, str, str], datetime] = {}
    out = []
    for s in sorted(log.steps, key=lambda x: (x.run_id, x.source, x.dataset, x.at)):
        key = (s.run_id, s.source, s.dataset)
        before = last.get(key, started.get(key, s.at))
        last[key] = s.at
        out.append({"run_id": s.run_id, "day": business_day(s.at, tz), "dataset": f"{s.source}.{s.dataset}",
                    "step": s.step, "kind": kind_of(s.step) or "pipeline", "duration_s": _secs(s.at, before)})
    return out


def ingestion_tables(log: RunLogSnapshot, tz: str) -> tuple[list[Row], list[Row]]:
    acc: dict[tuple[str, str, str], dict[str, int]] = {}
    held = []
    for f in log.files:
        day, name = business_day(f.at, tz), f"{f.source}.{f.dataset}"
        a = acc.setdefault((day, f.source, name), {"files_loaded": 0, "files_duplicate": 0, "files_rejected": 0,
                                                   "rows_loaded": 0})
        if f.status == "loaded":
            a["files_loaded"] += 1
            a["rows_loaded"] += f.rows
        elif f.status.startswith("skipped"):
            a["files_duplicate"] += 1
        else:
            a["files_rejected"] += 1
        if f.status != "loaded":
            held.append({"run_id": f.run_id, "day": day, "source": f.source, "dataset": name,
                         "file_name": f.file_name, "status": f.status, "rows": f.rows, "reason": f.reason})
    daily = [{"day": d, "source": s, "dataset": n, **v} for (d, s, n), v in sorted(acc.items())]
    return daily, held


def freshness_tables(results: list[FreshnessResult], tz: str) -> tuple[list[Row], list[Row]]:
    rows = [{"checked_at": r.checked_at.isoformat(), "day": business_day(r.checked_at, tz),
             "dataset": f"{r.source}.{r.dataset}", "key": r.key, "subject": r.subject, "basis": r.basis,
             "age_hours": r.age_hours, "warn_hours": r.warn_hours, "error_hours": r.error_hours,
             "state": r.state, "tier": r.tier, "breach": r.state in FAILING} for r in results]
    slo: dict[tuple[str, str], bool] = {}
    for r in rows:
        if not r["key"]:
            key = (str(r["day"]), str(r["dataset"]))
            slo[key] = slo.get(key, True) and r["state"] == "ok"
    return rows, [{"day": d, "dataset": n, "met": m, "met_pct": 100.0 if m else 0.0}
                  for (d, n), m in sorted(slo.items())]


def usage_daily(system: SystemSnapshot, tz: str) -> list[Row]:
    users: dict[tuple[str, str], set[str]] = defaultdict(set)
    reads: dict[tuple[str, str], int] = defaultdict(int)
    for a in system.access:
        if a.action == "read":
            key = (business_day(a.at, tz), a.object)
            reads[key] += 1
            users[key].add(a.user)
    return [{"day": d, "table": t, "reads": reads[(d, t)], "readers": len(users[(d, t)])}
            for d, t in sorted(reads)]


def lineage_table(system: SystemSnapshot, tz: str) -> list[Row]:
    return [{"day": business_day(x.at, tz), "run_id": x.run_id, "source_table": x.source_table,
             "target_table": x.target_table} for x in system.lineage]


def alert_tables(events: list[Event], tz: str) -> tuple[list[Row], list[Row]]:
    rows = [{"at": e.at.isoformat(), "day": business_day(e.at, tz), "rule": e.rule, "subject": e.subject,
             "kind": e.kind, "channel": e.channel, "to": e.to, "run_id": e.run_id} for e in events]
    counts = [{"rule": r, "day": d, "alerts": n} for (r, d), n in sorted(counts_per_rule_per_day(events, tz).items())]
    return rows, counts


def build(log: RunLogSnapshot, system: SystemSnapshot, freshness: list[FreshnessResult], events: list[Event],
          tz: str, pipeline: list[TableUpdateRow] | None = None) -> Tables:
    ds_runs = dataset_runs_table(log, system, tz, pipeline)
    ingestion, held = ingestion_tables(log, tz)
    fresh, slo = freshness_tables(freshness, tz)
    alerts, alert_counts = alert_tables(events, tz)
    errors = [{"day": business_day(e.at, tz), "error_class": e.error_class} for e in log.errors]
    return {
        "ops_runs": runs_table(log, system, tz),
        "ops_dataset_runs": ds_runs,
        "ops_run_status_daily": _count_by(ds_runs, ("day", "status")),
        "ops_errors_daily": _count_by(errors, ("day", "error_class")),
        "ops_steps": steps_table(log, tz),
        "ops_step_durations": step_durations_table(log, tz),
        "ops_ingestion_daily": ingestion,
        "ops_files_held": held,
        "ops_restatements": restatements_table(log, tz),
        "ops_freshness": fresh,
        "ops_slo_daily": slo,
        "ops_usage_daily": usage_daily(system, tz),
        "ops_lineage": lineage_table(system, tz),
        "ops_alerts": alerts,
        "ops_alert_counts": alert_counts,
    }


# ------------------------------------------------------------------------------------------------ alert signals


def _maker(targets: list[FreshnessTarget]) -> Any:
    by_name = {t.name: t for t in targets}

    def make(signal: str, subject: str, dataset: str, failing: bool, run_id: str = "", detail: str = "") -> Signal:
        t = by_name.get(dataset)
        owners = {"alerts": t.alert_to, "dataset": t.owner} if t else {}
        return Signal(signal, subject, failing, t.tier if t else 3, owners, run_id, detail)

    return make


def run_signals(run: RunLogSnapshot, targets: list[FreshnessTarget]) -> list[Signal]:
    """A finished run's signals (pushed as it ends): every dataset it ran, failing or not, so recoveries resolve.
    Detectors and checks come only from the step rows (the one step format); details stay with the owner, and the
    alert carries the owner's screen."""
    make = _maker(targets)
    out = []
    for d in run.datasets:
        name = f"{d.source}.{d.dataset}"
        out.append(make("run_failed", name, name, d.status == "failed", d.run_id, d.message))
    for s in run.steps:
        kind, name = kind_of(s.step), f"{s.source}.{s.dataset}"
        see = f" (see {SCREEN[owner_of(s.step)]})" if kind else ""
        if kind == DETECT:
            out.append(make("detector", f"{name}/{s.step.split('.', 1)[1]}", name, s.status != "succeeded",
                            s.run_id, s.message + see))
        elif kind in ("quality", "recon"):
            out.append(make("check_failed", f"{name}/{s.step}", name, s.status == "failed", s.run_id,
                            s.message + see))
    return out


def freshness_signals(freshness: list[FreshnessResult], targets: list[FreshnessTarget]) -> list[Signal]:
    make = _maker(targets)
    return [make("freshness", r.subject, f"{r.source}.{r.dataset}", r.state in FAILING,
                 detail=f"{r.basis} {r.age_hours} h > {r.error_hours} h" if r.state in FAILING else "")
            for r in freshness]
