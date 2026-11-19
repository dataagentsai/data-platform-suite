"""The ops dashboards as definitions (G8, DPC-0057, ADR-024). Product-free: a realm compiles them to its BI tool's
file (a dashboard-file adapter writes them into the realm's dashboards folder), and `evaluate` runs any of them on
the ops tables in memory, so a test can prove every widget has data before anything is deployed.

Three sets, separate (ADR-024 rule 1):
    platform-native   adopted, not rebuilt: the job monitoring screen and job failure notifications (obs/native.py)
    observability     ours, here, only what the platform cannot see: platform health, freshness and SLOs,
                      performance, sources and ingestion (usage is the platform's own, see obs/native.py)
    DES               data expectation services own their screens (dataplatform/des/dashboards.py); observability
                      shows only a summary of step severities with a link to the owner's screen

A query is deliberately small: one ops table, optional filters, a group-by and aggregates. An aggregate query reads
the ops metric view over its table (obs/views.py; `validate` checks every measure is certified there), a listing
reads the compact ops table. Anything bigger belongs in the ops builder (obs/ops.py), where it is tested once and
every tool shows the same number. Cost is out of scope (ADR-024 rule 8): no cost widget.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from dataplatform.obs import views
from dataplatform.obs.ops import TABLES, Tables

AGGS = ("sum", "count", "avg", "max", "min", "count_distinct")
KINDS = ("counter", "line", "bar", "table")
OPS = ("=", "!=", "in")
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


@dataclass(frozen=True)
class Measure:
    name: str
    agg: str
    column: str = ""  # empty for count


@dataclass(frozen=True)
class Query:
    id: str
    table: str
    group_by: tuple[str, ...] = ()
    measures: tuple[Measure, ...] = ()
    columns: tuple[str, ...] = ()  # a plain listing (no group, no measures)
    where: tuple[tuple[str, str, Any], ...] = ()
    order_by: tuple[str, ...] = ()
    descending: bool = False
    limit: int | None = None

    @property
    def fields(self) -> tuple[str, ...]:
        return self.columns or (*self.group_by, *(m.name for m in self.measures))


@dataclass(frozen=True)
class Widget:
    id: str
    title: str
    kind: str
    query: str
    x: str = ""
    y: str = ""
    color: str = ""
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dashboard:
    id: str
    title: str
    purpose: str
    queries: tuple[Query, ...]
    widgets: tuple[Widget, ...]
    discharges: tuple[str, ...] = field(default=("DPC-0057", "OPS-03"))
    owner: str = "observability"  # observability | des (ADR-024 rule 1)

    def query(self, query_id: str) -> Query:
        for q in self.queries:
            if q.id == query_id:
                return q
        raise KeyError(query_id)


class DefinitionError(ValueError):
    pass


def validate(d: Dashboard) -> None:
    ids = [q.id for q in d.queries]
    if len(set(ids)) != len(ids):
        raise DefinitionError(f"{d.id}: query ids repeat")
    for q in d.queries:
        if q.table not in TABLES:
            raise DefinitionError(f"{d.id}.{q.id}: no ops table {q.table}")
        names = [q.id, *q.group_by, *q.columns, *q.order_by, *(w[0] for w in q.where),
                 *(m.name for m in q.measures), *(m.column for m in q.measures if m.column)]
        bad = [n for n in names if not _IDENT.match(n)]
        if bad:
            raise DefinitionError(f"{d.id}.{q.id}: not a plain name: {bad}")
        if any(m.agg not in AGGS for m in q.measures) or any(op not in OPS for _, op, _ in q.where):
            raise DefinitionError(f"{d.id}.{q.id}: unknown aggregate or filter")
        if bool(q.columns) == bool(q.measures or q.group_by):
            raise DefinitionError(f"{d.id}.{q.id}: a query lists columns or aggregates, not both")
        if q.measures:
            problem = views.problem(q.table, q.group_by, tuple(w[0] for w in q.where),
                                    tuple((m.name, m.agg, m.column) for m in q.measures))
            if problem:
                raise DefinitionError(f"{d.id}.{q.id}: {problem}")
    for w in d.widgets:
        if w.kind not in KINDS or w.query not in ids:
            raise DefinitionError(f"{d.id}.{w.id}: unknown kind or query")
        used = [c for c in (w.x, w.y, w.color, *w.columns) if c]
        missing = [c for c in used if c not in d.query(w.query).fields]
        if missing:
            raise DefinitionError(f"{d.id}.{w.id}: fields {missing} not in query {w.query}")


# ------------------------------------------------------------------------------------------------ local evaluation


def _match(row: dict[str, Any], where: tuple[tuple[str, str, Any], ...]) -> bool:
    for col, op, value in where:
        v = row.get(col)
        if (op == "=" and v != value) or (op == "!=" and v == value) or (op == "in" and v not in value):
            return False
    return True


def _agg(m: Measure, rows: list[dict[str, Any]]) -> Any:
    if m.agg == "count":
        return len(rows)
    vals = [r[m.column] for r in rows if r.get(m.column) is not None]
    if m.agg == "count_distinct":
        return len(set(vals))
    if not vals:
        return None
    nums = [float(v) for v in vals]
    pick = {"sum": sum(nums), "max": max(nums), "min": min(nums), "avg": sum(nums) / len(nums)}
    return pick[m.agg]


def _sort_key(keys: tuple[str, ...]) -> Any:
    def key(r: dict[str, Any]) -> tuple[Any, ...]:
        return tuple((0, r[k]) if isinstance(r.get(k), int | float) else (1, str(r.get(k))) for k in keys)

    return key


def evaluate(q: Query, tables: Tables) -> list[dict[str, Any]]:
    rows = [r for r in tables.get(q.table, []) if _match(r, q.where)]
    if q.columns:
        rows.sort(key=_sort_key(q.order_by), reverse=q.descending)
        out = [{c: r.get(c) for c in q.columns} for r in rows]
    else:
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            groups[tuple(r.get(g) for g in q.group_by)].append(r)
        if not q.group_by and not rows:
            groups[()] = []
        out = [{**dict(zip(q.group_by, k, strict=True)), **{m.name: _agg(m, g) for m in q.measures}}
               for k, g in groups.items()]
        out.sort(key=_sort_key(q.order_by or q.group_by), reverse=q.descending)
    return out[: q.limit] if q.limit else out


# ------------------------------------------------------------------------------------------------ observability's


def _m(name: str, agg: str, column: str = "") -> Measure:
    return Measure(name, agg, column)


PLATFORM_HEALTH = Dashboard(
    "platform_health", "Platform health",
    "Did the loads run, and did they succeed? Runs per day by outcome, errors by class, what failed, the alerts that "
    "went out, and each owner's step results by severity (the detail is on the owner's screen). Job-level runs, "
    "their count and run time are the platform's own job monitoring screen (adopted, not rebuilt): this screen "
    "starts below the job, at the dataset.",
    queries=(
        Query("status_daily", "ops_run_status_daily", ("day", "status"), (_m("dataset_runs", "sum", "n"),)),
        Query("errors_daily", "ops_errors_daily", ("day", "error_class"), (_m("errors", "sum", "n"),)),
        Query("not_succeeded", "ops_dataset_runs",
              columns=("day", "dataset", "status", "message", "origin", "run_id"),
              where=(("status", "in", ("failed", "skipped")),), order_by=("day",), descending=True),
        Query("alerts", "ops_alerts", columns=("day", "rule", "subject", "kind", "channel", "to", "run_id"),
              order_by=("at",), descending=True),
        Query("alert_counts", "ops_alert_counts", ("day", "rule"), (_m("alerts", "sum", "alerts"),)),
        Query("severity_daily", "ops_steps", ("day", "owner", "severity"), (_m("steps", "count"),)),
        Query("steps_not_passed", "ops_steps",
              columns=("day", "dataset", "owner", "step", "severity", "message", "screen", "run_id"),
              where=(("passed", "=", False),), order_by=("day",)),
    ),
    widgets=(
        Widget("status", "Dataset runs per day by outcome", "bar", "status_daily", "day", "dataset_runs", "status"),
        Widget("errors", "Errors per day by class", "bar", "errors_daily", "day", "errors", "error_class"),
        Widget("alerts_per_day", "Alerts sent per rule per day", "bar", "alert_counts", "day", "alerts", "rule"),
        Widget("severity", "Step results per day by severity (owners judge, observability reports)", "bar",
               "severity_daily", "day", "steps", "severity"),
        Widget("failed", "Dataset runs that failed or were blocked", "table", "not_succeeded",
               columns=("day", "dataset", "status", "message", "origin", "run_id")),
        Widget("steps_not_passed", "Steps not passed, with the owner's screen", "table", "steps_not_passed",
               columns=("day", "dataset", "owner", "step", "severity", "message", "screen")),
        Widget("alert_log", "Alert log", "table", "alerts",
               columns=("day", "rule", "subject", "kind", "channel", "to", "run_id")),
    ),
    discharges=("DPC-0057", "OPS-03", "OPS-01", "OPS-06"),
)

FRESHNESS = Dashboard(
    "freshness_slos", "Freshness and SLOs",
    "Is the data as fresh as each dataset's contract promises? Breaches of the manifest's own targets, per dataset "
    "and per feed, and how often each target was met. Learned freshness and completeness alerts per table are the "
    "platform's anomaly detection (adopted, not rebuilt); this screen keeps only the promised targets it cannot know.",
    queries=(
        Query("breaches", "ops_freshness", columns=("day", "dataset", "key", "age_hours", "error_hours", "tier"),
              where=(("breach", "=", True),), order_by=("day",)),
        Query("breaches_daily", "ops_freshness", ("day",), (_m("breaches", "count"),),
              where=(("breach", "=", True),)),
        Query("slo", "ops_slo_daily", ("dataset",), (_m("met_pct", "avg", "met_pct"),)),
    ),
    widgets=(
        Widget("slo_met", "Share of checks within target (%)", "bar", "slo", "dataset", "met_pct"),
        Widget("breaches_per_day", "Freshness breaches per day", "bar", "breaches_daily", "day", "breaches"),
        Widget("breach_list", "Breaches (dataset or feed past its error target)", "table", "breaches",
               columns=("day", "dataset", "key", "age_hours", "error_hours", "tier")),
    ),
    discharges=("DPC-0057", "FRESH-01", "OPS-01", "LATE-01"),
)

PERFORMANCE = Dashboard(
    "performance", "Performance",
    "Where does each load spend its time, and is it getting slower? Time per dataset and per step, runs at least "
    "twice their own usual time, and re-runs. Only what needs the run log: job-level performance is the platform's "
    "own job monitoring screen, and spend belongs to the separate cost product.",
    queries=(
        Query("duration", "ops_dataset_runs", ("dataset",), (_m("avg_duration_s", "avg", "duration_s"),
                                                             _m("max_duration_s", "max", "duration_s"))),
        Query("steps", "ops_step_durations", ("step",), (_m("avg_step_s", "avg", "duration_s"),),
              where=(("kind", "=", "pipeline"),)),
        Query("slow", "ops_dataset_runs", columns=("day", "dataset", "duration_s", "duration_ratio", "origin",
                                                   "run_id"),
              where=(("slow", "=", True),), order_by=("day",)),
        Query("retries_daily", "ops_dataset_runs", ("day", "dataset"), (_m("retries", "sum", "retry"),),
              where=(("retry", "=", 1),)),
    ),
    widgets=(
        Widget("time", "Average and longest run time per dataset (s)", "bar", "duration", "dataset",
               "avg_duration_s"),
        Widget("step_time", "Average time per pipeline step (s)", "bar", "steps", "step", "avg_step_s"),
        Widget("retries", "Re-runs per day by dataset", "bar", "retries_daily", "day", "retries", "dataset"),
        Widget("slow_runs", "Slow-downs: runs at least twice their usual time", "table", "slow",
               columns=("day", "dataset", "duration_s", "duration_ratio", "origin")),
    ),
    discharges=("DPC-0057", "OPS-03"),
)

SOURCES = Dashboard(
    "sources_ingestion", "Sources and ingestion",
    "What arrived from each source, and what was held back? Files loaded, duplicates skipped and deliveries "
    "rejected, rows loaded per source, the reason for every file not loaded, and the detectors that met a load.",
    queries=(
        Query("files_daily", "ops_ingestion_daily", ("day",), (_m("loaded", "sum", "files_loaded"),
                                                               _m("duplicate", "sum", "files_duplicate"),
                                                               _m("rejected", "sum", "files_rejected"))),
        Query("rows_daily", "ops_dataset_runs", ("day", "source"), (_m("rows", "sum", "rows_written"),)),
        Query("held", "ops_files_held", columns=("day", "dataset", "file_name", "status", "rows", "reason",
                                                 "run_id"), order_by=("day",)),
        Query("by_dataset", "ops_dataset_runs", ("dataset",), (_m("runs", "count"),
                                                               _m("rows", "sum", "rows_written"))),
        Query("detectors", "ops_steps", columns=("day", "dataset", "step", "severity", "count", "message",
                                                 "run_id"),
              where=(("kind", "=", "detect"),), order_by=("day",)),
    ),
    widgets=(
        Widget("rows", "Rows loaded per day by source", "line", "rows_daily", "day", "rows", "source"),
        Widget("rejected", "Files rejected per day", "bar", "files_daily", "day", "rejected"),
        Widget("duplicates", "Duplicate files skipped per day", "bar", "files_daily", "day", "duplicate"),
        Widget("held_files", "Files not loaded, with the reason", "table", "held",
               columns=("day", "dataset", "file_name", "status", "rows", "reason")),
        Widget("detector_log", "Detectors that met a load", "table", "detectors",
               columns=("day", "dataset", "step", "severity", "count", "message")),
        Widget("datasets", "Runs and rows per dataset", "table", "by_dataset", columns=("dataset", "runs", "rows")),
    ),
    discharges=("DPC-0057", "BATCH-01"),
)

# No usage screen of our own (13 Nov, "adopt what we can, build only the missing parts"): reads per table, readers
# and table-to-table flows are the platform's own table insights, audit log and lineage views (obs/native.py). The ops
# builder still builds ops_usage_daily and ops_lineage (no screen reads them now); dropping that copy is a follow-up.

DASHBOARDS = (PLATFORM_HEALTH, FRESHNESS, PERFORMANCE, SOURCES)


def every() -> tuple[Dashboard, ...]:
    """Observability's dashboards and the DES-owned ones (generated side by side, owned apart)."""
    from dataplatform.des.dashboards import DASHBOARDS as DES  # noqa: PLC0415 - des builds on obs, not the reverse

    return (*DASHBOARDS, *DES)


def by_id(dashboard_id: str) -> Dashboard:
    for d in every():
        if d.id == dashboard_id:
            return d
    raise KeyError(dashboard_id)


def render(d: Dashboard, tables: Tables) -> dict[str, list[dict[str, Any]]]:
    """Every widget's rows, by widget id."""
    return {w.id: evaluate(d.query(w.query), tables) for w in d.widgets}
