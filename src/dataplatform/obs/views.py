"""Certified ops definitions (ADR-024 rule 7): ops metric views over the compact ops tables. Product-free.

Screens, the conversation interface and agents read these views, never raw telemetry: one definition of "runs",
"breaches" or "alerts" wherever it is shown. A realm generates them to its SQL the way the business metric views are
generated (W5): the realm's dashboards generator writes one metric-view file per view (an adapter in
dataplatform/adapters), committed and drift-checked. Every aggregate query of a dashboard reads the view
over its table, and `problem` refuses a dashboard measure the view does not certify.
"""

from __future__ import annotations

from dataclasses import dataclass

from dataplatform.obs.ops import TABLES


@dataclass(frozen=True)
class ViewMeasure:
    name: str
    agg: str  # count | count_distinct | sum | avg | max | min
    column: str = ""  # empty for count
    comment: str = ""


@dataclass(frozen=True)
class OpsView:
    name: str
    table: str
    comment: str
    dimensions: tuple[str, ...]
    measures: tuple[ViewMeasure, ...]

    def measure(self, name: str) -> ViewMeasure | None:
        return next((m for m in self.measures if m.name == name), None)


def _vm(name: str, agg: str, column: str = "", comment: str = "") -> ViewMeasure:
    return ViewMeasure(name, agg, column, comment)


VIEWS = (
    OpsView("mv_ops_runs", "ops_runs", "Runs: one row per run (run log, or a platform job run with no log).",
            ("day", "status", "origin", "env", "trigger", "run_type"),
            (_vm("runs", "count", comment="Runs started."),
             _vm("avg_duration_s", "avg", "duration_s", "Average run time in seconds."))),
    OpsView("mv_ops_dataset_runs", "ops_dataset_runs",
            "Dataset runs: status, time, rows, re-runs and slow-downs per dataset (run log, pipeline event log or "
            "telemetry).",
            ("day", "dataset", "source", "status", "origin", "retry", "slow"),
            (_vm("runs", "count", comment="Dataset runs."),
             _vm("rows", "sum", "rows_written", "Rows written (empty without a run log)."),
             _vm("avg_duration_s", "avg", "duration_s", "Average dataset run time in seconds."),
             _vm("max_duration_s", "max", "duration_s", "Longest dataset run time in seconds."),
             _vm("retries", "sum", "retry", "Re-runs: attempts after the first on the same business day."))),
    OpsView("mv_ops_run_status", "ops_run_status_daily", "Dataset runs per day per outcome.", ("day", "status"),
            (_vm("dataset_runs", "sum", "n", "Dataset runs."),)),
    OpsView("mv_ops_errors", "ops_errors_daily", "Errors per day per class.", ("day", "error_class"),
            (_vm("errors", "sum", "n", "Errors logged."),)),
    OpsView("mv_ops_steps", "ops_steps",
            "Every owner's step result in the one step format: owner, kind and severity (ADR-024 rule 2).",
            ("day", "dataset", "owner", "kind", "step", "severity", "passed"),
            (_vm("steps", "count", comment="Step results recorded."),)),
    OpsView("mv_ops_step_time", "ops_step_durations", "Time per step of each dataset run.",
            ("day", "dataset", "step", "kind"),
            (_vm("avg_step_s", "avg", "duration_s", "Average step time in seconds."),)),
    OpsView("mv_ops_ingestion", "ops_ingestion_daily", "Files per day per dataset by outcome.",
            ("day", "source", "dataset"),
            (_vm("loaded", "sum", "files_loaded", "Files loaded."),
             _vm("duplicate", "sum", "files_duplicate", "Duplicate files skipped."),
             _vm("rejected", "sum", "files_rejected", "Files rejected or held."))),
    OpsView("mv_ops_freshness", "ops_freshness", "Freshness checks against the manifest's targets.",
            ("day", "dataset", "key", "state", "tier", "breach"),
            (_vm("breaches", "count", comment="Checks (filter breach = true for breaches)."),)),
    OpsView("mv_ops_slo", "ops_slo_daily", "Freshness target met per day per dataset.", ("day", "dataset"),
            (_vm("met_pct", "avg", "met_pct", "Share of days the target was met at every check (%)."),)),
    OpsView("mv_ops_usage", "ops_usage_daily", "Reads per day per table (daily copy of the audit telemetry).",
            ("day", "table"),
            (_vm("reads", "sum", "reads", "Reads."), _vm("readers", "max", "readers", "Most readers in a day."))),
    OpsView("mv_ops_lineage", "ops_lineage", "Table-to-table flows (daily copy of the lineage telemetry).",
            ("day", "source_table", "target_table"),
            (_vm("runs", "count_distinct", "run_id", "Distinct runs that wrote the flow."),)),
    OpsView("mv_ops_alerts", "ops_alert_counts", "Alerts sent per rule per day (OPS-06 evidence).", ("day", "rule"),
            (_vm("alerts", "sum", "alerts", "Alerts sent."),)),
)


def view_for(table: str) -> OpsView | None:
    return next((v for v in VIEWS if v.table == table), None)


def problem(table: str, group_by: tuple[str, ...], filtered: tuple[str, ...],
            measures: tuple[tuple[str, str, str], ...]) -> str | None:
    """Why an aggregate query cannot be answered by the certified view over its table (None when it can)."""
    v = view_for(table)
    if v is None:
        return f"no ops metric view over {table}"
    loose = [c for c in (*group_by, *filtered) if c not in v.dimensions]
    if loose:
        return f"{v.name} has no dimension(s) {loose}"
    for name, agg, column in measures:
        m = v.measure(name)
        if m is None or (m.agg, m.column) != (agg, column):
            return f"{v.name} certifies no measure {name} = {agg}({column})"
    return None


def lint() -> list[str]:
    """Definition errors in VIEWS (an unknown table, a repeated name, a view per table at most)."""
    out = []
    names = [v.name for v in VIEWS]
    if len(set(names)) != len(names):
        out.append("view names repeat")
    tables = [v.table for v in VIEWS]
    if len(set(tables)) != len(tables):
        out.append("more than one view over a table")
    for v in VIEWS:
        if v.table not in TABLES:
            out.append(f"{v.name}: no ops table {v.table}")
        if not v.name.startswith("mv_ops_"):
            out.append(f"{v.name}: an ops metric view is named mv_ops_<name>")
        if len({m.name for m in v.measures}) != len(v.measures):
            out.append(f"{v.name}: measure names repeat")
    return out
