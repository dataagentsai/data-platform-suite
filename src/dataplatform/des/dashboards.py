"""The DES screens (ADR-024 rule 1, set c): owned by data expectation services, not by observability.

Moved out of observability's "data trust" dashboard. Until W1e builds DES's own result tables (quality results,
reconciliation entries with their hop and keys, profiles, quarantine), the definition reads DES's step rows from the
compact ops tables (`ops_steps`, owner `des`) so the screen exists and is generated; W1e replaces the queries with
its own detail and adds quarantine and release. Observability links here from its step summary (obs/steps.py SCREEN).
"""

from __future__ import annotations

from dataplatform.obs.dashboards import Dashboard, Measure, Query, Widget

OWNED = (("owner", "=", "des"),)

DATA_EXPECTATIONS = Dashboard(
    "des_data_expectations", "Data expectations",
    "Is the data right? Quality rules, reconciliation between hops and profiles, each with its severity, owned by "
    "the data expectation services. W1e fills in the detail (failed rows, the hop and keys of a reconciliation "
    "entry, quarantine and release).",
    queries=(
        Query("by_kind", "ops_steps", ("day", "kind"), (Measure("steps", "count"),), where=OWNED),
        Query("not_passed_daily", "ops_steps", ("day", "severity"), (Measure("steps", "count"),),
              where=(*OWNED, ("passed", "=", False))),
        Query("results", "ops_steps", columns=("day", "dataset", "kind", "step", "severity", "count", "message",
                                               "run_id"),
              where=OWNED, order_by=("day",), descending=True),
        Query("failed", "ops_steps", columns=("day", "dataset", "kind", "step", "severity", "message", "run_id"),
              where=(*OWNED, ("passed", "=", False)), order_by=("day",)),
        Query("restated", "ops_restatements", columns=("day", "table", "window", "reason", "measure", "before",
                                                      "after", "change", "run_id"),
              order_by=("day",), descending=True),
    ),
    widgets=(
        Widget("checks_per_day", "Results per day by kind", "bar", "by_kind", "day", "steps", "kind"),
        Widget("not_passed", "Results not passed per day by severity", "bar", "not_passed_daily", "day", "steps",
               "severity"),
        Widget("failures", "Results not passed", "table", "failed",
               columns=("day", "dataset", "kind", "step", "severity", "message")),
        Widget("results", "Every result", "table", "results",
               columns=("day", "dataset", "kind", "step", "severity", "count", "message")),
        Widget("restatements", "Published numbers that changed: window, reason, old and new (RSTM-01)", "table",
               "restated", columns=("day", "table", "window", "reason", "measure", "before", "after", "change")),
    ),
    discharges=("DPC-0057", "DQ-04", "REC-01", "SEV-01", "RSTM-01"),
    owner="des",
)

DASHBOARDS = (DATA_EXPECTATIONS,)
