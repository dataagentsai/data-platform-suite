"""Platform-native screens and notifications, adopted, not rebuilt (ADR-024 rule 1, set a).

What the platform already gives for free is referenced or imported by the realm, never re-implemented here. Each
entry says what it covers, so observability's own screens leave it out (job-level run history and duration, job
failure e-mails, lineage and audit browsing). A realm binds each id to its product (an adapter in
dataplatform/adapters, generated into the realm's dashboards folder as native.yml).

    kind   screen        a ready-made dashboard or page: imported from the platform's template, or linked
           notification  the platform's own notification on a job event, set on every job the realm deploys
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Native:
    id: str
    kind: str  # screen | notification
    covers: str
    instead_of: str  # what observability therefore does not build


NATIVE = (
    Native("jobs_monitoring", "screen",
           "Job runs, their duration, result and retries across the workspace, from the platform's job telemetry.",
           "job-level history and performance; observability keeps per-dataset and per-step time (run log)"),
    Native("job_failure_notification", "notification",
           "A message from the platform on a job's failure, to the job's owners' destination.",
           "a run-failed e-mail of our own; our alerts add routing by tier and the noise rules (OPS-06)"),
    Native("lineage_view", "screen", "Table and column lineage, browsed in the catalog.",
           "a lineage browser and a table-to-table screen of our own"),
    Native("audit_view", "screen", "Who read or changed what, from the platform's audit log.",
           "an audit browser of our own"),
    Native("table_insights", "screen",
           "Per table: how often it is read, by whom, and the frequent queries and joins, from the platform's "
           "query and audit telemetry.",
           "a usage and adoption screen of our own (dropped 13 Nov)"),
    Native("anomaly_detection", "screen",
           "Per table in a watched schema: freshness and completeness checked against limits learned from the "
           "table's own history, with an alert when a table is stale or short.",
           "learned freshness alerts and a latest-age listing of our own; observability keeps only the contract's "
           "promised targets (freshness and SLOs), which the platform cannot know"),
)
KINDS = ("screen", "notification")
