"""The pipeline event-log port (dataplatform.obs.pipeline_events) on Lakeflow Declarative Pipelines' event log.

STUB until silver or gold runs as a declarative pipeline: the query and the mapping are written and run in the
contract table against a fake SQL runner that answers like the event log; reading a real pipeline is an integration
test. Each pipeline's event log is read through the `event_log()` table-valued function with the pipeline id as a
named parameter; nothing user-supplied is put into SQL text. `flow_progress` events give per-flow status and
`num_output_rows`; the update's `dp_run_id` configuration value (set by the orchestrator, obs/recorder.py RUN_TAG)
joins the update to the platform run.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from dataplatform.obs.pipeline_events import TableUpdateRow

SqlRunner = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

FLOWS_SQL = """
WITH updates AS (
  SELECT origin.update_id AS update_id,
         MAX(details:create_update.config.configuration.dp_run_id) AS run_id
  FROM event_log(:pipeline_id)
  WHERE event_type = 'create_update'
  GROUP BY origin.update_id
)
SELECT f.origin.update_id AS update_id,
       f.origin.flow_name AS table_name,
       MIN(f.timestamp) AS started_at,
       MAX(f.timestamp) AS ended_at,
       MAX_BY(f.details:flow_progress.status, f.timestamp) AS status,
       SUM(CAST(f.details:flow_progress.metrics.num_output_rows AS BIGINT)) AS rows_written,
       MAX(u.run_id) AS run_id
FROM event_log(:pipeline_id) AS f
LEFT JOIN updates AS u ON f.origin.update_id = u.update_id
WHERE f.event_type = 'flow_progress'
GROUP BY f.origin.update_id, f.origin.flow_name
HAVING MIN(f.timestamp) >= :since AND MIN(f.timestamp) < :until
"""

STATUS = {"COMPLETED": "succeeded", "FAILED": "failed", "SKIPPED": "skipped", "EXCLUDED": "skipped",
          "STOPPED": "failed", "RUNNING": "running", "STARTING": "running", "QUEUED": "running"}


def _ts(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


class DatabricksPipelineEvents:
    def __init__(self, sql: SqlRunner, pipelines: dict[str, str]) -> None:
        """`pipelines`: pipeline name -> pipeline id, from the bundle (silver and gold pipelines of a domain)."""
        self.sql, self.pipelines = sql, pipelines

    def table_updates(self, since: datetime, until: datetime) -> list[TableUpdateRow]:
        out = []
        for name, pipeline_id in sorted(self.pipelines.items()):
            params = {"pipeline_id": pipeline_id, "since": since.isoformat(), "until": until.isoformat()}
            for r in self.sql(FLOWS_SQL, params):
                rows = r.get("rows_written")
                out.append(TableUpdateRow(name, str(r["update_id"]), str(r["table_name"]),
                                          STATUS.get(str(r.get("status") or "").upper(), "running"),
                                          None if rows is None else int(rows), _ts(r["started_at"]),
                                          None if r.get("ended_at") is None else _ts(r["ended_at"]),
                                          str(r.get("run_id") or "")))
        return sorted(out, key=lambda r: (r.started_at, r.pipeline, r.table))
