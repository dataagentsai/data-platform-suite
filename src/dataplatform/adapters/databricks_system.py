"""The system-records port (dataplatform.obs.system) on Databricks system tables.

Reads job run timelines, table lineage and the Unity Catalog audit log through a SQL runner: any callable
`(sql, params) -> list[dict]` (the statement execution API from outside, `spark.sql` inside a job). The queries
are constants with named parameters; nothing user-supplied is ever put into SQL text.

The platform run id travels as the job parameter `dp_run_id` (and as the job tag of the same name, see
obs/recorder.py job_tags); this adapter resolves every lineage row to it through the job run id. Billing is not read:
spend belongs to the separate cost product (ADR-024 rule 8), which attributes it by the same tags.
Read once a day through obs/system.py DailyCopy (with a watermark), never polled (ADR-024 rule 4).
Contract: tests/dataplatform/test_obs_system.py runs the same table against this adapter (with a runner that answers
like the system tables do) and the memory adapter (M5). Reading a real workspace is an integration test.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from dataplatform.obs.system import AccessRow, JobRunRow, LineageRow

SqlRunner = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

JOBS_SQL = """
SELECT CAST(t.run_id AS STRING) AS job_run_id,
       COALESCE(MAX(j.name), CAST(t.job_id AS STRING)) AS job_name,
       COALESCE(MAX(t.job_parameters['dp_run_id']), '') AS run_id,
       MIN(t.period_start_time) AS started_at,
       MAX(t.period_end_time) AS ended_at,
       MAX_BY(t.result_state, t.period_end_time) AS result_state,
       ANY_VALUE(j.tags) AS tags
FROM system.lakeflow.job_run_timeline AS t
LEFT JOIN system.lakeflow.jobs AS j ON t.workspace_id = j.workspace_id AND t.job_id = j.job_id
GROUP BY t.run_id, t.job_id
HAVING MIN(t.period_start_time) >= :since AND MIN(t.period_start_time) < :until
"""

LINEAGE_SQL = """
SELECT event_time,
       source_table_full_name,
       target_table_full_name,
       COALESCE(CAST(entity_run_id AS STRING), '') AS job_run_id
FROM system.access.table_lineage
WHERE event_time >= :since AND event_time < :until
  AND source_table_full_name IS NOT NULL AND target_table_full_name IS NOT NULL
"""

ACCESS_SQL = """
SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params['full_name_arg'] AS object_name
FROM system.access.audit
WHERE service_name = 'unityCatalog' AND action_name IN ('getTable', 'createTable', 'updateTables')
  AND event_time >= :since AND event_time < :until
"""

RESULTS = {"SUCCEEDED": "succeeded", "SUCCESS": "succeeded", "FAILED": "failed", "ERROR": "failed",
           "TIMED_OUT": "failed", "TIMEDOUT": "failed", "CANCELED": "cancelled", "CANCELLED": "cancelled",
           "SKIPPED": "cancelled"}
ACTIONS = {"getTable": "read", "createTable": "write", "updateTables": "write"}


def _ts(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def _opt_ts(value: Any) -> datetime | None:
    return None if value is None else _ts(value)


class DatabricksSystemTables:
    def __init__(self, sql: SqlRunner) -> None:
        self.sql = sql

    def _params(self, since: datetime, until: datetime) -> dict[str, Any]:
        return {"since": since.isoformat(), "until": until.isoformat()}

    def jobs(self, since: datetime, until: datetime) -> list[JobRunRow]:
        rows = [JobRunRow(str(r["job_run_id"]), str(r["job_name"]), str(r["run_id"] or ""),
                          _ts(r["started_at"]), _opt_ts(r["ended_at"]),
                          RESULTS.get(str(r["result_state"] or "").upper(), "running"),
                          {str(k): str(v) for k, v in (r.get("tags") or {}).items()})
                for r in self.sql(JOBS_SQL, self._params(since, until))]
        return sorted(rows, key=lambda j: (j.started_at, j.job_run_id))

    def _run_ids(self, since: datetime, until: datetime) -> dict[str, str]:
        """Job run id -> platform run id, over a window a day wider than asked (a run may start the day before)."""
        day = timedelta(days=1)
        return {j.job_run_id: j.run_id for j in self.jobs(since - day, until + day) if j.run_id}

    def lineage(self, since: datetime, until: datetime) -> list[LineageRow]:
        runs = self._run_ids(since, until)
        rows = [LineageRow(_ts(r["event_time"]), str(r["source_table_full_name"]),
                           str(r["target_table_full_name"]), runs.get(str(r["job_run_id"]), ""),
                           str(r["job_run_id"]))
                for r in self.sql(LINEAGE_SQL, self._params(since, until))]
        return sorted(rows, key=lambda x: (x.at, x.source_table, x.target_table))

    def access(self, since: datetime, until: datetime) -> list[AccessRow]:
        rows = [AccessRow(_ts(r["event_time"]), str(r["user_email"]), ACTIONS.get(str(r["action_name"]), "other"),
                          str(r["object_name"]))
                for r in self.sql(ACCESS_SQL, self._params(since, until)) if r.get("object_name")]
        return sorted(rows, key=lambda a: (a.at, a.user, a.object))
