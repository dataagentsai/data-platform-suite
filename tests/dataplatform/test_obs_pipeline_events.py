"""Contract table for the pipeline event-log port (ADR-024 rule 5): declarative pipelines report themselves.

The memory adapter and the Databricks event-log adapter (on a runner that answers like `event_log()`) return the
same rows; the ops builder turns them into dataset-run rows with origin `pipeline_events`. Reading a real pipeline is
an integration test (pending until silver or gold runs as a declarative pipeline).
"""

from datetime import UTC, datetime, timedelta

import pytest

from dataplatform.adapters.databricks_pipeline_events import FLOWS_SQL, DatabricksPipelineEvents
from dataplatform.adapters.memory_pipeline_events import MemoryPipelineEvents
from dataplatform.obs import ops
from dataplatform.obs.pipeline_events import TableUpdateRow
from dataplatform.obs.runlog import RunLogSnapshot
from dataplatform.obs.system import SystemSnapshot

T = datetime(2026, 11, 9, 18, tzinfo=UTC)
ROWS = [
    TableUpdateRow("silver_sales", "u1", "dev_silver.sales.order_line", "succeeded", 120, T, T + timedelta(minutes=2),
                   "a" * 32),
    TableUpdateRow("silver_sales", "u1", "dev_silver.sales.delivery", "failed", None, T + timedelta(minutes=1),
                   T + timedelta(minutes=3), "a" * 32),
    TableUpdateRow("silver_sales", "u0", "dev_silver.sales.order_line", "succeeded", 100, T - timedelta(days=2),
                   T - timedelta(days=2), ""),
]
STATE = {"succeeded": "COMPLETED", "failed": "FAILED"}


def event_log_runner(sql, params):
    assert sql == FLOWS_SQL
    assert params["pipeline_id"] == "p-123"
    lo, hi = datetime.fromisoformat(params["since"]), datetime.fromisoformat(params["until"])
    return [{"update_id": r.update_id, "table_name": r.table, "started_at": r.started_at.isoformat(),
             "ended_at": r.ended_at, "status": STATE[r.status], "rows_written": r.rows_written,
             "run_id": r.run_id or None} for r in ROWS if lo <= r.started_at < hi]


ADAPTERS = {"memory": lambda: MemoryPipelineEvents(ROWS),
            "databricks": lambda: DatabricksPipelineEvents(event_log_runner, {"silver_sales": "p-123"})}
DAY = (T - timedelta(hours=1), T + timedelta(hours=1))


@pytest.mark.parametrize("adapter", list(ADAPTERS))
@pytest.mark.parametrize(
    ("case", "act", "expect"),
    [
        ("table updates in the window, oldest first", lambda p: p.table_updates(*DAY), ROWS[:2]),
        ("an empty window is empty", lambda p: p.table_updates(T + timedelta(days=3), T + timedelta(days=4)), []),
    ],
)
def test_pipeline_events_contract(adapter, case, act, expect):
    assert act(ADAPTERS[adapter]()) == expect


def test_pipeline_rows_become_dataset_runs():
    rows = ops.build(RunLogSnapshot(), SystemSnapshot(), [], [], "UTC", ROWS[:2])["ops_dataset_runs"]
    assert [(r["dataset"], r["status"], r["rows_written"], r["origin"]) for r in rows] == [
        ("dev_silver.sales.order_line", "succeeded", 120, "pipeline_events"),
        ("dev_silver.sales.delivery", "failed", None, "pipeline_events")]


def test_query_takes_only_named_parameters():
    assert ":pipeline_id" in FLOWS_SQL
    assert ":since" in FLOWS_SQL
    assert "{" not in FLOWS_SQL
