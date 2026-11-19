"""Contract table for the system-records port (ADR-020, M5): the memory adapter and the Databricks system-tables
adapter return the same port rows for the same records. The Databricks adapter runs against a SQL runner that answers
the way the system tables do (rows keyed by the columns its queries select); a real workspace is an integration test.
"""

from datetime import UTC, datetime, timedelta

import pytest

from dataplatform.adapters.databricks_system import (
    ACCESS_SQL,
    JOBS_SQL,
    LINEAGE_SQL,
    DatabricksSystemTables,
)
from dataplatform.adapters.memory_system import MemorySystemTables
from dataplatform.obs.system import AccessRow, DailyCopy, JobRunRow, LineageRow, read_all

T = datetime(2026, 11, 9, 17, 30, tzinfo=UTC)
JOBS = [JobRunRow("901", "erp.orders", "a" * 32, T, T + timedelta(minutes=3), "succeeded",
                  {"dp_domain": "erp", "dp_data_product": "sales"}),
        JobRunRow("902", "erp.returns", "", T + timedelta(hours=1), None, "running"),
        JobRunRow("903", "erp.orders", "b" * 32, T - timedelta(days=3), T - timedelta(days=3), "failed")]
LINEAGE = [LineageRow(T + timedelta(minutes=2), "dev.bronze.orders", "dev.silver.orders", "a" * 32, "901")]
ACCESS = [AccessRow(T + timedelta(minutes=5), "ana@x.example", "read", "prod.gold.fact_otif")]


def memory():
    m = MemorySystemTables()
    m.job_rows, m.lineage_rows, m.access_rows = list(JOBS), list(LINEAGE), list(ACCESS)
    return m


def system_tables_runner(sql, params):
    """Answers like the system tables: raw states, custom tags, job parameters, ISO times, the window applied."""
    lo, hi = datetime.fromisoformat(params["since"]), datetime.fromisoformat(params["until"])
    if sql == JOBS_SQL:
        state = {"succeeded": "SUCCEEDED", "failed": "FAILED", "running": None}
        return [{"job_run_id": j.job_run_id, "job_name": j.job_name, "run_id": j.run_id or None,
                 "started_at": j.started_at.isoformat(), "ended_at": j.ended_at and j.ended_at.isoformat(),
                 "result_state": state[j.result], "tags": j.tags or None} for j in JOBS if lo <= j.started_at < hi]
    if sql == LINEAGE_SQL:
        return [{"event_time": x.at, "source_table_full_name": x.source_table,
                 "target_table_full_name": x.target_table, "job_run_id": x.job_run_id} for x in LINEAGE
                if lo <= x.at < hi]
    if sql == ACCESS_SQL:
        return [{"event_time": a.at.isoformat(), "user_email": a.user, "action_name": "getTable",
                 "object_name": a.object} for a in ACCESS if lo <= a.at < hi] + [
                    {"event_time": T.isoformat(), "user_email": "x", "action_name": "getTable", "object_name": None}]
    raise AssertionError(f"unexpected query: {sql[:40]}")


def databricks():
    return DatabricksSystemTables(system_tables_runner)


ADAPTERS = {"memory": memory, "databricks": databricks}
DAY = (T.replace(hour=0, minute=0), T.replace(hour=0, minute=0) + timedelta(days=1))

CASES = [
    ("jobs in the window, in start order", lambda p: p.jobs(*DAY), JOBS[:2]),
    ("lineage edge with the run that wrote it", lambda p: p.lineage(*DAY), LINEAGE),
    ("access reads, rows without an object dropped", lambda p: p.access(*DAY), ACCESS),
    ("an empty window is empty", lambda p: read_all(p, T + timedelta(days=5), T + timedelta(days=5, hours=1)).jobs,
     []),
]


@pytest.mark.parametrize("adapter", list(ADAPTERS))
@pytest.mark.parametrize(("case", "act", "expect"), CASES, ids=[c[0] for c in CASES])
def test_system_tables_contract(adapter, case, act, expect):
    assert act(ADAPTERS[adapter]()) == expect


@pytest.mark.parametrize("sql", [JOBS_SQL, LINEAGE_SQL, ACCESS_SQL])
def test_queries_take_only_named_parameters(sql):
    assert ":since" in sql
    assert ":until" in sql
    assert "{" not in sql
    assert "%" not in sql


def test_no_billing_is_read():
    """ADR-024 rule 8: spend belongs to the cost product; the adapter reads no billing table."""
    import dataplatform.adapters.databricks_system as adapter

    sql = " ".join(v for k, v in vars(adapter).items() if k.endswith("_SQL"))
    assert "billing" not in sql
    assert "list_prices" not in sql


@pytest.mark.parametrize(
    ("case", "steps", "expect"),
    [
        ("the first copy reads from the watermark", [T + timedelta(days=1)], ([True], 3, 1)),
        ("a second copy the same business day reads nothing", [T + timedelta(days=1), T + timedelta(days=1, hours=2)],
         ([True, False], 3, 1)),
        ("the next day copies only what is new, once", [T - timedelta(hours=1), T + timedelta(days=1)],
         ([True, True], 3, 1)),
        ("before the watermark nothing is read", [T - timedelta(days=4)], ([False], 0, 0)),
    ],
)
def test_daily_copy_with_a_watermark(case, steps, expect):
    copy = DailyCopy(memory(), T - timedelta(days=3, hours=1), "UTC")
    done = [copy.copy(at) for at in steps]
    assert (done, len(copy.rows.jobs), len(copy.rows.access)) == expect


def test_on_demand_reads_past_the_watermark_without_moving_it():
    copy = DailyCopy(memory(), T - timedelta(days=3, hours=1), "UTC")
    copy.copy(T - timedelta(hours=1))
    mark = copy.watermark
    assert (len(copy.rows.jobs), len(copy.on_demand(T + timedelta(days=1)).jobs), copy.watermark) == (1, 3, mark)
