"""EXAMPLE SKELETON: a customer's own run log on the run-log reader port (level 2 of ADR-024 rule 6).

A customer who already keeps a run log (their own tables, an orchestrator's history) connects it here instead of
adopting ours: one query per record kind, and a column mapping from their names to the port's rows. Nothing else in
observability changes; screens and tools read the port only (rule 6: never the log directly).

Pending: no customer log is connected yet. The contract row in tests/dataplatform/test_obs_runlog.py is marked
pending until a customer's mapping and a sample of their rows exist; then it runs the same table as the memory and
relational adapters. To fill it in: write `RUNS_SQL` and friends against the customer's tables (named parameters
only), set `COLUMNS` to their column names, and drop the NotImplementedError.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from dataplatform.obs.runlog import RunLogSnapshot

SqlRunner = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

# port field -> the customer's column, per record kind (filled in per customer)
COLUMNS: dict[str, dict[str, str]] = {
    "runs": {"run_id": "", "domain": "", "env": "", "trigger": "", "run_type": "", "status": "", "started_at": "",
             "ended_at": ""},
    "datasets": {"run_id": "", "source": "", "dataset": "", "status": "", "started_at": "", "ended_at": ""},
    "steps": {"run_id": "", "source": "", "dataset": "", "step": "", "status": "", "message": "", "at": ""},
}


class CustomerRunLog:
    def __init__(self, sql: SqlRunner, columns: dict[str, dict[str, str]] | None = None) -> None:
        self.sql, self.columns = sql, columns or COLUMNS

    def snapshot(self, domain: str, since: datetime, until: datetime) -> RunLogSnapshot:
        raise NotImplementedError("example skeleton: map the customer's run log first (see the module docstring)")
