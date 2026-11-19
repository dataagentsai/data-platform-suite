"""The run-log reader port (dataplatform.obs.runlog) on PostgreSQL: one call to runtime.ops_run_log
(sql/postgres_obs.sql), installed beside the metadata store's own schema (postgres_meta.sql). No table names here
(DPC-0033). Timestamps are the store's own (set by the database when each record was written).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from dataplatform.obs.runlog import (
    DatasetRunRow,
    ErrorRow,
    FileRow,
    RunLogSnapshot,
    RunRow,
    StepRow,
)

DDL = Path(__file__).with_name("sql") / "postgres_obs.sql"


def install(conn: Any) -> None:
    """Install (or update) the read procedure. Needs the store's schema first (postgres_meta.install)."""
    conn.execute(DDL.read_text())
    conn.commit()


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _at(value: str) -> datetime:
    return datetime.fromisoformat(value)


def parse(doc: dict[str, Any]) -> RunLogSnapshot:
    """The procedure's document as port rows (pure; tested on its own)."""
    return RunLogSnapshot(
        runs=[RunRow(r["run_id"], r["domain"], r["env"], r["trigger"], r["run_type"], r["status"],
                     _at(r["started_at"]), _ts(r["ended_at"])) for r in doc["runs"]],
        datasets=[DatasetRunRow(d["run_id"], d["source"], d["dataset"], d["status"], d["config_fingerprint"],
                                d["watermark_before"], d["watermark_after"], dict(d["counts"]), d["message"],
                                _at(d["started_at"]), _ts(d["ended_at"])) for d in doc["datasets"]],
        steps=[StepRow(s["run_id"], s["source"], s["dataset"], s["step"], s["status"], dict(s["counts"]),
                       s["message"], _at(s["at"])) for s in doc["steps"]],
        files=[FileRow(f["run_id"], f["source"], f["dataset"], f["batch_key"], f["file_name"], f["window"],
                       int(f["rows"]), f["status"], f["reason"], _at(f["at"])) for f in doc["files"]],
        errors=[ErrorRow(e["run_id"], e["source"], e["dataset"], e["step"], e["error_class"], e["message"],
                         _at(e["at"])) for e in doc["errors"]],
    )


class PostgresRunLogReader:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def snapshot(self, domain: str, since: datetime, until: datetime) -> RunLogSnapshot:
        row = self.conn.execute("SELECT runtime.ops_run_log(%s, %s, %s)", (domain, since, until)).fetchone()
        return parse(row[0])
