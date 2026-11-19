"""Silver on DuckDB (through SQLFrame) and local Delta tables (through delta-rs): the same stages
(dataplatform.stages) as the Spark adapter, other IO (docs/DUCKDB-SPIKE.md). For laptop and CI runs only: nothing
here writes a governed table on Databricks.

Needs `duckdb_frames.activate()` before this module is imported.

    read edge    a Delta folder read by delta-rs into a DuckDB table of the session's connection, seen as a frame
    write edge   delta-rs: append kinds append with an app transaction (silver:<table>, the file-log mark), and a
                 mark already committed is skipped, so writing the same result twice changes nothing; other kinds
                 MERGE on _row_key (deletes, updates, inserts; new columns merged into the schema)
    stage runner the stages' rows run once into a DuckDB table (the Spark runner's localCheckpoint)
"""

from __future__ import annotations

import uuid
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from dataplatform.adapters.duckdb_frames import checkpoint
from dataplatform.adapters.spark_gold import as_text
from dataplatform.adapters.spark_silver import DeltaSilverTables
from dataplatform.silver.plan import SilverPlan
from dataplatform.silver.ports import StageError, StageResult
from dataplatform.stages import run_stages


def to_arrow(df: Any) -> Any:
    """A SQLFrame frame's rows as an Arrow table (the frame's query run on its DuckDB connection)."""
    query = df.sql(dialect="duckdb", optimize=False, pretty=False)
    return df.session._conn.execute(query).fetch_arrow_table()


def read_delta(session: Any, path: Path, version: int | None = None) -> Any | None:
    """A Delta folder as a frame (at `version` when given: time travel, INSP-01): delta-rs reads it, DuckDB holds
    the rows in a table of the session."""
    if not (path / "_delta_log").exists():
        return None
    from deltalake import DeltaTable

    data = DeltaTable(str(path), version=version).to_pyarrow_table()
    name = f"_delta_{uuid.uuid4().hex}"
    conn = session._conn
    conn.register(f"{name}_arrow", data)
    # names generated here (a fresh uuid), nothing from data
    # shiftleft: allow SL-SEC-01 -- identifiers made here from uuid4; no value from data or from a caller
    conn.execute(f'CREATE TABLE "{name}" AS SELECT * FROM "{name}_arrow"')  # noqa: S608 -- generated names only
    conn.unregister(f"{name}_arrow")
    return session.table(name)


class DuckStageRunner:
    """stage_runner on DuckDB: the same run_stages, the rows run once into a table before they are counted and
    written (the counts and the write read the same rows)."""

    def __init__(self, hold_each_stage: bool = True):
        self.hold_each_stage = hold_each_stage  # each stage's rows into a table before the next stage (speed)

    def run(self, plan: SilverPlan, bronze: Any, current: Any | None, run_id: str, as_of: str) -> StageResult:
        from datetime import UTC, datetime

        bronze = checkpoint(bronze)
        try:
            rows, quarantine, tally = run_stages(plan, bronze, current, run_id, as_of, datetime.now(UTC),
                                                 hold=checkpoint if self.hold_each_stage else None)
            rows = checkpoint(rows)
        except StageError:
            raise
        except Exception as exc:  # an engine error on this batch's data
            raise StageError(f"{plan.table}: {type(exc).__name__}: {str(exc)[:500]}") from exc
        if quarantine is not None:
            quarantine = checkpoint(quarantine)
        counts = {"rows_in": bronze.count()}
        by_action = {r["_action"]: r["n"] for r in rows.groupBy("_action").agg(F.count(F.lit(1)).alias("n")).collect()}
        counts["rows_out"] = int(by_action.get("upsert", 0))
        counts["rows_removed"] = int(by_action.get("delete", 0))
        parts = [f.select(F.lit(name).alias("_what")) for name, f in tally.items()]
        if quarantine is not None:
            parts.append(quarantine.select(F.concat(F.col("_stage"), F.lit(".quarantined")).alias("_what")))
        counts.update(dict.fromkeys(tally, 0))
        if parts:
            for r in reduce(lambda a, b: a.unionByName(b), parts).groupBy("_what").count().collect():
                counts[str(r["_what"])] = int(r["count"])
        return StageResult(rows, quarantine, counts)

    def release(self, result: StageResult) -> None:
        del result


class DeltaRsSilverTables(DeltaSilverTables):
    """The read and write edges on Delta folders, by delta-rs; the frames are DuckDB's (SQLFrame)."""

    def _read(self, path: Path) -> Any | None:
        return read_delta(self.spark, path)

    def _append(self, df: Any, path: Path, app: str, txn: str) -> None:
        from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake

        if (path / "_delta_log").exists():
            done = DeltaTable(str(path)).transaction_version(app)
            if done is not None and done >= int(txn):
                return  # this mark was written before: an idempotent append writes nothing
        write_deltalake(str(path), to_arrow(df), mode="append", schema_mode="merge",
                        commit_properties=CommitProperties(app_transactions=[Transaction(app, int(txn))]))

    # shiftleft: allow SL-PURE-02 -- the write edge of silver (ADR-013): IO belongs here
    def _merge(self, rows: Any, path: Path) -> None:
        from deltalake import DeltaTable

        data = to_arrow(rows)
        cols = [c for c in data.column_names if c != "_action"]
        values = {c: f"s.{c}" for c in cols}  # column names from the frame's schema (checked names), never data
        (
            DeltaTable(str(path))
            .merge(data, "t._row_key = s._row_key", source_alias="s", target_alias="t", merge_schema=True)
            .when_matched_delete(predicate="s._action = 'delete'")
            .when_matched_update(updates=values, predicate="s._action = 'upsert'")
            .when_not_matched_insert(updates=values, predicate="s._action = 'upsert'")
            .execute()
        )

    # shiftleft: allow SL-PURE-06 -- the read edge of silver (ADR-013): IO belongs here
    def count_by_hour(self, plan: SilverPlan, column: str, start: str, end: str) -> dict[str, int]:
        df = self.current(plan)
        if df is None:
            return {}
        live = df.where(F.col("_is_current") & ~F.col("_is_deleted"))
        t, t0 = F.col(column), F.to_timestamp(F.lit(start))
        secs = F.unix_timestamp(t) - F.unix_timestamp(t0)
        bucket = F.timestamp_seconds(F.unix_timestamp(t0) + F.floor(secs / 3600) * 3600)
        rows = (live.where((t >= t0) & (t < F.to_timestamp(F.lit(end))))
                .groupBy(as_text(bucket, ("yyyy-MM-dd", "T", "HH:mm:ss", "+00:00")).alias("h"))
                .agg(F.count(F.lit(1)).alias("n")).collect())
        return {str(r["h"]): int(r["n"]) for r in rows}
