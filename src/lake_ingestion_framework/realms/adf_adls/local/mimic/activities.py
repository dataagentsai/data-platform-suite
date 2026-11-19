"""Activity executors for the ADF Mimic.

Seven activity types, one executor each. All share the same protocol:
take the activity JSON + run state + execution context; return an
``ActivityResult``. Per ``docs/adf_mimic_design.md`` §6.

Activity-specific notes:

- **Lookup** — runs SQL against a metadata DB connection. The generator
  often emits ``EXEC meta.sp_*`` stored-procedure calls. SQLite has no
  stored procs; for v1 the mimic detects these and runs an inline
  fallback that emulates the proc's effect (writes a run_log row,
  returns its row_id). Anything else is run literally.
- **Copy** — invokes a connector's ``read_query`` (or ``read_file`` /
  ``read_endpoint``) and pumps the batches into ``LocalDeltaWriter``.
- **ForEach** — sequential dispatch (per design §4.2; cloud parallelizes).
- **SetVariable** — trivial assignment from an evaluated expression.
- **SqlServerStoredProcedure** — same fallback story as Lookup for the
  meta procs.
- **ExecutePipeline** — recursive runner instantiation, parent_run_id linked.
- **Switch** — multi-way branch by comparing ``on`` to each case's value.

Anything else: ``UnsupportedActivityError`` with a helpful message.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.core.source_connectors.mssql import (
    MssqlConnectionParams,
    MssqlConnector,
)
from lake_ingestion_framework.core.source_connectors.mysql import (
    MysqlConnectionParams,
    MysqlConnector,
)
from lake_ingestion_framework.core.source_connectors.oracle import (
    OracleConnectionParams,
    OracleConnector,
)
from lake_ingestion_framework.core.source_connectors.postgres import (
    PostgresConnectionParams,
    PostgresConnector,
)
from lake_ingestion_framework.core.writers import LocalDeltaWriter
from lake_ingestion_framework.realms.adf_adls.local.mimic.expressions import evaluate
from lake_ingestion_framework.realms.adf_adls.local.mimic.resolvers import (
    ArtifactRegistry,
    resolve_dataset,
    resolve_linked_service,
)
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState

if TYPE_CHECKING:
    from lake_ingestion_framework.realms.adf_adls.local.mimic.runner import (
        ADFMimicRunner,
    )


# ──────────────────────────────────────────────────────────────────────
# Result + context
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ActivityResult:
    """What an executor returns to the dispatcher."""

    status: str  # "Succeeded" | "Failed" | "Skipped"
    output: dict[str, Any]
    error: str | None = None


@dataclass
class ExecutionContext:
    """Shared services activity executors need.

    Keeps explicit deps (vs. a sprawling globals object). Activities
    take what they use; tests inject fakes here.
    """

    registry: ArtifactRegistry
    metadata_db_path: Path
    lakes_root: Path
    runner: ADFMimicRunner | None = None  # for ExecutePipeline recursion


class UnsupportedActivityError(LakeIngestError):
    """The activity type isn't in the v1 whitelist."""


# ──────────────────────────────────────────────────────────────────────
# Activity executor protocol
# ──────────────────────────────────────────────────────────────────────


class ActivityExecutor(Protocol):
    activity_type: str

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult: ...


# ──────────────────────────────────────────────────────────────────────
# Lookup
# ──────────────────────────────────────────────────────────────────────


_META_PROC_PREFIXES = (
    "EXEC meta.sp_run_log_started",
    "EXEC meta.sp_run_log_finished",
    "EXEC meta.sp_master_run_log_started",
    "EXEC meta.sp_master_run_log_finished",
)


class LookupExecutor:
    activity_type = "Lookup"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        source = type_props.get("source", {})
        sql_node = source.get("sqlReaderQuery") or source.get("query")
        if sql_node is None:
            return ActivityResult(
                status="Failed",
                output={},
                error="Lookup activity has no sqlReaderQuery / query",
            )
        sql = evaluate(sql_node, run_state)

        ds_ref = type_props.get("dataset")
        is_metadata_db = bool(
            ds_ref and ds_ref.get("referenceName", "").startswith("DS_METADATA_DB")
        )
        first_row_only = bool(type_props.get("firstRowOnly", True))

        if not is_metadata_db or ds_ref is None:
            ref_name = ds_ref.get("referenceName") if ds_ref else None
            return ActivityResult(
                status="Failed",
                output={},
                error=(
                    "Lookup against non-metadata datasets is not supported in v1 "
                    f"(dataset={ref_name})"
                ),
            )

        rows = _execute_metadata_sql(
            sql=str(sql).strip(),
            db_path=ctx.metadata_db_path,
            run_state=run_state,
        )

        # Cloud ADF Lookup output shape:
        #   {"value": [...rows...], "count": N, "firstRow": rows[0]?}
        first_row = rows[0] if rows else None
        if first_row_only and first_row is not None:
            rows = [first_row]
        output: dict[str, Any] = {"value": rows, "count": len(rows)}
        if first_row is not None:
            output["firstRow"] = first_row
        return ActivityResult(status="Succeeded", output=output)


def _execute_metadata_sql(*, sql: str, db_path: Path, run_state: RunState) -> list[dict[str, Any]]:
    """Run SQL against the local SQLite metadata DB.

    Translates ``EXEC meta.sp_*`` stored-proc calls (the generator's
    typical Lookup body) to inline SQLite SQL that emulates the proc's
    effect. Bare SELECTs are run as-is, with the ``meta.`` schema
    prefix stripped (SQLite has no schemas).
    """
    if any(sql.upper().startswith(p.upper()) for p in _META_PROC_PREFIXES):
        return _emulate_meta_proc(sql=sql, db_path=db_path, run_state=run_state)

    sqlite_sql = _strip_meta_prefix(sql)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sqlite_sql)
        return [dict(row) for row in cursor.fetchall()]


_META_PREFIX_RE = re.compile(r"\bmeta\.", re.IGNORECASE)


def _strip_meta_prefix(sql: str) -> str:
    """Remove ``meta.`` schema qualifications for SQLite (which has no schemas)."""
    return _META_PREFIX_RE.sub("", sql)


def _emulate_meta_proc(*, sql: str, db_path: Path, run_state: RunState) -> list[dict[str, Any]]:
    """SQLite emulation of meta.sp_* procs the generator emits.

    The procs the generator emits are simple INSERT-and-return-id /
    UPDATE patterns. We pattern-match the proc name and execute the
    SQLite equivalent.
    """
    upper = sql.upper()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if upper.startswith("EXEC META.SP_RUN_LOG_STARTED"):
            cur.execute(
                "INSERT INTO run_log (master_run_id, dataset_id, status, "
                "started_at, run_type, pipeline_name, correlation_id) "
                "VALUES (?, ?, 'STARTED', CURRENT_TIMESTAMP, ?, ?, ?)",
                (
                    _extract_arg(sql, "master_run_id") or run_state.run_id,
                    _extract_arg(sql, "dataset_id"),
                    _extract_arg(sql, "run_type") or "INCREMENTAL",
                    _extract_arg(sql, "pipeline_name"),
                    _extract_arg(sql, "correlation_id") or run_state.run_id,
                ),
            )
            new_run_id = cur.lastrowid
            conn.commit()
            return [{"run_id": new_run_id, "status": "STARTED"}]

        if upper.startswith("EXEC META.SP_RUN_LOG_FINISHED"):
            run_id_arg = _extract_arg(sql, "run_id")
            status_arg = _extract_arg(sql, "status") or "SUCCESS"
            cur.execute(
                "UPDATE run_log SET status = ?, completed_at = CURRENT_TIMESTAMP, "
                "rows_processed = ?, error_message = ? WHERE run_id = ?",
                (
                    status_arg,
                    _extract_arg(sql, "rows_processed"),
                    _extract_arg(sql, "error_message"),
                    run_id_arg,
                ),
            )
            conn.commit()
            return [{"run_id": run_id_arg, "status": status_arg}]

        if upper.startswith("EXEC META.SP_MASTER_RUN_LOG_STARTED"):
            cur.execute(
                "INSERT INTO master_run_log (status, started_at, pipeline_name) "
                "VALUES ('STARTED', CURRENT_TIMESTAMP, ?)",
                (_extract_arg(sql, "pipeline_name"),),
            )
            new_master_id = cur.lastrowid
            conn.commit()
            return [{"master_run_id": new_master_id, "status": "STARTED"}]

        if upper.startswith("EXEC META.SP_MASTER_RUN_LOG_FINISHED"):
            master_id_arg = _extract_arg(sql, "master_run_id")
            master_status = _extract_arg(sql, "status") or "SUCCESS"
            cur.execute(
                "UPDATE master_run_log SET status = ?, completed_at = CURRENT_TIMESTAMP "
                "WHERE master_run_id = ?",
                (master_status, master_id_arg),
            )
            conn.commit()
            return [{"master_run_id": master_id_arg, "status": master_status}]

    return []


def _extract_arg(sql: str, name: str) -> str | None:
    """Extract ``@<name> = <value>`` from an EXEC string. Returns the value."""
    pattern = re.compile(rf"@{re.escape(name)}\s*=\s*('[^']*'|[^,\s]+)", re.IGNORECASE)
    match = pattern.search(sql)
    if not match:
        return None
    raw = match.group(1).strip()
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    return raw


# ──────────────────────────────────────────────────────────────────────
# SetVariable
# ──────────────────────────────────────────────────────────────────────


class SetVariableExecutor:
    activity_type = "SetVariable"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        var_name = str(type_props["variableName"])
        value = evaluate(type_props.get("value"), run_state)
        run_state.set_variable(var_name, value)
        return ActivityResult(status="Succeeded", output={"variable": var_name, "value": value})


# ──────────────────────────────────────────────────────────────────────
# Switch
# ──────────────────────────────────────────────────────────────────────


class SwitchExecutor:
    activity_type = "Switch"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        on_value = evaluate(type_props.get("on"), run_state)
        cases = type_props.get("cases", [])
        default_acts = type_props.get("defaultActivities", [])

        matched = None
        for case in cases:
            if str(case.get("value")) == str(on_value):
                matched = case.get("activities", [])
                break
        nested_activities = matched if matched is not None else default_acts

        if ctx.runner is None:
            return ActivityResult(
                status="Failed",
                output={},
                error=(
                    "Switch requires a runner (ExecutionContext.runner) "
                    "to dispatch nested activities."
                ),
            )

        results = ctx.runner.dispatch_activities(nested_activities, run_state, ctx)
        if any(r.status == "Failed" for r in results):
            return ActivityResult(status="Failed", output={"branch": str(on_value)})
        return ActivityResult(status="Succeeded", output={"branch": str(on_value)})


# ──────────────────────────────────────────────────────────────────────
# ForEach
# ──────────────────────────────────────────────────────────────────────


class ForEachExecutor:
    activity_type = "ForEach"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        items = evaluate(type_props.get("items"), run_state)
        nested = type_props.get("activities", [])
        if not isinstance(items, list):
            return ActivityResult(
                status="Failed",
                output={},
                error=f"ForEach items did not resolve to a list (got {type(items).__name__})",
            )

        if ctx.runner is None:
            return ActivityResult(
                status="Failed",
                output={},
                error="ForEach requires a runner (ExecutionContext.runner) to dispatch iterations.",
            )

        any_failure = False
        iteration_count = 0
        for idx, item in enumerate(items):
            iter_state = run_state.fork_for_iteration(idx, item)
            results = ctx.runner.dispatch_activities(nested, iter_state, ctx)
            iteration_count += 1
            if any(r.status == "Failed" for r in results):
                any_failure = True
        return ActivityResult(
            status="Failed" if any_failure else "Succeeded",
            output={"iterations": iteration_count},
        )


# ──────────────────────────────────────────────────────────────────────
# ExecutePipeline
# ──────────────────────────────────────────────────────────────────────


class ExecutePipelineExecutor:
    activity_type = "ExecutePipeline"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        child_name = type_props["pipeline"]["referenceName"]
        param_overrides = evaluate(type_props.get("parameters", {}), run_state)

        if ctx.runner is None:
            return ActivityResult(
                status="Failed",
                output={},
                error="ExecutePipeline requires a runner (ExecutionContext.runner).",
            )
        child_result = ctx.runner.run_pipeline(
            pipeline_name=child_name,
            parameters=param_overrides,
            ctx=ctx,
            parent_run_id=run_state.run_id,
        )
        return ActivityResult(
            status="Succeeded" if child_result.status == "Succeeded" else "Failed",
            output={"child_run_id": child_result.run_id, "child_pipeline": child_name},
            error=child_result.error,
        )


# ──────────────────────────────────────────────────────────────────────
# SqlServerStoredProcedure (Script)
# ──────────────────────────────────────────────────────────────────────


class SqlServerStoredProcedureExecutor:
    activity_type = "SqlServerStoredProcedure"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        proc_name = evaluate(type_props.get("storedProcedureName"), run_state)
        params_block = type_props.get("storedProcedureParameters", {})
        # Translate to EXEC SQL string for the same path the Lookup uses.
        param_assignments = ", ".join(
            f"@{name} = {_quote_value(evaluate(spec.get('value'), run_state))}"
            for name, spec in params_block.items()
        )
        sql = f"EXEC {proc_name} {param_assignments}".strip()
        rows = _execute_metadata_sql(sql=sql, db_path=ctx.metadata_db_path, run_state=run_state)
        return ActivityResult(status="Succeeded", output={"value": rows, "count": len(rows)})


def _quote_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


# ──────────────────────────────────────────────────────────────────────
# Copy — the headline activity
# ──────────────────────────────────────────────────────────────────────


class CopyExecutor:
    """Connector → LocalDeltaWriter for the local Bronze landing."""

    activity_type = "Copy"

    def execute(
        self, activity_json: dict[str, Any], run_state: RunState, ctx: ExecutionContext
    ) -> ActivityResult:
        type_props = activity_json.get("typeProperties", {})
        source_block = type_props.get("source", {})

        # Resolve input + output dataset references
        inputs = activity_json.get("inputs", [])
        outputs = activity_json.get("outputs", [])
        if not inputs or not outputs:
            return ActivityResult(
                status="Failed",
                output={},
                error="Copy activity requires both inputs[] and outputs[] dataset references.",
            )

        source_ds = resolve_dataset(inputs[0], ctx.registry, run_state)
        sink_ds = resolve_dataset(outputs[0], ctx.registry, run_state)
        source_ls = resolve_linked_service(source_ds.linked_service_ref, ctx.registry, run_state)

        # Connector + read iterator (SQL / file / endpoint)
        try:
            connector, batches = _open_source_read(
                source_ls=source_ls,
                source_ds=source_ds,
                source_block=source_block,
                run_state=run_state,
            )
        except Exception as exc:
            return ActivityResult(
                status="Failed",
                output={},
                error=f"Failed to open source read: {exc}",
            )

        sink_path = _resolve_sink_path(sink_ds, lakes_root=ctx.lakes_root)
        writer = LocalDeltaWriter(table_path=sink_path)

        try:
            stats = writer.write_batched(batches, run_id=run_state.run_id)
        except Exception as exc:
            return ActivityResult(
                status="Failed",
                output={},
                error=f"Sink write failed: {exc}",
            )
        finally:
            close = getattr(connector, "close", None)
            if callable(close):
                close()

        return ActivityResult(
            status="Succeeded",
            output={
                "rowsRead": stats.rows_written,
                "rowsCopied": stats.rows_written,
                "batchesWritten": stats.batches_written,
                "sinkPath": str(sink_path),
            },
        )


def _open_source_read(
    *,
    source_ls: Any,
    source_ds: Any,
    source_block: dict[str, Any],
    run_state: RunState,
) -> tuple[Any, Any]:
    """Build the connector + open the read iterator. Returns (connector, batches).

    Selects the right read method (read_query / read_file / read_endpoint)
    by the dataset type. New dataset types extend this dispatch.
    """
    ls_type = source_ls.type
    if ls_type in {"AzurePostgreSqlSource", "Postgres", "Postgresql"} or _is_postgres(source_ds):
        connector = _build_postgres_connector(source_ls.properties)
        connector.connect()
        sql = _resolved_sql_from_source_block(source_block, run_state, source_ds)
        return connector, connector.read_query(sql)
    if _is_mysql(source_ds):
        connector = _build_mysql_connector(source_ls.properties)
        connector.connect()
        sql = _resolved_sql_from_source_block(source_block, run_state, source_ds)
        return connector, connector.read_query(sql)
    if _is_mssql(source_ds):
        connector = _build_mssql_connector(source_ls.properties)
        connector.connect()
        sql = _resolved_sql_from_source_block(source_block, run_state, source_ds)
        return connector, connector.read_query(sql)
    if _is_oracle(source_ds):
        connector = _build_oracle_connector(source_ls.properties)
        connector.connect()
        sql = _resolved_sql_from_source_block(source_block, run_state, source_ds)
        return connector, connector.read_query(sql)
    raise NotImplementedError(
        f"Local Copy executor does not yet support source type {source_ds.type!r}. "
        "v1 supports postgres / mysql / mssql / oracle SQL sources; "
        "SFTP + REST are scaffolded but not yet wired in the Copy executor."
    )


def _is_postgres(ds: Any) -> bool:
    return ds.type in {"PostgresqlTable", "AzurePostgreSqlTable", "PostgresTable"}


def _is_mysql(ds: Any) -> bool:
    return ds.type in {"MySqlTable", "MariaDbTable"}


def _is_mssql(ds: Any) -> bool:
    return ds.type in {"AzureSqlTable", "SqlServerTable"}


def _is_oracle(ds: Any) -> bool:
    return bool(ds.type == "OracleTable")


def _resolved_sql_from_source_block(
    source_block: dict[str, Any], run_state: RunState, source_ds: Any
) -> str:
    """Resolve the SQL the Copy activity should run against the source.

    The generator emits ``sqlReaderQuery`` (Expression) for parameterised
    queries. If absent, fall back to a ``SELECT *`` over the dataset's
    schema_name + table_name.
    """
    sql_node = source_block.get("sqlReaderQuery") or source_block.get("query")
    if sql_node is not None:
        return str(evaluate(sql_node, run_state))
    schema = source_ds.type_properties.get("schema") or source_ds.type_properties.get("schema_name")
    table = source_ds.type_properties.get("table") or source_ds.type_properties.get("table_name")
    # Identifiers come from the resolved dataset config (deploy-time-controlled
    # metadata), not user input. Quoted to match cloud ADF's emitted SQL shape.
    if schema and table:
        return f'SELECT * FROM "{schema}"."{table}"'  # noqa: S608 — identifiers from metadata
    if table:
        return f'SELECT * FROM "{table}"'  # noqa: S608 — identifier from metadata
    raise ValueError("Cannot derive source SQL: no sqlReaderQuery and no schema/table on dataset.")


def _build_postgres_connector(props: dict[str, Any]) -> Any:
    type_props = props.get("typeProperties", {})
    return PostgresConnector(
        PostgresConnectionParams(
            host=str(type_props.get("server", "")),
            port=int(type_props.get("port", 5432)),
            database=str(type_props.get("database", "")),
            user=str(type_props.get("username", "")),
            password=str(type_props.get("password", {}).get("value", ""))
            if isinstance(type_props.get("password"), dict)
            else str(type_props.get("password", "")),
            sslmode=str(type_props.get("sslmode", "disable")),
        )
    )


def _build_mysql_connector(props: dict[str, Any]) -> Any:
    type_props = props.get("typeProperties", {})
    return MysqlConnector(
        MysqlConnectionParams(
            server=str(type_props.get("server", "")),
            port=int(type_props.get("port", 3306)),
            database=str(type_props.get("database", "")),
            user=str(type_props.get("username", "")),
            password=str(type_props.get("password", "")),
        )
    )


def _build_mssql_connector(props: dict[str, Any]) -> Any:
    type_props = props.get("typeProperties", {})
    return MssqlConnector(
        MssqlConnectionParams(
            server=str(type_props.get("server", "")),
            port=int(type_props.get("port", 1433)),
            database=str(type_props.get("database", "")),
            user=str(type_props.get("username", "")),
            password=str(type_props.get("password", "")),
            encrypt=str(type_props.get("encrypt", "no")),
            trust_server_certificate=str(type_props.get("trust_server_certificate", "yes")),
        )
    )


def _build_oracle_connector(props: dict[str, Any]) -> Any:
    type_props = props.get("typeProperties", {})
    return OracleConnector(
        OracleConnectionParams(
            server=str(type_props.get("server", "")),
            port=int(type_props.get("port", 1521)),
            service_name=str(type_props.get("serviceName", "")),
            user=str(type_props.get("username", "")),
            password=str(type_props.get("password", "")),
        )
    )


def _resolve_sink_path(sink_ds: Any, *, lakes_root: Path) -> Path:
    """Resolve a sink Dataset to a local Delta path.

    Cloud ADF sinks point at ADLS via container + folder. Locally we
    map to ``<lakes_root>/<container>/<folder_path>``.
    """
    type_props = sink_ds.type_properties
    location = type_props.get("location", {})
    container = location.get("fileSystem") or type_props.get("container") or "bronze"
    folder = location.get("folderPath") or type_props.get("folder_path") or ""
    return lakes_root / container / folder


# ──────────────────────────────────────────────────────────────────────
# Factory
# ──────────────────────────────────────────────────────────────────────


def build_activity_executors() -> dict[str, ActivityExecutor]:
    """Return the v1 activity-type → executor map."""
    return {
        "Lookup": LookupExecutor(),
        "SetVariable": SetVariableExecutor(),
        "Switch": SwitchExecutor(),
        "ForEach": ForEachExecutor(),
        "ExecutePipeline": ExecutePipelineExecutor(),
        "SqlServerStoredProcedure": SqlServerStoredProcedureExecutor(),
        "Copy": CopyExecutor(),
    }


__all__ = [
    "ActivityExecutor",
    "ActivityResult",
    "CopyExecutor",
    "ExecutePipelineExecutor",
    "ExecutionContext",
    "ForEachExecutor",
    "LookupExecutor",
    "SetVariableExecutor",
    "SqlServerStoredProcedureExecutor",
    "SwitchExecutor",
    "UnsupportedActivityError",
    "build_activity_executors",
]
