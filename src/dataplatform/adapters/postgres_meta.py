"""The metadata store on PostgreSQL (ADR-010). Every read and write is one call to a function in
adapters/sql/postgres_meta.sql; this file holds no table names (DPC-0033, checked by
tests/dataplatform/test_meta_store.py).

One adapter for every PostgreSQL: the laptop's server, a managed one, or the engine's managed PostgreSQL (Lakebase on
Databricks). The binding picks only the connection string, read from DP_META_DSN (default dbname=dp_meta locally);
there is no second code path per product. Needs psycopg 3 (`--with 'psycopg[binary]'`). The DSN never holds a password
in code: local sockets use peer auth; a cloud binding resolves the secret or token at run time.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from dataplatform.meta.port import (
    BackfillPlan,
    Column,
    Correction,
    DatasetRef,
    DqResult,
    DqRule,
    FileRecord,
    InvalidArgument,
    MetaError,
    NotFound,
    RunStateError,
    Setting,
    SettingDef,
    StepRecord,
    UnknownSetting,
)

DEFAULT_DSN = "dbname=dp_meta"
DDL = Path(__file__).with_name("sql") / "postgres_meta.sql"
ERRORS: dict[str, type[MetaError]] = {"DPM01": UnknownSetting, "DPM02": NotFound, "DPM03": InvalidArgument,
                                      "DPM04": RunStateError}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"not a safe identifier: {name!r}")
    return f'"{name}"'


def _dbname(dsn: str) -> str:
    return dict(p.split("=", 1) for p in dsn.split() if "=" in p).get("dbname", "")


def create_database(dsn: str) -> None:
    """Create the database named in a key=value DSN if it is missing (local and tests; a cloud binding makes it)."""
    import psycopg

    name = _dbname(dsn)
    with psycopg.connect(dsn.replace(f"dbname={name}", "dbname=postgres"), autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            admin.execute(f"CREATE DATABASE {_ident(name)}")  # nosec B608 - identifier checked by _ident()


def install(conn: Any, service_role: str | None = None) -> None:
    """Create or update the schemas, tables and functions (idempotent). With a service role, grant it the functions
    and nothing else: the role can call every procedure and read no table (DPC-0033)."""
    conn.execute(DDL.read_text())
    if service_role:
        r = _ident(service_role)
        for stmt in (f"GRANT USAGE ON SCHEMA config, runtime TO {r}",
                     f"REVOKE ALL ON ALL TABLES IN SCHEMA config, runtime FROM {r}",
                     f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA config, runtime TO {r}"):
            conn.execute(stmt)
    conn.commit()


class PostgresMetaStore:
    def __init__(self, conn: Any):
        self.conn = conn

    @classmethod
    def connect(cls, dsn: str | None = None, create: bool = False, install_schema: bool = False) -> PostgresMetaStore:
        import psycopg

        dsn = dsn or os.environ.get("DP_META_DSN", DEFAULT_DSN)
        if create:
            create_database(dsn)
        conn = psycopg.connect(dsn)
        if install_schema:
            install(conn)
        conn.autocommit = True  # each procedure call is its own transaction
        return cls(conn)

    def _call(self, sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        import psycopg

        try:
            return list(self.conn.execute(sql, args).fetchall())
        except psycopg.Error as exc:
            kind = ERRORS.get(getattr(exc, "sqlstate", "") or "")
            if kind is None:
                raise
            raise kind(str(getattr(exc.diag, "message_primary", "") or exc)) from exc

    def _one(self, sql: str, args: tuple[Any, ...] = ()) -> Any:
        rows = self._call(sql, args)
        return rows[0][0] if rows else None

    @staticmethod
    def _json(x: Any) -> str:
        return json.dumps(x, default=str)

    # ---- configuration ----
    def register_settings(self, settings: list[SettingDef]) -> None:
        self._call("SELECT config.register_settings(%s::jsonb)",
                   (self._json([{**asdict(d), "allowed": list(d.allowed) if d.allowed else None} for d in settings]),))

    def load_domain(self, doc: dict[str, Any]) -> None:
        self._call("SELECT config.load_domain(%s::jsonb)", (self._json(doc),))

    def load_dataset(self, doc: dict[str, Any]) -> None:
        self._call("SELECT config.load_dataset(%s::jsonb)", (self._json(doc),))

    def settings_registry(self) -> list[SettingDef]:
        return [SettingDef(key=k, scope=s, value_type=t, default=d, allowed=tuple(a) if a else None, description=desc,
                           status=st)
                for s, k, t, d, a, desc, st in self._call("SELECT * FROM config.settings_registry()")]

    def datasets(self, domain: str) -> list[DatasetRef]:
        return [DatasetRef(*r) for r in self._call("SELECT * FROM config.list_datasets(%s)", (domain,))]

    def settings(self, source: str, dataset: str) -> list[Setting]:
        return [Setting(*r) for r in self._call("SELECT * FROM config.dataset_settings(%s, %s)", (source, dataset))]

    def columns(self, source: str, dataset: str) -> list[Column]:
        return [Column(*r) for r in self._call("SELECT * FROM config.dataset_columns(%s, %s)", (source, dataset))]

    def business_keys(self, source: str, dataset: str, kind: str = "primary") -> list[str]:
        return [r[0] for r in self._call("SELECT * FROM config.business_keys(%s, %s, %s)", (source, dataset, kind))]

    def dataset_view(self, source: str, dataset: str) -> dict[str, Any]:
        view: dict[str, Any] = self._one("SELECT config.dataset_view(%s, %s)", (source, dataset))
        return view

    # ---- run log ----
    def start_run(self, run_id: str, domain: str, env: str, trigger: str = "manual", run_type: str = "normal") -> None:
        self._call("SELECT runtime.start_run(%s, %s, %s, %s, %s)", (run_id, domain, env, trigger, run_type))

    def start_dataset_run(self, run_id: str, source: str, dataset: str, config_fingerprint: str,
                          config: dict[str, Any]) -> str | None:
        before: str | None = self._one("SELECT runtime.start_dataset_run(%s, %s, %s, %s, %s::jsonb)",
                                       (run_id, source, dataset, config_fingerprint, self._json(config)))
        return before

    def log_step(self, step: StepRecord) -> None:
        self._call("SELECT runtime.log_step(%s, %s, %s, %s, %s, %s::jsonb, %s)",
                   (step.run_id, step.source, step.dataset, step.step, step.status, self._json(step.counts),
                    step.message))

    def log_file(self, file: FileRecord) -> None:
        self._call("SELECT runtime.log_file(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (file.run_id, file.source, file.dataset, file.batch_key, file.file_name, file.window,
                    file.checksum, file.declared_rows, file.rows, file.status, file.reason, file.load_type))

    def batch_status(self, source: str, dataset: str, batch_key: str) -> str | None:
        status: str | None = self._one("SELECT runtime.batch_status(%s, %s, %s)", (source, dataset, batch_key))
        return status

    def log_error(self, run_id: str, source: str | None, dataset: str | None, step: str, error_class: str,
                  message: str) -> None:
        self._call("SELECT runtime.log_error(%s, %s, %s, %s, %s, %s)",
                   (run_id, source, dataset, step, error_class, message))

    def finish_dataset_run(self, run_id: str, source: str, dataset: str, status: str,
                           counts: dict[str, int] | None = None, watermark: str | None = None,
                           bookmarks: dict[str, str] | None = None, message: str = "") -> None:
        self._call("SELECT runtime.finish_dataset_run(%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s)",
                   (run_id, source, dataset, status, self._json(counts or {}), watermark,
                    self._json(bookmarks or {}), message))

    def finish_run(self, run_id: str) -> str:
        return str(self._one("SELECT runtime.finish_run(%s)", (run_id,)))

    def watermark(self, source: str, dataset: str) -> str | None:
        mark: str | None = self._one("SELECT runtime.get_watermark(%s, %s)", (source, dataset))
        return mark

    def bookmarks(self, source: str, dataset: str) -> dict[str, str]:
        return dict(self._call("SELECT * FROM runtime.get_bookmarks(%s, %s)", (source, dataset)))

    def reset_watermark(self, source: str, dataset: str, value: str | None, reason: str, changed_by: str) -> None:
        self._call("SELECT runtime.reset_watermark(%s, %s, %s, %s, %s)", (source, dataset, value, reason, changed_by))

    def run_report(self, run_id: str) -> dict[str, Any]:
        report: dict[str, Any] = self._one("SELECT runtime.run_report(%s)", (run_id,))
        return report

    def dataset_files(self, source: str, dataset: str, window: str | None = None) -> list[FileRecord]:
        return [FileRecord(run_id=r, source=source, dataset=dataset, batch_key=k, file_name=n, window=w, checksum=c,
                           declared_rows=dr, rows=rows, status=st, reason=why, load_type=lt)
                for r, k, n, w, c, dr, rows, st, why, lt
                in self._call("SELECT * FROM runtime.dataset_files(%s, %s, %s)", (source, dataset, window))]

    # ---- corrections ----
    def record_corrections(self, plan: BackfillPlan, corrections: list[Correction]) -> None:
        rows = [{"key": c.key, "field": c.field, "value": c.value, "was": c.was, "applies_from": c.applies_from,
                 "applies_to": c.applies_to} for c in corrections]
        self._call("SELECT runtime.record_corrections(%s::jsonb, %s::jsonb)",
                   (self._json({**asdict(plan), "windows": list(plan.windows)}), self._json(rows)))

    def corrections(self, source: str, dataset: str, as_of: str | None = None) -> list[Correction]:
        return [Correction(key=k, field=f, value=v, was=w, applies_from=a, applies_to=b, plan_id=p, status=st)
                for k, f, v, w, a, b, p, st
                in self._call("SELECT * FROM runtime.corrections(%s, %s, %s)", (source, dataset, as_of))]

    def backfill_plans(self, source: str, dataset: str) -> list[BackfillPlan]:
        return [BackfillPlan(plan_id=p, source=source, dataset=dataset, windows=tuple(w), activate_on=a, reason=r,
                             recorded_by=by)
                for p, w, a, r, by in self._call("SELECT * FROM runtime.backfill_plans(%s, %s)", (source, dataset))]

    def acquire_lease(self, table: str, run_id: str, ttl_seconds: int) -> str | None:
        holder: str | None = self._one("SELECT runtime.acquire_lease(%s, %s, %s)", (table, run_id, ttl_seconds))
        return holder

    def release_lease(self, table: str, run_id: str) -> None:
        self._call("SELECT runtime.release_lease(%s, %s)", (table, run_id))

    def void_batch(self, source: str, dataset: str, batch_key: str, reason: str, approved_by: str) -> None:
        self._call("SELECT runtime.void_batch(%s, %s, %s, %s, %s)", (source, dataset, batch_key, reason, approved_by))

    def save_profile(self, run_id: str, source: str, dataset: str, profile: dict[str, Any]) -> None:
        self._call("SELECT runtime.save_profile(%s, %s, %s, %s::jsonb)", (run_id, source, dataset, self._json(profile)))

    def last_profile(self, source: str, dataset: str) -> dict[str, Any] | None:
        rows = self._call("SELECT runtime.last_profile(%s, %s)", (source, dataset))
        return dict(rows[0][0]) if rows and rows[0][0] is not None else None

    # ---- data quality ----
    def set_dq_rules(self, source: str, dataset: str, rules: list[DqRule]) -> None:
        rows = [{"rule_id": r.rule_id, "column": r.column, "check": r.check, "params": r.params,
                 "severity": r.severity, "action": r.action, "enabled": r.enabled} for r in rules]
        self._call("SELECT config.set_dq_rules(%s, %s, %s::jsonb)", (source, dataset, self._json(rows)))

    def dq_rules(self, source: str, dataset: str) -> list[DqRule]:
        return [DqRule(rule_id=i, column=c, check=k, params=dict(p or {}), severity=s, action=a, enabled=bool(e))
                for i, c, k, p, s, a, e in self._call("SELECT * FROM config.dq_rules(%s, %s)", (source, dataset))]

    def log_dq_results(self, results: list[DqResult]) -> None:
        rows = [{"run_id": r.run_id, "source": r.source, "dataset": r.dataset, "rule_id": r.rule_id,
                 "checked": r.checked, "failed": r.failed, "action": r.action, "detail": r.detail} for r in results]
        self._call("SELECT runtime.log_dq_results(%s::jsonb)", (self._json(rows),))

    def dq_results(self, run_id: str, source: str | None = None, dataset: str | None = None) -> list[DqResult]:
        return [DqResult(run_id=run_id, source=s, dataset=d, rule_id=i, checked=int(c), failed=int(f), action=a,
                         detail=x)
                for s, d, i, c, f, a, x in self._call("SELECT * FROM runtime.dq_results(%s, %s, %s)",
                                                       (run_id, source, dataset))]
