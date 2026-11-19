"""Metadata repository — the boundary between Python and the metadata DB.

Defines a ``MetadataRepository`` Protocol (port) and a SQLite
implementation (adapter). Real Azure SQL DB adapter lands in the realm
package later; tests use SQLite in-memory.

Per ``docs/code_design.md`` → "Dependency Inversion": use cases depend
on the Protocol; the concrete SQLite class is wired at the composition
root (CLI). Nothing in ``core/use_cases/`` knows whether it's talking
to SQLite or Azure SQL DB.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Protocol

from lake_ingestion_framework.core.dq.types import (
    Action,
    Container,
    DqRule,
    DqRuleSet,
    InjectionPoint,
    Layer,
    QuarantineStrategy,
    RuleSource,
    RuleType,
    Severity,
    Tech,
)
from lake_ingestion_framework.core.exceptions import (
    MetadataConnectionError,
    MetadataError,
    UnknownKeyError,
    ValidationError,
)
from lake_ingestion_framework.core.metadata.keys import (
    KeyTable,
    lookup_key,
    parse_value,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import SourceEnv

# ──────────────────────────────────────────────────────────────────────
# Row dataclasses
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SourceRow:
    """One row of meta.source_master."""

    source_id: int
    source_name: str
    source_env: str
    source_type: SourceType
    is_enabled: bool


@dataclass(frozen=True)
class DatasetRow:
    """One row of meta.dataset_master."""

    dataset_id: int
    source_id: int
    dataset_name: str
    is_enabled: bool


@dataclass(frozen=True)
class DatasetColumnRow:
    """One row of meta.dataset_config_columns.

    Wave-1 (schema v3) extends with column-transform metadata. All new
    fields default to None / 'strict' / () so existing call sites stay
    backwards-compatible.
    """

    dataset_id: int
    column_name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    is_primary_key: bool
    is_excluded: bool
    pii_class: str
    source_path: str | None
    # Wave 1 — column-transform fields. ``column_name`` is the BRONZE
    # name; everything below is what processing should produce.
    silver_column_name: str | None = None
    """Rename target. When None, Silver inherits the Bronze name."""
    silver_data_type: str | None = None
    """Target type for safe_cast. When None, inherit Bronze's type."""
    cast_safety: str = "strict"
    """``'strict'`` (default) raises on cast failure; ``'try_cast'``
    returns NULL on failure."""
    null_replacement: str | None = None
    """Literal substituted for NULL post-transform."""
    transforms: tuple[str, ...] = ()
    """Opt-in processor names (e.g. ``phone_normalize_e164``) layered
    on top of the group defaults."""
    skip_groups: tuple[str, ...] = ()
    """Group names to bypass for this column."""
    skip_processors: tuple[str, ...] = ()
    """Individual processor names to bypass for this column."""
    column_role: str | None = None
    """Optional role tag (``'code'``, ``'identifier'``, ...) used by
    ``case_normalize`` to decide auto-uppercase / auto-lowercase."""


@dataclass(frozen=True)
class DqRuleRow:
    """One row of meta.dq_rules — what comes out of the DB.

    Distinct from :class:`DqRule` in ``core.dq.types`` because this
    carries the surrogate keys (``rule_id``, ``dataset_id``) the DB
    assigns. The pure :class:`DqRule` is name-keyed for portability
    across DBs.
    """

    rule_id: int
    rule_name: str
    dataset_id: int
    layer: Layer
    rule_type: RuleType
    tech: Tech
    container: Container
    predicate: str
    injection_point: InjectionPoint
    severity: Severity
    action: Action
    quarantine_strategy: QuarantineStrategy | None
    sample_cap: int
    is_enabled: bool
    source: RuleSource
    authored_by: str | None
    description: str


@dataclass(frozen=True)
class PiiPolicyRow:
    """One row of meta.pii_policy.

    See ``docs/standard_processors.md`` §6 (pii group). One row per
    ``(pii_class, env)`` pair; uniqueness is enforced by the DDL.
    """

    policy_id: int
    pii_class: str
    env: str
    action: str  # 'pass_through' | 'hash' | 'mask' | 'tokenize' | 'redact'
    salt_kv_ref: str | None
    redact_value: str | None
    mask_visible: int


@dataclass(frozen=True)
class DqResultRow:
    """One row of meta.dq_results — what a single rule evaluation produced.

    ``passed=True`` and ``error_message is None`` is a clean pass.
    ``passed=False`` and ``error_message is None`` is a predicate-fired
    failure. ``error_message is not None`` is an engine error and
    ``passed`` is forced to False (the rule didn't pass — but it also
    didn't really run).
    """

    result_id: int
    rule_id: int
    run_id: str
    evaluated_at: str
    passed: bool
    violators_total: int
    """Lower bound on violator rows. -1 means "not counted" (the
    cheap-eval bool path with ``sample_cap=0``)."""

    sample_captured: int
    """How many rows landed in dq_violations for this result. Always
    ≤ ``rule.sample_cap``."""

    action_taken: str
    """One of :class:`ActionTaken` — the action engine's decision."""

    duration_ms: int
    error_message: str | None


@dataclass(frozen=True)
class DqViolationRow:
    """One row of meta.dq_violations — captured violator identity."""

    violation_id: int
    rule_id: int
    result_id: int
    pk_columns_json: str
    violated_at: str


@dataclass(frozen=True)
class DqRuleSyncResult:
    """What ``upsert_dq_rule_set`` did. All counts are per-rule.

    ``soft_deleted`` covers rules in DB but not in YAML — set to
    ``is_enabled=0`` to keep the audit trail. ``hard_deleted`` covers
    explicit ``--delete-missing`` deletes.
    """

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    soft_deleted: int = 0
    hard_deleted: int = 0


# ──────────────────────────────────────────────────────────────────────
# Repository Protocol — what use cases depend on
# ──────────────────────────────────────────────────────────────────────


class MetadataRepository(Protocol):
    """Metadata-DB operations that Python performs.

    All writes are validated against the key registry. Implementations
    are expected to be transactional per public method.
    """

    # ── Bootstrap
    def bootstrap_schema(self) -> None:
        """Create tables if they don't exist. Idempotent."""

    def schema_version(self) -> int:
        """Return ``platform_config.metadata_schema_version``."""

    # ── Sources
    def upsert_source(
        self,
        *,
        source_name: str,
        source_env: str,
        source_type: SourceType,
        is_enabled: bool,
        config: dict[str, str],
    ) -> int:
        """Insert or update a source. Returns the ``source_id``."""

    def get_source(self, source_name: str, source_env: str) -> SourceRow | None:
        """Find a source by (name, env). Returns None if absent."""

    def list_sources(self) -> list[SourceRow]:
        """Return all sources."""

    def get_source_config(self, source_id: int) -> dict[str, str]:
        """Return the ``source_config`` KV dict for one source."""

    def get_dataset_config(self, dataset_id: int) -> dict[str, str]:
        """Return the ``dataset_config`` KV dict for one dataset."""

    def list_platform_config(self) -> dict[str, str]:
        """Return the entire ``platform_config`` KV dict for the env."""

    # ── Datasets
    def upsert_dataset(
        self,
        *,
        source_id: int,
        dataset_name: str,
        is_enabled: bool,
        config: dict[str, str],
        columns: list[DatasetColumnRow] | None = None,
    ) -> int:
        """Insert or update a dataset. Returns the ``dataset_id``."""

    def get_dataset(self, source_id: int, dataset_name: str) -> DatasetRow | None: ...

    def list_datasets(self, source_id: int | None = None) -> list[DatasetRow]: ...

    def list_dataset_columns(self, dataset_id: int) -> list[DatasetColumnRow]:
        """Return all column rows for one dataset, ordered by
        ``ordinal_position``. Used by the processor engine and by the
        ``processors describe`` CLI."""

    # ── Platform config
    def upsert_platform_config(self, kv: dict[str, str]) -> None:
        """Bulk-upsert platform_config keys. Validates each against the registry."""

    # ── DQ rules (Phase 1 of the DQ engine)
    def upsert_dq_rule_set(
        self,
        rule_set: DqRuleSet,
        *,
        dataset_id: int,
        delete_missing: bool = False,
    ) -> DqRuleSyncResult:
        """Upsert all rules in ``rule_set`` for the given ``(dataset_id, layer)``.

        Diff against existing rules in the DB:
          - new rules → INSERT
          - changed rules (predicate/severity/action/etc. differ) → UPDATE
          - unchanged → no-op
          - rules in DB but not in ``rule_set`` → soft-delete
            (``is_enabled=0``) by default; hard-delete only when
            ``delete_missing=True``

        ``rule_set.dataset_name`` is informational; the DB row uses
        ``dataset_id``. Caller (config sync) resolves the name to ID.
        """

    def list_dq_rules(
        self,
        *,
        dataset_id: int | None = None,
        layer: Layer | None = None,
        only_enabled: bool = True,
    ) -> list[DqRuleRow]:
        """Read DQ rules from the metadata DB; runtime entry point."""

    def get_dq_rule(self, rule_id: int) -> DqRuleRow | None:
        """Fetch one rule by surrogate key."""

    # ── DQ results + violations (Phase 3 of the DQ engine)
    def record_dq_result(
        self,
        *,
        rule_id: int,
        run_id: str,
        passed: bool,
        violators_total: int,
        sample_captured: int,
        action_taken: str,
        duration_ms: int = 0,
        error_message: str | None = None,
    ) -> int:
        """Insert one ``meta.dq_results`` row; return ``result_id``.

        Audit posture: every evaluation lands a row (passed, failed,
        errored alike). The action engine is the only writer.
        """

    def record_dq_violations(
        self,
        *,
        rule_id: int,
        result_id: int,
        rows: list[dict[str, object]],
    ) -> int:
        """Bulk-insert violator identity rows; return rows inserted.

        Each ``row`` is serialised to ``pk_columns_json``. Caller
        slices to PKs first when known; otherwise the entire sample
        row lands here (named ``pk_columns_json`` for schema legacy —
        the column is "captured violator identity" semantically).
        """

    def list_dq_results(
        self,
        *,
        rule_id: int | None = None,
        run_id: str | None = None,
    ) -> list[DqResultRow]:
        """Read DQ results (test + CLI helper)."""

    def list_dq_violations(self, *, result_id: int) -> list[DqViolationRow]:
        """Read DQ violations attached to one result."""

    # ── PII policy (Wave 2 of standard processors)
    def upsert_pii_policy(
        self,
        *,
        pii_class: str,
        env: str,
        action: str,
        salt_kv_ref: str | None = None,
        redact_value: str | None = None,
        mask_visible: int = 4,
    ) -> int:
        """Insert or update one ``(pii_class, env)`` row; returns ``policy_id``."""

    def list_pii_policy(self, *, env: str | None = None) -> list[PiiPolicyRow]:
        """Read PII policy rows; optionally filtered to one env."""


# ──────────────────────────────────────────────────────────────────────
# SQLite implementation — for local dev + tests
# ──────────────────────────────────────────────────────────────────────


class SqliteMetadataRepository:
    """SQLite-backed adapter. Mostly used in tests + local dev.

    Use ``:memory:`` as the path for an ephemeral test DB, or a real
    file path for a local-dev DB. Connection is opened lazily on first
    use; closed via ``close()``.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection | None = None

    # ── connection management

    def _connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        try:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
        except sqlite3.Error as exc:
            raise MetadataConnectionError(
                f"Cannot open SQLite DB {self._db_path!r}: {exc}"
            ) from exc
        self._conn = conn
        return conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """Run a block as one transaction; rollback on exception."""
        conn = self._connect()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    # ── bootstrap

    def bootstrap_schema(self) -> None:
        ddl = (
            resources.files("lake_ingestion_framework.core.metadata")
            .joinpath("schema_sqlite.sql")
            .read_text()
        )
        with self._tx() as conn:
            conn.executescript(ddl)

    def schema_version(self) -> int:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT value FROM platform_config WHERE key = 'metadata_schema_version'"
            )
        except sqlite3.OperationalError as exc:
            # Most likely the schema isn't bootstrapped yet.
            raise MetadataError(f"metadata schema not bootstrapped: {exc}") from exc
        row = cur.fetchone()
        if row is None:
            raise MetadataError("metadata_schema_version row missing — bootstrap first.")
        return int(row["value"])

    # ── sources

    def upsert_source(
        self,
        *,
        source_name: str,
        source_env: str,
        source_type: SourceType,
        is_enabled: bool,
        config: dict[str, str],
    ) -> int:
        # Validate identity
        SourceEnv.parse(source_env)

        # Validate every config key against the registry (with source_type scope)
        _validate_kv(KeyTable.SOURCE_CONFIG, config, source_type=source_type)

        with self._tx() as conn:
            existing = conn.execute(
                "SELECT source_id FROM source_master WHERE source_name = ? AND source_env = ?",
                (source_name, source_env),
            ).fetchone()
            if existing is None:
                cur = conn.execute(
                    "INSERT INTO source_master (source_name, source_env, source_type, is_enabled) "
                    "VALUES (?, ?, ?, ?)",
                    (source_name, source_env, source_type.value, 1 if is_enabled else 0),
                )
                source_id = int(cur.lastrowid or 0)
            else:
                source_id = int(existing["source_id"])
                conn.execute(
                    "UPDATE source_master SET source_type = ?, is_enabled = ?, "
                    "updated_at = CURRENT_TIMESTAMP WHERE source_id = ?",
                    (source_type.value, 1 if is_enabled else 0, source_id),
                )

            # Replace KV rows wholesale — simpler than per-key diff at this layer
            conn.execute("DELETE FROM source_config WHERE source_id = ?", (source_id,))
            conn.executemany(
                "INSERT INTO source_config (source_id, key, value) VALUES (?, ?, ?)",
                [(source_id, k, v) for k, v in config.items()],
            )

        return source_id

    def get_source(self, source_name: str, source_env: str) -> SourceRow | None:
        conn = self._connect()
        row = conn.execute(
            "SELECT * FROM source_master WHERE source_name = ? AND source_env = ?",
            (source_name, source_env),
        ).fetchone()
        return _row_to_source(row) if row is not None else None

    def list_sources(self) -> list[SourceRow]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM source_master ORDER BY source_name, source_env"
        ).fetchall()
        return [_row_to_source(r) for r in rows]

    def get_source_config(self, source_id: int) -> dict[str, str]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT key, value FROM source_config WHERE source_id = ? ORDER BY key",
            (source_id,),
        ).fetchall()
        return {str(r["key"]): str(r["value"]) for r in rows if r["value"] is not None}

    def get_dataset_config(self, dataset_id: int) -> dict[str, str]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT key, value FROM dataset_config WHERE dataset_id = ? ORDER BY key",
            (dataset_id,),
        ).fetchall()
        return {str(r["key"]): str(r["value"]) for r in rows if r["value"] is not None}

    def list_platform_config(self) -> dict[str, str]:
        conn = self._connect()
        rows = conn.execute("SELECT key, value FROM platform_config ORDER BY key").fetchall()
        return {str(r["key"]): str(r["value"]) for r in rows if r["value"] is not None}

    # ── datasets

    def upsert_dataset(
        self,
        *,
        source_id: int,
        dataset_name: str,
        is_enabled: bool,
        config: dict[str, str],
        columns: list[DatasetColumnRow] | None = None,
    ) -> int:
        _validate_kv(KeyTable.DATASET_CONFIG, config)

        with self._tx() as conn:
            existing = conn.execute(
                "SELECT dataset_id FROM dataset_master WHERE source_id = ? AND dataset_name = ?",
                (source_id, dataset_name),
            ).fetchone()
            if existing is None:
                cur = conn.execute(
                    "INSERT INTO dataset_master (source_id, dataset_name, is_enabled) "
                    "VALUES (?, ?, ?)",
                    (source_id, dataset_name, 1 if is_enabled else 0),
                )
                dataset_id = int(cur.lastrowid or 0)
            else:
                dataset_id = int(existing["dataset_id"])
                conn.execute(
                    "UPDATE dataset_master SET is_enabled = ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE dataset_id = ?",
                    (1 if is_enabled else 0, dataset_id),
                )

            conn.execute("DELETE FROM dataset_config WHERE dataset_id = ?", (dataset_id,))
            conn.executemany(
                "INSERT INTO dataset_config (dataset_id, key, value) VALUES (?, ?, ?)",
                [(dataset_id, k, v) for k, v in config.items()],
            )

            if columns is not None:
                conn.execute(
                    "DELETE FROM dataset_config_columns WHERE dataset_id = ?", (dataset_id,)
                )
                conn.executemany(
                    "INSERT INTO dataset_config_columns "
                    "(dataset_id, column_name, data_type, is_nullable, ordinal_position, "
                    "is_primary_key, is_excluded, pii_class, source_path, "
                    "silver_column_name, silver_data_type, cast_safety, null_replacement, "
                    "transforms_json, skip_groups_json, skip_processors_json, column_role) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            dataset_id,
                            c.column_name,
                            c.data_type,
                            1 if c.is_nullable else 0,
                            c.ordinal_position,
                            1 if c.is_primary_key else 0,
                            1 if c.is_excluded else 0,
                            c.pii_class,
                            c.source_path,
                            c.silver_column_name,
                            c.silver_data_type,
                            c.cast_safety,
                            c.null_replacement,
                            json.dumps(list(c.transforms)) if c.transforms else None,
                            json.dumps(list(c.skip_groups)) if c.skip_groups else None,
                            json.dumps(list(c.skip_processors)) if c.skip_processors else None,
                            c.column_role,
                        )
                        for c in columns
                    ],
                )

        return dataset_id

    def get_dataset(self, source_id: int, dataset_name: str) -> DatasetRow | None:
        conn = self._connect()
        row = conn.execute(
            "SELECT * FROM dataset_master WHERE source_id = ? AND dataset_name = ?",
            (source_id, dataset_name),
        ).fetchone()
        return _row_to_dataset(row) if row is not None else None

    def list_datasets(self, source_id: int | None = None) -> list[DatasetRow]:
        conn = self._connect()
        if source_id is None:
            rows = conn.execute(
                "SELECT * FROM dataset_master ORDER BY source_id, dataset_name"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM dataset_master WHERE source_id = ? ORDER BY dataset_name",
                (source_id,),
            ).fetchall()
        return [_row_to_dataset(r) for r in rows]

    def list_dataset_columns(self, dataset_id: int) -> list[DatasetColumnRow]:
        rows = (
            self._connect()
            .execute(
                "SELECT * FROM dataset_config_columns WHERE dataset_id = ? "
                "ORDER BY ordinal_position",
                (dataset_id,),
            )
            .fetchall()
        )
        return [_row_to_dataset_column(r) for r in rows]

    # ── platform_config

    def upsert_platform_config(self, kv: dict[str, str]) -> None:
        _validate_kv(KeyTable.PLATFORM_CONFIG, kv)
        with self._tx() as conn:
            conn.executemany(
                "INSERT INTO platform_config (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = CURRENT_TIMESTAMP",
                list(kv.items()),
            )

    # ── dq_rules

    def upsert_dq_rule_set(
        self,
        rule_set: DqRuleSet,
        *,
        dataset_id: int,
        delete_missing: bool = False,
    ) -> DqRuleSyncResult:
        # Cache existing rules for this (dataset_id, layer) for the diff.
        # No N+1: one SELECT, in-memory comparison, then INSERT/UPDATE per row.
        result = DqRuleSyncResult()
        yaml_names = {r.name for r in rule_set.rules}

        with self._tx() as conn:
            existing_rows = conn.execute(
                "SELECT * FROM dq_rules WHERE dataset_id = ? AND layer = ?",
                (dataset_id, rule_set.layer.value),
            ).fetchall()
            existing_by_name = {row["rule_name"]: row for row in existing_rows}

            for rule in rule_set.rules:
                existing = existing_by_name.get(rule.name)
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO dq_rules (
                            rule_name, dataset_id, layer, rule_type, tech, container,
                            predicate, injection_point, severity, action,
                            quarantine_strategy, sample_cap, is_enabled, source,
                            authored_by, description
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            rule.name,
                            dataset_id,
                            rule.layer.value,
                            rule.rule_type.value,
                            rule.tech.value,
                            rule.container.value,
                            rule.predicate,
                            rule.injection_point.value,
                            rule.severity.value,
                            rule.action.value,
                            rule.quarantine_strategy.value if rule.quarantine_strategy else None,
                            rule.sample_cap,
                            1 if rule.is_enabled else 0,
                            rule.source.value,
                            rule.authored_by,
                            rule.description,
                        ),
                    )
                    result = _bump(result, inserted=1)
                elif _row_differs_from_rule(existing, rule):
                    conn.execute(
                        """
                        UPDATE dq_rules SET
                            rule_type = ?, tech = ?, container = ?, predicate = ?,
                            injection_point = ?, severity = ?, action = ?,
                            quarantine_strategy = ?, sample_cap = ?, is_enabled = ?,
                            authored_by = ?, description = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE rule_id = ?
                        """,
                        (
                            rule.rule_type.value,
                            rule.tech.value,
                            rule.container.value,
                            rule.predicate,
                            rule.injection_point.value,
                            rule.severity.value,
                            rule.action.value,
                            rule.quarantine_strategy.value if rule.quarantine_strategy else None,
                            rule.sample_cap,
                            1 if rule.is_enabled else 0,
                            rule.authored_by,
                            rule.description,
                            int(existing["rule_id"]),
                        ),
                    )
                    result = _bump(result, updated=1)
                else:
                    result = _bump(result, unchanged=1)

            # Rules in DB but not in YAML: soft-delete (is_enabled=0) by
            # default. Hard-delete only when delete_missing=True.
            stale = [row for name, row in existing_by_name.items() if name not in yaml_names]
            for row in stale:
                if delete_missing:
                    conn.execute(
                        "DELETE FROM dq_rules WHERE rule_id = ?",
                        (int(row["rule_id"]),),
                    )
                    result = _bump(result, hard_deleted=1)
                elif int(row["is_enabled"]) == 1:
                    # Already disabled rules don't bump the counter; only count
                    # actual transitions enabled→disabled.
                    conn.execute(
                        "UPDATE dq_rules SET is_enabled = 0, "
                        "updated_at = CURRENT_TIMESTAMP WHERE rule_id = ?",
                        (int(row["rule_id"]),),
                    )
                    result = _bump(result, soft_deleted=1)

        return result

    def list_dq_rules(
        self,
        *,
        dataset_id: int | None = None,
        layer: Layer | None = None,
        only_enabled: bool = True,
    ) -> list[DqRuleRow]:
        clauses: list[str] = []
        params: list[object] = []
        if dataset_id is not None:
            clauses.append("dataset_id = ?")
            params.append(dataset_id)
        if layer is not None:
            clauses.append("layer = ?")
            params.append(layer.value)
        if only_enabled:
            clauses.append("is_enabled = 1")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = (
            self._connect()
            .execute(
                f"SELECT * FROM dq_rules{where} ORDER BY rule_id",  # noqa: S608 — clauses are literal
                params,
            )
            .fetchall()
        )
        return [_row_to_dq_rule(r) for r in rows]

    def get_dq_rule(self, rule_id: int) -> DqRuleRow | None:
        row = (
            self._connect()
            .execute("SELECT * FROM dq_rules WHERE rule_id = ?", (rule_id,))
            .fetchone()
        )
        return _row_to_dq_rule(row) if row is not None else None

    # ── dq_results + dq_violations (Phase 3)

    def record_dq_result(
        self,
        *,
        rule_id: int,
        run_id: str,
        passed: bool,
        violators_total: int,
        sample_captured: int,
        action_taken: str,
        duration_ms: int = 0,
        error_message: str | None = None,
    ) -> int:
        with self._tx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO dq_results (
                    rule_id, run_id, passed, violators_total,
                    sample_captured, action_taken, duration_ms, error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule_id,
                    run_id,
                    1 if passed else 0,
                    violators_total,
                    sample_captured,
                    action_taken,
                    duration_ms,
                    error_message,
                ),
            )
            result_id = cursor.lastrowid
            if result_id is None:
                raise MetadataError("dq_results insert returned no row id")
            return int(result_id)

    def record_dq_violations(
        self,
        *,
        rule_id: int,
        result_id: int,
        rows: list[dict[str, object]],
    ) -> int:
        if not rows:
            return 0
        # JSON-encode each row to a compact, sortable form. We don't
        # care about key order for storage, but determinism helps when
        # diffing audit logs across runs.
        payload = [
            (rule_id, result_id, json.dumps(row, sort_keys=True, default=str)) for row in rows
        ]
        with self._tx() as conn:
            conn.executemany(
                """
                INSERT INTO dq_violations (rule_id, result_id, pk_columns_json)
                VALUES (?, ?, ?)
                """,
                payload,
            )
        return len(rows)

    def list_dq_results(
        self,
        *,
        rule_id: int | None = None,
        run_id: str | None = None,
    ) -> list[DqResultRow]:
        clauses: list[str] = []
        params: list[object] = []
        if rule_id is not None:
            clauses.append("rule_id = ?")
            params.append(rule_id)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = (
            self._connect()
            .execute(
                f"SELECT * FROM dq_results{where} ORDER BY result_id",  # noqa: S608 — clauses literal
                params,
            )
            .fetchall()
        )
        return [_row_to_dq_result(r) for r in rows]

    def list_dq_violations(self, *, result_id: int) -> list[DqViolationRow]:
        rows = (
            self._connect()
            .execute(
                "SELECT * FROM dq_violations WHERE result_id = ? ORDER BY violation_id",
                (result_id,),
            )
            .fetchall()
        )
        return [_row_to_dq_violation(r) for r in rows]

    # ── pii_policy (Wave 2)

    def upsert_pii_policy(
        self,
        *,
        pii_class: str,
        env: str,
        action: str,
        salt_kv_ref: str | None = None,
        redact_value: str | None = None,
        mask_visible: int = 4,
    ) -> int:
        with self._tx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO pii_policy (
                    pii_class, env, action, salt_kv_ref, redact_value, mask_visible
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(pii_class, env) DO UPDATE SET
                    action       = excluded.action,
                    salt_kv_ref  = excluded.salt_kv_ref,
                    redact_value = excluded.redact_value,
                    mask_visible = excluded.mask_visible,
                    updated_at   = CURRENT_TIMESTAMP
                """,
                (pii_class, env, action, salt_kv_ref, redact_value, mask_visible),
            )
            policy_id = cursor.lastrowid
            if policy_id is None or policy_id == 0:
                # ON CONFLICT may not return lastrowid; look it up.
                row = conn.execute(
                    "SELECT policy_id FROM pii_policy WHERE pii_class = ? AND env = ?",
                    (pii_class, env),
                ).fetchone()
                policy_id = int(row["policy_id"]) if row else -1
            return int(policy_id)

    def list_pii_policy(self, *, env: str | None = None) -> list[PiiPolicyRow]:
        if env is None:
            rows = (
                self._connect()
                .execute("SELECT * FROM pii_policy ORDER BY pii_class, env")
                .fetchall()
            )
        else:
            rows = (
                self._connect()
                .execute(
                    "SELECT * FROM pii_policy WHERE env = ? ORDER BY pii_class",
                    (env,),
                )
                .fetchall()
            )
        return [_row_to_pii_policy(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _validate_kv(
    table: KeyTable,
    kv: dict[str, str],
    *,
    source_type: SourceType | None = None,
) -> None:
    """Reject any key not in the registry; reject values that don't parse."""
    for key, raw in kv.items():
        try:
            kd = lookup_key(table, key, source_type=source_type)
        except UnknownKeyError:
            raise
        # Parse for validation side effects (raises on bad value).
        # The parsed result is discarded — we're only writing strings to the DB.
        try:
            parse_value(kd, raw)
        except ValidationError:
            raise


def _row_to_source(row: sqlite3.Row) -> SourceRow:
    return SourceRow(
        source_id=int(row["source_id"]),
        source_name=str(row["source_name"]),
        source_env=str(row["source_env"]),
        source_type=SourceType(row["source_type"]),
        is_enabled=bool(row["is_enabled"]),
    )


def _row_to_dataset(row: sqlite3.Row) -> DatasetRow:
    return DatasetRow(
        dataset_id=int(row["dataset_id"]),
        source_id=int(row["source_id"]),
        dataset_name=str(row["dataset_name"]),
        is_enabled=bool(row["is_enabled"]),
    )


def _row_to_dq_rule(row: sqlite3.Row) -> DqRuleRow:
    raw_qs = row["quarantine_strategy"]
    return DqRuleRow(
        rule_id=int(row["rule_id"]),
        rule_name=str(row["rule_name"]),
        dataset_id=int(row["dataset_id"]),
        layer=Layer(row["layer"]),
        rule_type=RuleType(row["rule_type"]),
        tech=Tech(row["tech"]),
        container=Container(row["container"]),
        predicate=str(row["predicate"]),
        injection_point=InjectionPoint(row["injection_point"]),
        severity=Severity(row["severity"]),
        action=Action(row["action"]),
        quarantine_strategy=QuarantineStrategy(raw_qs) if raw_qs else None,
        sample_cap=int(row["sample_cap"]),
        is_enabled=bool(row["is_enabled"]),
        source=RuleSource(row["source"]),
        authored_by=row["authored_by"],
        description=str(row["description"] or ""),
    )


def _row_to_dataset_column(row: sqlite3.Row) -> DatasetColumnRow:
    """Convert a SQLite row to ``DatasetColumnRow``.

    JSON fields (transforms_json / skip_*_json) decode to tuples; absent
    columns (older schemas) tolerated via ``.keys()`` membership check.
    """
    keys = set(row.keys())
    transforms_raw = row["transforms_json"] if "transforms_json" in keys else None
    skip_groups_raw = row["skip_groups_json"] if "skip_groups_json" in keys else None
    skip_processors_raw = row["skip_processors_json"] if "skip_processors_json" in keys else None
    return DatasetColumnRow(
        dataset_id=int(row["dataset_id"]),
        column_name=str(row["column_name"]),
        data_type=str(row["data_type"]),
        is_nullable=bool(row["is_nullable"]),
        ordinal_position=int(row["ordinal_position"]),
        is_primary_key=bool(row["is_primary_key"]),
        is_excluded=bool(row["is_excluded"]),
        pii_class=str(row["pii_class"]),
        source_path=row["source_path"],
        silver_column_name=row["silver_column_name"] if "silver_column_name" in keys else None,
        silver_data_type=row["silver_data_type"] if "silver_data_type" in keys else None,
        cast_safety=str(row["cast_safety"] if "cast_safety" in keys else "strict"),
        null_replacement=row["null_replacement"] if "null_replacement" in keys else None,
        transforms=tuple(json.loads(transforms_raw)) if transforms_raw else (),
        skip_groups=tuple(json.loads(skip_groups_raw)) if skip_groups_raw else (),
        skip_processors=tuple(json.loads(skip_processors_raw)) if skip_processors_raw else (),
        column_role=row["column_role"] if "column_role" in keys else None,
    )


def _row_to_dq_result(row: sqlite3.Row) -> DqResultRow:
    return DqResultRow(
        result_id=int(row["result_id"]),
        rule_id=int(row["rule_id"]),
        run_id=str(row["run_id"]),
        evaluated_at=str(row["evaluated_at"]),
        passed=bool(row["passed"]),
        violators_total=int(row["violators_total"]),
        sample_captured=int(row["sample_captured"]),
        action_taken=str(row["action_taken"]),
        duration_ms=int(row["duration_ms"]),
        error_message=row["error_message"],
    )


def _row_to_pii_policy(row: sqlite3.Row) -> PiiPolicyRow:
    return PiiPolicyRow(
        policy_id=int(row["policy_id"]),
        pii_class=str(row["pii_class"]),
        env=str(row["env"]),
        action=str(row["action"]),
        salt_kv_ref=row["salt_kv_ref"],
        redact_value=row["redact_value"],
        mask_visible=int(row["mask_visible"]),
    )


def _row_to_dq_violation(row: sqlite3.Row) -> DqViolationRow:
    return DqViolationRow(
        violation_id=int(row["violation_id"]),
        rule_id=int(row["rule_id"]),
        result_id=int(row["result_id"]),
        pk_columns_json=str(row["pk_columns_json"]),
        violated_at=str(row["violated_at"]),
    )


def _row_differs_from_rule(row: sqlite3.Row, rule: DqRule) -> bool:
    """Compare a DB row vs a parsed rule. Used by upsert to skip no-ops.

    Compares only fields the customer can author — surrogate keys
    (``rule_id``, ``dataset_id``) and timestamps don't count.
    """
    raw_qs = row["quarantine_strategy"]
    db_qs = QuarantineStrategy(raw_qs) if raw_qs else None
    return (
        row["rule_type"] != rule.rule_type.value
        or row["tech"] != rule.tech.value
        or row["container"] != rule.container.value
        or row["predicate"] != rule.predicate
        or row["injection_point"] != rule.injection_point.value
        or row["severity"] != rule.severity.value
        or row["action"] != rule.action.value
        or db_qs != rule.quarantine_strategy
        or int(row["sample_cap"]) != rule.sample_cap
        or bool(row["is_enabled"]) != rule.is_enabled
        or (row["authored_by"] or None) != rule.authored_by
        or str(row["description"] or "") != rule.description
    )


def _bump(result: DqRuleSyncResult, **deltas: int) -> DqRuleSyncResult:
    """Return a new ``DqRuleSyncResult`` with the given fields incremented.

    Counts are dataclass fields; the dataclass is frozen so we
    construct a fresh one. Cheaper than mutability and matches the
    rest of the codebase's frozen-dataclass posture.
    """
    return DqRuleSyncResult(
        inserted=result.inserted + deltas.get("inserted", 0),
        updated=result.updated + deltas.get("updated", 0),
        unchanged=result.unchanged + deltas.get("unchanged", 0),
        soft_deleted=result.soft_deleted + deltas.get("soft_deleted", 0),
        hard_deleted=result.hard_deleted + deltas.get("hard_deleted", 0),
    )


__all__ = [
    "DatasetColumnRow",
    "DatasetRow",
    "DqRuleRow",
    "DqRuleSyncResult",
    "MetadataRepository",
    "SourceRow",
    "SqliteMetadataRepository",
]
