"""Oracle Database source connector — read-only catalog introspection via oracledb.

Different catalog from Postgres / MySQL / MSSQL: Oracle uses ``ALL_*``
views rather than ``INFORMATION_SCHEMA``. The "schema" concept also
differs — in Oracle, schema = database user (an ``OWNER`` of objects).
One Oracle database hosts many schemas.

Driver: ``oracledb`` (Oracle's modern Python driver, replaces
cx_Oracle). **Thin mode** is the default since 2.0 — pure Python, no
Instant Client install required.

Install with::

    pip install lake-ingest[oracle]

Imported lazily so the framework stays importable without the extra.

Phase 4 ships **basic SQL auth** (``user:password``). Wallet-based
auth (Oracle Cloud Autonomous DB) and Kerberos are deferred.

Identifier case: Oracle uppercases unquoted identifiers at the
catalog level. So an operator-supplied ``schema=hr`` and table=customers``
is stored as ``HR`` / ``CUSTOMERS``. The connector pre-uppercases its
parameters before binding to ALL_TAB_COLUMNS lookups; case-sensitive
lookups (rare; require quoted identifiers) are out of scope.

Per ``docs/discovery.md`` → "Discovery is read-only". This module
issues no DDL / DML against the source.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from lake_ingestion_framework.core.exceptions import (
    AuthenticationError,
    QueryFailedError,
    SourceUnavailableError,
)
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    SecretResolver,
)
from lake_ingestion_framework.core.source_connectors.protocol import (
    ColumnInfo,
    QueryBatch,
    TableMeta,
)
from lake_ingestion_framework.core.types.value_objects import KvRef


@dataclass(frozen=True)
class OracleConnectionParams:
    """Resolved connection params for an Oracle source.

    Built from ``source_config`` after secret resolution. Held only
    for the lifetime of one ``OracleConnector`` — never persisted.
    """

    server: str
    port: int
    service_name: str
    user: str
    password: str
    application_name: str = "lake-ingest-discover"
    connect_timeout_seconds: int = 30


class OracleConnector:
    """Oracle ``SourceConnector`` implementation."""

    def __init__(self, params: OracleConnectionParams) -> None:
        self._params = params
        # Held as Any — oracledb's Connection type is only available when the
        # [oracle] extra is installed.
        self._conn: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> OracleConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys: ``server``, ``service_name``, ``connection_secret_name``.

        Optional:
        - ``port`` (1521)
        - ``connect_timeout_seconds`` (30)
        - ``application_name`` ("lake-ingest-discover")

        The secret resolves to ``user:password`` (single colon-separated
        form) — same shape as the other RDBMS connectors.
        """
        server = _required(config, "server")
        service_name = _required(config, "service_name")
        secret_ref_raw = _required(config, "connection_secret_name")
        port = int(config.get("port", "1521"))
        timeout = int(config.get("connect_timeout_seconds", "30"))
        app_name = config.get("application_name", "lake-ingest-discover")

        ref = KvRef.parse(secret_ref_raw)
        secret = secret_resolver.resolve(ref)
        user, password = _resolve_user_password(config, secret)

        return cls(
            OracleConnectionParams(
                server=server,
                port=port,
                service_name=service_name,
                user=user,
                password=password,
                application_name=app_name,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._conn is not None:
            return
        oracledb = _lazy_import_oracledb()
        dsn = f"{self._params.server}:{self._params.port}/{self._params.service_name}"
        try:
            self._conn = oracledb.connect(
                user=self._params.user,
                password=self._params.password,
                dsn=dsn,
                tcp_connect_timeout=self._params.connect_timeout_seconds,
            )
        except oracledb.DatabaseError as exc:
            if _is_auth_failure(exc):
                raise AuthenticationError(
                    f"Oracle rejected credentials for {self._params.user}@{dsn}: {exc}"
                ) from exc
            raise SourceUnavailableError(f"Cannot reach Oracle at {dsn}: {exc}") from exc

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def list_schemas(self) -> list[str]:
        """List user schemas. Excludes Oracle-maintained system schemas.

        Oracle 12c+ exposes ``ORACLE_MAINTAINED='Y'`` for system schemas.
        We rely on that flag rather than an extensive blocklist of
        system-user names (SYS, SYSTEM, OUTLN, XDB, CTXSYS, MDSYS, ...).
        """
        rows = self._fetchall(
            """
            SELECT username
            FROM all_users
            WHERE oracle_maintained = 'N'
            ORDER BY username
            """,
            {},
        )
        return [str(r[0]) for r in rows]

    def list_tables(self, schema: str) -> list[TableMeta]:
        owner = schema.upper()
        rows = self._fetchall(
            """
            SELECT object_name, object_type FROM (
                SELECT table_name AS object_name, 'TABLE' AS object_type
                FROM all_tables
                WHERE owner = :owner
                UNION ALL
                SELECT view_name AS object_name, 'VIEW' AS object_type
                FROM all_views
                WHERE owner = :owner
            )
            ORDER BY object_name
            """,
            {"owner": owner},
        )
        out: list[TableMeta] = []
        for name, type_ in rows:
            out.append(
                TableMeta(
                    schema=schema,
                    name=str(name),
                    table_type=str(type_),
                    row_count_estimate=None,  # all_tables.num_rows requires DBMS_STATS run
                )
            )
        return out

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        owner = schema.upper()
        table_name = table.upper()
        rows = self._fetchall(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.nullable,
                c.column_id,
                c.char_length,
                c.data_precision,
                c.data_scale,
                CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
            FROM all_tab_columns c
            LEFT JOIN (
                SELECT cc.column_name
                FROM all_constraints con
                JOIN all_cons_columns cc
                  ON con.constraint_name = cc.constraint_name
                 AND con.owner = cc.owner
                WHERE con.constraint_type = 'P'
                  AND con.owner = :owner
                  AND con.table_name = :table_name
            ) pk ON pk.column_name = c.column_name
            WHERE c.owner = :owner
              AND c.table_name = :table_name
            ORDER BY c.column_id
            """,
            {"owner": owner, "table_name": table_name},
        )
        return [
            ColumnInfo(
                name=str(name),
                data_type=str(data_type),
                is_nullable=(nullable == "Y"),
                ordinal_position=int(col_id),
                max_length=int(char_length)
                if char_length is not None and char_length > 0
                else None,
                numeric_precision=int(precision) if precision is not None else None,
                numeric_scale=int(scale) if scale is not None else None,
                is_primary_key=bool(is_pk),
            )
            for (name, data_type, nullable, col_id, char_length, precision, scale, is_pk) in rows
        ]

    # ── IngestReader protocol

    def read_query(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Execute ``sql`` and yield batches via a tuned cursor.

        Sets ``cursor.arraysize = batch_size`` so oracledb's underlying
        fetch loop pulls one batch worth of rows per round-trip. Memory
        is bounded by ``batch_size``.

        Bind values use oracledb's named ``:name`` style (preferred) or
        positional ``:1`` / ``:2``. ``params`` may be a dict or a
        sequence accordingly.
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "Oracle connection not established after connect() — internal invariant violated."
            )

        oracledb = _lazy_import_oracledb()
        cur = self._conn.cursor()
        cur.arraysize = batch_size
        try:
            try:
                cur.execute(sql, params or {})
            except oracledb.DatabaseError as exc:
                raise QueryFailedError(query_hash="read_query", message=str(exc)) from exc

            if cur.description is None:
                raise QueryFailedError(
                    query_hash="read_query",
                    message="Statement returned no result set; read_query expects a SELECT.",
                )
            columns = tuple(col[0] for col in cur.description)

            while True:
                try:
                    rows = cur.fetchmany(batch_size)
                except oracledb.DatabaseError as exc:
                    raise QueryFailedError(query_hash="read_query", message=str(exc)) from exc
                if not rows:
                    break
                yield QueryBatch(columns=columns, rows=[tuple(r) for r in rows])
        finally:
            cur.close()

    # ── Internal helpers

    def _fetchall(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "Oracle connection not established after connect() — internal invariant violated."
            )
        oracledb = _lazy_import_oracledb()
        try:
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params)
                return [tuple(row) for row in cur.fetchall()]
            finally:
                cur.close()
        except oracledb.DatabaseError as exc:
            raise QueryFailedError(query_hash="discover", message=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"Oracle source config missing required key {key!r}.",
        )
    return value


def _resolve_user_password(config: dict[str, str], secret: str) -> tuple[str, str]:
    """Pick (user, password) based on whether ``connection_user`` is set."""
    config_user = config.get("connection_user")
    if config_user:
        if not secret:
            raise AuthenticationError(
                "connection_user set but resolved secret is empty (expected password)."
            )
        return config_user, secret
    return _split_user_password(secret)


def _split_user_password(secret: str) -> tuple[str, str]:
    """Parse ``user:password`` from the resolved secret string."""
    if ":" not in secret:
        raise AuthenticationError("Oracle connection secret must be in 'user:password' form.")
    user, password = secret.split(":", 1)
    if not user or not password:
        raise AuthenticationError("Oracle connection secret has empty user or password.")
    return user, password


def _lazy_import_oracledb() -> Any:
    """Import oracledb on first use; surface a clean error if missing."""
    try:
        import oracledb
    except ImportError as exc:
        raise SourceUnavailableError(
            "oracledb not installed. Install with: pip install lake-ingest[oracle]"
        ) from exc
    return oracledb


def _is_auth_failure(exc: Exception) -> bool:
    """Detect Oracle auth failures.

    Oracle raises ORA-01017 ("invalid username/password; logon denied")
    on SQL auth failures. Network errors are ORA-12541 (no listener),
    ORA-12170 (timeout), ORA-12514 (service not registered), etc.
    """
    msg = str(exc)
    return "ORA-01017" in msg or "logon denied" in msg.lower()


__all__ = [
    "OracleConnectionParams",
    "OracleConnector",
]
