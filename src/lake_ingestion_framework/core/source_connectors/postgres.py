"""Postgres source connector — read-only catalog introspection via psycopg3.

Used by ``lake-ingest discover`` to walk ``information_schema`` and
propose dataset metadata.

Driver: ``psycopg`` (psycopg3). Install with::

    pip install lake-ingest[postgres]

The connector imports psycopg lazily so the framework stays importable
even when the extra isn't installed; the import error surfaces only
when someone actually tries to connect.

Per ``docs/discovery.md`` → "Discovery is read-only". This module
issues no DDL or DML against the source.
"""

from __future__ import annotations

import uuid
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

# System schemas to exclude from list_schemas() output.
_SYSTEM_SCHEMAS = frozenset({"pg_catalog", "information_schema", "pg_toast"})


@dataclass(frozen=True)
class PostgresConnectionParams:
    """Resolved connection params for a Postgres source.

    Built from ``source_config`` rows after ``KvRef`` resolution. Held
    only for the duration of one ``PostgresConnector`` lifecycle —
    never persisted.
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "require"
    application_name: str = "lake-ingest-discover"
    connect_timeout_seconds: int = 30


class PostgresConnector:
    """Postgres ``SourceConnector`` implementation."""

    def __init__(self, params: PostgresConnectionParams) -> None:
        self._params = params
        # Held as Any because psycopg's Connection type is only available
        # when the [postgres] extra is installed; this keeps the framework
        # importable without it.
        self._conn: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> PostgresConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys: ``server``, ``database``, ``connection_secret_name``.
        Optional: ``connect_timeout_seconds``, plus a ``port`` key (Postgres
        defaults to 5432) and ``sslmode``.

        The secret pointed to by ``connection_secret_name`` must contain
        ``user:password`` (single colon-separated form). Format chosen
        because Postgres connection strings already encode it that way.
        """
        host = _required(config, "server")
        database = _required(config, "database")
        secret_ref_raw = _required(config, "connection_secret_name")
        port = int(config.get("port", "5432"))
        sslmode = config.get("sslmode", "require")
        timeout = int(config.get("connect_timeout_seconds", "30"))
        app_name = config.get("application_name", "lake-ingest-discover")

        ref = KvRef.parse(secret_ref_raw)
        secret = secret_resolver.resolve(ref)
        user, password = _resolve_user_password(config, secret)

        return cls(
            PostgresConnectionParams(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                sslmode=sslmode,
                application_name=app_name,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._conn is not None:
            return
        psycopg = _lazy_import_psycopg()
        try:
            self._conn = psycopg.connect(
                host=self._params.host,
                port=self._params.port,
                dbname=self._params.database,
                user=self._params.user,
                password=self._params.password,
                sslmode=self._params.sslmode,
                application_name=self._params.application_name,
                connect_timeout=self._params.connect_timeout_seconds,
                autocommit=True,
            )
        except psycopg.OperationalError as exc:
            # Distinguish auth from network/availability failures.
            if _is_auth_failure(exc):
                raise AuthenticationError(
                    f"Postgres rejected credentials for {self._params.user}@"
                    f"{self._params.host}: {exc}"
                ) from exc
            raise SourceUnavailableError(
                f"Cannot reach Postgres at {self._params.host}:{self._params.port}: {exc}"
            ) from exc

    def close(self) -> None:
        if self._conn is None:
            return
        # psycopg connections are context-manager-friendly; explicit close is fine too
        self._conn.close()
        self._conn = None

    def list_schemas(self) -> list[str]:
        rows = self._fetchall(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN %(system)s
              AND schema_name NOT LIKE 'pg_temp_%%'
              AND schema_name NOT LIKE 'pg_toast_temp_%%'
            ORDER BY schema_name
            """,
            {"system": tuple(_SYSTEM_SCHEMAS)},
        )
        return [str(r[0]) for r in rows]

    def list_tables(self, schema: str) -> list[TableMeta]:
        rows = self._fetchall(
            """
            SELECT t.table_name, t.table_type,
                   COALESCE(c.reltuples::bigint, 0) AS row_estimate
            FROM information_schema.tables t
            LEFT JOIN pg_class c
              ON c.relname = t.table_name
             AND c.relnamespace = (
                 SELECT oid FROM pg_namespace WHERE nspname = t.table_schema
             )
            WHERE t.table_schema = %(schema)s
              AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name
            """,
            {"schema": schema},
        )
        out: list[TableMeta] = []
        for name, type_, row_estimate in rows:
            out.append(
                TableMeta(
                    schema=schema,
                    name=str(name),
                    table_type=("TABLE" if type_ == "BASE TABLE" else "VIEW"),
                    row_count_estimate=int(row_estimate) if row_estimate is not None else None,
                )
            )
        return out

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        rows = self._fetchall(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                CASE WHEN pk.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %(schema)s
                  AND tc.table_name = %(table)s
            ) pk ON pk.column_name = c.column_name
            WHERE c.table_schema = %(schema)s
              AND c.table_name = %(table)s
            ORDER BY c.ordinal_position
            """,
            {"schema": schema, "table": table},
        )
        return [
            ColumnInfo(
                name=str(name),
                data_type=str(data_type),
                is_nullable=(is_nullable == "YES"),
                ordinal_position=int(pos),
                max_length=int(maxlen) if maxlen is not None else None,
                numeric_precision=int(precision) if precision is not None else None,
                numeric_scale=int(scale) if scale is not None else None,
                is_primary_key=bool(is_pk),
            )
            for (name, data_type, is_nullable, pos, maxlen, precision, scale, is_pk) in rows
        ]

    # ── IngestReader protocol

    def read_query(  # noqa: PLR0912 — flat error-mapping reads cleaner than refactor
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Execute ``sql`` and yield batches via a server-side cursor.

        Uses a named (server-side) cursor so memory stays bounded by
        ``batch_size`` even on multi-million-row pulls. The cursor is
        closed deterministically when iteration completes or the
        generator is collected.

        Bind values use psycopg's positional ``%s`` style; pass
        ``params`` as a sequence in the order the placeholders appear.
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "Postgres connection not established after connect() — internal invariant violated."
            )

        psycopg = _lazy_import_psycopg()
        # Server-side cursors require a unique name per query. UUID4 is overkill
        # but avoids any chance of collision across concurrent reads on the same
        # connection in the future.
        cursor_name = f"lake_ingest_read_{uuid.uuid4().hex}"

        # Server-side cursors require an open transaction. The connector
        # opens its connection with autocommit=True (catalog reads run faster
        # that way), so we toggle autocommit off for the cursor's lifetime
        # and restore on the way out.
        prev_autocommit = self._conn.autocommit
        if prev_autocommit:
            self._conn.autocommit = False

        try:
            try:
                with self._conn.cursor(name=cursor_name) as cur:
                    # itersize controls how many rows the driver buffers per
                    # fetch round-trip; aligning to batch_size keeps wire
                    # chatter low.
                    cur.itersize = batch_size
                    cur.execute(sql, params)
                    # cursor.description is populated after execute()
                    if cur.description is None:
                        # Statement returned no result set (e.g. someone passed
                        # a DDL statement). Refuse to yield silently — read_query
                        # is for SELECTs.
                        raise QueryFailedError(
                            query_hash="read_query",
                            message=(
                                "Statement returned no result set; " "read_query expects a SELECT."
                            ),
                        )
                    columns = tuple(col.name for col in cur.description)

                    while True:
                        rows = cur.fetchmany(batch_size)
                        if not rows:
                            break
                        yield QueryBatch(columns=columns, rows=list(rows))
            except psycopg.Error as exc:
                raise QueryFailedError(query_hash="read_query", message=str(exc)) from exc
            else:
                self._conn.commit()
        finally:
            # Always restore the previous autocommit state, even if the caller
            # broke out of the iterator early (psycopg requires no open
            # transaction before flipping autocommit back on).
            try:
                if not self._conn.autocommit:
                    self._conn.rollback()
            except psycopg.Error:
                pass
            if prev_autocommit:
                self._conn.autocommit = True

    # ── Internal helpers

    def _fetchall(self, sql: str, params: object) -> list[tuple[Any, ...]]:
        if self._conn is None:
            self.connect()
        if self._conn is None:
            # connect() should have populated this; if it didn't, something is
            # very wrong. Surface a real exception, not an assert.
            raise SourceUnavailableError(
                "Postgres connection not established after connect() — internal invariant violated."
            )
        psycopg = _lazy_import_psycopg()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
        except psycopg.Error as exc:
            raise QueryFailedError(query_hash="discover", message=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"Postgres source config missing required key {key!r}.",
        )
    return value


def _split_user_password(secret: str) -> tuple[str, str]:
    """Parse ``user:password`` from the resolved secret string."""
    if ":" not in secret:
        raise AuthenticationError("Postgres connection secret must be in 'user:password' form.")
    user, password = secret.split(":", 1)
    if not user or not password:
        raise AuthenticationError("Postgres connection secret has empty user or password.")
    return user, password


def _resolve_user_password(config: dict[str, str], secret: str) -> tuple[str, str]:
    """Pick (user, password) based on whether ``connection_user`` is set.

    - If ``connection_user`` is in source_config: secret is the password alone,
      user comes from the config key. Matches the ADF LinkedService shape.
    - Else: secret is ``user:password`` (legacy). Split it.
    """
    config_user = config.get("connection_user")
    if config_user:
        if not secret:
            raise AuthenticationError(
                "connection_user set but resolved secret is empty (expected password)."
            )
        return config_user, secret
    return _split_user_password(secret)


def _lazy_import_psycopg() -> Any:
    """Import psycopg on first use; surface a clean error if missing."""
    try:
        import psycopg
    except ImportError as exc:
        raise SourceUnavailableError(
            "psycopg not installed. Install with: pip install lake-ingest[postgres]"
        ) from exc
    return psycopg


def _is_auth_failure(exc: Exception) -> bool:
    """Detect Postgres auth failures from psycopg's OperationalError messages.

    psycopg surfaces auth failures as OperationalError with a message
    containing "authentication failed" or "no pg_hba.conf entry".
    Network errors look different ("could not translate host name",
    "connection refused").
    """
    msg = str(exc).lower()
    return "authentication failed" in msg or "pg_hba.conf" in msg or "password" in msg


__all__ = [
    "PostgresConnectionParams",
    "PostgresConnector",
]
