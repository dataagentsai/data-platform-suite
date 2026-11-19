"""MySQL / MariaDB source connector — read-only catalog introspection via pymysql.

Used by ``lake-ingest discover`` to walk MySQL's ``information_schema``
and propose dataset metadata.

Driver: ``pymysql`` — pure Python, works with both MySQL and MariaDB.
Install with::

    pip install lake-ingest[mysql]

Imported lazily so the framework stays importable when the extra
isn't installed.

Phase 4 ships **basic SQL auth** (``user:password``). SSL/TLS
options + AAD-token auth (Azure Database for MySQL) are deferred —
they require a real Azure context to test.

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

# MySQL system schemas — excluded from list_schemas() output.
_SYSTEM_SCHEMAS = frozenset(
    {
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
    }
)


@dataclass(frozen=True)
class MysqlConnectionParams:
    """Resolved connection params for a MySQL / MariaDB source.

    Built from ``source_config`` after secret resolution. Held only
    for the lifetime of one ``MysqlConnector`` — never persisted.
    """

    server: str
    port: int
    database: str
    user: str
    password: str
    charset: str = "utf8mb4"
    application_name: str = "lake-ingest-discover"
    connect_timeout_seconds: int = 30


class MysqlConnector:
    """MySQL ``SourceConnector`` implementation."""

    def __init__(self, params: MysqlConnectionParams) -> None:
        self._params = params
        # Held as Any — pymysql's Connection type is only available when the
        # [mysql] extra is installed.
        self._conn: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> MysqlConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys: ``server``, ``database``, ``connection_secret_name``.

        Optional:
        - ``port`` (3306)
        - ``charset`` ("utf8mb4")
        - ``connect_timeout_seconds`` (30)
        - ``application_name`` ("lake-ingest-discover")

        The secret resolves to ``user:password`` (single colon-separated
        form) — same shape as the Postgres / SQL Server connectors.
        """
        server = _required(config, "server")
        database = _required(config, "database")
        secret_ref_raw = _required(config, "connection_secret_name")
        port = int(config.get("port", "3306"))
        charset = config.get("charset", "utf8mb4")
        timeout = int(config.get("connect_timeout_seconds", "30"))
        app_name = config.get("application_name", "lake-ingest-discover")

        ref = KvRef.parse(secret_ref_raw)
        secret = secret_resolver.resolve(ref)
        user, password = _resolve_user_password(config, secret)

        return cls(
            MysqlConnectionParams(
                server=server,
                port=port,
                database=database,
                user=user,
                password=password,
                charset=charset,
                application_name=app_name,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._conn is not None:
            return
        pymysql = _lazy_import_pymysql()
        try:
            self._conn = pymysql.connect(
                host=self._params.server,
                port=self._params.port,
                user=self._params.user,
                password=self._params.password,
                database=self._params.database,
                charset=self._params.charset,
                connect_timeout=self._params.connect_timeout_seconds,
                program_name=self._params.application_name,
                cursorclass=pymysql.cursors.Cursor,
                autocommit=True,
            )
        except pymysql.err.OperationalError as exc:
            if _is_auth_failure(exc):
                raise AuthenticationError(
                    f"MySQL rejected credentials for {self._params.user}@"
                    f"{self._params.server}: {exc}"
                ) from exc
            raise SourceUnavailableError(
                f"Cannot reach MySQL at {self._params.server}:{self._params.port}: {exc}"
            ) from exc

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def list_schemas(self) -> list[str]:
        # Use a placeholder list because pymysql's paramstyle='format'
        # (`%s`) doesn't expand a tuple into multiple placeholders.
        placeholders = ",".join(["%s"] * len(_SYSTEM_SCHEMAS))
        rows = self._fetchall(
            # Schema names from a trusted internal frozenset, not user input.
            f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ({placeholders})
            ORDER BY schema_name
            """,  # noqa: S608  # nosec B608
            tuple(_SYSTEM_SCHEMAS),
        )
        return [str(r[0]) for r in rows]

    def list_tables(self, schema: str) -> list[TableMeta]:
        rows = self._fetchall(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
            """,
            (schema,),
        )
        out: list[TableMeta] = []
        for name, type_ in rows:
            out.append(
                TableMeta(
                    schema=schema,
                    name=str(name),
                    table_type=("TABLE" if type_ == "BASE TABLE" else "VIEW"),
                    row_count_estimate=None,  # information_schema.tables.table_rows is approximate
                )
            )
        return out

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        # MySQL has a convenient ``column_key`` field — 'PRI' for primary keys.
        # Saves us the table_constraints / key_column_usage join.
        rows = self._fetchall(
            """
            SELECT
                column_name,
                data_type,
                is_nullable,
                ordinal_position,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                CASE WHEN column_key = 'PRI' THEN 1 ELSE 0 END AS is_primary_key
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
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

    def read_query(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[QueryBatch]:
        """Execute ``sql`` and yield batches via a streaming cursor.

        Uses pymysql's ``SSCursor`` (server-side / unbuffered) so memory
        stays bounded by ``batch_size``. The default ``Cursor`` would
        materialize the whole result on the client.

        Bind values use pymysql's positional ``%s`` style; pass
        ``params`` as a sequence in the order placeholders appear.
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "MySQL connection not established after connect() — internal invariant violated."
            )

        pymysql = _lazy_import_pymysql()
        try:
            with self._conn.cursor(pymysql.cursors.SSCursor) as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    raise QueryFailedError(
                        query_hash="read_query",
                        message="Statement returned no result set; read_query expects a SELECT.",
                    )
                columns = tuple(col[0] for col in cur.description)

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    yield QueryBatch(columns=columns, rows=[tuple(r) for r in rows])
        except pymysql.err.MySQLError as exc:
            raise QueryFailedError(query_hash="read_query", message=str(exc)) from exc

    # ── Internal helpers

    def _fetchall(self, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "MySQL connection not established after connect() — internal invariant violated."
            )
        pymysql = _lazy_import_pymysql()
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                return [tuple(row) for row in cur.fetchall()]
        except pymysql.err.MySQLError as exc:
            raise QueryFailedError(query_hash="discover", message=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"MySQL source config missing required key {key!r}.",
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
        raise AuthenticationError("MySQL connection secret must be in 'user:password' form.")
    user, password = secret.split(":", 1)
    if not user or not password:
        raise AuthenticationError("MySQL connection secret has empty user or password.")
    return user, password


def _lazy_import_pymysql() -> Any:
    """Import pymysql on first use; surface a clean error if missing."""
    try:
        import pymysql
        import pymysql.cursors
        import pymysql.err
    except ImportError as exc:
        raise SourceUnavailableError(
            "pymysql not installed. Install with: pip install lake-ingest[mysql]"
        ) from exc
    return pymysql


def _is_auth_failure(exc: Exception) -> bool:
    """Detect MySQL auth failures.

    pymysql raises ``OperationalError(1045, "Access denied for user ...")``
    for SQL auth failures. Network errors look different (1043, 1044,
    2002, 2003).
    """
    # MySQL error code constants — see
    # https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
    er_access_denied_error = 1045
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int):
        return args[0] == er_access_denied_error
    msg = str(exc).lower()
    return "access denied" in msg


__all__ = [
    "MysqlConnectionParams",
    "MysqlConnector",
]
