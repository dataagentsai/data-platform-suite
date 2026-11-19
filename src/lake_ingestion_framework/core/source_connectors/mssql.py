"""Microsoft SQL Server / Azure SQL DB source connector — pyodbc + ODBC.

One class covers both ``azure_sql_db`` and ``sql_server`` source types
because they share the wire protocol (TDS), the driver (pyodbc), and
the catalog surface (``INFORMATION_SCHEMA``). Differences between
Azure SQL DB and on-prem SQL Server live in the source's
``source_config`` keys (``encrypt``, ``trust_server_certificate``,
``port``, auth method).

Driver: ``pyodbc`` plus Microsoft ODBC Driver 18 for SQL Server,
installed at the OS level. Install:

    # macOS
    brew install unixodbc
    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
    brew install msodbcsql18

    # Ubuntu / Debian
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    sudo apt-get install -y unixodbc-dev msodbcsql18

Then::

    pip install lake-ingest[azure_sql_db]   # or [sql_server]

The connector imports pyodbc lazily so the framework stays importable
when the extra isn't installed; the import error surfaces only at
``connect()`` time.

Phase 4 ships **SQL auth only** (basic username/password). Managed
Identity + AAD-token authentication land later when we wire up the
Azure realm runner (Phase 7+) — they require a real Azure context to
test, which is out of scope for this phase.

Per ``docs/discovery.md``: read-only catalog walk; no DDL or DML.
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

# Schemas to exclude from list_schemas() — built-in SQL Server roles + sys.
_SYSTEM_SCHEMAS = frozenset(
    {
        "sys",
        "INFORMATION_SCHEMA",
        "guest",
        "db_owner",
        "db_accessadmin",
        "db_securityadmin",
        "db_ddladmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_denydatareader",
        "db_denydatawriter",
    }
)

_DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"


@dataclass(frozen=True)
class MssqlConnectionParams:
    """Resolved connection params for SQL Server / Azure SQL DB.

    Built from ``source_config`` after ``KvRef`` resolution. Held only
    for the lifetime of one ``MssqlConnector`` instance — never
    persisted.
    """

    server: str
    port: int
    database: str
    user: str
    password: str
    driver: str = _DEFAULT_DRIVER
    encrypt: str = "yes"  # "yes" / "no" / "strict" (ODBC Driver 18 supports strict)
    trust_server_certificate: str = "no"  # "yes" for local Docker; "no" for prod
    application_name: str = "lake-ingest-discover"
    connect_timeout_seconds: int = 30


class MssqlConnector:
    """Source connector for SQL Server and Azure SQL DB."""

    def __init__(self, params: MssqlConnectionParams) -> None:
        self._params = params
        # Held as Any because pyodbc's Connection type is only available when
        # the [azure_sql_db] / [sql_server] extra is installed; this keeps the
        # framework importable without it.
        self._conn: Any = None

    # ── Construction

    @classmethod
    def from_config(
        cls,
        config: dict[str, str],
        secret_resolver: SecretResolver,
    ) -> MssqlConnector:
        """Build a connector from a ``source_config`` KV dict.

        Required keys: ``server``, ``database``, ``connection_secret_name``.

        Optional keys (with defaults):
        - ``port`` (1433)
        - ``encrypt`` ("yes"; switch to "no" only for local non-TLS Docker)
        - ``trust_server_certificate`` ("no"; "yes" for local Docker)
        - ``connect_timeout_seconds`` (30)
        - ``application_name`` ("lake-ingest-discover")
        - ``driver_name`` (Microsoft ODBC Driver 18 for SQL Server)

        The secret pointed to by ``connection_secret_name`` must contain
        ``user:password`` (single colon-separated form) — same shape as
        the Postgres connector for consistency.
        """
        server = _required(config, "server")
        database = _required(config, "database")
        secret_ref_raw = _required(config, "connection_secret_name")
        port = int(config.get("port", "1433"))
        encrypt = config.get("encrypt", "yes")
        trust_cert = config.get("trust_server_certificate", "no")
        timeout = int(config.get("connect_timeout_seconds", "30"))
        app_name = config.get("application_name", "lake-ingest-discover")
        driver = config.get("driver_name", _DEFAULT_DRIVER)

        ref = KvRef.parse(secret_ref_raw)
        secret = secret_resolver.resolve(ref)
        user, password = _resolve_user_password(config, secret)

        return cls(
            MssqlConnectionParams(
                server=server,
                port=port,
                database=database,
                user=user,
                password=password,
                driver=driver,
                encrypt=encrypt,
                trust_server_certificate=trust_cert,
                application_name=app_name,
                connect_timeout_seconds=timeout,
            )
        )

    # ── SourceConnector protocol

    def connect(self) -> None:
        if self._conn is not None:
            return
        pyodbc = _lazy_import_pyodbc()
        conn_str = self._build_connection_string()
        try:
            self._conn = pyodbc.connect(conn_str, timeout=self._params.connect_timeout_seconds)
        except pyodbc.Error as exc:
            if _is_auth_failure(exc):
                raise AuthenticationError(
                    f"SQL Server rejected credentials for {self._params.user}@"
                    f"{self._params.server}: {exc}"
                ) from exc
            raise SourceUnavailableError(
                f"Cannot reach SQL Server at {self._params.server}:{self._params.port}: {exc}"
            ) from exc

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def list_schemas(self) -> list[str]:
        # The f-string only expands placeholder count (e.g. "?,?,?,?") because
        # pyodbc's qmark style can't expand a tuple natively. The values come
        # from an internal frozenset of trusted constants (_SYSTEM_SCHEMAS),
        # not user input — no SQL injection vector. Suppress S608.
        placeholders = ",".join("?" for _ in _SYSTEM_SCHEMAS)
        rows = self._fetchall(
            f"""
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ({placeholders})
            ORDER BY SCHEMA_NAME
            """,  # noqa: S608  # nosec B608
            tuple(_SYSTEM_SCHEMAS),
        )
        return [str(r[0]) for r in rows]

    def list_tables(self, schema: str) -> list[TableMeta]:
        rows = self._fetchall(
            """
            SELECT t.TABLE_NAME, t.TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_SCHEMA = ?
              AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY t.TABLE_NAME
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
                    row_count_estimate=None,  # sys.partitions read deferred — needs extra grants
                )
            )
        return out

    def describe_table(self, schema: str, table: str) -> list[ColumnInfo]:
        rows = self._fetchall(
            """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.ORDINAL_POSITION,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                 AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND tc.TABLE_SCHEMA = ?
                  AND tc.TABLE_NAME = ?
            ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = ?
              AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
            """,
            (schema, table, schema, table),
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
        """Execute ``sql`` and yield batches via a forward-only cursor.

        pyodbc's default cursor is forward-only and server-side under
        SQL Server's TDS protocol, so memory stays bounded by
        ``batch_size`` without extra configuration.

        Bind values use pyodbc's positional ``?`` qmark style; pass
        ``params`` as a sequence in the order placeholders appear.
        """
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "SQL Server connection not established after connect() — "
                "internal invariant violated."
            )

        pyodbc = _lazy_import_pyodbc()
        cur = self._conn.cursor()
        try:
            try:
                cur.execute(sql, params or ())
            except pyodbc.Error as exc:
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
                except pyodbc.Error as exc:
                    raise QueryFailedError(query_hash="read_query", message=str(exc)) from exc
                if not rows:
                    break
                yield QueryBatch(columns=columns, rows=[tuple(r) for r in rows])
        finally:
            cur.close()

    # ── Internal helpers

    def _build_connection_string(self) -> str:
        """Build an ODBC connection string from params."""
        parts = [
            f"Driver={{{self._params.driver}}}",
            f"Server=tcp:{self._params.server},{self._params.port}",
            f"Database={self._params.database}",
            f"UID={self._params.user}",
            f"PWD={self._params.password}",
            f"Encrypt={self._params.encrypt}",
            f"TrustServerCertificate={self._params.trust_server_certificate}",
            f"Connection Timeout={self._params.connect_timeout_seconds}",
            f"Application Name={self._params.application_name}",
        ]
        return ";".join(parts) + ";"

    def _fetchall(self, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if self._conn is None:
            self.connect()
        if self._conn is None:
            raise SourceUnavailableError(
                "SQL Server connection not established after connect() — "
                "internal invariant violated."
            )
        pyodbc = _lazy_import_pyodbc()
        try:
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params)
                return [tuple(row) for row in cur.fetchall()]
            finally:
                cur.close()
        except pyodbc.Error as exc:
            raise QueryFailedError(query_hash="discover", message=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _required(config: dict[str, str], key: str) -> str:
    value = config.get(key)
    if not value:
        raise QueryFailedError(
            query_hash="config",
            message=f"SQL Server source config missing required key {key!r}.",
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
    """Parse ``user:password`` from the resolved secret string.

    Same shape as the Postgres connector for consistency.
    """
    if ":" not in secret:
        raise AuthenticationError("SQL Server connection secret must be in 'user:password' form.")
    user, password = secret.split(":", 1)
    if not user or not password:
        raise AuthenticationError("SQL Server connection secret has empty user or password.")
    return user, password


def _lazy_import_pyodbc() -> Any:
    """Import pyodbc on first use; surface a clean error if missing."""
    try:
        import pyodbc
    except ImportError as exc:
        raise SourceUnavailableError(
            "pyodbc not installed. Install with: pip install lake-ingest[azure_sql_db] "
            "(or [sql_server]). The Microsoft ODBC Driver 18 must also be installed at "
            "the OS level — see docs/sources/ for setup."
        ) from exc
    return pyodbc


def _is_auth_failure(exc: Exception) -> bool:
    """Detect SQL auth failures from pyodbc error messages.

    pyodbc surfaces login failures with messages containing
    "Login failed for user" (SQL auth) or AAD-related strings.
    Network failures look different ("cannot connect", "could not
    open a connection", "host not found").
    """
    msg = str(exc).lower()
    return (
        "login failed" in msg
        or "password did not match" in msg
        or "user authentication" in msg
        or "cannot login" in msg
    )


__all__ = [
    "MssqlConnectionParams",
    "MssqlConnector",
]
