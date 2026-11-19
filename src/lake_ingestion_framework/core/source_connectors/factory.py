"""Connector factory — picks the right ``SourceConnector`` for a source_type.

Use cases (``discover``) call ``make_connector(source, config,
secret_resolver)``; the factory imports the per-type module lazily so
that an installation without the relevant extra still works for other
source types.
"""

from __future__ import annotations

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.core.metadata.repository import SourceRow
from lake_ingestion_framework.core.source_connectors._secret_resolver import (
    SecretResolver,
)
from lake_ingestion_framework.core.source_connectors.protocol import SourceConnector
from lake_ingestion_framework.core.types.enums import SourceType


class UnsupportedSourceTypeError(LakeIngestError):
    """No connector implementation exists for this source_type yet."""


def make_connector(
    source: SourceRow,
    config: dict[str, str],
    secret_resolver: SecretResolver,
) -> SourceConnector:
    """Return a connector instance for the given source.

    Per-type connectors are imported lazily so the framework stays
    importable when only some extras are installed.
    """
    if source.source_type is SourceType.POSTGRES:
        # Lazy import: keeps the framework importable when the [postgres]
        # extra isn't installed.
        from lake_ingestion_framework.core.source_connectors.postgres import (
            PostgresConnector,
        )

        return PostgresConnector.from_config(config, secret_resolver)

    if source.source_type in (SourceType.AZURE_SQL_DB, SourceType.SQL_SERVER):
        # Same TDS protocol + pyodbc driver covers both. Differences live in
        # the customer's source_config (encrypt, trust_server_certificate).
        # Lazy import for the [azure_sql_db] / [sql_server] extra.
        from lake_ingestion_framework.core.source_connectors.mssql import (
            MssqlConnector,
        )

        return MssqlConnector.from_config(config, secret_resolver)

    if source.source_type is SourceType.SFTP:
        # File-shaped source. Lazy import for the [sftp] extra.
        from lake_ingestion_framework.core.source_connectors.sftp import (
            SftpConnector,
        )

        return SftpConnector.from_config(config, secret_resolver)

    if source.source_type is SourceType.MYSQL:
        # Lazy import for the [mysql] extra.
        from lake_ingestion_framework.core.source_connectors.mysql import (
            MysqlConnector,
        )

        return MysqlConnector.from_config(config, secret_resolver)

    if source.source_type is SourceType.ORACLE:
        # Lazy import for the [oracle] extra. oracledb runs in thin mode
        # (no Instant Client install required).
        from lake_ingestion_framework.core.source_connectors.oracle import (
            OracleConnector,
        )

        return OracleConnector.from_config(config, secret_resolver)

    if source.source_type is SourceType.REST_API:
        # Lazy import for the [rest_api] extra (httpx).
        from lake_ingestion_framework.core.source_connectors.rest_api import (
            RestApiConnector,
        )

        return RestApiConnector.from_config(config, secret_resolver)

    raise UnsupportedSourceTypeError(
        f"No connector for source_type={source.source_type.value!r} yet. "
        f"Phase 4 ships Postgres, Azure SQL DB, SQL Server, MySQL, Oracle, "
        f"SFTP, and REST API."
    )


__all__ = ["UnsupportedSourceTypeError", "make_connector"]
