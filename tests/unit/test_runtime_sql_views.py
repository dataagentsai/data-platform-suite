"""Phase 6a — runtime SQL views.

Two layers:

1. **Loader** — the Python helper exposes the right views in the right
   dialect.
2. **SQLite end-to-end** — apply the SQLite views to an in-memory
   metadata DB, seed a few sources / datasets, and assert each view
   projects exactly the columns Phase 5d ADF Lookups query.

The T-SQL canonical isn't executed here (no Azure SQL DB in unit
tests); sqlfluff catches its syntax in the lint stage. Phase 8 will
add an integration test that applies the T-SQL DDL to a real Azure
SQL DB.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest
from lake_ingestion_framework.core.metadata.repository import (
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.realms.adf_adls.runtime_sql import (
    VIEW_NAMES,
    all_views,
    bootstrap_sqlite_tables,
    bootstrap_sqlite_views,
    view_sql,
)


class TestLoader:
    def test_view_names_match_loader_inventory(self) -> None:
        """The loader must expose Phase 5d runtime-dispatch views +
        observability views (see docs/operability.md)."""
        assert set(VIEW_NAMES) == {
            # Runtime dispatch (Phase 5d)
            "v_enabled_sources",
            "v_source_config",
            "v_enabled_datasets",
            # Observability Phase 1
            "v_dataset_health",
            "v_recent_failures",
            # Observability Phase 2
            "v_master_run_summary",
            "v_data_volume_trend",
            "v_slow_runs",
            # Cost attribution (cost concern; finance-facing)
            "v_cost_attribution",
            # Data quality
            "v_row_count_anomalies",
            "v_volume_drift",
            # Reliability
            "v_watermark_status",
            "v_retry_chains",
        }

    def test_azure_sql_db_dialect_is_t_sql(self) -> None:
        sql = view_sql("v_enabled_sources", dialect="azure_sql_db")
        assert "CREATE OR ALTER VIEW meta.v_enabled_sources" in sql
        # T-SQL bracket-quoted identifiers
        sources_cfg = view_sql("v_source_config", dialect="azure_sql_db")
        assert "[key]" in sources_cfg

    def test_sqlite_dialect_uses_drop_create(self) -> None:
        sql = view_sql("v_enabled_sources", dialect="sqlite")
        assert "DROP VIEW IF EXISTS v_enabled_sources" in sql
        assert "CREATE VIEW v_enabled_sources" in sql
        # No `meta.` schema in SQLite (ignoring comment lines).
        non_comment = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        assert "meta." not in non_comment

    def test_unknown_view_name_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown view"):
            view_sql("v_does_not_exist", dialect="sqlite")

    def test_all_views_returns_deploy_order(self) -> None:
        out = all_views(dialect="sqlite")
        assert [name for name, _ in out] == list(VIEW_NAMES)


# ──────────────────────────────────────────────────────────────────────
# SQLite end-to-end against the in-memory metadata DB
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo() -> Iterator[SqliteMetadataRepository]:
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    # Apply views directly to the same connection used by the repo.
    # We open a separate connection to the same in-memory DB? No —
    # ":memory:" databases are connection-private, so we must reach the
    # repo's connection. The repo doesn't expose it, so we go via a
    # private accessor inside the test only (acceptable for this
    # narrowly-scoped check).
    conn = r._connect()  # test-only access to apply views to the same in-memory DB
    # Phase 1 observability views (v_dataset_health, v_recent_failures)
    # depend on master_run_log + run_log; bootstrap those alongside the
    # views so all test scenarios resolve at SELECT time.
    bootstrap_sqlite_tables(conn)
    bootstrap_sqlite_views(conn)
    try:
        yield r
    finally:
        r.close()


def _query_view(repo: SqliteMetadataRepository, sql: str) -> list[sqlite3.Row]:
    conn = repo._connect()  # see fixture comment
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql)
    return list(cur.fetchall())


class TestVEnabledSources:
    def test_only_enabled_rows(self, repo: SqliteMetadataRepository) -> None:
        repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        repo.upsert_source(
            source_name="legacy",
            source_env="dev",
            source_type=SourceType.POSTGRES,
            is_enabled=False,
            config={"server": "x", "database": "d", "connection_user": "u"},
        )
        rows = _query_view(repo, "SELECT source_id, source_type FROM v_enabled_sources")
        # Disabled source absent
        assert len(rows) == 1
        assert rows[0]["source_type"] == "azure_sql_db"


class TestVSourceConfig:
    def test_azure_sql_db_projects_server_database(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "server": "orders.database.windows.net",
                "database": "orders",
            },
        )
        rows = _query_view(
            repo,
            f"SELECT p_server, p_database, p_user, p_password_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        assert rows[0]["p_server"] == "orders.database.windows.net"
        assert rows[0]["p_database"] == "orders"
        # MSI source has no user / secret — NULL is the right answer
        assert rows[0]["p_user"] is None
        assert rows[0]["p_password_secret"] is None

    def test_postgres_strips_kv_prefix_from_secret(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="orders_pg",
            source_env="dev",
            source_type=SourceType.POSTGRES,
            is_enabled=True,
            config={
                "server": "pg.example.com",
                "database": "orders",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://prod-vault/orders-pg-password",
            },
        )
        rows = _query_view(
            repo,
            f"SELECT p_server, p_database, p_user, p_password_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert rows[0]["p_user"] == "ingestor"
        # Bare secret name — kv://<vault>/ prefix stripped
        assert rows[0]["p_password_secret"] == "orders-pg-password"

    def test_oracle_uses_service_name_not_database(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="erp",
            source_env="dev",
            source_type=SourceType.ORACLE,
            is_enabled=True,
            config={
                "server": "oracle.example.com",
                "service_name": "ORCLCDB",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://my-vault/erp-password",
            },
        )
        rows = _query_view(
            repo,
            f"SELECT p_server, p_service_name, p_database "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert rows[0]["p_server"] == "oracle.example.com"
        assert rows[0]["p_service_name"] == "ORCLCDB"
        assert rows[0]["p_database"] is None

    def test_snowflake_projects_account_warehouse_role(
        self, repo: SqliteMetadataRepository
    ) -> None:
        sid = repo.upsert_source(
            source_name="warehouse_sf",
            source_env="dev",
            source_type=SourceType.SNOWFLAKE,
            is_enabled=True,
            config={
                "account": "myorg-myaccount",
                "warehouse": "INGEST_WH",
                "database": "ANALYTICS",
                "role": "INGEST_ROLE",
                "connection_user": "INGEST_USER",
                "connection_secret_name": "kv://my-vault/sf-password",
            },
        )
        rows = _query_view(
            repo,
            "SELECT p_account, p_warehouse, p_database, p_role, "  # noqa: S608
            f"p_user, p_password_secret FROM v_source_config WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["p_account"] == "myorg-myaccount"
        assert row["p_warehouse"] == "INGEST_WH"
        assert row["p_database"] == "ANALYTICS"
        assert row["p_role"] == "INGEST_ROLE"
        assert row["p_user"] == "INGEST_USER"
        # kv:// prefix stripped to bare secret name
        assert row["p_password_secret"] == "sf-password"

    def test_salesforce_projects_oauth_params(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="customer_crm",
            source_env="dev",
            source_type=SourceType.SALESFORCE,
            is_enabled=True,
            config={
                "auth_method": "service_account",
                "environment_url": "https://my-org.my.salesforce.com",
                "client_id": "test-client-id",  # gitleaks:allow
                "connection_secret_name": "kv://my-vault/sf-client-secret",
            },
        )
        rows = _query_view(
            repo,
            "SELECT p_environment_url, p_client_id, p_client_secret_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["p_environment_url"] == "https://my-org.my.salesforce.com"
        assert row["p_client_id"] == "test-client-id"
        # Client secret strips the kv:// prefix to bare secret name
        assert row["p_client_secret_secret"] == "sf-client-secret"

    def test_azure_blob_projects_storage_account(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="partner_blob",
            source_env="dev",
            source_type=SourceType.AZURE_BLOB_X,
            is_enabled=True,
            config={
                "auth_method": "managed_identity",
                "storage_account": "partnerlandingsa",
            },
        )
        rows = _query_view(
            repo,
            f"SELECT p_storage_account FROM v_source_config WHERE source_id = {sid}",  # noqa: S608
        )
        assert len(rows) == 1
        assert rows[0]["p_storage_account"] == "partnerlandingsa"

    def test_s3_projects_access_key_id_and_secret(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="legacy_s3",
            source_env="dev",
            source_type=SourceType.S3,
            is_enabled=True,
            config={
                "auth_method": "basic",
                "access_key_id": "AKIA0000",
                "connection_secret_name": "kv://my-vault/s3-secret",
            },
        )
        rows = _query_view(
            repo,
            "SELECT p_access_key_id, p_secret_access_key_secret, p_password_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["p_access_key_id"] == "AKIA0000"
        # All three secret-name columns project the same bare-secret-name
        assert row["p_secret_access_key_secret"] == "s3-secret"
        assert row["p_password_secret"] == "s3-secret"

    def test_bigquery_projects_project_id_and_key_file_secret(
        self, repo: SqliteMetadataRepository
    ) -> None:
        sid = repo.upsert_source(
            source_name="warehouse_bq",
            source_env="dev",
            source_type=SourceType.BIGQUERY,
            is_enabled=True,
            config={
                "auth_method": "service_account",
                "project_id": "my-gcp-analytics",
                "connection_secret_name": "kv://my-vault/bq-sa-key",
            },
        )
        rows = _query_view(
            repo,
            "SELECT p_project_id, p_key_file_secret, p_password_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["p_project_id"] == "my-gcp-analytics"
        # p_key_file_secret + p_password_secret carry the same bare-secret-name
        assert row["p_key_file_secret"] == "bq-sa-key"
        assert row["p_password_secret"] == "bq-sa-key"

    def test_rest_api_projects_base_url(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="hubspot",
            source_env="dev",
            source_type=SourceType.REST_API,
            is_enabled=True,
            config={
                "base_url": "https://api.hubapi.com",
                "connection_secret_name": "kv://my-vault/hubspot-token",
            },
        )
        rows = _query_view(
            repo,
            f"SELECT p_base_url, p_password_secret "  # noqa: S608
            f"FROM v_source_config WHERE source_id = {sid}",
        )
        assert rows[0]["p_base_url"] == "https://api.hubapi.com"
        assert rows[0]["p_password_secret"] == "hubspot-token"

    def test_disabled_source_excluded(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="paused",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=False,
            config={"server": "x", "database": "d"},
        )
        rows = _query_view(
            repo,
            f"SELECT source_id FROM v_source_config WHERE source_id = {sid}",  # noqa: S608
        )
        assert rows == []


class TestVEnabledDatasets:
    def test_dataset_projects_lookup_columns(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "load_type": "INCREMENTAL",
                "schema_name": "dbo",
                "object_name": "customers",
                "watermark_column": "modified_at",
                "target_container": "lake",
                "target_path_template": "bronze/orders/dbo/customers/",
                "file_format": "PARQUET",
            },
        )
        rows = _query_view(
            repo,
            "SELECT dataset_id, schema_name, table_name, watermark_column, "  # noqa: S608
            "watermark_value, container, folder_path, file_format, "
            "relative_url, file_name, folder_path_sink "
            f"FROM v_enabled_datasets WHERE source_id = {sid}",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["schema_name"] == "dbo"
        assert row["table_name"] == "customers"  # object_name -> table_name rename
        assert row["watermark_column"] == "modified_at"
        assert row["watermark_value"] is None  # no watermark seeded yet
        assert row["container"] == "lake"
        assert row["folder_path"] == "bronze/orders/dbo/customers/"
        # file_format projects from `file_format_data` (source-side format,
        # csv / json_lines for SFTP). RDBMS sources have no source file
        # format, so the view returns NULL — used only by the SFTP Switch.
        assert row["file_format"] is None
        # REST / SFTP-only columns are NULL for an RDBMS dataset
        assert row["relative_url"] is None
        assert row["file_name"] is None

    def test_disabled_dataset_excluded(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=False,
            config={"realm": "adf-adls", "dataset_type": "TABLE"},
        )
        rows = _query_view(
            repo,
            f"SELECT dataset_id FROM v_enabled_datasets WHERE source_id = {sid}",  # noqa: S608
        )
        assert rows == []

    def test_watermark_value_joined_in(self, repo: SqliteMetadataRepository) -> None:
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        did = repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "schema_name": "dbo",
                "object_name": "customers",
                "watermark_column": "modified_at",
                "target_container": "lake",
                "target_path_template": "bronze/orders/dbo/customers/",
            },
        )
        # Seed a watermark row directly
        conn = repo._connect()
        conn.execute(
            "INSERT INTO watermark (dataset_id, watermark_value, rows_loaded) " "VALUES (?, ?, ?)",
            (did, "2026-04-25T23:59:59Z", 12_847),
        )
        conn.commit()
        rows = _query_view(
            repo,
            f"SELECT watermark_value FROM v_enabled_datasets WHERE dataset_id = {did}",  # noqa: S608
        )
        assert rows[0]["watermark_value"] == "2026-04-25T23:59:59Z"

    def test_azure_blob_container_projects_to_bucket_column(
        self, repo: SqliteMetadataRepository
    ) -> None:
        """Both s3_bucket and azure_blob_container project to the
        shared `bucket` column via COALESCE — the per-source driver
        switches on source_type to know which it's reading."""
        sid = repo.upsert_source(
            source_name="partner_blob",
            source_env="dev",
            source_type=SourceType.AZURE_BLOB_X,
            is_enabled=True,
            config={
                "auth_method": "managed_identity",
                "storage_account": "partnerlandingsa",
            },
        )
        did = repo.upsert_dataset(
            source_id=sid,
            dataset_name="orders_csv",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "azure_blob_container": "incoming",
                "sftp_folder_path": "/orders/",
                "file_name": "orders.csv",
                "file_format_data": "csv",
                "target_container": "lake",
                "target_path_template": "bronze/azure_blob/partner_blob/orders/",
            },
        )
        rows = _query_view(
            repo,
            "SELECT bucket, folder_path, file_name, file_format "  # noqa: S608
            f"FROM v_enabled_datasets WHERE dataset_id = {did}",
        )
        assert rows[0]["bucket"] == "incoming"  # via azure_blob_container, not s3_bucket
        assert rows[0]["folder_path"] == "/orders/"
        assert rows[0]["file_name"] == "orders.csv"
        assert rows[0]["file_format"] == "csv"

    def test_sftp_folder_path_takes_precedence(self, repo: SqliteMetadataRepository) -> None:
        """For SFTP datasets, sftp_folder_path is the source dir;
        target_path_template is the sink. Both must surface so the
        Per-Source Driver can pass folder_path (source) and
        folder_path_sink (target) into the SFTP ingest pipeline.
        """
        sid = repo.upsert_source(
            source_name="partner_drop",
            source_env="dev",
            source_type=SourceType.SFTP,
            is_enabled=True,
            config={
                "server": "sftp.partner.com",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://my-vault/sftp-password",
            },
        )
        did = repo.upsert_dataset(
            source_id=sid,
            dataset_name="orders_csv",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "file_format_data": "csv",
                "file_name": "orders.csv",
                "sftp_folder_path": "/incoming/orders/",
                "target_container": "lake",
                "target_path_template": "bronze/sftp/partner_drop/orders/",
            },
        )
        rows = _query_view(
            repo,
            "SELECT folder_path, folder_path_sink, file_name, file_format "  # noqa: S608
            f"FROM v_enabled_datasets WHERE dataset_id = {did}",
        )
        assert rows[0]["folder_path"] == "/incoming/orders/"
        assert rows[0]["folder_path_sink"] == "bronze/sftp/partner_drop/orders/"
        assert rows[0]["file_name"] == "orders.csv"
        assert rows[0]["file_format"] == "csv"


class TestPhase5dContract:
    """The view column projection must match what Phase 5d Lookup queries
    expect — drift between the view and the ADF SQL would silently
    break runtime ingestion, which unit tests can't otherwise catch.
    """

    def test_v_enabled_datasets_columns_match_lookup_query(
        self, repo: SqliteMetadataRepository
    ) -> None:
        from lake_ingestion_framework.realms.adf_adls.generator.pipelines import (
            _ENABLED_DATASETS_LOOKUP_QUERY,
        )

        # Extract SELECT columns from the Lookup query
        lookup_columns = _extract_select_columns(_ENABLED_DATASETS_LOOKUP_QUERY)
        # Verify the view returns at least these columns
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "x", "database": "d"},
        )
        repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "schema_name": "dbo",
                "object_name": "customers",
            },
        )
        select_clause = ", ".join(lookup_columns)
        rows = _query_view(
            repo,
            f"SELECT {select_clause} FROM v_enabled_datasets WHERE source_id = {sid}",  # noqa: S608
        )
        assert len(rows) == 1  # query parsed cleanly against the SQLite view


# ──────────────────────────────────────────────────────────────────────
# Phase 1 observability views — see docs/operability.md
# ──────────────────────────────────────────────────────────────────────


def _seed_dataset(
    repo: SqliteMetadataRepository,
    *,
    source_name: str = "orders",
    dataset_name: str = "customers",
    sla_freshness_hours: int | None = None,
    is_enabled: bool = True,
) -> tuple[int, int]:
    """Create one source + dataset; return (source_id, dataset_id)."""
    sid = repo.upsert_source(
        source_name=source_name,
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"server": "s", "database": "d"},
    )
    cfg: dict[str, str] = {
        "realm": "adf-adls",
        "dataset_type": "TABLE",
        "schema_name": "dbo",
        "object_name": dataset_name,
    }
    if sla_freshness_hours is not None:
        cfg["sla_freshness_hours"] = str(sla_freshness_hours)
    did = repo.upsert_dataset(
        source_id=sid,
        dataset_name=dataset_name,
        is_enabled=is_enabled,
        config=cfg,
    )
    return sid, did


def _insert_run_log(
    repo: SqliteMetadataRepository,
    *,
    dataset_id: int,
    started_at: str,
    status: str,
    error_message: str | None = None,
) -> int:
    """Insert a run_log row directly; returns run_id."""
    conn = repo._connect()
    cur = conn.execute(
        "INSERT INTO run_log "
        "(dataset_id, source_env, realm, run_type, started_at, status, error_message) "
        "VALUES (?, 'dev', 'adf-adls', 'INCREMENTAL', ?, ?, ?)",
        (dataset_id, started_at, status, error_message),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


class TestVDatasetHealth:
    def test_red_when_no_runs_yet(self, repo: SqliteMetadataRepository) -> None:
        """Dataset enabled but never run → red (last_success_at NULL)."""
        _seed_dataset(repo)
        rows = _query_view(repo, "SELECT health, last_success_at FROM v_dataset_health")
        assert len(rows) == 1
        assert rows[0]["health"] == "red"
        assert rows[0]["last_success_at"] is None

    def test_green_when_recent_success_within_sla(self, repo: SqliteMetadataRepository) -> None:
        """Recent SUCCESS, well inside SLA → green."""
        _, did = _seed_dataset(repo, sla_freshness_hours=24)
        # Successful run 1 hour ago — within 75% of 24h SLA (18h)
        _insert_run_log(
            repo,
            dataset_id=did,
            started_at="datetime('now', '-1 hour')",
            status="SUCCESS",
        )
        # Use SQL expression for the timestamp by going through
        # `julianday`-friendly literal — easier: use literal datetime.
        # Replace started_at with a known recent value via UPDATE.
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-1 hour') " "WHERE dataset_id = ?",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT health FROM v_dataset_health")
        assert rows[0]["health"] == "green"

    def test_yellow_when_recent_success_in_warning_zone(
        self, repo: SqliteMetadataRepository
    ) -> None:
        """Last success between 75% and 100% of SLA window → yellow."""
        _, did = _seed_dataset(repo, sla_freshness_hours=10)
        # Successful run 8 hours ago — 80% of 10h SLA (warning zone)
        _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-8 hours') " "WHERE dataset_id = ?",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT health FROM v_dataset_health")
        assert rows[0]["health"] == "yellow"

    def test_red_when_last_status_failed(self, repo: SqliteMetadataRepository) -> None:
        """Most recent run was FAILED → red even if there's a prior SUCCESS."""
        _, did = _seed_dataset(repo, sla_freshness_hours=24)
        # First a SUCCESS, then a later FAILED — most recent wins.
        _insert_run_log(repo, dataset_id=did, started_at="2026-04-30T01:00:00", status="SUCCESS")
        _insert_run_log(repo, dataset_id=did, started_at="2026-04-30T02:00:00", status="FAILED")
        rows = _query_view(repo, "SELECT health, last_status FROM v_dataset_health")
        assert rows[0]["last_status"] == "FAILED"
        assert rows[0]["health"] == "red"

    def test_red_when_sla_breached(self, repo: SqliteMetadataRepository) -> None:
        """Last SUCCESS older than SLA → red (freshness breach)."""
        _, did = _seed_dataset(repo, sla_freshness_hours=2)
        _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        # Push the success 5 hours back — way outside 2h SLA
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-5 hours') " "WHERE dataset_id = ?",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT health FROM v_dataset_health")
        assert rows[0]["health"] == "red"

    def test_uses_dataset_config_sla_override(self, repo: SqliteMetadataRepository) -> None:
        """Per-dataset sla_freshness_hours override flows into the view."""
        _, did = _seed_dataset(repo, sla_freshness_hours=2)
        rows = _query_view(repo, "SELECT sla_freshness_hours FROM v_dataset_health")
        assert rows[0]["sla_freshness_hours"] == 2

    def test_falls_back_to_24h_default(self, repo: SqliteMetadataRepository) -> None:
        """Datasets without sla_freshness_hours fall back to 24h default."""
        _seed_dataset(repo)  # no SLA override
        rows = _query_view(repo, "SELECT sla_freshness_hours FROM v_dataset_health")
        assert rows[0]["sla_freshness_hours"] == 24

    def test_excludes_disabled_datasets(self, repo: SqliteMetadataRepository) -> None:
        """is_enabled=0 datasets are absent from the view."""
        _seed_dataset(repo, dataset_name="customers", is_enabled=False)
        rows = _query_view(repo, "SELECT dataset_id FROM v_dataset_health")
        assert rows == []

    def test_carries_source_context(self, repo: SqliteMetadataRepository) -> None:
        """One row per dataset; source_name + source_env reach the row."""
        _seed_dataset(repo)
        rows = _query_view(
            repo,
            "SELECT source_name, source_env, dataset_name FROM v_dataset_health",
        )
        assert rows[0]["source_name"] == "orders"
        assert rows[0]["source_env"] == "dev"
        assert rows[0]["dataset_name"] == "customers"


class TestVRecentFailures:
    def test_only_failed_status_returned(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        _insert_run_log(repo, dataset_id=did, started_at="2026-04-30T00:00:00", status="SUCCESS")
        _insert_run_log(
            repo,
            dataset_id=did,
            started_at="2026-04-30T01:00:00",
            status="FAILED",
            error_message="connection refused",
        )
        # Push the rows into the recent window so they survive the
        # 7-day filter.
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-1 hour') " "WHERE status = 'FAILED'"
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id, error_message FROM v_recent_failures")
        assert len(rows) == 1
        assert rows[0]["error_message"] == "connection refused"

    def test_excludes_failures_older_than_7_days(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        _insert_run_log(repo, dataset_id=did, started_at="x", status="FAILED")
        # Push it out to 10 days ago — outside the 7-day window
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-10 days') " "WHERE dataset_id = ?",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_recent_failures")
        assert rows == []

    def test_carries_dataset_and_source_context(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        _insert_run_log(
            repo,
            dataset_id=did,
            started_at="x",
            status="FAILED",
            error_message="boom",
        )
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-1 hour') " "WHERE dataset_id = ?",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT dataset_name, source_name, source_type, source_env " "FROM v_recent_failures",
        )
        assert rows[0]["dataset_name"] == "customers"
        assert rows[0]["source_name"] == "orders"
        assert rows[0]["source_type"] == "azure_sql_db"
        assert rows[0]["source_env"] == "dev"


def _insert_master_run_log(
    repo: SqliteMetadataRepository,
    *,
    source_id: int,
    started_at: str,
    ended_at: str | None = None,
    status: str = "SUCCESS",
    total_attempted: int | None = None,
    succeeded: int | None = None,
    failed: int | None = None,
    skipped: int | None = None,
    retry_of_master_run_id: int | None = None,
) -> int:
    """Insert a master_run_log row directly; returns master_run_id."""
    conn = repo._connect()
    cur = conn.execute(
        "INSERT INTO master_run_log "
        "(source_id, source_env, run_type, started_at, ended_at, status, "
        " total_attempted, succeeded, failed, skipped, retry_of_master_run_id) "
        "VALUES (?, 'dev', 'INCREMENTAL', ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            source_id,
            started_at,
            ended_at,
            status,
            total_attempted,
            succeeded,
            failed,
            skipped,
            retry_of_master_run_id,
        ),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


class TestVMasterRunSummary:
    def test_carries_source_context_and_duration(self, repo: SqliteMetadataRepository) -> None:
        sid, _ = _seed_dataset(repo)
        _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="2026-04-30T10:00:00",
            ended_at="2026-04-30T10:05:30",
            status="SUCCESS",
            total_attempted=4,
            succeeded=4,
            failed=0,
            skipped=0,
        )
        # Push the row into the recent window so it survives the 7-day filter.
        repo._connect().execute(
            "UPDATE master_run_log SET "
            "started_at = datetime('now', '-1 hour'), "
            "ended_at   = datetime('now', '-55 minutes')"
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT source_name, source_env, status, total_attempted, "
            "succeeded, success_rate_pct, duration_seconds "
            "FROM v_master_run_summary",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["source_name"] == "orders"
        assert row["source_env"] == "dev"
        assert row["status"] == "SUCCESS"
        assert row["total_attempted"] == 4
        assert row["succeeded"] == 4
        assert row["success_rate_pct"] == 100.0
        # 5-minute window → ~300s
        assert row["duration_seconds"] is not None
        assert 240 <= row["duration_seconds"] <= 360

    def test_excludes_runs_older_than_7_days(self, repo: SqliteMetadataRepository) -> None:
        sid, _ = _seed_dataset(repo)
        _insert_master_run_log(repo, source_id=sid, started_at="x", status="SUCCESS")
        # Push it 10 days back — outside the 7-day window
        repo._connect().execute(
            "UPDATE master_run_log SET started_at = datetime('now', '-10 days')"
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT master_run_id FROM v_master_run_summary")
        assert rows == []

    def test_success_rate_null_when_total_attempted_zero(
        self, repo: SqliteMetadataRepository
    ) -> None:
        """master runs that picked up zero datasets keep success_rate
        NULL rather than 0/0 = error."""
        sid, _ = _seed_dataset(repo)
        _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="SUCCESS",
            total_attempted=0,
            succeeded=0,
        )
        repo._connect().execute("UPDATE master_run_log SET started_at = datetime('now', '-1 hour')")
        repo._connect().commit()
        rows = _query_view(repo, "SELECT success_rate_pct FROM v_master_run_summary")
        assert rows[0]["success_rate_pct"] is None


class TestVDataVolumeTrend:
    def test_aggregates_same_day_success_runs(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        # Two SUCCESS runs same day; pre-set bytes/rows
        for i in range(2):
            rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
            repo._connect().execute(
                "UPDATE run_log SET rows_extracted=?, bytes_read=?, "
                "bytes_written=?, file_count=? WHERE run_id=?",
                (100 + i, 1000 + i, 800 + i, 1, rid),
            )
        repo._connect().execute("UPDATE run_log SET started_at = datetime('now', '-1 hour')")
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT daily_runs, daily_rows, daily_bytes_read, "
            "daily_bytes_written, daily_files FROM v_data_volume_trend",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["daily_runs"] == 2
        assert row["daily_rows"] == 201  # 100 + 101
        assert row["daily_bytes_read"] == 2001  # 1000 + 1001
        assert row["daily_bytes_written"] == 1601  # 800 + 801
        assert row["daily_files"] == 2

    def test_excludes_failed_rows(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="FAILED")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=999, bytes_read=999, "
            "started_at = datetime('now', '-1 hour') "
            "WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT daily_runs FROM v_data_volume_trend")
        assert rows == []

    def test_excludes_runs_older_than_7_days(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET started_at = datetime('now', '-10 days') " "WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT daily_runs FROM v_data_volume_trend")
        assert rows == []


class TestVSlowRuns:
    def test_run_above_3x_avg_is_flagged(self, repo: SqliteMetadataRepository) -> None:
        """A run far slower than the dataset's recent average gets
        flagged. Note: the baseline AVG includes all SUCCESS runs in
        the 14-day window, including the slow one — so the slow value
        must clear 3x of the *blended* average for the row to show up.
        With 3x1000ms baseline + 1x50000ms slow:
        AVG = 13250, threshold = 39750, 50000 > 39750 → flagged.
        """
        _, did = _seed_dataset(repo)
        # Baseline: 3 SUCCESS runs at 1000ms each
        for _ in range(3):
            rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
            repo._connect().execute(
                "UPDATE run_log SET source_duration_ms=1000, "
                "started_at = datetime('now', '-2 days') WHERE run_id=?",
                (rid,),
            )
        # Slow run at 50000ms within the last 7 days
        slow_rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET source_duration_ms=50000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (slow_rid,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT run_id, source_duration_ms, slowness_ratio FROM v_slow_runs",
        )
        assert len(rows) == 1
        assert rows[0]["run_id"] == slow_rid
        assert rows[0]["source_duration_ms"] == 50000
        assert rows[0]["slowness_ratio"] > 3.0

    def test_run_within_3x_avg_not_flagged(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        # Baseline + a 2x run (2000ms vs 1000ms avg) — under 3x threshold
        for _ in range(3):
            rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
            repo._connect().execute(
                "UPDATE run_log SET source_duration_ms=1000, "
                "started_at = datetime('now', '-2 days') WHERE run_id=?",
                (rid,),
            )
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET source_duration_ms=2000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_slow_runs")
        assert rows == []

    def test_excludes_datasets_with_too_few_baseline_samples(
        self, repo: SqliteMetadataRepository
    ) -> None:
        """sample_size < 3 in the 14-day baseline window — no flagging
        even if the latest run is much slower than the prior ones."""
        _, did = _seed_dataset(repo)
        # Only 2 baseline runs (at 100ms) + 1 slow recent run
        for _ in range(2):
            rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
            repo._connect().execute(
                "UPDATE run_log SET source_duration_ms=100, "
                "started_at = datetime('now', '-2 days') WHERE run_id=?",
                (rid,),
            )
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET source_duration_ms=10000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_slow_runs")
        assert rows == []  # no flagging without baseline


class TestVCostAttribution:
    """v_cost_attribution — DIU-hours per dataset/source/realm/cost_center.

    Formula: DIU-hours = used_dius * source_duration_ms / 3.6e6.
    See docs/cost.md.
    """

    def test_diu_hours_formula(self, repo: SqliteMetadataRepository) -> None:
        """One run with used_dius=4 and source_duration_ms=900000 (15 min)
        → 4 * 900000 / 3.6e6 = 1.0 DIU-hour."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET used_dius=4, source_duration_ms=900000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT diu_hours, total_duration_ms, run_count " "FROM v_cost_attribution",
        )
        assert len(rows) == 1
        assert rows[0]["diu_hours"] == pytest.approx(1.0, rel=1e-6)
        assert rows[0]["total_duration_ms"] == 900000
        assert rows[0]["run_count"] == 1

    def test_aggregates_same_day_runs(self, repo: SqliteMetadataRepository) -> None:
        """Two SUCCESS runs same day sum cleanly into a single row."""
        _, did = _seed_dataset(repo)
        for _ in range(2):
            rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
            repo._connect().execute(
                "UPDATE run_log SET used_dius=2, source_duration_ms=1800000, "
                "started_at = datetime('now', '-1 hour') WHERE run_id=?",
                (rid,),
            )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT diu_hours, run_count FROM v_cost_attribution",
        )
        assert len(rows) == 1
        # 2 runs * (2 DIU * 30 min) = 2 DIU-hours
        assert rows[0]["diu_hours"] == pytest.approx(2.0, rel=1e-6)
        assert rows[0]["run_count"] == 2

    def test_excludes_runs_with_null_dius(self, repo: SqliteMetadataRepository) -> None:
        """Runs without used_dius captured (older runs, retries) are
        excluded — cost attribution can't be computed without DIU data."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        # used_dius left NULL; source_duration_ms set
        repo._connect().execute(
            "UPDATE run_log SET source_duration_ms=900000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT diu_hours FROM v_cost_attribution")
        assert rows == []

    def test_excludes_failed_runs(self, repo: SqliteMetadataRepository) -> None:
        """FAILED runs may have skewed durations from partial work — excluded."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="FAILED")
        repo._connect().execute(
            "UPDATE run_log SET used_dius=4, source_duration_ms=900000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT diu_hours FROM v_cost_attribution")
        assert rows == []

    def test_cost_center_flows_from_dataset_config(self, repo: SqliteMetadataRepository) -> None:
        """cost_center is read from dataset_config (per-dataset KV)."""
        sid = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"server": "s", "database": "d"},
        )
        did = repo.upsert_dataset(
            source_id=sid,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "schema_name": "dbo",
                "object_name": "customers",
                "cost_center": "FIN-1234",
            },
        )
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET used_dius=4, source_duration_ms=900000, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT cost_center FROM v_cost_attribution")
        assert rows[0]["cost_center"] == "FIN-1234"

    def test_excludes_runs_older_than_30_days(self, repo: SqliteMetadataRepository) -> None:
        """30-day window — older runs drop out so dashboards stay focused
        on recent attribution."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET used_dius=4, source_duration_ms=900000, "
            "started_at = datetime('now', '-45 days') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT diu_hours FROM v_cost_attribution")
        assert rows == []


class TestVRowCountAnomalies:
    """v_row_count_anomalies — SUCCESS runs with rows_extracted !=
    rows_written. Bronze does no transformation, so any delta means
    rows were dropped or duplicated. See docs/operability.md.
    """

    def test_flags_run_with_row_count_mismatch(self, repo: SqliteMetadataRepository) -> None:
        """A SUCCESS run with extracted=100, written=98 surfaces in the
        view with delta=2 and delta_pct=2.0."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, rows_written=98, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT row_count_delta, delta_pct, rows_extracted, rows_written "
            "FROM v_row_count_anomalies",
        )
        assert len(rows) == 1
        assert rows[0]["row_count_delta"] == 2
        assert rows[0]["delta_pct"] == pytest.approx(2.0, rel=1e-6)
        assert rows[0]["rows_extracted"] == 100
        assert rows[0]["rows_written"] == 98

    def test_clean_run_not_flagged(self, repo: SqliteMetadataRepository) -> None:
        """SUCCESS run where rows_extracted == rows_written → not flagged."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, rows_written=100, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_row_count_anomalies")
        assert rows == []

    def test_excludes_runs_with_null_counts(self, repo: SqliteMetadataRepository) -> None:
        """Runs without both counts captured (older runs, partial
        instrumentation) can't be evaluated — excluded."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        # rows_extracted set, rows_written NULL — can't compute delta
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_row_count_anomalies")
        assert rows == []

    def test_excludes_failed_runs(self, repo: SqliteMetadataRepository) -> None:
        """FAILED runs are off-topic for this view — they're already
        in v_recent_failures."""
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="FAILED")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, rows_written=80, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_row_count_anomalies")
        assert rows == []

    def test_excludes_runs_older_than_7_days(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, rows_written=98, "
            "started_at = datetime('now', '-10 days') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT run_id FROM v_row_count_anomalies")
        assert rows == []

    def test_carries_dataset_and_source_context(self, repo: SqliteMetadataRepository) -> None:
        _, did = _seed_dataset(repo)
        rid = _insert_run_log(repo, dataset_id=did, started_at="x", status="SUCCESS")
        repo._connect().execute(
            "UPDATE run_log SET rows_extracted=100, rows_written=99, "
            "started_at = datetime('now', '-1 hour') WHERE run_id=?",
            (rid,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT dataset_name, source_name, source_type, source_env "
            "FROM v_row_count_anomalies",
        )
        assert rows[0]["dataset_name"] == "customers"
        assert rows[0]["source_name"] == "orders"
        assert rows[0]["source_type"] == "azure_sql_db"
        assert rows[0]["source_env"] == "dev"


class TestVWatermarkStatus:
    """v_watermark_status — current watermark + time since advance +
    last operator-driven reset (when present). Reliability concern.
    """

    def test_never_loaded_dataset_is_never_loaded(self, repo: SqliteMetadataRepository) -> None:
        """Enabled dataset with no row in meta.watermark → health
        is 'never_loaded' and current_watermark is NULL."""
        _seed_dataset(repo)
        rows = _query_view(
            repo,
            "SELECT health, current_watermark, last_loaded_at " "FROM v_watermark_status",
        )
        assert len(rows) == 1
        assert rows[0]["health"] == "never_loaded"
        assert rows[0]["current_watermark"] is None
        assert rows[0]["last_loaded_at"] is None

    def test_recently_loaded_is_healthy(self, repo: SqliteMetadataRepository) -> None:
        """Watermark advanced in the last 24h → health='healthy'."""
        _, did = _seed_dataset(repo)
        repo._connect().execute(
            "INSERT INTO watermark (dataset_id, watermark_value, last_loaded_at, rows_loaded) "
            "VALUES (?, '2026-04-30T10:00:00', datetime('now', '-1 hour'), 100)",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT health, current_watermark FROM v_watermark_status",
        )
        assert rows[0]["health"] == "healthy"
        assert rows[0]["current_watermark"] == "2026-04-30T10:00:00"

    def test_stale_watermark_flagged_possibly_stuck(self, repo: SqliteMetadataRepository) -> None:
        """Last advance > 24h ago → possibly_stuck. Operators
        investigate; the view doesn't try to distinguish 'genuinely
        idle source' from 'broken pipeline'."""
        _, did = _seed_dataset(repo)
        repo._connect().execute(
            "INSERT INTO watermark (dataset_id, watermark_value, last_loaded_at, rows_loaded) "
            "VALUES (?, 'x', datetime('now', '-30 hours'), 100)",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(repo, "SELECT health FROM v_watermark_status")
        assert rows[0]["health"] == "possibly_stuck"

    def test_reset_row_surfaces_in_last_reset_columns(self, repo: SqliteMetadataRepository) -> None:
        """When watermark_audit has a row, the most recent reset
        flows into last_reset_* columns. This verifies the view's
        Phase 6b-3 readiness — populating watermark_audit doesn't
        require any view changes."""
        _, did = _seed_dataset(repo)
        repo._connect().execute(
            "INSERT INTO watermark (dataset_id, watermark_value, last_loaded_at, rows_loaded) "
            "VALUES (?, 'after', datetime('now', '-1 hour'), 100)",
            (did,),
        )
        repo._connect().execute(
            "INSERT INTO watermark_audit "
            "(dataset_id, old_watermark_value, new_watermark_value, "
            " change_type, reason, changed_by, changed_at) "
            "VALUES (?, 'before', 'after', 'RESET', "
            "'backfill 2024-01', 'cli:ops@acme', datetime('now', '-2 hours'))",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT last_reset_from_value, last_reset_to_value, "
            "last_reset_change_type, last_reset_changed_by, last_reset_reason "
            "FROM v_watermark_status",
        )
        assert rows[0]["last_reset_from_value"] == "before"
        assert rows[0]["last_reset_to_value"] == "after"
        assert rows[0]["last_reset_change_type"] == "RESET"
        assert rows[0]["last_reset_changed_by"] == "cli:ops@acme"
        assert rows[0]["last_reset_reason"] == "backfill 2024-01"

    def test_only_most_recent_reset_surfaces(self, repo: SqliteMetadataRepository) -> None:
        """Multiple resets logged → view shows only the most recent."""
        _, did = _seed_dataset(repo)
        repo._connect().execute(
            "INSERT INTO watermark (dataset_id, watermark_value, last_loaded_at, rows_loaded) "
            "VALUES (?, 'now', datetime('now', '-1 hour'), 100)",
            (did,),
        )
        # Older reset
        repo._connect().execute(
            "INSERT INTO watermark_audit "
            "(dataset_id, change_type, reason, changed_by, changed_at) "
            "VALUES (?, 'RESET', 'old reason', 'old-user', datetime('now', '-7 days'))",
            (did,),
        )
        # Newer reset
        repo._connect().execute(
            "INSERT INTO watermark_audit "
            "(dataset_id, change_type, reason, changed_by, changed_at) "
            "VALUES (?, 'RESET', 'new reason', 'new-user', datetime('now', '-1 day'))",
            (did,),
        )
        repo._connect().commit()
        rows = _query_view(
            repo,
            "SELECT last_reset_changed_by, last_reset_reason FROM v_watermark_status",
        )
        assert rows[0]["last_reset_changed_by"] == "new-user"
        assert rows[0]["last_reset_reason"] == "new reason"

    def test_excludes_disabled_datasets(self, repo: SqliteMetadataRepository) -> None:
        _seed_dataset(repo, is_enabled=False)
        rows = _query_view(repo, "SELECT dataset_id FROM v_watermark_status")
        assert rows == []


class TestVRetryChains:
    """v_retry_chains — per-chain rollup over master_run_log
    retry_of_master_run_id chains. Audits the L4 auto-retry layer.
    See docs/recovery.md.
    """

    def test_single_attempt_run_is_excluded(self, repo: SqliteMetadataRepository) -> None:
        """Chains of length 1 (no retry happened) are filtered out —
        v_master_run_summary already covers regular runs."""
        sid, _ = _seed_dataset(repo)
        _insert_master_run_log(repo, source_id=sid, started_at="x", status="SUCCESS")
        rows = _query_view(repo, "SELECT chain_root_id FROM v_retry_chains")
        assert rows == []

    def test_two_attempt_chain_eventually_succeeded(self, repo: SqliteMetadataRepository) -> None:
        """First run PARTIAL, retry SUCCESS → chain_length=2,
        chain_outcome='eventually_succeeded'."""
        sid, _ = _seed_dataset(repo)
        root_id = _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="2026-04-30T10:00:00",
            ended_at="2026-04-30T10:05:00",
            status="PARTIAL",
            total_attempted=10,
            succeeded=8,
            failed=2,
        )
        retry_id = _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="2026-04-30T10:30:00",
            ended_at="2026-04-30T10:32:00",
            status="SUCCESS",
            total_attempted=2,
            succeeded=2,
            failed=0,
            retry_of_master_run_id=root_id,
        )
        rows = _query_view(
            repo,
            "SELECT chain_root_id, chain_length, final_master_run_id, "
            "final_status, chain_outcome, total_attempts_across_chain, "
            "total_succeeded_across_chain "
            "FROM v_retry_chains",
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["chain_root_id"] == root_id
        assert row["chain_length"] == 2
        assert row["final_master_run_id"] == retry_id
        assert row["final_status"] == "SUCCESS"
        assert row["chain_outcome"] == "eventually_succeeded"
        assert row["total_attempts_across_chain"] == 12  # 10 + 2
        assert row["total_succeeded_across_chain"] == 10  # 8 + 2

    def test_chain_still_failing_when_leaf_failed(self, repo: SqliteMetadataRepository) -> None:
        """Leaf=FAILED → chain_outcome='still_failing' (manual
        intervention required; auto-retry exhausted)."""
        sid, _ = _seed_dataset(repo)
        root_id = _insert_master_run_log(repo, source_id=sid, started_at="x", status="FAILED")
        _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="FAILED",
            retry_of_master_run_id=root_id,
        )
        rows = _query_view(repo, "SELECT chain_outcome FROM v_retry_chains")
        assert rows[0]["chain_outcome"] == "still_failing"

    def test_chain_in_progress_when_leaf_running(self, repo: SqliteMetadataRepository) -> None:
        """Leaf=RUNNING → chain_outcome='in_progress'. Dashboards
        treat these as 'don't alert yet, watch for completion'."""
        sid, _ = _seed_dataset(repo)
        root_id = _insert_master_run_log(repo, source_id=sid, started_at="x", status="PARTIAL")
        _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="RUNNING",
            retry_of_master_run_id=root_id,
        )
        rows = _query_view(repo, "SELECT chain_outcome FROM v_retry_chains")
        assert rows[0]["chain_outcome"] == "in_progress"

    def test_three_attempt_chain_aggregates_correctly(self, repo: SqliteMetadataRepository) -> None:
        """Root → retry1 → retry2: chain_length=3, leaf is the last
        retry, aggregations sum across all three."""
        sid, _ = _seed_dataset(repo)
        root = _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="FAILED",
            total_attempted=5,
            succeeded=0,
            failed=5,
        )
        mid = _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="PARTIAL",
            total_attempted=5,
            succeeded=3,
            failed=2,
            retry_of_master_run_id=root,
        )
        leaf = _insert_master_run_log(
            repo,
            source_id=sid,
            started_at="x",
            status="SUCCESS",
            total_attempted=2,
            succeeded=2,
            failed=0,
            retry_of_master_run_id=mid,
        )
        rows = _query_view(
            repo,
            "SELECT chain_root_id, chain_length, final_master_run_id, "
            "total_attempts_across_chain FROM v_retry_chains",
        )
        assert len(rows) == 1
        assert rows[0]["chain_root_id"] == root
        assert rows[0]["chain_length"] == 3
        assert rows[0]["final_master_run_id"] == leaf
        assert rows[0]["total_attempts_across_chain"] == 12  # 5 + 5 + 2


def _insert_daily_success_run(
    repo: SqliteMetadataRepository,
    *,
    dataset_id: int,
    days_ago: int,
    rows: int,
    bytes_written: int = 0,
) -> int:
    """Insert a SUCCESS run with started_at = now - days_ago. Used by
    v_volume_drift tests to seed daily baseline data.
    """
    rid = _insert_run_log(repo, dataset_id=dataset_id, started_at="x", status="SUCCESS")
    repo._connect().execute(
        "UPDATE run_log SET rows_extracted=?, bytes_written=?, "
        "started_at = datetime('now', ?) WHERE run_id=?",
        (rows, bytes_written, f"-{days_ago} days", rid),
    )
    repo._connect().commit()
    return rid


class TestVVolumeDrift:
    """v_volume_drift — daily-volume deviation from 7-day baseline.
    See docs/operability.md.
    """

    def test_stable_volume_not_flagged(self, repo: SqliteMetadataRepository) -> None:
        """Daily 100 rows for 5 days, today 100 → no drift; row is
        filtered (severity='normal' → excluded by view)."""
        _, did = _seed_dataset(repo)
        # 5 baseline days
        for d in range(5, 0, -1):
            _insert_daily_success_run(repo, dataset_id=did, days_ago=d, rows=100)
        # Today (close to baseline)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=0, rows=100)
        rows = _query_view(repo, "SELECT dataset_id FROM v_volume_drift")
        assert rows == []

    def test_volume_spike_flagged_anomaly(self, repo: SqliteMetadataRepository) -> None:
        """Baseline ~100, today 200 → 100% delta → anomaly."""
        _, did = _seed_dataset(repo)
        for d in range(5, 0, -1):
            _insert_daily_success_run(repo, dataset_id=did, days_ago=d, rows=100)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=0, rows=200)
        rows = _query_view(
            repo,
            "SELECT drift_severity, rows_delta_pct, daily_rows, "
            "baseline_avg_rows FROM v_volume_drift",
        )
        assert len(rows) == 1
        assert rows[0]["drift_severity"] == "anomaly"
        assert rows[0]["daily_rows"] == 200
        assert rows[0]["baseline_avg_rows"] == pytest.approx(100.0, rel=1e-6)
        assert rows[0]["rows_delta_pct"] == pytest.approx(100.0, rel=1e-6)

    def test_volume_drop_flagged_anomaly(self, repo: SqliteMetadataRepository) -> None:
        """Baseline ~100, today 30 → -70% delta → anomaly. Negative
        delta is a more serious signal — typical cause is source
        outage. The signed delta_pct distinguishes spike from drop."""
        _, did = _seed_dataset(repo)
        for d in range(5, 0, -1):
            _insert_daily_success_run(repo, dataset_id=did, days_ago=d, rows=100)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=0, rows=30)
        rows = _query_view(
            repo,
            "SELECT drift_severity, rows_delta_pct FROM v_volume_drift",
        )
        assert len(rows) == 1
        assert rows[0]["drift_severity"] == "anomaly"
        assert rows[0]["rows_delta_pct"] == pytest.approx(-70.0, rel=1e-6)

    def test_mid_range_drift_flagged_warning(self, repo: SqliteMetadataRepository) -> None:
        """30% delta is between thresholds → warning, not anomaly."""
        _, did = _seed_dataset(repo)
        for d in range(5, 0, -1):
            _insert_daily_success_run(repo, dataset_id=did, days_ago=d, rows=100)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=0, rows=130)
        rows = _query_view(
            repo,
            "SELECT drift_severity, rows_delta_pct FROM v_volume_drift",
        )
        assert len(rows) == 1
        assert rows[0]["drift_severity"] == "warning"
        assert rows[0]["rows_delta_pct"] == pytest.approx(30.0, rel=1e-6)

    def test_insufficient_baseline_excluded(self, repo: SqliteMetadataRepository) -> None:
        """Only 2 baseline days, today is a huge spike — but no flag,
        because we can't tell drift from natural variance with < 3
        baseline samples."""
        _, did = _seed_dataset(repo)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=2, rows=100)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=1, rows=100)
        _insert_daily_success_run(repo, dataset_id=did, days_ago=0, rows=10000)
        rows = _query_view(repo, "SELECT dataset_id FROM v_volume_drift")
        assert rows == []


def _extract_select_columns(query: str) -> list[str]:
    """Tiny SELECT parser — pulls column names out of a single-clause
    SELECT for cross-checking against a view's projection.
    """
    upper = query.upper()
    sel_idx = upper.index("SELECT")
    from_idx = upper.index("FROM")
    cols_block = query[sel_idx + len("SELECT") : from_idx].strip()
    return [c.strip() for c in cols_block.split(",")]
