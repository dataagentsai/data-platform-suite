"""The key registry — the canonical list of valid keys per KV table.

Mirrors ``docs/metadata_keys.md``. CI should catch drift between the
doc and this file (test asserts every key in the doc has a row here).

Organized in three sections by KV table. Source-type-scoped keys are
grouped under ``SOURCE_CONFIG_KEYS`` with their ``source_type_scope``
tuple set. Common keys (no scope) come first.
"""

from __future__ import annotations

from lake_ingestion_framework.core.metadata.keys._definitions import (
    KeyDef,
    KeyTable,
    KeyType,
)
from lake_ingestion_framework.core.types.enums import SourceType

# ──────────────────────────────────────────────────────────────────────
# platform_config — global defaults
# ──────────────────────────────────────────────────────────────────────

PLATFORM_CONFIG_KEYS: list[KeyDef] = [
    KeyDef(
        name="default_realm",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("adf-adls",),
        description="Realm used when dataset_config.realm is not set.",
    ),
    KeyDef(
        name="default_file_format",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.ENUM,
        required=True,
        default="PARQUET",
        enum_values=("PARQUET", "DELTA", "JSON"),
        description="File format when dataset_config.file_format is not set.",
    ),
    KeyDef(
        name="default_compression",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.ENUM,
        default="snappy",
        enum_values=("snappy", "gzip", "zstd", "none"),
        description="Parquet compression codec.",
    ),
    KeyDef(
        name="target_storage_account",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        required=True,
        description="ADLS Gen2 account name.",
    ),
    KeyDef(
        name="target_container_default",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        required=True,
        default="lake",
        description="Default ADLS container.",
    ),
    KeyDef(
        name="key_vault_uri",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.URI,
        required=True,
        description="Vault for all secret references (https://<vault>.vault.azure.net/).",
    ),
    KeyDef(
        name="metadata_db_server",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description=(
            "FQDN of the Azure SQL DB hosting metadata for this env "
            "(e.g. 'mycustomer-dev-sql.database.windows.net'). Drives "
            "LS_METADATA_DB; required when ADF runtime needs to Lookup "
            "v_enabled_datasets / watermark / etc."
        ),
    ),
    KeyDef(
        name="metadata_db_database",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description="Database name within metadata_db_server (e.g. 'lake_ingest_meta').",
    ),
    KeyDef(
        name="runtime_environment",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.ENUM,
        default="prod",
        enum_values=("dev", "staging", "prod"),
        description="Tag written to run_log.runtime_environment.",
    ),
    KeyDef(
        name="default_schedule_cron",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.CRON,
        default="0 2 * * *",
        description="Used when dataset_config.schedule_cron is absent.",
    ),
    KeyDef(
        name="default_batch_count",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=10,
        description="Cross-dataset parallelism cap (ADF ForEach batchCount).",
    ),
    KeyDef(
        name="metadata_schema_version",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        required=True,
        description="Bumped on each breaking schema migration.",
    ),
    KeyDef(
        name="redaction_extra_patterns",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.JSON,
        description="Extra regex patterns appended to the runner's redaction list.",
    ),
    KeyDef(
        name="lifecycle_cool_days",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=30,
        description="Hot to Cool tier age threshold for ADLS lifecycle policy.",
    ),
    KeyDef(
        name="lifecycle_archive_days",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=180,
        description="Cool to Archive tier age threshold.",
    ),
    KeyDef(
        name="shir_name",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description="Default Self-Hosted IR name for *_onprem_* connection patterns.",
    ),
    KeyDef(
        name="staging_linked_service",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description="Linked service name for ADF Copy staged copies.",
    ),
    KeyDef(
        name="large_table_row_threshold",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=10_000_000,
        description="Row count at which auto-classifier marks a dataset large.",
    ),
    KeyDef(
        name="huge_table_row_threshold",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=1_000_000_000,
        description="Row count at which classifier escalates to huge.",
    ),
    KeyDef(
        name="auto_retry_on_partial",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Auto-trigger TR_RERUN_FAILED after a PARTIAL master run.",
    ),
    KeyDef(
        name="auto_retry_max_attempts",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=2,
        description="Cap on auto-retries before requiring manual intervention.",
    ),
    KeyDef(
        name="auto_retry_delay_minutes",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.INT,
        default=30,
        description="Delay between failed master run and the next auto-retry attempt.",
    ),
    KeyDef(
        name="flatten_function_url",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.URI,
        description="Azure Function endpoint that runs custom flattening (when used).",
    ),
    KeyDef(
        name="flatten_function_secret_name",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.KV_REF,
        description="When Function uses Function-Key auth instead of MSI.",
    ),
    KeyDef(
        name="flatten_databricks_workspace",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.URI,
        description="Databricks workspace URL for the flattening notebook.",
    ),
    KeyDef(
        name="flatten_databricks_cluster_id",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description="Cluster running the flattening notebook.",
    ),
    KeyDef(
        name="flatten_databricks_notebook_path",
        table=KeyTable.PLATFORM_CONFIG,
        type=KeyType.STRING,
        description="Workspace path to the framework's flatten notebook.",
    ),
    # ── Standard processors (Wave 1-4) ──
    # Group enable/disable per env. Modes: enabled / disabled / opt_in /
    # dataset_decides. See docs/standard_processors.md.
    *(
        KeyDef(
            name=f"processor_groups.{group}",
            table=KeyTable.PLATFORM_CONFIG,
            type=KeyType.ENUM,
            enum_values=("enabled", "disabled", "opt_in", "dataset_decides"),
            description=f"Env-wide enable/disable for the {group!r} processor group.",
        )
        for group in (
            "hygiene",
            "type_safety",
            "case_normalize",
            "timestamp_normalize",
            "standardization",
            "pii",
            "dedup",
            "provenance",
        )
    ),
]


# ──────────────────────────────────────────────────────────────────────
# source_config — per-source connection + behavior
# ──────────────────────────────────────────────────────────────────────

# Common keys (any source_type)
SOURCE_CONFIG_COMMON_KEYS: list[KeyDef] = [
    KeyDef(
        name="connection_pattern",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description="Dispatch axis for PL_INGESTOR_INVOKER (e.g. oracle.onprem_basic).",
    ),
    KeyDef(
        name="auth_method",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=(
            "basic",
            "connstr",
            "wallet",
            "oauth2_client_credentials",
            "bearer",
            "aws_access_key",
            "managed_identity",
            "app_only_secret",
            "app_only_cert",
            "delegated_oauth",
            "ssh_key",
            "key_pair",
            "service_account",
        ),
        description="How the source authenticates; drives KV-secret shape.",
    ),
    KeyDef(
        name="connection_secret_name",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.KV_REF,
        description="Required when auth_method != managed_identity. kv://<vault>/<secret>.",
        cross_key_dependency="auth_method",
    ),
    KeyDef(
        name="connection_user",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description=(
            "Database / system username for basic auth. When set, the secret "
            "pointed to by connection_secret_name is treated as the password "
            "alone (no 'user:' prefix). When absent, the secret must be in "
            "'user:password' form (legacy)."
        ),
        cross_key_dependency="auth_method",
    ),
    KeyDef(
        name="connect_timeout_seconds",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.INT,
        default=30,
        description="TCP / handshake timeout.",
    ),
    KeyDef(
        name="integration_runtime",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Per-source IR override (multi-SHIR shops).",
    ),
    KeyDef(
        name="max_concurrent_datasets",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.INT,
        description="Caps cross-dataset parallelism for this source.",
    ),
    KeyDef(
        name="max_concurrent_segments",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.INT,
        description="Caps parallel segments hitting one source.",
    ),
    KeyDef(
        name="rate_limit_rps",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.INT,
        description="For rest_api source types — caps requests/sec.",
    ),
    KeyDef(
        name="description",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Free-text label.",
    ),
]

# Per-source-type keys
_RDBMS_TYPES = (SourceType.AZURE_SQL_DB, SourceType.SQL_SERVER, SourceType.SYNAPSE_SQL_POOL)
_OAUTH_TYPES = (SourceType.REST_API, SourceType.SHAREPOINT, SourceType.DATAVERSE)

SOURCE_CONFIG_PER_TYPE_KEYS: list[KeyDef] = [
    # Azure SQL DB / SQL Server / Synapse — JDBC-shaped
    KeyDef(
        name="server",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description="Server FQDN (DB host or SFTP host).",
        source_type_scope=(
            *_RDBMS_TYPES,
            SourceType.ORACLE,
            SourceType.POSTGRES,
            SourceType.MYSQL,
            SourceType.SAP_HANA,
            SourceType.SFTP,
        ),
    ),
    KeyDef(
        name="database",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description="Database name.",
        source_type_scope=(
            *_RDBMS_TYPES,
            SourceType.POSTGRES,
            SourceType.MYSQL,
            SourceType.SNOWFLAKE,
        ),
    ),
    KeyDef(
        name="service_name",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "Oracle service name (Oracle uses service_name in lieu of "
            "database; SID is legacy and not supported)."
        ),
        source_type_scope=(SourceType.ORACLE,),
    ),
    KeyDef(
        name="schema_default",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Used when dataset_config.schema_name is absent.",
        source_type_scope=(
            *_RDBMS_TYPES,
            SourceType.POSTGRES,
            SourceType.MYSQL,
            SourceType.SAP_HANA,
        ),
    ),
    KeyDef(
        name="application_name",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        default="lake-ingest",
        description="Written to source DB session metadata for attribution.",
        source_type_scope=_RDBMS_TYPES,
    ),
    KeyDef(
        name="read_replica_hint",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Request the read replica (Azure SQL: ApplicationIntent=ReadOnly).",
        source_type_scope=_RDBMS_TYPES,
    ),
    # SAP HANA-specific
    KeyDef(
        name="sap_client",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="MANDT filter applied to multi-client HANA tables.",
        source_type_scope=(SourceType.SAP_HANA,),
    ),
    # Snowflake-specific
    KeyDef(
        name="account",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "Snowflake account identifier — e.g. 'myorg-myaccount' "
            "or 'xy12345.us-east-1'. Excludes the .snowflakecomputing.com "
            "suffix (ADF appends it)."
        ),
        source_type_scope=(SourceType.SNOWFLAKE,),
    ),
    KeyDef(
        name="warehouse",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description="Snowflake compute warehouse to run extraction queries on.",
        source_type_scope=(SourceType.SNOWFLAKE,),
    ),
    KeyDef(
        name="role",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description=(
            "Snowflake role to assume at session open. Optional; the user's "
            "default role is used when absent."
        ),
        source_type_scope=(SourceType.SNOWFLAKE,),
    ),
    # BigQuery-specific
    KeyDef(
        name="project_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "GCP project ID that owns the BigQuery datasets being read. "
            "BigQuery uses (project, dataset, table) as the 3-part fully "
            "qualified name; the framework's schema_name maps to BigQuery's "
            "dataset, table_name to BigQuery's table."
        ),
        source_type_scope=(SourceType.BIGQUERY,),
    ),
    # S3-specific
    KeyDef(
        name="access_key_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "AWS access key ID for the IAM user / role with read access "
            "to the S3 buckets. Pair with connection_secret_name (KV holds "
            "the secret access key)."
        ),
        source_type_scope=(SourceType.S3,),
    ),
    # Azure Blob (cross-tenant readable; AZURE_BLOB_X) — MSI auth in v1
    KeyDef(
        name="storage_account",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "Azure Storage account name (without .blob.core.windows.net). "
            "ADF MSI must have Storage Blob Data Reader on the account or "
            "specific container. SAS / account-key auth variants land in "
            "Phase B."
        ),
        source_type_scope=(SourceType.AZURE_BLOB_X,),
    ),
    # Salesforce-specific (uses ADF's SalesforceV2 connector +
    # OAuth 2.0 Client Credentials grant via a Connected App)
    KeyDef(
        name="environment_url",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        required=True,
        description=(
            "SaaS instance / environment URL. Salesforce: 'My Domain' URL "
            "(e.g. 'https://my-org.my.salesforce.com'). ServiceNow: instance "
            "URL (e.g. 'https://acme.service-now.com'). The ADF LS for each "
            "type maps this to its own connector field (Salesforce → "
            "environmentUrl, ServiceNow → endpoint)."
        ),
        source_type_scope=(SourceType.SALESFORCE, SourceType.SERVICENOW),
    ),
    KeyDef(
        name="client_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        required=True,
        description=(
            "Connected App's OAuth client ID (consumer key). The customer "
            "creates a Connected App in Salesforce Setup with the OAuth 2.0 "
            "Client Credentials grant enabled, paired with connection_secret_name "
            "(KV holds the client secret / consumer secret)."
        ),
        source_type_scope=(SourceType.SALESFORCE,),
    ),
    KeyDef(
        name="api_version",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        default="v59.0",
        description=(
            "Salesforce API version (e.g. 'v59.0'). Optional; defaults to "
            "a recent stable. Pin to a specific version to insulate against "
            "Salesforce's three-yearly release cadence."
        ),
        source_type_scope=(SourceType.SALESFORCE,),
    ),
    # REST API generic
    KeyDef(
        name="base_url",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        required=True,
        description="API base, no trailing slash.",
        source_type_scope=(SourceType.REST_API,),
    ),
    KeyDef(
        name="endpoints",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.JSON,
        required=True,
        description=(
            "JSON list of endpoint specs: "
            '[{"name": "<dataset_name>", "path": "/v1/customers"}, ...]. '
            "Each entry maps to one logical dataset under this source."
        ),
        source_type_scope=(SourceType.REST_API,),
    ),
    KeyDef(
        name="default_record_path",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description=(
            "Dotted path to the records array in JSON responses "
            "(e.g. 'data', 'items', 'results'). Empty string = root is the array."
        ),
        source_type_scope=(SourceType.REST_API,),
    ),
    # OAuth2 (REST + Dataverse + SharePoint flows that need it)
    KeyDef(
        name="client_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="OAuth2 / AAD app client ID.",
        source_type_scope=_OAUTH_TYPES,
    ),
    KeyDef(
        name="token_url",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        description="OAuth2 token endpoint.",
        source_type_scope=(SourceType.REST_API,),
    ),
    KeyDef(
        name="scope",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="OAuth2 scope.",
        source_type_scope=(SourceType.REST_API,),
    ),
    # Snowflake
    KeyDef(
        name="snowflake_warehouse",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Snowflake compute warehouse.",
        source_type_scope=(SourceType.SNOWFLAKE,),
    ),
    KeyDef(
        name="snowflake_role",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Snowflake role.",
        source_type_scope=(SourceType.SNOWFLAKE,),
    ),
    # Synapse
    KeyDef(
        name="synapse_pool_kind",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("dedicated", "serverless"),
        description="dedicated SQL Pool vs serverless query.",
        source_type_scope=(SourceType.SYNAPSE_SQL_POOL,),
    ),
    KeyDef(
        name="synapse_auto_resume",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.BOOL,
        default=True,
        description="Wake dedicated pool before query if paused.",
        source_type_scope=(SourceType.SYNAPSE_SQL_POOL,),
    ),
    # SharePoint / OneDrive (via Microsoft Graph)
    KeyDef(
        name="tenant_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Azure AD tenant.",
        source_type_scope=(SourceType.SHAREPOINT, SourceType.DATAVERSE),
    ),
    KeyDef(
        name="sharepoint_site_url",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        description="https://<tenant>.sharepoint.com/sites/<name>.",
        source_type_scope=(SourceType.SHAREPOINT,),
    ),
    KeyDef(
        name="sharepoint_drive_id",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="Specific drive; default is site Documents library.",
        source_type_scope=(SourceType.SHAREPOINT,),
    ),
    KeyDef(
        name="sharepoint_user_principal_name",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.STRING,
        description="When targeting OneDrive instead of a site.",
        source_type_scope=(SourceType.SHAREPOINT,),
    ),
    KeyDef(
        name="graph_api_base",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        default="https://graph.microsoft.com/v1.0/",
        description="Microsoft Graph base URL.",
        source_type_scope=(SourceType.SHAREPOINT, SourceType.DATAVERSE),
    ),
    # Dataverse
    KeyDef(
        name="dataverse_environment_url",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.URI,
        description="https://<env>.crm.dynamics.com/.",
        source_type_scope=(SourceType.DATAVERSE,),
    ),
    KeyDef(
        name="dataverse_access_mode",
        table=KeyTable.SOURCE_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("web_api", "tds"),
        description="Web API (OData) or TDS endpoint.",
        source_type_scope=(SourceType.DATAVERSE,),
    ),
]

SOURCE_CONFIG_KEYS = SOURCE_CONFIG_COMMON_KEYS + SOURCE_CONFIG_PER_TYPE_KEYS


# ──────────────────────────────────────────────────────────────────────
# dataset_config — per-dataset ingest behavior
# ──────────────────────────────────────────────────────────────────────

DATASET_CONFIG_KEYS: list[KeyDef] = [
    # Required (any dataset)
    KeyDef(
        name="realm",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("adf-adls",),
        description="Dispatch realm.",
    ),
    KeyDef(
        name="dataset_type",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("TABLE", "VIEW", "QUERY", "DOCUMENT"),
        description="Source shape.",
    ),
    KeyDef(
        name="load_type",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        required=True,
        enum_values=("FULL", "INCREMENTAL"),
        description="Load mode.",
    ),
    KeyDef(
        name="target_path_template",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.PATH_TEMPLATE,
        required=True,
        description=(
            "Tokens: {source_env}, {source}, {schema}, {object}, "
            "{yyyy}, {mm}, {dd}, {hh}, {segment}."
        ),
    ),
    # Source shape
    KeyDef(
        name="schema_name",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Source schema; required for TABLE/VIEW.",
    ),
    KeyDef(
        name="object_name",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Source object name; required for TABLE/VIEW.",
    ),
    KeyDef(
        name="custom_query",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Single-statement SQL; required for QUERY datasets.",
    ),
    # Incremental
    KeyDef(
        name="watermark_column",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Required when load_type=INCREMENTAL.",
        cross_key_dependency="load_type",
    ),
    KeyDef(
        name="watermark_initial_value",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Used when meta.watermark row is missing.",
    ),
    KeyDef(
        name="watermark_expression",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Derived watermark expression for SAP HANA-style sources.",
    ),
    # Target
    KeyDef(
        name="target_container",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="ADLS container name; falls back to platform default.",
    ),
    KeyDef(
        name="file_format",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("PARQUET", "DELTA", "JSON"),
        description="File format; falls back to platform default.",
    ),
    KeyDef(
        name="compression",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("snappy", "gzip", "zstd", "none"),
        description="Compression codec.",
    ),
    KeyDef(
        name="parquet_row_group_mb",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=128,
        description="Parquet row-group size in MB.",
    ),
    # Schedule
    KeyDef(
        name="schedule_cron",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.CRON,
        description="Groups dataset into a driver pipeline. Falls back to platform default.",
    ),
    KeyDef(
        name="schedule_timezone",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default="UTC",
        description="IANA timezone for the cron expression.",
    ),
    # Performance hints
    KeyDef(
        name="partition_column",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Enables ADF DynamicRange partitioned source read.",
    ),
    KeyDef(
        name="partition_lower",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Inclusive lower bound for partitioned read.",
    ),
    KeyDef(
        name="partition_upper",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Inclusive upper bound for partitioned read.",
    ),
    KeyDef(
        name="partition_count",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=4,
        description="Number of source-side partitions.",
    ),
    KeyDef(
        name="parallel_copies",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=4,
        description="Copy Activity parallelCopies.",
    ),
    KeyDef(
        name="intended_dius",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="auto",
        enum_values=("auto", "4", "8", "16", "32"),
        description="ADF Copy DataIntegrationUnits.",
    ),
    KeyDef(
        name="enable_staging",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Enables ADF staged copy.",
    ),
    KeyDef(
        name="sink_max_concurrent_connections",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=4,
        description="Copy Activity sink-side connection pool size.",
    ),
    # Timeouts and retries
    KeyDef(
        name="query_timeout_seconds",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=7200,
        description="Source queryTimeout — how long the source DB has to return rows.",
    ),
    KeyDef(
        name="timeout_seconds",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=14400,
        description="Copy Activity policy.timeout — wall-clock cap.",
    ),
    KeyDef(
        name="retry_count",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=2,
        description="Copy Activity policy.retry — transient-failure retries.",
    ),
    KeyDef(
        name="retry_interval_seconds",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=60,
        description="Copy Activity policy.retryIntervalInSeconds.",
    ),
    # Cost / lifecycle
    KeyDef(
        name="lifecycle_cool_days",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="Per-dataset override of platform_config.lifecycle_cool_days.",
    ),
    KeyDef(
        name="lifecycle_archive_days",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="Per-dataset override.",
    ),
    KeyDef(
        name="lifecycle_delete_days",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=0,
        description="When > 0, ADLS lifecycle deletes blobs of this age.",
    ),
    KeyDef(
        name="cost_center",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Passed through to Azure resource tags.",
    ),
    # Segmented loads
    KeyDef(
        name="load_profile",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="auto",
        enum_values=("small", "large", "huge", "auto"),
        description="Classifier mode.",
    ),
    KeyDef(
        name="estimated_row_count",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="From --discover or hand-set; drives auto-classification.",
    ),
    KeyDef(
        name="estimated_size_bytes",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="Optional refinement of classifier.",
    ),
    KeyDef(
        name="segmentation_strategy",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=(
            "none",
            "range_numeric",
            "range_temporal",
            "modulo",
            "dynamic_range",
            "file_per_segment",
        ),
        description="Required for large/huge datasets.",
    ),
    KeyDef(
        name="segmentation_column",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Required for range_*/modulo strategies.",
    ),
    KeyDef(
        name="segment_count",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=8,
        description="Target segment count.",
    ),
    KeyDef(
        name="segment_size_rows",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="Alternative to segment_count.",
    ),
    KeyDef(
        name="resegment_on_run",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Recompute segment bounds every run.",
    ),
    KeyDef(
        name="load_consistency_mode",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="segmented_eventual",
        enum_values=("single_query", "segmented_pit", "segmented_eventual"),
        description="Snapshot guarantee.",
    ),
    KeyDef(
        name="allow_dirty_reads",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="SQL Server only — WITH (NOLOCK) opt-in for FULL loads.",
    ),
    # Flattening
    KeyDef(
        name="flattening_mode",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("preserve", "stringify", "flatten_explicit"),
        description="Per-source-type default; overridable per dataset.",
    ),
    KeyDef(
        name="flattening_engine",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="auto",
        enum_values=("auto", "adf", "function", "databricks"),
        description="Where flattening runs.",
    ),
    KeyDef(
        name="flattening_path_separator",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="_",
        enum_values=("_", ".", ":"),
        description="Path joiner for flattened column names.",
    ),
    KeyDef(
        name="preserve_raw_record",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=True,
        description="Write _raw column for flatten_explicit datasets.",
    ),
    KeyDef(
        name="array_strategy",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("preserve", "stringify", "explode", "take_first_only"),
        description="How arrays are handled when flattening.",
    ),
    KeyDef(
        name="primary_array_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Required when array_strategy=explode.",
    ),
    KeyDef(
        name="array_max_explode_rows",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=1000,
        description="Per-record cap on explode.",
    ),
    # File-source-specific (when source_type is a file substrate)
    KeyDef(
        name="file_format_data",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=(
            "csv",
            "tsv",
            "pipe",
            "fixed_width",
            "json_lines",
            "json_object",
            "xml",
            "excel",
            "parquet",
            "avro",
        ),
        description="Source file format (distinct from sink file_format).",
    ),
    KeyDef(
        name="file_pattern",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Glob pattern for which files to ingest.",
    ),
    KeyDef(
        name="file_encoding",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="utf-8",
        enum_values=("utf-8", "utf-8-bom", "cp1252", "latin-1", "utf-16"),
        description="Source file encoding.",
    ),
    KeyDef(
        name="csv_has_header",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=True,
        description="CSV header row drives column names.",
    ),
    KeyDef(
        name="csv_delimiter",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default=",",
        description="CSV column delimiter (1 char).",
    ),
    KeyDef(
        name="csv_quote_char",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default='"',
        description="CSV quote character (1 char).",
    ),
    KeyDef(
        name="csv_escape_char",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default='"',
        description="CSV escape character (1 char).",
    ),
    KeyDef(
        name="csv_infer_types",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=True,
        description="When false, all CSV columns are STRING.",
    ),
    KeyDef(
        name="csv_skip_rows",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=0,
        description="Skip banner / footer rows.",
    ),
    KeyDef(
        name="excel_sheet_name",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="For Excel format; default is first sheet.",
    ),
    KeyDef(
        name="excel_header_row",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=1,
        description="1-indexed row number of header.",
    ),
    KeyDef(
        name="xml_record_xpath",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="XPath to record-level node; required for XML.",
    ),
    # DOCUMENT-specific
    KeyDef(
        name="document_extract_text",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Run text extraction at ingest.",
    ),
    KeyDef(
        name="document_text_extractor",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("pdfminer", "tika", "pandoc", "plain"),
        description="Required when document_extract_text=true.",
    ),
    KeyDef(
        name="document_offload_threshold_bytes",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=268_435_456,
        description="Files larger have content_bytes=NULL and offload.",
    ),
    KeyDef(
        name="document_max_size_bytes",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=1_073_741_824,
        description="Files larger are skipped + warned.",
    ),
    KeyDef(
        name="document_mime_filter",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default="*",
        description="Comma-separated allow-list (application/pdf,text/html).",
    ),
    # REST-API-specific
    KeyDef(
        name="rest_endpoint_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Path under source_config.base_url.",
    ),
    KeyDef(
        name="rest_record_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "Per-dataset override of source_config.default_record_path "
            "(dotted path to the records array in the JSON response)."
        ),
    ),
    KeyDef(
        name="rest_method",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="GET",
        enum_values=("GET", "POST"),
        description="HTTP method.",
    ),
    KeyDef(
        name="pagination_style",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("cursor", "offset", "link_header", "none"),
        description="Pagination mechanism.",
    ),
    KeyDef(
        name="pagination_cursor_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="JSONPath into response for cursor-style pagination.",
    ),
    KeyDef(
        name="pagination_cursor_param",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Request param name for the cursor.",
    ),
    KeyDef(
        name="pagination_page_param",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Request param name for offset-style.",
    ),
    KeyDef(
        name="pagination_size_param",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Request param name for page size.",
    ),
    KeyDef(
        name="pagination_size_default",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        default=100,
        description="Default page size.",
    ),
    KeyDef(
        name="response_data_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default="$",
        description="JSONPath to records array in response.",
    ),
    KeyDef(
        name="watermark_param",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Query param for server-side watermark filter.",
    ),
    KeyDef(
        name="watermark_format",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        default="iso8601",
        enum_values=("iso8601", "unix_seconds", "unix_millis", "yyyymmdd"),
        description="Watermark serialization format for the URL.",
    ),
    # Salesforce
    KeyDef(
        name="salesforce_object",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="SObject API name (e.g. Account).",
    ),
    KeyDef(
        name="salesforce_fields",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Comma-separated field list; overrides discovery.",
    ),
    KeyDef(
        name="exclude_deleted",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Filter IsDeleted=false (Salesforce).",
    ),
    KeyDef(
        name="bulk_api_version",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        default="v2",
        description="Salesforce Bulk API version.",
    ),
    # Snowflake
    KeyDef(
        name="extraction_mode",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("staged", "jdbc"),
        description="staged via COPY INTO/EXPORT DATA, or direct JDBC SELECT.",
    ),
    # BigQuery
    KeyDef(
        name="bigquery_project",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Source BigQuery project ID.",
    ),
    KeyDef(
        name="bigquery_dataset",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Source BigQuery dataset (BQ schema).",
    ),
    KeyDef(
        name="bigquery_partition_column",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.IDENTIFIER,
        description="Partition column for predicate pushdown.",
    ),
    # Dataverse
    KeyDef(
        name="dataverse_entity_set",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Entity set name (plural form, e.g. accounts).",
    ),
    KeyDef(
        name="dataverse_select",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="OData $select field list.",
    ),
    KeyDef(
        name="dataverse_filter",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Additional $filter expression.",
    ),
    KeyDef(
        name="dataverse_expand",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="OData $expand for related entities.",
    ),
    # SharePoint
    KeyDef(
        name="sharepoint_path_prefix",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Sub-path under the drive root.",
    ),
    # Re-ingestion runtime overrides (set by CLI, not customer YAML)
    KeyDef(
        name="target_load_date_override",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Runtime: set by reingest/backfill CLI; cleared after run.",
    ),
    KeyDef(
        name="reingest_strategy",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=("single-partition", "per-day-partition"),
        description="Runtime: set by backfill CLI.",
    ),
    KeyDef(
        name="reingest_data_window_from",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Runtime: backfill window start.",
    ),
    KeyDef(
        name="reingest_data_window_to",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Runtime: backfill window end.",
    ),
    # Operational flags
    KeyDef(
        name="enabled_overrides_master",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.BOOL,
        default=False,
        description="Rare; if true, dataset_master.is_enabled=0 is ignored.",
    ),
    KeyDef(
        name="notes",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Free-text; surfaced in deploy summary.",
    ),
    KeyDef(
        name="sla_freshness_hours",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.INT,
        description="Per-dataset SLA for the freshness alert.",
    ),
    # File-source dataset bits (SFTP / S3 / Azure Blob / HTTPS pull / files)
    KeyDef(
        name="file_name",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "Single source file name (SFTP file under sftp_folder_path, "
            "S3 object under s3_bucket, or Azure Blob blob under "
            "azure_blob_container). Wildcard / glob support is deferred."
        ),
        source_type_scope=(
            SourceType.SFTP,
            SourceType.S3,
            SourceType.AZURE_BLOB_X,
        ),
    ),
    KeyDef(
        name="sftp_folder_path",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "Source folder path for file-substrate datasets — SFTP "
            "folder, S3 prefix, or Azure Blob virtual path within the "
            "container. Distinct from target_path_template (the ADLS "
            "sink folder). Key name is a misnomer beyond SFTP; future "
            "cleanup will rename to source_folder_path."
        ),
        source_type_scope=(
            SourceType.SFTP,
            SourceType.S3,
            SourceType.AZURE_BLOB_X,
        ),
    ),
    KeyDef(
        name="s3_bucket",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "S3 bucket name (without s3:// prefix) for this dataset. One "
            "S3 source can read from multiple buckets, so bucket is per-"
            "dataset rather than per-source."
        ),
        source_type_scope=(SourceType.S3,),
    ),
    KeyDef(
        name="azure_blob_container",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "Azure Blob container name for this dataset. One Azure Blob "
            "source can read from multiple containers in the same "
            "storage account, so container is per-dataset rather than "
            "per-source. Reuses the view's `bucket` column alongside "
            "s3_bucket."
        ),
        source_type_scope=(SourceType.AZURE_BLOB_X,),
    ),
    # REST-source dataset bits
    KeyDef(
        name="relative_url",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description=(
            "REST endpoint path appended to source_config.base_url " "(e.g. /v3/objects/contacts)."
        ),
        source_type_scope=(SourceType.REST_API,),
    ),
    # ── Standard processors (Wave 1-4) — per-dataset overrides ──
    *(
        KeyDef(
            name=f"processor_groups.{group}",
            table=KeyTable.DATASET_CONFIG,
            type=KeyType.ENUM,
            enum_values=("enabled", "disabled", "opt_in", "dataset_decides"),
            description=f"Per-dataset override for the {group!r} processor group.",
        )
        for group in (
            "hygiene",
            "type_safety",
            "case_normalize",
            "timestamp_normalize",
            "standardization",
            "pii",
            "dedup",
            "provenance",
        )
    ),
    # Wave 3 — row-level dedup config (per dataset)
    KeyDef(
        name="dedup.mode",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.ENUM,
        enum_values=(
            "none",
            "exact",
            "business_key_first",
            "business_key_latest",
            "flag",
        ),
        description="Row-level dedup mode applied before _record_hash recompute.",
    ),
    KeyDef(
        name="dedup.business_keys",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Comma-separated business-key columns for dedup (when mode "
        "is business_key_first / business_key_latest / flag).",
    ),
    KeyDef(
        name="dedup.order_by",
        table=KeyTable.DATASET_CONFIG,
        type=KeyType.STRING,
        description="Column used to resolve 'latest' for " "dedup.mode=business_key_latest.",
    ),
]


# Flat list — convenient for tests, doc generation, CLI
ALL_KEYS: list[KeyDef] = PLATFORM_CONFIG_KEYS + SOURCE_CONFIG_KEYS + DATASET_CONFIG_KEYS
