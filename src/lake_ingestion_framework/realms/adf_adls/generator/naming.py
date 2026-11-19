"""ADF resource naming conventions.

Centralised so per-resource builders never hand-roll name strings.

Rules:

- Use uppercase prefixes that match ADF community convention:
  ``LS_`` (LinkedService), ``DS_`` (Dataset), ``PL_`` (Pipeline),
  ``TR_`` (Trigger).
- Resources are **per source-type**, not per source. LinkedServices
  carry connection details as ADF parameters; the Per-Source Driver
  pipeline reads per-source config from ``meta.v_source_config`` at
  run time and forwards values into the parameterised LS.
- SFTP / REST split per **auth method** because ADF's
  ``authenticationType`` field is not parameterisable — Basic vs
  SSH-Key SFTP need separate LSs.
- Identifiers are validated as ``Identifier`` shape upstream — these
  helpers don't sanitise (they assume input is already clean).
- Names are case-preserved; ADF accepts the casing we hand it.

Adding a new source of an *existing* source type is a metadata-only
change (no deploy). Adding a new source type adds one LS, one DS,
one ingest pipeline, one per-source-driver pipeline, and one Switch
case in the Master Driver.
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────
# LinkedServices
# ──────────────────────────────────────────────────────────────────────


def linked_service_azure_sql_db() -> str:
    """LinkedService for Azure SQL DB sources (MSI auth, parameterised)."""
    return "LS_AZURE_SQL_DB"


def linked_service_postgres() -> str:
    """LinkedService for Postgres sources (basic auth, parameterised)."""
    return "LS_POSTGRES"


def linked_service_mysql() -> str:
    """LinkedService for MySQL sources (basic auth, parameterised)."""
    return "LS_MYSQL"


def linked_service_oracle() -> str:
    """LinkedService for Oracle sources (basic auth, parameterised)."""
    return "LS_ORACLE"


def linked_service_sql_server() -> str:
    """LinkedService for SQL Server sources (SQL-auth, parameterised).

    Covers on-prem SQL Server, SQL Server in Azure VMs, and SQL MI.
    Customers needing Self-Hosted Integration Runtime for on-prem
    boxes edit ``connectVia`` post-deploy (the LS ships with default
    AutoResolveIntegrationRuntime).
    """
    return "LS_SQL_SERVER"


def linked_service_snowflake_basic() -> str:
    """LinkedService for Snowflake sources using user/password (parameterised).

    Snowflake announced password auth is being phased out as the
    default; ship key-pair (below) when possible. Basic stays for
    legacy accounts that still allow it.
    """
    return "LS_SNOWFLAKE_BASIC"


def linked_service_snowflake_key_pair() -> str:
    """LinkedService for Snowflake sources using key-pair auth (parameterised).

    The KV secret carries the PEM-encoded RSA private key. Snowflake's
    recommended path; pair it with ``ALTER USER ... SET RSA_PUBLIC_KEY``
    on the Snowflake side.
    """
    return "LS_SNOWFLAKE_KEY_PAIR"


def linked_service_bigquery() -> str:
    """LinkedService for BigQuery sources (service-account auth).

    Uses ADF's ``GoogleBigQueryV2`` connector. The KV secret holds the
    full service-account JSON key file content; ADF reads it as
    ``keyFileContent`` at activity time.
    """
    return "LS_BIGQUERY"


def linked_service_synapse_sql_pool() -> str:
    """LinkedService for Synapse Dedicated SQL Pool sources (MSI auth).

    Uses ADF's ``AzureSqlDW`` connector — distinct from
    ``AzureSqlDatabase`` (Azure SQL DB) and ``SqlServer``. Phase A
    ships MSI auth only; SQL-auth variant comes later if customers
    need it.
    """
    return "LS_SYNAPSE_SQL_POOL"


def linked_service_s3() -> str:
    """LinkedService for AWS S3 sources (access-key auth, parameterised).

    Uses ADF's ``AmazonS3`` connector. Parameters: ``p_access_key_id``,
    ``p_secret_access_key_secret``. The KV secret holds the AWS secret
    access key (paired with the access_key_id from source_config).
    """
    return "LS_S3"


def linked_service_azure_blob() -> str:
    """LinkedService for Azure Blob sources (MSI auth, parameterised).

    Uses ADF's ``AzureBlobStorage`` connector. Parameter:
    ``p_storage_account``. The ADF MSI must hold ``Storage Blob Data
    Reader`` on the account (or specific containers). SAS / account-key
    auth variants land in Phase B.
    """
    return "LS_AZURE_BLOB"


def linked_service_servicenow() -> str:
    """LinkedService for ServiceNow sources (basic auth, parameterised).

    Uses ADF's legacy ``ServiceNow`` connector with HTTP Basic auth —
    a service-account user with REST API access on the target instance.
    OAuth 2.0 (via the newer ``ServiceNowV2`` connector) lands in
    Phase B for orgs that mandate OAuth instead of password auth.
    """
    return "LS_SERVICENOW"


def linked_service_sap_hana() -> str:
    """LinkedService for SAP HANA sources (basic auth, parameterised).

    Uses ADF's ``SapHana`` connector — works against SAP HANA Cloud,
    on-prem HANA, and HANA Express. Customers running on-prem HANA
    bind a Self-Hosted Integration Runtime via ``connectVia`` post-
    deploy. Phase A ships username/password (Basic) auth; Windows /
    Kerberos auth is a Phase B addition.
    """
    return "LS_SAP_HANA"


def linked_service_salesforce() -> str:
    """LinkedService for Salesforce sources (OAuth 2.0 Client Credentials).

    Uses ADF's ``SalesforceV2`` connector — Salesforce's recommended
    auth path now that username + password is being deprecated.
    Customers create a Connected App with OAuth 2.0 Client Credentials
    grant enabled and supply (clientId, clientSecret) via metadata.
    JWT bearer flow is a Phase B addition for orgs that require it.
    """
    return "LS_SALESFORCE"


def linked_service_sftp_basic() -> str:
    """LinkedService for SFTP sources using password auth (parameterised)."""
    return "LS_SFTP_BASIC"


def linked_service_sftp_ssh_key() -> str:
    """LinkedService for SFTP sources using SSH-key auth (parameterised)."""
    return "LS_SFTP_SSH_KEY"


def linked_service_rest_api_anonymous() -> str:
    """LinkedService for REST API sources with no/bearer auth (parameterised).

    Bearer-token headers are injected at activity time via the dataset's
    ``additionalHeaders`` — the LS itself stays Anonymous.
    """
    return "LS_REST_API_ANONYMOUS"


def linked_service_rest_api_basic() -> str:
    """LinkedService for REST API sources using HTTP basic auth (parameterised)."""
    return "LS_REST_API_BASIC"


def linked_service_metadata_db() -> str:
    """LinkedService name for the env's metadata Azure SQL DB.

    Used by driver pipelines for ``Lookup`` activities against
    ``meta.v_enabled_datasets`` / ``meta.v_source_config`` / friends.
    One per env.
    """
    return "LS_METADATA_DB"


def linked_service_adls_gen2() -> str:
    """LinkedService name for the env's ADLS Gen2 sink."""
    return "LS_ADLS_GEN2"


def linked_service_key_vault() -> str:
    """LinkedService name for the env's Key Vault (credential resolution)."""
    return "LS_KEY_VAULT"


# ──────────────────────────────────────────────────────────────────────
# Datasets
# ──────────────────────────────────────────────────────────────────────


def dataset_azure_sql_db_table() -> str:
    """One generic dataset; parameterised over schema + table + LS conn."""
    return "DS_AZURE_SQL_DB_TABLE"


def dataset_postgres_table() -> str:
    """Generic Postgres dataset; parameterised over schema + table + LS conn."""
    return "DS_POSTGRES_TABLE"


def dataset_mysql_table() -> str:
    """Generic MySQL dataset; parameterised over schema + table + LS conn."""
    return "DS_MYSQL_TABLE"


def dataset_oracle_table() -> str:
    """Generic Oracle dataset; parameterised over schema + table + LS conn.

    Oracle's "schema" is the owning user; the dataset still calls it
    ``schema_name`` for consistency across RDBMS connectors.
    """
    return "DS_ORACLE_TABLE"


def dataset_sql_server_table() -> str:
    """Generic SQL Server table dataset; parameterised over schema + table + LS conn."""
    return "DS_SQL_SERVER_TABLE"


def dataset_snowflake_table() -> str:
    """Generic Snowflake table dataset; parameterised over schema + table + LS conn."""
    return "DS_SNOWFLAKE_TABLE"


def dataset_bigquery_table() -> str:
    """Generic BigQuery table dataset; parameterised over BQ dataset + table + LS conn.

    BigQuery's "dataset" is a logical namespace under a project — it
    occupies the ``schema_name`` slot in the framework's parameter
    convention. ``table_name`` is the BQ table.
    """
    return "DS_BIGQUERY_TABLE"


def dataset_synapse_sql_pool_table() -> str:
    """Generic Synapse SQL Pool table dataset; parameterised over schema + table + LS conn."""
    return "DS_SYNAPSE_SQL_POOL_TABLE"


def dataset_s3_csv() -> str:
    """Generic S3 CSV dataset; parameterised over bucket + folder + filename + LS conn."""
    return "DS_S3_CSV"


def dataset_s3_json_lines() -> str:
    """Generic S3 JSON Lines dataset; parameterised over bucket + folder + filename + LS conn."""
    return "DS_S3_JSON_LINES"


def dataset_azure_blob_csv() -> str:
    """Generic Azure Blob CSV dataset; container + folder + filename + LS conn parameters."""
    return "DS_AZURE_BLOB_CSV"


def dataset_servicenow_object() -> str:
    """Generic ServiceNow object dataset; parameterised over LS conn + tableName.

    The framework's ``table_name`` parameter slot carries the ServiceNow
    table name (e.g. ``incident``, ``problem``, ``cmdb_ci``). One dataset
    covers any ServiceNow table.
    """
    return "DS_SERVICENOW_OBJECT"


def dataset_sap_hana_table() -> str:
    """Generic SAP HANA table dataset; parameterised over schema + table + LS conn.

    SAP HANA's "schema" maps directly to the framework's ``schema_name``;
    ``table_name`` is the HANA table or view. ADF's ``SapHanaTable``
    dataset accepts a SQL ``query`` at activity time, so the
    typeProperties stay minimal — the Copy Activity's ``SapHanaSource``
    carries the SELECT.
    """
    return "DS_SAP_HANA_TABLE"


def dataset_salesforce_object() -> str:
    """Generic Salesforce SObject dataset; parameterised over object_name + LS conn.

    The ``object_name`` parameter is the SObject API name (e.g. 'Account',
    'Contact', 'Opportunity__c'). One dataset covers any SObject.
    """
    return "DS_SALESFORCE_OBJECT"


def dataset_azure_blob_json_lines() -> str:
    """Generic Azure Blob JSON Lines dataset; container + folder + filename + LS conn."""
    return "DS_AZURE_BLOB_JSON_LINES"


def dataset_sftp_csv() -> str:
    """Generic SFTP CSV dataset; parameterised over folder + filename + LS conn.

    Bound to ``LS_SFTP_BASIC`` by default; the SSH-key variant rebinds
    per-source via the LS parameters supplied by the Per-Source Driver.
    """
    return "DS_SFTP_CSV"


def dataset_sftp_json_lines() -> str:
    """Generic SFTP JSON Lines dataset; parameterised over folder + filename + LS conn."""
    return "DS_SFTP_JSON_LINES"


def dataset_rest_resource() -> str:
    """Generic REST resource dataset; parameterised over relative URL + LS conn."""
    return "DS_REST_RESOURCE"


def dataset_metadata_db_query() -> str:
    """Generic dataset bound to ``LS_METADATA_DB`` for Lookup activities.

    Carries no parameters — the Lookup carries the SQL.
    """
    return "DS_METADATA_DB_QUERY"


def dataset_adls_parquet_sink() -> str:
    """One generic dataset; parameterised over container + folder_path."""
    return "DS_ADLS_PARQUET_SINK"


# ──────────────────────────────────────────────────────────────────────
# Pipelines
# ──────────────────────────────────────────────────────────────────────


def pipeline_ingest_azure_sql_db() -> str:
    """Per-source-type ingest pipeline for Azure SQL DB."""
    return "PL_INGEST_AZURE_SQL_DB"


def pipeline_ingest_postgres() -> str:
    """Per-source-type ingest pipeline for Postgres."""
    return "PL_INGEST_POSTGRES"


def pipeline_ingest_mysql() -> str:
    """Per-source-type ingest pipeline for MySQL."""
    return "PL_INGEST_MYSQL"


def pipeline_ingest_oracle() -> str:
    """Per-source-type ingest pipeline for Oracle."""
    return "PL_INGEST_ORACLE"


def pipeline_ingest_sql_server() -> str:
    """Per-source-type ingest pipeline for SQL Server."""
    return "PL_INGEST_SQL_SERVER"


def pipeline_ingest_snowflake() -> str:
    """Per-source-type ingest pipeline for Snowflake."""
    return "PL_INGEST_SNOWFLAKE"


def pipeline_ingest_bigquery() -> str:
    """Per-source-type ingest pipeline for BigQuery."""
    return "PL_INGEST_BIGQUERY"


def pipeline_ingest_synapse_sql_pool() -> str:
    """Per-source-type ingest pipeline for Synapse Dedicated SQL Pool."""
    return "PL_INGEST_SYNAPSE_SQL_POOL"


def pipeline_ingest_s3_csv() -> str:
    """Per-source-type ingest pipeline for S3 CSV objects."""
    return "PL_INGEST_S3_CSV"


def pipeline_ingest_s3_json_lines() -> str:
    """Per-source-type ingest pipeline for S3 JSON Lines objects."""
    return "PL_INGEST_S3_JSON_LINES"


def pipeline_ingest_sap_hana() -> str:
    """Per-source-type ingest pipeline for SAP HANA."""
    return "PL_INGEST_SAP_HANA"


def pipeline_ingest_servicenow() -> str:
    """Per-source-type ingest pipeline for ServiceNow tables."""
    return "PL_INGEST_SERVICENOW"


def pipeline_ingest_salesforce() -> str:
    """Per-source-type ingest pipeline for Salesforce SObjects."""
    return "PL_INGEST_SALESFORCE"


def pipeline_ingest_azure_blob_csv() -> str:
    """Per-source-type ingest pipeline for Azure Blob CSV objects."""
    return "PL_INGEST_AZURE_BLOB_CSV"


def pipeline_ingest_azure_blob_json_lines() -> str:
    """Per-source-type ingest pipeline for Azure Blob JSON Lines objects."""
    return "PL_INGEST_AZURE_BLOB_JSON_LINES"


def pipeline_ingest_sftp_csv() -> str:
    """Per-source-type ingest pipeline for SFTP CSV files."""
    return "PL_INGEST_SFTP_CSV"


def pipeline_ingest_sftp_json_lines() -> str:
    """Per-source-type ingest pipeline for SFTP JSON Lines files."""
    return "PL_INGEST_SFTP_JSON_LINES"


def pipeline_ingest_rest_api() -> str:
    """Per-source-type ingest pipeline for REST API endpoints."""
    return "PL_INGEST_REST_API"


def pipeline_per_source_driver(source_type: str) -> str:
    """Per-source-type orchestration pipeline.

    Takes ``source_id`` as parameter; reads connection config from
    ``meta.v_source_config`` (Lookup) and dataset list from
    ``meta.v_enabled_datasets``, then iterates datasets via ForEach.
    One per source type that is configured in the customer's metadata.
    """
    return f"PL_PER_SOURCE_DRIVER_{source_type.upper()}"


def pipeline_master_driver() -> str:
    """Top-level master driver — fans out to every per-type driver.

    Fired by ``TR_DAILY``. One per env. Reads enabled sources from
    ``meta.v_enabled_sources`` at run time and dispatches by
    ``source_type`` via a Switch activity inside ForEach.
    """
    return "PL_MASTER_DRIVER"


# ──────────────────────────────────────────────────────────────────────
# Triggers
# ──────────────────────────────────────────────────────────────────────


def trigger_daily() -> str:
    """The default daily trigger that fires the master driver."""
    return "TR_DAILY"


__all__ = [
    "dataset_adls_parquet_sink",
    "dataset_azure_blob_csv",
    "dataset_azure_blob_json_lines",
    "dataset_azure_sql_db_table",
    "dataset_bigquery_table",
    "dataset_metadata_db_query",
    "dataset_mysql_table",
    "dataset_oracle_table",
    "dataset_postgres_table",
    "dataset_rest_resource",
    "dataset_s3_csv",
    "dataset_s3_json_lines",
    "dataset_salesforce_object",
    "dataset_sap_hana_table",
    "dataset_servicenow_object",
    "dataset_sftp_csv",
    "dataset_sftp_json_lines",
    "dataset_snowflake_table",
    "dataset_sql_server_table",
    "dataset_synapse_sql_pool_table",
    "linked_service_adls_gen2",
    "linked_service_azure_blob",
    "linked_service_azure_sql_db",
    "linked_service_bigquery",
    "linked_service_key_vault",
    "linked_service_metadata_db",
    "linked_service_mysql",
    "linked_service_oracle",
    "linked_service_postgres",
    "linked_service_rest_api_anonymous",
    "linked_service_rest_api_basic",
    "linked_service_s3",
    "linked_service_salesforce",
    "linked_service_sap_hana",
    "linked_service_servicenow",
    "linked_service_sftp_basic",
    "linked_service_sftp_ssh_key",
    "linked_service_snowflake_basic",
    "linked_service_snowflake_key_pair",
    "linked_service_sql_server",
    "linked_service_synapse_sql_pool",
    "pipeline_ingest_azure_blob_csv",
    "pipeline_ingest_azure_blob_json_lines",
    "pipeline_ingest_azure_sql_db",
    "pipeline_ingest_bigquery",
    "pipeline_ingest_mysql",
    "pipeline_ingest_oracle",
    "pipeline_ingest_postgres",
    "pipeline_ingest_rest_api",
    "pipeline_ingest_s3_csv",
    "pipeline_ingest_s3_json_lines",
    "pipeline_ingest_salesforce",
    "pipeline_ingest_sap_hana",
    "pipeline_ingest_servicenow",
    "pipeline_ingest_sftp_csv",
    "pipeline_ingest_sftp_json_lines",
    "pipeline_ingest_snowflake",
    "pipeline_ingest_sql_server",
    "pipeline_ingest_synapse_sql_pool",
    "pipeline_master_driver",
    "pipeline_per_source_driver",
    "trigger_daily",
]
