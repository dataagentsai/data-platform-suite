"""Unit tests for the Azure Databricks ADF realm generator.

Three layers:

1. **Builder tests** — each per-resource builder produces the expected
   ADF JSON shape.
2. **Serializer tests** — deterministic JSON output (sorted keys,
   consistent indent, trailing newline).
3. **Assembly + golden-diff** — end-to-end against an in-memory
   metadata DB, verifying byte-identical re-runs.

Phase 5d: artifacts are **per source type**, not per source. Adding
a second Postgres database is a metadata-only change — the same
``LS_POSTGRES`` / ``DS_POSTGRES_TABLE`` / ``PL_INGEST_POSTGRES`` /
``PL_PER_SOURCE_DRIVER_POSTGRES`` artifacts handle both.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from lake_ingestion_framework.core.metadata.repository import (
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.realms.adf_adls.generator import (
    plan_artifacts,
)
from lake_ingestion_framework.realms.adf_adls.generator._security_scan import (
    scan_artifact_for_literal_secrets,
)
from lake_ingestion_framework.realms.adf_adls.generator._serializer import (
    serialize_artifact,
)
from lake_ingestion_framework.realms.adf_adls.generator.datasets import (
    build_adls_parquet_sink_dataset,
    build_azure_sql_db_table_dataset,
    build_mysql_table_dataset,
    build_oracle_table_dataset,
    build_postgres_table_dataset,
)
from lake_ingestion_framework.realms.adf_adls.generator.lifecycle import (
    LIFECYCLE_RULE_NAME,
    build_adls_lifecycle_policy,
)
from lake_ingestion_framework.realms.adf_adls.generator.linked_services import (
    build_adls_gen2_linked_service,
    build_azure_blob_linked_service,
    build_azure_sql_db_linked_service,
    build_bigquery_linked_service,
    build_key_vault_linked_service,
    build_mysql_linked_service,
    build_oracle_linked_service,
    build_postgres_linked_service,
    build_s3_linked_service,
    build_salesforce_linked_service,
    build_sap_hana_linked_service,
    build_servicenow_linked_service,
    build_snowflake_basic_linked_service,
    build_snowflake_key_pair_linked_service,
    build_sql_server_linked_service,
    build_synapse_sql_pool_linked_service,
)
from lake_ingestion_framework.realms.adf_adls.generator.pipelines import (
    build_pl_ingest_azure_blob_csv,
    build_pl_ingest_azure_blob_json_lines,
    build_pl_ingest_azure_sql_db,
    build_pl_ingest_bigquery,
    build_pl_ingest_mysql,
    build_pl_ingest_oracle,
    build_pl_ingest_postgres,
    build_pl_ingest_s3_csv,
    build_pl_ingest_s3_json_lines,
    build_pl_ingest_salesforce,
    build_pl_ingest_sap_hana,
    build_pl_ingest_servicenow,
    build_pl_ingest_snowflake,
    build_pl_ingest_sql_server,
    build_pl_ingest_synapse_sql_pool,
    build_pl_per_source_driver,
)
from lake_ingestion_framework.realms.adf_adls.generator.triggers import (
    build_daily_trigger,
)

# ──────────────────────────────────────────────────────────────────────
# Serializer
# ──────────────────────────────────────────────────────────────────────


class TestSerializer:
    def test_sorted_keys(self) -> None:
        body = serialize_artifact({"zebra": 1, "apple": 2, "monkey": 3})
        # First non-quote char inside "..." should be 'a' (apple)
        assert body.index('"apple"') < body.index('"monkey"')
        assert body.index('"monkey"') < body.index('"zebra"')

    def test_trailing_newline(self) -> None:
        assert serialize_artifact({"x": 1}).endswith("\n")

    def test_indent_is_4_spaces(self) -> None:
        body = serialize_artifact({"a": {"b": 1}})
        # Inner key should be indented 8 spaces
        assert '        "b": 1' in body

    def test_unicode_preserved(self) -> None:
        body = serialize_artifact({"name": "DS_TÉST"})
        assert "TÉST" in body


# ──────────────────────────────────────────────────────────────────────
# LinkedService builders (parameterised — one per type)
# ──────────────────────────────────────────────────────────────────────


class TestLinkedServices:
    def test_azure_sql_db_is_parameterised(self) -> None:
        ls = build_azure_sql_db_linked_service()
        assert ls["name"] == "LS_AZURE_SQL_DB"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AzureSqlDatabase"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {"p_server", "p_database"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "ManagedIdentity"
        cs = str(type_props["connectionString"])
        assert "@{linkedService().p_server}" in cs
        assert "@{linkedService().p_database}" in cs

    def test_adls_gen2_shape(self) -> None:
        ls = build_adls_gen2_linked_service(storage_account="mysa")
        assert ls["name"] == "LS_ADLS_GEN2"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AzureBlobFS"
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["url"] == "https://mysa.dfs.core.windows.net"

    def test_key_vault_shape(self) -> None:
        ls = build_key_vault_linked_service(key_vault_uri="https://my-kv.vault.azure.net")
        assert ls["name"] == "LS_KEY_VAULT"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AzureKeyVault"

    def test_no_secrets_in_output(self) -> None:
        """No password/secret values appear anywhere in LinkedService JSON.

        Per CLAUDE.md security rules — secrets resolve through Key Vault
        at runtime, never in the artifact itself. With parameterised LSs
        we don't even embed the secret *name* — that's a runtime
        parameter resolved by the Per-Source Driver pipeline.
        """
        artifacts = [
            build_azure_sql_db_linked_service(),
            build_adls_gen2_linked_service(storage_account="mysa"),
            build_key_vault_linked_service(key_vault_uri="https://my-kv.vault.azure.net"),
            build_postgres_linked_service(),
            build_mysql_linked_service(),
            build_oracle_linked_service(),
        ]
        for art in artifacts:
            text = json.dumps(art).lower()
            for forbidden in ("password=", "pwd=", "secret=", "accountkey="):
                assert forbidden not in text, f"Forbidden token {forbidden!r} in {art['name']!r}"

    def test_postgres_linked_service_is_parameterised(self) -> None:
        ls = build_postgres_linked_service()
        assert ls["name"] == "LS_POSTGRES"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "PostgreSqlV2"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {
            "p_server",
            "p_database",
            "p_user",
            "p_password_secret",
        }
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        # Password resolves through KV, with secret name as an LS parameter
        password = type_props["password"]
        assert isinstance(password, dict)
        assert password["type"] == "AzureKeyVaultSecret"
        secret_name = password["secretName"]
        assert isinstance(secret_name, dict)
        assert "p_password_secret" in str(secret_name["value"])

    def test_mysql_linked_service_is_parameterised(self) -> None:
        ls = build_mysql_linked_service()
        assert ls["name"] == "LS_MYSQL"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "MySql"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert "p_server" in params
        assert "p_password_secret" in params

    def test_oracle_linked_service_is_parameterised(self) -> None:
        ls = build_oracle_linked_service()
        assert ls["name"] == "LS_ORACLE"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "Oracle"
        params = props["parameters"]
        assert isinstance(params, dict)
        # Oracle uses service_name (not database)
        assert "p_service_name" in params
        assert "p_database" not in params
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        cs = str(type_props["connectionString"])
        assert "@{linkedService().p_server}" in cs
        assert "@{linkedService().p_service_name}" in cs

    def test_sql_server_linked_service_is_parameterised(self) -> None:
        ls = build_sql_server_linked_service()
        assert ls["name"] == "LS_SQL_SERVER"
        props = ls["properties"]
        assert isinstance(props, dict)
        # SqlServer (not AzureSqlDatabase) — distinct ADF connector
        assert props["type"] == "SqlServer"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {
            "p_server",
            "p_database",
            "p_user",
            "p_password_secret",
        }
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        cs = str(type_props["connectionString"])
        assert "Data Source=@{linkedService().p_server}" in cs
        assert "Initial Catalog=@{linkedService().p_database}" in cs
        # Password resolves through KV with secret-name as a parameter
        password = type_props["password"]
        assert isinstance(password, dict)
        assert password["type"] == "AzureKeyVaultSecret"

    def test_snowflake_basic_linked_service_is_parameterised(self) -> None:
        ls = build_snowflake_basic_linked_service()
        assert ls["name"] == "LS_SNOWFLAKE_BASIC"
        props = ls["properties"]
        assert isinstance(props, dict)
        # SnowflakeV2 (not legacy Snowflake) — modern ADF connector.
        assert props["type"] == "SnowflakeV2"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {
            "p_account",
            "p_database",
            "p_warehouse",
            "p_user",
            "p_password_secret",
            "p_role",
        }
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "Basic"
        # Password resolves through KV with secret-name as a parameter
        password = type_props["password"]
        assert isinstance(password, dict)
        assert password["type"] == "AzureKeyVaultSecret"

    def test_snowflake_key_pair_linked_service_uses_private_key(self) -> None:
        ls = build_snowflake_key_pair_linked_service()
        assert ls["name"] == "LS_SNOWFLAKE_KEY_PAIR"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "SnowflakeV2"
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "KeyPair"
        # Private key (not password) resolves through KV
        private_key = type_props["privateKey"]
        assert isinstance(private_key, dict)
        assert private_key["type"] == "AzureKeyVaultSecret"
        # Parameter set names p_private_key_secret rather than p_password_secret
        params = props["parameters"]
        assert isinstance(params, dict)
        assert "p_private_key_secret" in params
        assert "p_password_secret" not in params

    def test_salesforce_linked_service_uses_oauth_client_credentials(self) -> None:
        ls = build_salesforce_linked_service()
        assert ls["name"] == "LS_SALESFORCE"
        props = ls["properties"]
        assert isinstance(props, dict)
        # SalesforceV2 (modern OAuth-capable) — not the legacy Salesforce LS
        assert props["type"] == "SalesforceV2"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {
            "p_environment_url",
            "p_client_id",
            "p_client_secret_secret",
        }
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "OAuth2ClientCredentials"
        # Client secret resolves through KV with secret-name as a parameter
        cs = type_props["clientSecret"]
        assert isinstance(cs, dict)
        assert cs["type"] == "AzureKeyVaultSecret"
        # Username / password fields not used in OAuth flow
        assert "password" not in type_props
        assert "username" not in type_props
        assert "securityToken" not in type_props

    def test_azure_blob_linked_service_uses_msi_auth(self) -> None:
        ls = build_azure_blob_linked_service()
        assert ls["name"] == "LS_AZURE_BLOB"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AzureBlobStorage"
        params = props["parameters"]
        assert isinstance(params, dict)
        # MSI auth — only the storage account name parameter
        assert set(params.keys()) == {"p_storage_account"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        # Service endpoint URL templated with the parameterised storage account
        endpoint = type_props["serviceEndpoint"]
        assert isinstance(endpoint, dict)
        assert "p_storage_account" in str(endpoint["value"])
        assert "blob.core.windows.net" in str(endpoint["value"])
        # No credential fields — implicit MSI
        assert "sasUri" not in type_props
        assert "connectionString" not in type_props
        assert "accountKey" not in type_props

    def test_s3_linked_service_uses_access_key_auth(self) -> None:
        ls = build_s3_linked_service()
        assert ls["name"] == "LS_S3"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AmazonS3"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {"p_access_key_id", "p_secret_access_key_secret"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "AccessKey"
        # Secret access key resolves through KV
        sak = type_props["secretAccessKey"]
        assert isinstance(sak, dict)
        assert sak["type"] == "AzureKeyVaultSecret"

    def test_synapse_sql_pool_linked_service_is_msi(self) -> None:
        ls = build_synapse_sql_pool_linked_service()
        assert ls["name"] == "LS_SYNAPSE_SQL_POOL"
        props = ls["properties"]
        assert isinstance(props, dict)
        # AzureSqlDW (not AzureSqlDatabase / SqlServer) — distinct connector
        assert props["type"] == "AzureSqlDW"
        params = props["parameters"]
        assert isinstance(params, dict)
        # MSI auth — no user/secret in the parameter set
        assert set(params.keys()) == {"p_server", "p_database"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "ManagedIdentity"
        cs = str(type_props["connectionString"])
        assert "@{linkedService().p_server}" in cs
        assert "@{linkedService().p_database}" in cs

    def test_servicenow_linked_service_uses_basic_auth(self) -> None:
        ls = build_servicenow_linked_service()
        assert ls["name"] == "LS_SERVICENOW"
        props = ls["properties"]
        assert isinstance(props, dict)
        # Legacy ServiceNow connector (V2 + OAuth2 lands in Phase B)
        assert props["type"] == "ServiceNow"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {
            "p_environment_url",
            "p_user",
            "p_password_secret",
        }
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "Basic"
        # Endpoint URL flows from the LS parameter (mapped from
        # source_config.environment_url)
        endpoint = type_props["endpoint"]
        assert isinstance(endpoint, dict)
        assert "p_environment_url" in str(endpoint["value"])
        # Password resolves through KV
        pwd = type_props["password"]
        assert isinstance(pwd, dict)
        assert pwd["type"] == "AzureKeyVaultSecret"

    def test_sap_hana_linked_service_uses_basic_auth(self) -> None:
        ls = build_sap_hana_linked_service()
        assert ls["name"] == "LS_SAP_HANA"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "SapHana"
        params = props["parameters"]
        assert isinstance(params, dict)
        # No p_database — HANA has no per-connection database concept.
        assert set(params.keys()) == {"p_server", "p_user", "p_password_secret"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "Basic"
        # Server / userName flow from LS parameters.
        srv = type_props["server"]
        assert isinstance(srv, dict)
        assert "p_server" in str(srv["value"])
        # Password resolves through KV
        pwd = type_props["password"]
        assert isinstance(pwd, dict)
        assert pwd["type"] == "AzureKeyVaultSecret"

    def test_bigquery_linked_service_uses_service_account_auth(self) -> None:
        ls = build_bigquery_linked_service()
        assert ls["name"] == "LS_BIGQUERY"
        props = ls["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "GoogleBigQueryV2"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert set(params.keys()) == {"p_project_id", "p_key_file_secret"}
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["authenticationType"] == "ServiceAuthentication"
        # Key file content resolves through KV
        kfc = type_props["keyFileContent"]
        assert isinstance(kfc, dict)
        assert kfc["type"] == "AzureKeyVaultSecret"
        # No password / private key fields — service-account-only path
        assert "password" not in type_props
        assert "privateKey" not in type_props

    def test_basic_auth_lses_use_kv_reference(self) -> None:
        """KV reference path — never embeds the actual password."""
        artifacts = [
            build_postgres_linked_service(),
            build_mysql_linked_service(),
            build_oracle_linked_service(),
        ]
        for art in artifacts:
            text = json.dumps(art)
            # password value must always be a KV reference dict
            assert "AzureKeyVaultSecret" in text


# ──────────────────────────────────────────────────────────────────────
# Dataset builders
# ──────────────────────────────────────────────────────────────────────


class TestDatasets:
    def test_azure_sql_table_dataset_forwards_conn_params(self) -> None:
        ds = build_azure_sql_db_table_dataset()
        assert ds["name"] == "DS_AZURE_SQL_DB_TABLE"
        props = ds["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "AzureSqlTable"
        params = props["parameters"]
        assert isinstance(params, dict)
        # Schema/table + LS conn params
        assert set(params.keys()) == {"schema_name", "table_name", "p_server", "p_database"}
        # LS reference forwards conn params
        ls_ref = props["linkedServiceName"]
        assert isinstance(ls_ref, dict)
        assert ls_ref["referenceName"] == "LS_AZURE_SQL_DB"
        ls_params = ls_ref["parameters"]
        assert isinstance(ls_params, dict)
        assert set(ls_params.keys()) == {"p_server", "p_database"}

    def test_adls_parquet_sink_dataset(self) -> None:
        ds = build_adls_parquet_sink_dataset()
        assert ds["name"] == "DS_ADLS_PARQUET_SINK"
        props = ds["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "Parquet"
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        assert type_props["compressionCodec"] == "snappy"

    def test_postgres_table_dataset(self) -> None:
        ds = build_postgres_table_dataset()
        assert ds["name"] == "DS_POSTGRES_TABLE"
        props = ds["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "PostgreSqlV2Table"
        params = props["parameters"]
        assert isinstance(params, dict)
        assert "p_password_secret" in params

    def test_mysql_table_dataset(self) -> None:
        ds = build_mysql_table_dataset()
        assert ds["name"] == "DS_MYSQL_TABLE"
        props = ds["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "MySqlTable"

    def test_oracle_table_dataset(self) -> None:
        ds = build_oracle_table_dataset()
        assert ds["name"] == "DS_ORACLE_TABLE"
        props = ds["properties"]
        assert isinstance(props, dict)
        assert props["type"] == "OracleTable"
        params = props["parameters"]
        assert isinstance(params, dict)
        # Oracle datasets carry service_name, not database
        assert "p_service_name" in params


# ──────────────────────────────────────────────────────────────────────
# Pipeline + Trigger builders
# ──────────────────────────────────────────────────────────────────────


def _find_activity(pl: dict[str, object], name: str) -> dict[str, object]:
    """Look up an activity in a pipeline JSON by its ``name``.

    Pipelines now wrap the Copy Activity with run-log + watermark
    activities (Phase 6b-1b), so tests can no longer assume
    ``activities[0]`` is the Copy.
    """
    activities = pl["properties"]["activities"]  # type: ignore[index]
    assert isinstance(activities, list)
    for act in activities:
        if isinstance(act, dict) and act.get("name") == name:
            return act
    raise AssertionError(f"activity {name!r} not found in pipeline {pl.get('name')!r}")


class TestPipelinesAndTriggers:
    def test_pl_ingest_azure_sql_db_uses_sql_reader_query(self) -> None:
        pl = build_pl_ingest_azure_sql_db()
        assert pl["name"] == "PL_INGEST_AZURE_SQL_DB"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        type_props = copy_act["typeProperties"]
        assert isinstance(type_props, dict)
        source = type_props["source"]
        assert isinstance(source, dict)
        assert source["type"] == "AzureSqlSource"
        assert "sqlReaderQuery" in source

    def test_pl_ingest_postgres_uses_query_field(self) -> None:
        pl = build_pl_ingest_postgres()
        assert pl["name"] == "PL_INGEST_POSTGRES"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "PostgreSqlV2Source"
        assert "query" in source
        # Postgres uses double-quote identifier quoting
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert '"@{pipeline().parameters.schema_name}"' in query_text

    def test_pl_ingest_mysql_uses_backtick_quotes(self) -> None:
        pl = build_pl_ingest_mysql()
        assert pl["name"] == "PL_INGEST_MYSQL"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "MySqlSource"
        query_text = str(source["query"]["value"])  # type: ignore[index]
        # MySQL uses backticks for identifier quoting
        assert "`@{pipeline().parameters.schema_name}`" in query_text

    def test_pl_ingest_oracle_uses_oracle_reader_query(self) -> None:
        pl = build_pl_ingest_oracle()
        assert pl["name"] == "PL_INGEST_ORACLE"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "OracleSource"
        assert "oracleReaderQuery" in source

    def test_pl_ingest_sql_server_uses_sql_server_source(self) -> None:
        pl = build_pl_ingest_sql_server()
        assert pl["name"] == "PL_INGEST_SQL_SERVER"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        # SqlServerSource (not AzureSqlSource) — Phase 5d ships both;
        # PL_INGEST_AZURE_SQL_DB is the MSI-auth Azure SaaS variant,
        # PL_INGEST_SQL_SERVER is the SQL-auth on-prem / VM variant.
        assert source["type"] == "SqlServerSource"
        assert "sqlReaderQuery" in source
        # T-SQL bracket quoting same as Azure SQL DB
        query_text = str(source["sqlReaderQuery"]["value"])  # type: ignore[index]
        assert "[@{pipeline().parameters.schema_name}]" in query_text

    def test_pl_ingest_sql_server_carries_user_and_secret_params(self) -> None:
        pl = build_pl_ingest_sql_server()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        # SQL auth — needs both user and secret params (unlike Azure SQL DB MSI)
        assert "p_user" in params
        assert "p_password_secret" in params
        assert "p_server" in params
        assert "p_database" in params

    def test_pl_ingest_snowflake_uses_snowflake_v2_source(self) -> None:
        pl = build_pl_ingest_snowflake()
        assert pl["name"] == "PL_INGEST_SNOWFLAKE"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "SnowflakeV2Source"
        assert "query" in source
        # Snowflake uses double-quote identifier quoting (same as PG / Oracle)
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert '"@{pipeline().parameters.schema_name}"' in query_text

    def test_pl_ingest_salesforce_uses_soql_query(self) -> None:
        pl = build_pl_ingest_salesforce()
        assert pl["name"] == "PL_INGEST_SALESFORCE"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "SalesforceV2Source"
        assert "query" in source
        # SOQL: NO identifier quoting (unlike SQL); watermark value
        # passed unquoted (SOQL date literals are unquoted ISO-8601)
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert "FROM @{pipeline().parameters.table_name}" in query_text
        # Watermark value is not wrapped in single quotes
        assert "'@{pipeline().parameters.watermark_value}'" not in query_text
        assert "> @{pipeline().parameters.watermark_value}" in query_text
        # readBehavior: 'query' (active records); not soft-deleted
        assert source["readBehavior"] == "query"

    def test_pl_ingest_salesforce_carries_oauth_params(self) -> None:
        pl = build_pl_ingest_salesforce()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_environment_url" in params
        assert "p_client_id" in params
        assert "p_client_secret_secret" in params
        # No SQL-server-style server / database params
        assert "p_server" not in params
        assert "p_database" not in params
        # schema_name is declared (unused) for symmetry with the
        # per-source driver's parameter passing
        assert "schema_name" in params
        assert "table_name" in params  # holds the SObject API name

    def test_pl_ingest_azure_blob_csv_uses_blob_read_settings(self) -> None:
        pl = build_pl_ingest_azure_blob_csv()
        assert pl["name"] == "PL_INGEST_AZURE_BLOB_CSV"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "DelimitedTextSource"
        store = source["storeSettings"]
        assert isinstance(store, dict)
        assert store["type"] == "AzureBlobStorageReadSettings"

    def test_pl_ingest_azure_blob_json_lines_carries_container_param(self) -> None:
        pl = build_pl_ingest_azure_blob_json_lines()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        # Per-dataset container_name (one source can read from multiple
        # containers in the same storage account).
        assert "container_name" in params
        assert "folder_path" in params
        assert "file_name" in params
        assert "p_storage_account" in params
        # MSI auth — no secret param at the per-source-driver level
        assert "p_password_secret" not in params

    def test_pl_ingest_s3_csv_uses_amazon_s3_read_settings(self) -> None:
        pl = build_pl_ingest_s3_csv()
        assert pl["name"] == "PL_INGEST_S3_CSV"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "DelimitedTextSource"
        store = source["storeSettings"]
        assert isinstance(store, dict)
        assert store["type"] == "AmazonS3ReadSettings"

    def test_pl_ingest_s3_json_lines_carries_bucket_param(self) -> None:
        pl = build_pl_ingest_s3_json_lines()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        # S3-specific addition: bucket is per-dataset (one source can read
        # from multiple buckets with the same access key).
        assert "bucket" in params
        assert "folder_path" in params
        assert "file_name" in params
        assert "p_access_key_id" in params
        assert "p_secret_access_key_secret" in params

    def test_pl_ingest_synapse_sql_pool_uses_sql_dw_source(self) -> None:
        pl = build_pl_ingest_synapse_sql_pool()
        assert pl["name"] == "PL_INGEST_SYNAPSE_SQL_POOL"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        # SqlDWSource (not AzureSqlSource / SqlServerSource) — distinct
        # ADF activity type for the dedicated pool connector.
        assert source["type"] == "SqlDWSource"
        assert "sqlReaderQuery" in source
        # T-SQL bracket quoting (same as Azure SQL DB)
        query_text = str(source["sqlReaderQuery"]["value"])  # type: ignore[index]
        assert "[@{pipeline().parameters.schema_name}]" in query_text

    def test_pl_ingest_synapse_has_no_user_param(self) -> None:
        """MSI-auth Synapse — no user/secret params (like Azure SQL DB)."""
        pl = build_pl_ingest_synapse_sql_pool()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_user" not in params
        assert "p_password_secret" not in params
        assert "p_server" in params
        assert "p_database" in params

    def test_pl_ingest_servicenow_uses_encoded_query(self) -> None:
        pl = build_pl_ingest_servicenow()
        assert pl["name"] == "PL_INGEST_SERVICENOW"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "ServiceNowSource"
        assert "query" in source
        # ServiceNow encoded query: NO identifier quoting, value
        # unquoted (different from SQL — also different from SOQL
        # since encoded queries are entirely positional, not
        # SELECT-shaped)
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert "@{pipeline().parameters.watermark_column}>" in query_text
        assert ">@{pipeline().parameters.watermark_value}" in query_text
        # No SELECT or FROM — encoded query is just the WHERE
        assert "SELECT" not in query_text
        assert "FROM" not in query_text

    def test_pl_ingest_servicenow_carries_basic_auth_params(self) -> None:
        pl = build_pl_ingest_servicenow()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_environment_url" in params
        assert "p_user" in params
        assert "p_password_secret" in params
        # No SQL-server-style server / database params
        assert "p_server" not in params
        assert "p_database" not in params
        # schema_name is declared (unused) for symmetry with the
        # per-source driver's parameter passing
        assert "schema_name" in params
        assert "table_name" in params  # holds the ServiceNow table name

    def test_pl_ingest_sap_hana_uses_sap_hana_source(self) -> None:
        pl = build_pl_ingest_sap_hana()
        assert pl["name"] == "PL_INGEST_SAP_HANA"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "SapHanaSource"
        assert "query" in source
        # ANSI SQL with double-quoted identifiers (same as Postgres / Oracle).
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert '"@{pipeline().parameters.schema_name}"' in query_text
        assert '"@{pipeline().parameters.watermark_column}"' in query_text

    def test_pl_ingest_sap_hana_carries_basic_auth_params(self) -> None:
        """SAP HANA — server + user + password params, no p_database."""
        pl = build_pl_ingest_sap_hana()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_server" in params
        assert "p_user" in params
        assert "p_password_secret" in params
        # HANA has no per-connection database concept.
        assert "p_database" not in params

    def test_pl_ingest_bigquery_uses_bigquery_v2_source(self) -> None:
        pl = build_pl_ingest_bigquery()
        assert pl["name"] == "PL_INGEST_BIGQUERY"
        copy_act = _find_activity(pl, "CopyFromSourceToParquet")
        source = copy_act["typeProperties"]["source"]  # type: ignore[index]
        assert isinstance(source, dict)
        assert source["type"] == "GoogleBigQueryV2Source"
        assert "query" in source
        # BigQuery uses backtick quoting for identifiers (same as MySQL)
        query_text = str(source["query"]["value"])  # type: ignore[index]
        assert "`@{pipeline().parameters.schema_name}`" in query_text

    def test_pl_ingest_bigquery_carries_project_id_and_key_file_secret(self) -> None:
        pl = build_pl_ingest_bigquery()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_project_id" in params
        assert "p_key_file_secret" in params
        # BigQuery doesn't use server / database / user — project_id is the
        # full namespace handle, and the JSON key file carries the identity.
        assert "p_server" not in params
        assert "p_database" not in params
        assert "p_user" not in params

    def test_pl_ingest_snowflake_carries_account_warehouse_role(self) -> None:
        pl = build_pl_ingest_snowflake()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_account" in params
        assert "p_warehouse" in params
        assert "p_role" in params
        assert "p_user" in params
        assert "p_password_secret" in params
        assert "p_database" in params
        # Snowflake doesn't use p_server (it uses p_account)
        assert "p_server" not in params

    def test_ingest_pipelines_wrap_copy_with_run_log_activities(self) -> None:
        """Phase 6b-1b — every ingest pipeline runs a 6-activity dance:
        RunLogStarted -> SetRunId -> Copy -> RunLogFinishedSuccess
                                          -> AdvanceWatermark
                                     -> RunLogFinishedFailure
        """
        for build in (
            build_pl_ingest_azure_sql_db,
            build_pl_ingest_postgres,
            build_pl_ingest_mysql,
            build_pl_ingest_oracle,
        ):
            pl = build()
            names = {
                a["name"]  # type: ignore[index]
                for a in pl["properties"]["activities"]  # type: ignore[index]
            }
            assert "RunLogStarted" in names
            assert "SetRunId" in names
            assert "CopyFromSourceToParquet" in names
            assert "RunLogFinishedSuccess" in names
            assert "AdvanceWatermark" in names
            assert "RunLogFinishedFailure" in names

    def test_run_log_finished_success_carries_observability_metrics(self) -> None:
        """RunLogFinishedSuccess passes the cost/perf metrics from Copy
        output (bytes_read, used_parallel_copies, used_dius, throughput)
        into sp_run_log_finished. These feed v_dataset_health and
        cost-attribution views — see docs/operability.md.
        """
        pl = build_pl_ingest_postgres()
        succ = _find_activity(pl, "RunLogFinishedSuccess")
        params = succ["typeProperties"]["storedProcedureParameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        # Cost-relevant Copy outputs flow into the proc
        for proc_param, copy_field in (
            ("bytes_read", "dataRead"),
            ("used_parallel_copies", "usedParallelCopies"),
            ("used_dius", "usedDataIntegrationUnits"),
            ("throughput_kbps", "throughput"),
        ):
            assert proc_param in params, f"missing param {proc_param!r}"
            expr = params[proc_param]["value"]["value"]  # type: ignore[index]
            assert copy_field in expr, f"{proc_param} should reference {copy_field}"

    def test_run_log_finished_failure_depends_on_copy_failed(self) -> None:
        pl = build_pl_ingest_postgres()
        fail = _find_activity(pl, "RunLogFinishedFailure")
        deps = fail["dependsOn"]  # type: ignore[index]
        assert isinstance(deps, list)
        assert deps[0]["activity"] == "CopyFromSourceToParquet"
        assert deps[0]["dependencyConditions"] == ["Failed"]

    def test_advance_watermark_calls_sp_advance_watermark(self) -> None:
        pl = build_pl_ingest_postgres()
        adv = _find_activity(pl, "AdvanceWatermark")
        assert adv["type"] == "SqlServerStoredProcedure"
        type_props = adv["typeProperties"]
        assert isinstance(type_props, dict)
        assert "sp_advance_watermark" in str(type_props["storedProcedureName"])

    def test_ingest_pipeline_declares_run_log_params(self) -> None:
        for build in (build_pl_ingest_azure_sql_db, build_pl_ingest_postgres):
            pl = build()
            params = pl["properties"]["parameters"]  # type: ignore[index]
            assert isinstance(params, dict)
            assert "dataset_id" in params
            assert "master_run_id" in params
            variables = pl["properties"]["variables"]  # type: ignore[index]
            assert isinstance(variables, dict)
            assert "run_id" in variables

    def test_rdbms_pipelines_share_dataset_param_set(self) -> None:
        """All four RDBMS ingest pipelines accept the dataset/sink/watermark params.

        Connection params differ (Oracle uses service_name; Azure SQL DB
        is MSI-only with no user/secret). The Per-Source Driver passes
        the right subset based on the source type.
        """
        common_params = {
            "schema_name",
            "table_name",
            "watermark_column",
            "watermark_value",
            "container",
            "folder_path",
        }
        for build in (
            build_pl_ingest_azure_sql_db,
            build_pl_ingest_postgres,
            build_pl_ingest_mysql,
            build_pl_ingest_oracle,
        ):
            pl = build()
            params = pl["properties"]["parameters"]  # type: ignore[index]
            assert isinstance(params, dict)
            assert common_params.issubset(set(params.keys()))
            # All RDBMS pipelines must declare p_server (forwarded into LS)
            assert "p_server" in params

    def test_pl_ingest_oracle_carries_service_name_param(self) -> None:
        pl = build_pl_ingest_oracle()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_service_name" in params
        assert "p_database" not in params

    def test_pl_ingest_postgres_carries_password_secret_param(self) -> None:
        pl = build_pl_ingest_postgres()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_password_secret" in params
        assert "p_user" in params

    def test_pl_ingest_azure_sql_db_has_no_user_param(self) -> None:
        """Azure SQL DB uses MSI — no user/secret in the parameter list."""
        pl = build_pl_ingest_azure_sql_db()
        params = pl["properties"]["parameters"]  # type: ignore[index]
        assert isinstance(params, dict)
        assert "p_user" not in params
        assert "p_password_secret" not in params
        assert "p_server" in params
        assert "p_database" in params

    def test_per_source_driver_has_two_lookups(self) -> None:
        """Per-Source Driver runs Lookup → Set Variables → Lookup → ForEach."""
        pl = build_pl_per_source_driver(source_type="postgres")
        assert pl["name"] == "PL_PER_SOURCE_DRIVER_POSTGRES"
        props = pl["properties"]
        assert isinstance(props, dict)
        activities = props["activities"]
        assert isinstance(activities, list)
        names = [a["name"] for a in activities]  # type: ignore[index]
        assert "GetSourceConfig" in names
        assert "GetEnabledDatasets" in names
        assert "ForEachDataset" in names
        # One Set Variable per conn param
        set_vars = [n for n in names if isinstance(n, str) and n.startswith("Set_")]
        assert len(set_vars) == 4  # p_server, p_database, p_user, p_password_secret

    def test_per_source_driver_lookup_queries_reference_views(self) -> None:
        pl = build_pl_per_source_driver(source_type="postgres")
        rendered = json.dumps(pl)
        assert "v_source_config" in rendered
        assert "v_enabled_datasets" in rendered

    def test_per_source_driver_opens_and_closes_master_run_log(self) -> None:
        """Phase 6b-1b — Per-Source Driver wraps the ForEach with
        sp_master_run_log_started + sp_master_run_log_finished."""
        pl = build_pl_per_source_driver(source_type="postgres")
        names = {
            a["name"]  # type: ignore[index]
            for a in pl["properties"]["activities"]  # type: ignore[index]
        }
        assert "MasterRunStarted" in names
        assert "SetMasterRunId" in names
        assert "MasterRunFinished" in names
        # SQL refs to the procs
        rendered = json.dumps(pl)
        assert "sp_master_run_log_started" in rendered
        assert "sp_master_run_log_finished" in rendered

    def test_per_source_driver_passes_dataset_id_and_master_run_id(self) -> None:
        """Inner ExecutePipeline calls now forward dataset_id +
        master_run_id so each ingest invocation can write run_log
        rows tied back to the parent master run."""
        pl = build_pl_per_source_driver(source_type="postgres")
        rendered = json.dumps(pl)
        # @item().dataset_id passes per-iteration; @variables('master_run_id')
        # is captured from MasterRunStarted Lookup.
        assert "@item().dataset_id" in rendered
        assert "@int(variables('master_run_id'))" in rendered

    def test_master_run_finished_runs_on_foreach_completed(self) -> None:
        """MasterRunFinished must use Completed (not Succeeded) so a
        PARTIAL outcome still closes the master_run_log row."""
        pl = build_pl_per_source_driver(source_type="postgres")
        finish = next(
            a
            for a in pl["properties"]["activities"]  # type: ignore[index]
            if isinstance(a, dict) and a.get("name") == "MasterRunFinished"
        )
        deps = finish["dependsOn"]  # type: ignore[index]
        assert isinstance(deps, list)
        assert deps[0]["activity"] == "ForEachDataset"
        assert deps[0]["dependencyConditions"] == ["Completed"]

    def test_daily_trigger_default_cron(self) -> None:
        tr = build_daily_trigger()
        props = tr["properties"]
        assert isinstance(props, dict)
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        recur = type_props["recurrence"]
        assert isinstance(recur, dict)
        assert recur["frequency"] == "Day"
        sched = recur["schedule"]
        assert isinstance(sched, dict)
        assert sched["minutes"] == [0]
        assert sched["hours"] == [2]

    def test_daily_trigger_custom_cron(self) -> None:
        tr = build_daily_trigger(schedule_cron="30 5 * * *")
        props = tr["properties"]
        assert isinstance(props, dict)
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        recur = type_props["recurrence"]
        assert isinstance(recur, dict)
        sched = recur["schedule"]
        assert isinstance(sched, dict)
        assert sched["minutes"] == [30]
        assert sched["hours"] == [5]

    def test_daily_trigger_unparseable_cron_falls_back(self) -> None:
        tr = build_daily_trigger(schedule_cron="*/5 * * * *")  # not statically resolvable
        props = tr["properties"]
        assert isinstance(props, dict)
        type_props = props["typeProperties"]
        assert isinstance(type_props, dict)
        recur = type_props["recurrence"]
        assert isinstance(recur, dict)
        sched = recur["schedule"]
        assert isinstance(sched, dict)
        assert sched["minutes"] == [0]
        assert sched["hours"] == [2]


# ──────────────────────────────────────────────────────────────────────
# Assembly — end-to-end against in-memory metadata DB
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def seeded_repo() -> Iterator[SqliteMetadataRepository]:
    """One Azure SQL DB source + one dataset, anchored in env=dev."""
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    r.upsert_platform_config(
        {
            "default_realm": "adf-adls",
            "target_storage_account": "dummycustomeradevsa",
            "target_container_default": "lake",
            "key_vault_uri": "https://dummycustomera-dev-kv.vault.azure.net/",
            "metadata_db_server": "dummycustomera-dev-sql.database.windows.net",
            "metadata_db_database": "lake_ingest_meta",
            "default_schedule_cron": "15 3 * * *",
        }
    )
    sid = r.upsert_source(
        source_name="orders_db",
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={
            "connection_pattern": "azure_basic",
            "auth_method": "managed_identity",
            "server": "orders-server.database.windows.net",
            "database": "orders",
        },
    )
    r.upsert_dataset(
        source_id=sid,
        dataset_name="customers",
        is_enabled=True,
        config={
            "realm": "adf-adls",
            "dataset_type": "TABLE",
            "load_type": "INCREMENTAL",
            "target_path_template": "raw/{source}/{schema}/{object}",
            "schema_name": "dbo",
            "object_name": "customers",
            "watermark_column": "modified_at",
        },
    )
    try:
        yield r
    finally:
        r.close()


class TestLifecyclePolicy:
    """ADLS Gen2 management-policy generator (cost concern)."""

    def test_happy_path_emits_two_actions(self) -> None:
        policy = build_adls_lifecycle_policy(container="lake", cool_days=30, archive_days=180)
        rules = policy["policy"]["rules"]  # type: ignore[index]
        assert isinstance(rules, list)
        assert len(rules) == 1
        rule = rules[0]
        assert rule["name"] == LIFECYCLE_RULE_NAME
        assert rule["enabled"] is True
        assert rule["type"] == "Lifecycle"
        actions = rule["definition"]["actions"]["baseBlob"]
        # Cool + Archive present; no delete by default
        assert "tierToCool" in actions
        assert "tierToArchive" in actions
        assert "delete" not in actions
        assert actions["tierToCool"]["daysAfterModificationGreaterThan"] == 30
        assert actions["tierToArchive"]["daysAfterModificationGreaterThan"] == 180

    def test_delete_action_added_when_delete_days_set(self) -> None:
        policy = build_adls_lifecycle_policy(
            container="lake", cool_days=30, archive_days=180, delete_days=365
        )
        actions = policy["policy"]["rules"][0]["definition"]["actions"]["baseBlob"]  # type: ignore[index]
        assert "delete" in actions
        assert actions["delete"]["daysAfterModificationGreaterThan"] == 365

    def test_prefix_match_targets_container(self) -> None:
        policy = build_adls_lifecycle_policy(container="bronze", cool_days=30, archive_days=180)
        filters = policy["policy"]["rules"][0]["definition"]["filters"]  # type: ignore[index]
        assert filters["blobTypes"] == ["blockBlob"]
        assert filters["prefixMatch"] == ["bronze/"]

    def test_rejects_archive_days_not_greater_than_cool_days(self) -> None:
        with pytest.raises(ValueError, match="archive_days"):
            build_adls_lifecycle_policy(container="lake", cool_days=30, archive_days=30)

    def test_rejects_delete_days_not_greater_than_archive_days(self) -> None:
        with pytest.raises(ValueError, match="delete_days"):
            build_adls_lifecycle_policy(
                container="lake",
                cool_days=30,
                archive_days=180,
                delete_days=180,
            )

    def test_rejects_zero_cool_days(self) -> None:
        with pytest.raises(ValueError, match="cool_days"):
            build_adls_lifecycle_policy(container="lake", cool_days=0, archive_days=180)


class TestSecurityScan:
    """Generated-artifact secret-leakage scanner (security concern)."""

    def test_clean_artifact_returns_no_warnings(self) -> None:
        """A real LinkedService JSON (with KV references throughout)
        produces zero leakage warnings — the scanner shouldn't false-
        positive on the safe pattern."""
        from lake_ingestion_framework.realms.adf_adls.generator.linked_services import (
            build_postgres_linked_service,
        )

        ls = build_postgres_linked_service()
        rendered = serialize_artifact(ls)
        warnings = scan_artifact_for_literal_secrets(name="LS_POSTGRES", rendered=rendered)
        assert warnings == []

    def test_flags_literal_password_in_connection_string(self) -> None:
        rendered = '{"connectionString": "Server=x;User Id=u;Password=hunter2"}'
        warnings = scan_artifact_for_literal_secrets(name="LS_BAD", rendered=rendered)
        assert len(warnings) == 1
        assert "embedded password" in warnings[0]
        assert "LS_BAD" in warnings[0]

    def test_skips_password_when_value_is_adf_expression(self) -> None:
        """`Password=@{linkedService().p_password_secret}` is the safe
        pattern — must not flag."""
        rendered = '{"connectionString": "Server=x;Password=@{linkedService().p_pwd}"}'
        warnings = scan_artifact_for_literal_secrets(name="LS_OK", rendered=rendered)
        assert warnings == []

    def test_flags_literal_account_key(self) -> None:
        # gitleaks:allow — fake-but-realistic test value for the scanner.
        fake_key = "YWJjZGVmZ2hpamtsbW5vcA=="  # gitleaks:allow
        rendered = (
            '{"connectionString": '
            f'"DefaultEndpointsProtocol=https;AccountName=x;AccountKey={fake_key}"}}'
        )
        warnings = scan_artifact_for_literal_secrets(name="LS_BAD", rendered=rendered)
        assert any("storage account key" in w for w in warnings)

    def test_flags_literal_bearer_token(self) -> None:
        rendered = '{"additionalHeaders": "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9"}'
        warnings = scan_artifact_for_literal_secrets(name="DS_BAD", rendered=rendered)
        assert any("bearer token" in w for w in warnings)

    def test_flags_sas_signature(self) -> None:
        rendered = (
            '{"sasUri": '
            '"https://storage.blob.core.windows.net/c?sv=2024-01-01&sig=ABCDEFGHIJKLMNOPQRST123"}'
        )
        warnings = scan_artifact_for_literal_secrets(name="LS_BAD", rendered=rendered)
        assert any("SAS signature" in w for w in warnings)

    def test_flags_shared_access_key(self) -> None:
        # gitleaks:allow — fake-but-realistic test value for the scanner.
        fake_key = "abcdefghijklmnop1234567890"  # gitleaks:allow
        rendered = (
            '{"connectionString": '
            '"Endpoint=sb://example.servicebus.windows.net/;'
            f'SharedAccessKeyName=root;SharedAccessKey={fake_key}"}}'
        )
        warnings = scan_artifact_for_literal_secrets(name="LS_BAD", rendered=rendered)
        assert any("shared access key" in w for w in warnings)

    def test_snippet_capped_at_80_chars(self) -> None:
        """Snippets are capped so warnings don't leak the full secret
        into ops logs / dashboards."""
        long_token = "x" * 200
        rendered = f'{{"x": "Bearer {long_token}"}}'
        warnings = scan_artifact_for_literal_secrets(name="LS_BAD", rendered=rendered)
        assert len(warnings) == 1
        # Snippet is the substring inside quotes; ensure no >80 chars
        # appear unbroken anywhere in the warning text.
        assert long_token not in warnings[0]


class TestAssemble:
    def test_emits_per_type_artifacts(self, seeded_repo: SqliteMetadataRepository) -> None:
        artifacts, warnings = plan_artifacts(seeded_repo)
        kinds = sorted({a.kind for a in artifacts})
        assert kinds == ["dataset", "linkedService", "pipeline", "storagePolicy", "trigger"]
        names = sorted(a.name for a in artifacts)
        # ONE LS per type, not per source
        assert "LS_AZURE_SQL_DB" in names
        assert "LS_ADLS_GEN2" in names
        assert "LS_KEY_VAULT" in names
        assert "LS_METADATA_DB" in names
        # Datasets
        assert "DS_AZURE_SQL_DB_TABLE" in names
        assert "DS_ADLS_PARQUET_SINK" in names
        assert "DS_METADATA_DB_QUERY" in names
        # Pipelines: ingest + per-TYPE driver + master driver
        assert "PL_INGEST_AZURE_SQL_DB" in names
        assert "PL_PER_SOURCE_DRIVER_AZURE_SQL_DB" in names
        assert "PL_MASTER_DRIVER" in names
        # Trigger
        assert "TR_DAILY" in names
        # No warnings on the happy path
        assert warnings == []

    def test_lifecycle_policy_artifact_emitted(self, seeded_repo: SqliteMetadataRepository) -> None:
        """plan_artifacts emits an ADLS lifecycle policy with platform
        defaults when target_storage_account is set. See docs/cost.md."""
        artifacts, _ = plan_artifacts(seeded_repo)
        lifecycle_arts = [a for a in artifacts if a.kind == "storagePolicy"]
        assert len(lifecycle_arts) == 1
        arc = lifecycle_arts[0]
        assert arc.name == "lifecycle"
        assert arc.relative_path == "storagePolicy/lifecycle.json"
        # Rendered JSON contains the rule with platform defaults
        # (cool_days=30, archive_days=180 — the framework default).
        rendered = arc.rendered
        assert '"name": "lake-ingest-tiering"' in rendered
        assert '"daysAfterModificationGreaterThan": 30' in rendered
        assert '"daysAfterModificationGreaterThan": 180' in rendered

    def test_lifecycle_warns_on_per_dataset_overrides(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """Datasets with bespoke retention warn the operator; the
        emitted policy still uses platform defaults."""
        # Find one of the seeded datasets and add a lifecycle override
        ds_list = seeded_repo.list_datasets()
        assert ds_list, "fixture should seed at least one dataset"
        ds = ds_list[0]
        existing_cfg = seeded_repo.get_dataset_config(ds.dataset_id)
        existing_cfg["lifecycle_cool_days"] = "7"  # override (≠ platform 30)
        seeded_repo.upsert_dataset(
            source_id=ds.source_id,
            dataset_name=ds.dataset_name,
            is_enabled=True,
            config=existing_cfg,
        )
        _, warnings = plan_artifacts(seeded_repo)
        assert any("lifecycle override" in w for w in warnings), warnings

    def test_two_sources_same_type_share_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """Adding a second Azure SQL DB source produces NO new artifacts."""
        before, _ = plan_artifacts(seeded_repo)
        seeded_repo.upsert_source(
            source_name="invoices_db",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
                "server": "invoices-server.database.windows.net",
                "database": "invoices",
            },
        )
        after, _ = plan_artifacts(seeded_repo)
        # Same artifact count + same names — adding a source is metadata-only
        assert len(before) == len(after)
        assert sorted(a.name for a in before) == sorted(a.name for a in after)

    def test_trigger_wired_to_master_driver(self, seeded_repo: SqliteMetadataRepository) -> None:
        artifacts, _ = plan_artifacts(seeded_repo)
        trigger = next(a for a in artifacts if a.kind == "trigger" and a.name == "TR_DAILY")
        # The trigger's pipelines array should reference PL_MASTER_DRIVER
        assert "PL_MASTER_DRIVER" in trigger.rendered

    def test_master_driver_dispatches_via_switch(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        artifacts, _ = plan_artifacts(seeded_repo)
        master = next(a for a in artifacts if a.kind == "pipeline" and a.name == "PL_MASTER_DRIVER")
        # Lookup against meta.v_enabled_sources, ForEach + Switch dispatch
        assert "v_enabled_sources" in master.rendered
        assert "ForEach" in master.rendered
        assert "Switch" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_AZURE_SQL_DB" in master.rendered

    def test_per_source_driver_lookup_query_references_views(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        artifacts, _ = plan_artifacts(seeded_repo)
        driver = next(
            a
            for a in artifacts
            if a.kind == "pipeline" and a.name == "PL_PER_SOURCE_DRIVER_AZURE_SQL_DB"
        )
        # Both Phase 6 views must be referenced
        assert "v_source_config" in driver.rendered
        assert "v_enabled_datasets" in driver.rendered
        assert "Lookup" in driver.rendered
        assert "ForEach" in driver.rendered
        assert "SetVariable" in driver.rendered

    def test_drivers_skipped_without_metadata_db(self) -> None:
        """No metadata_db_* config → no driver pipelines emitted."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                # metadata_db_server / metadata_db_database intentionally missing
            }
        )
        r.upsert_source(
            source_name="orders_db",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
                "server": "x.database.windows.net",
                "database": "orders",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            assert any("metadata_db_server" in w for w in warnings)
            names = {a.name for a in artifacts}
            assert "PL_PER_SOURCE_DRIVER_AZURE_SQL_DB" not in names
            assert "PL_MASTER_DRIVER" not in names
            # Ingest pipeline still emitted (doesn't depend on metadata DB)
            assert "PL_INGEST_AZURE_SQL_DB" in names
        finally:
            r.close()

    def test_sftp_full_config_emits_two_lses_two_datasets(self) -> None:
        """SFTP emits LS_SFTP_BASIC + LS_SFTP_SSH_KEY (auth not parameterisable)."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="partner_drop",
            source_env="dev",
            source_type=SourceType.SFTP,
            is_enabled=True,
            config={
                "connection_pattern": "sftp_basic",
                "auth_method": "basic",
                "server": "sftp.partner.com",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://my-vault/sftp-password",
            },
        )
        try:
            artifacts, _ = plan_artifacts(r)
            names = {a.name for a in artifacts}
            # Both SFTP LS variants ship together so customers can flip auth
            # method per source via metadata
            assert "LS_SFTP_BASIC" in names
            assert "LS_SFTP_SSH_KEY" in names
            assert "DS_SFTP_CSV" in names
            assert "DS_SFTP_JSON_LINES" in names
            assert "PL_INGEST_SFTP_CSV" in names
            assert "PL_INGEST_SFTP_JSON_LINES" in names
            # Per-type driver for SFTP — should contain a Switch on file_format
            sftp_driver = next(a for a in artifacts if a.name == "PL_PER_SOURCE_DRIVER_SFTP")
            assert "Switch" in sftp_driver.rendered
            assert "json_lines" in sftp_driver.rendered
            assert "csv" in sftp_driver.rendered
        finally:
            r.close()

    def test_rest_api_full_config_emits_resources(self) -> None:
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="hubspot",
            source_env="dev",
            source_type=SourceType.REST_API,
            is_enabled=True,
            config={
                "connection_pattern": "rest_bearer",
                "auth_method": "bearer",
                "base_url": "https://api.hubapi.com",
                "endpoints": '[{"name":"contacts","path":"/crm/v3/objects/contacts"}]',
                "connection_secret_name": "kv://my-vault/hubspot-token",
            },
        )
        try:
            artifacts, _ = plan_artifacts(r)
            names = {a.name for a in artifacts}
            assert "LS_REST_API_ANONYMOUS" in names
            assert "LS_REST_API_BASIC" in names
            assert "DS_REST_RESOURCE" in names
            assert "PL_INGEST_REST_API" in names
            assert "PL_PER_SOURCE_DRIVER_REST_API" in names
        finally:
            r.close()

    def test_byte_identical_on_rerun(self, seeded_repo: SqliteMetadataRepository) -> None:
        first, _ = plan_artifacts(seeded_repo)
        second, _ = plan_artifacts(seeded_repo)
        assert len(first) == len(second)
        for a, b in zip(first, second, strict=True):
            assert a.relative_path == b.relative_path
            assert a.rendered == b.rendered

    def test_warns_on_unsupported_source_type(self, seeded_repo: SqliteMetadataRepository) -> None:
        # SAP HANA — registered RDBMS shape, but no generator support yet.
        seeded_repo.upsert_source(
            source_name="erp_hana",
            source_env="dev",
            source_type=SourceType.SAP_HANA,
            is_enabled=True,
            config={
                "connection_pattern": "hana_basic",
                "auth_method": "basic",
                "server": "hana.example.com",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        assert any("erp_hana" in w for w in warnings)
        assert not any(a.name.startswith("LS_SAP_HANA") for a in artifacts)

    def test_warns_on_missing_storage_account(self) -> None:
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_source(
            source_name="orders_db",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
                "server": "x.database.windows.net",
                "database": "orders",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            assert any("target_storage_account" in w for w in warnings)
            assert any("Ingest pipelines skipped" in w for w in warnings)
            kinds = {a.kind for a in artifacts}
            assert "linkedService" in kinds  # Per-type LS still emitted
            # ADLS sink + ingest pipeline skipped
            assert not any(a.name == "LS_ADLS_GEN2" for a in artifacts)
            assert not any(a.name == "PL_INGEST_AZURE_SQL_DB" for a in artifacts)
        finally:
            r.close()

    def test_multi_source_type_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """One source per type → one LS+DS+PL per type (not per source)."""
        seeded_repo.upsert_source(
            source_name="orders_pg",
            source_env="dev",
            source_type=SourceType.POSTGRES,
            is_enabled=True,
            config={
                "connection_pattern": "postgres_basic",
                "auth_method": "basic",
                "server": "pg.example.com",
                "database": "orders",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://dummycustomera-dev-kv/orders-pg-password",
            },
        )
        seeded_repo.upsert_source(
            source_name="orders_mysql",
            source_env="dev",
            source_type=SourceType.MYSQL,
            is_enabled=True,
            config={
                "connection_pattern": "mysql_basic",
                "auth_method": "basic",
                "server": "mysql.example.com",
                "database": "orders",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://dummycustomera-dev-kv/orders-mysql-password",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        # Per-type LinkedServices (ONE per type, no source name suffix)
        assert "LS_AZURE_SQL_DB" in names
        assert "LS_POSTGRES" in names
        assert "LS_MYSQL" in names
        # Per-type Datasets
        assert "DS_AZURE_SQL_DB_TABLE" in names
        assert "DS_POSTGRES_TABLE" in names
        assert "DS_MYSQL_TABLE" in names
        # Per-type Ingest Pipelines
        assert "PL_INGEST_AZURE_SQL_DB" in names
        assert "PL_INGEST_POSTGRES" in names
        assert "PL_INGEST_MYSQL" in names
        # Per-type Per-Source Drivers
        assert "PL_PER_SOURCE_DRIVER_AZURE_SQL_DB" in names
        assert "PL_PER_SOURCE_DRIVER_POSTGRES" in names
        assert "PL_PER_SOURCE_DRIVER_MYSQL" in names
        # Oracle has no source — no DS/PL emitted
        assert "DS_ORACLE_TABLE" not in names
        assert "PL_INGEST_ORACLE" not in names
        assert "PL_PER_SOURCE_DRIVER_ORACLE" not in names
        # Master driver Switch contains cases for the three types only
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "azure_sql_db" in master.rendered
        assert "postgres" in master.rendered
        assert "mysql" in master.rendered
        assert "oracle" not in master.rendered
        assert warnings == []

    def test_salesforce_source_emits_per_type_artifacts(self) -> None:
        """Salesforce source produces LS_SALESFORCE + dataset + ingest +
        per-type driver. Driver dispatches via _rdbms_ingest_call (single
        ExecutePipeline; no Switch dispatch since Salesforce has one
        dataset type — SObject)."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="customer_crm",
            source_env="dev",
            source_type=SourceType.SALESFORCE,
            is_enabled=True,
            config={
                "connection_pattern": "salesforce_oauth",
                "auth_method": "service_account",
                "environment_url": "https://my-org.my.salesforce.com",
                "client_id": "3MVG9...REDACTED",
                "connection_secret_name": "kv://my-vault/sf-client-secret",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            names = sorted(a.name for a in artifacts)
            assert "LS_SALESFORCE" in names
            assert "DS_SALESFORCE_OBJECT" in names
            assert "PL_INGEST_SALESFORCE" in names
            assert "PL_PER_SOURCE_DRIVER_SALESFORCE" in names
            master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
            assert "salesforce" in master.rendered
            assert "PL_PER_SOURCE_DRIVER_SALESFORCE" in master.rendered
            assert warnings == []
        finally:
            r.close()

    def test_azure_blob_source_emits_per_type_artifacts(self) -> None:
        """Azure Blob source produces LS_AZURE_BLOB + two datasets +
        two ingest pipelines + per-type driver with Switch on file_format."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="partner_blob",
            source_env="dev",
            source_type=SourceType.AZURE_BLOB_X,
            is_enabled=True,
            config={
                "connection_pattern": "azure_blob_msi",
                "auth_method": "managed_identity",
                "storage_account": "partnerlandingsa",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            names = sorted(a.name for a in artifacts)
            assert "LS_AZURE_BLOB" in names
            assert "DS_AZURE_BLOB_CSV" in names
            assert "DS_AZURE_BLOB_JSON_LINES" in names
            assert "PL_INGEST_AZURE_BLOB_CSV" in names
            assert "PL_INGEST_AZURE_BLOB_JSON_LINES" in names
            blob_driver = next(
                a for a in artifacts if a.name == "PL_PER_SOURCE_DRIVER_AZURE_BLOB_X"
            )
            assert "Switch" in blob_driver.rendered
            assert "csv" in blob_driver.rendered
            assert "json_lines" in blob_driver.rendered
            master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
            assert "PL_PER_SOURCE_DRIVER_AZURE_BLOB_X" in master.rendered
            assert warnings == []
        finally:
            r.close()

    def test_s3_source_emits_per_type_artifacts(self) -> None:
        """S3 source produces LS_S3 + two datasets (CSV + JSON Lines) +
        two ingest pipelines + per-type driver with Switch dispatch on
        file_format."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="legacy_s3",
            source_env="dev",
            source_type=SourceType.S3,
            is_enabled=True,
            config={
                "connection_pattern": "s3_access_key",
                "auth_method": "basic",
                "access_key_id": "AKIA...REDACTED...",
                "connection_secret_name": "kv://my-vault/aws-secret-key",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            names = sorted(a.name for a in artifacts)
            assert "LS_S3" in names
            assert "DS_S3_CSV" in names
            assert "DS_S3_JSON_LINES" in names
            assert "PL_INGEST_S3_CSV" in names
            assert "PL_INGEST_S3_JSON_LINES" in names
            # Per-type driver — should contain a Switch on file_format
            s3_driver = next(a for a in artifacts if a.name == "PL_PER_SOURCE_DRIVER_S3")
            assert "Switch" in s3_driver.rendered
            assert "csv" in s3_driver.rendered
            assert "json_lines" in s3_driver.rendered
            # Master driver dispatches to the new per-type driver
            master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
            assert "PL_PER_SOURCE_DRIVER_S3" in master.rendered
            assert warnings == []
        finally:
            r.close()

    def test_synapse_sql_pool_source_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A Synapse SQL Pool source produces LS_SYNAPSE_SQL_POOL +
        per-type pipeline + driver."""
        seeded_repo.upsert_source(
            source_name="warehouse_synapse",
            source_env="dev",
            source_type=SourceType.SYNAPSE_SQL_POOL,
            is_enabled=True,
            config={
                "connection_pattern": "synapse_basic",
                "auth_method": "managed_identity",
                "server": "synapse-ws.sql.azuresynapse.net",
                "database": "warehouse_pool",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        assert "LS_SYNAPSE_SQL_POOL" in names
        assert "DS_SYNAPSE_SQL_POOL_TABLE" in names
        assert "PL_INGEST_SYNAPSE_SQL_POOL" in names
        assert "PL_PER_SOURCE_DRIVER_SYNAPSE_SQL_POOL" in names
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "synapse_sql_pool" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_SYNAPSE_SQL_POOL" in master.rendered
        assert warnings == []

    def test_servicenow_source_emits_per_type_artifacts(self) -> None:
        """ServiceNow source produces LS_SERVICENOW + dataset + ingest +
        per-type driver. Driver dispatches via _rdbms_ingest_call (single
        ExecutePipeline; no Switch dispatch — ServiceNow has one dataset
        type per source: a table)."""
        r = SqliteMetadataRepository(":memory:")
        r.bootstrap_schema()
        r.upsert_platform_config(
            {
                "target_storage_account": "mysa",
                "metadata_db_server": "sql.example.com",
                "metadata_db_database": "meta",
            }
        )
        r.upsert_source(
            source_name="itsm_snow",
            source_env="dev",
            source_type=SourceType.SERVICENOW,
            is_enabled=True,
            config={
                "connection_pattern": "servicenow_basic",
                "auth_method": "basic",
                "environment_url": "https://acme.service-now.com",
                "connection_user": "ingest_svc",
                "connection_secret_name": "kv://my-vault/snow-password",
            },
        )
        try:
            artifacts, warnings = plan_artifacts(r)
            names = sorted(a.name for a in artifacts)
            assert "LS_SERVICENOW" in names
            assert "DS_SERVICENOW_OBJECT" in names
            assert "PL_INGEST_SERVICENOW" in names
            assert "PL_PER_SOURCE_DRIVER_SERVICENOW" in names
            master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
            assert "servicenow" in master.rendered
            assert "PL_PER_SOURCE_DRIVER_SERVICENOW" in master.rendered
            assert warnings == []
        finally:
            r.close()

    def test_sap_hana_source_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A SAP HANA source produces LS_SAP_HANA + per-type pipeline + driver."""
        seeded_repo.upsert_source(
            source_name="erp_hana",
            source_env="dev",
            source_type=SourceType.SAP_HANA,
            is_enabled=True,
            config={
                "connection_pattern": "sap_hana_basic",
                "auth_method": "basic",
                "server": "hana.example.com:30015",
                "connection_user": "INGEST_USER",
                "connection_secret_name": "kv://dummycustomera-dev-kv/hana-password",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        assert "LS_SAP_HANA" in names
        assert "DS_SAP_HANA_TABLE" in names
        assert "PL_INGEST_SAP_HANA" in names
        assert "PL_PER_SOURCE_DRIVER_SAP_HANA" in names
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "sap_hana" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_SAP_HANA" in master.rendered
        assert warnings == []

    def test_bigquery_source_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A BigQuery source produces LS_BIGQUERY + per-type pipeline + driver."""
        seeded_repo.upsert_source(
            source_name="warehouse_bq",
            source_env="dev",
            source_type=SourceType.BIGQUERY,
            is_enabled=True,
            config={
                "connection_pattern": "bigquery_service_account",
                "auth_method": "service_account",
                "project_id": "my-gcp-analytics",
                "connection_secret_name": "kv://dummycustomera-dev-kv/bq-sa-key-json",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        assert "LS_BIGQUERY" in names
        assert "DS_BIGQUERY_TABLE" in names
        assert "PL_INGEST_BIGQUERY" in names
        assert "PL_PER_SOURCE_DRIVER_BIGQUERY" in names
        # Master driver dispatches to the new per-type driver
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "bigquery" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_BIGQUERY" in master.rendered
        assert warnings == []

    def test_snowflake_source_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A Snowflake source produces both LSs (basic + key_pair) +
        per-type pipeline + per-type driver."""
        seeded_repo.upsert_source(
            source_name="warehouse_sf",
            source_env="dev",
            source_type=SourceType.SNOWFLAKE,
            is_enabled=True,
            config={
                "connection_pattern": "snowflake_basic",
                "auth_method": "basic",
                "account": "myorg-myaccount",
                "warehouse": "INGEST_WH",
                "database": "ANALYTICS",
                "connection_user": "INGEST_USER",
                "connection_secret_name": "kv://dummycustomera-dev-kv/snowflake-password",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        # Two LSs ship together (auth_method isn't parameterisable in ADF)
        assert "LS_SNOWFLAKE_BASIC" in names
        assert "LS_SNOWFLAKE_KEY_PAIR" in names
        # Per-type DS + ingest + driver
        assert "DS_SNOWFLAKE_TABLE" in names
        assert "PL_INGEST_SNOWFLAKE" in names
        assert "PL_PER_SOURCE_DRIVER_SNOWFLAKE" in names
        # Master driver dispatches to the new per-type driver
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "snowflake" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_SNOWFLAKE" in master.rendered
        assert warnings == []

    def test_sql_server_source_emits_per_type_artifacts(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A SQL Server source produces LS_SQL_SERVER + per-type pipeline."""
        seeded_repo.upsert_source(
            source_name="legacy_sql",
            source_env="dev",
            source_type=SourceType.SQL_SERVER,
            is_enabled=True,
            config={
                "connection_pattern": "mssql_basic",
                "auth_method": "basic",
                "server": "sql.example.com,1433",
                "database": "legacy",
                "connection_user": "ingestor",
                "connection_secret_name": "kv://dummycustomera-dev-kv/legacy-sql-password",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        names = sorted(a.name for a in artifacts)
        assert "LS_SQL_SERVER" in names
        assert "DS_SQL_SERVER_TABLE" in names
        assert "PL_INGEST_SQL_SERVER" in names
        assert "PL_PER_SOURCE_DRIVER_SQL_SERVER" in names
        # Master driver must dispatch to the new per-type driver
        master = next(a for a in artifacts if a.name == "PL_MASTER_DRIVER")
        assert "sql_server" in master.rendered
        assert "PL_PER_SOURCE_DRIVER_SQL_SERVER" in master.rendered
        assert warnings == []

    def test_basic_auth_source_missing_connection_user_warns(
        self, seeded_repo: SqliteMetadataRepository
    ) -> None:
        """A Postgres source without connection_user can't be wired to ADF.

        The Postgres LS itself is still emitted *if* another valid Postgres
        source is present. Here it's the only Postgres source, so no
        Postgres LS/DS/PL is emitted.
        """
        seeded_repo.upsert_source(
            source_name="orders_pg",
            source_env="dev",
            source_type=SourceType.POSTGRES,
            is_enabled=True,
            config={
                "connection_pattern": "postgres_basic",
                "auth_method": "basic",
                "server": "pg.example.com",
                "database": "orders",
                # connection_user intentionally omitted
                "connection_secret_name": "kv://dummycustomera-dev-kv/orders-pg-password",
            },
        )
        artifacts, warnings = plan_artifacts(seeded_repo)
        assert any("connection_user" in w for w in warnings)
        # No Postgres LS/DS/PL emitted — the only source of that type was bad
        assert not any(a.name == "LS_POSTGRES" for a in artifacts)
        assert not any(a.name == "DS_POSTGRES_TABLE" for a in artifacts)
        assert not any(a.name == "PL_INGEST_POSTGRES" for a in artifacts)
