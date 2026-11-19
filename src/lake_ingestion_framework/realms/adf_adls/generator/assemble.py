"""Assemble ADF artifacts from metadata.

Top-level entry point: ``plan_artifacts(repo)`` reads everything from
the metadata DB, runs the per-resource builders, and returns a list
of ``Artifact`` records describing what to write where.

Phase 5d shape (per-type artifacts, not per-source):

- LinkedServices: one per source type (or per type+auth-method where
  ADF forces a split — SFTP basic vs SSH-key, REST basic vs anonymous).
  Connection details are LS parameters, resolved at runtime by the
  Per-Source Driver pipeline. Plus shared ADLS Gen2 + Key Vault +
  Metadata DB LinkedServices (one per env).
- Datasets: one per source type (or per type+file-format for SFTP)
  + ADLS Parquet sink + metadata DB query dataset. Datasets declare
  conn-param parameters and forward them into the parameterised LS.
- Ingest pipelines: one per source type that's actually present.
- Driver pipelines: one ``PL_PER_SOURCE_DRIVER_<TYPE>`` per source
  type present, plus one ``PL_MASTER_DRIVER`` that fans out to all
  of them via Lookup → ForEach → Switch.
- Trigger: ``TR_DAILY`` wired to ``PL_MASTER_DRIVER`` (when present).

The big ergonomic win: **adding a new source of an existing type is a
metadata-only change**. The artifacts are byte-identical; only the
metadata DB grows a row. Customer doesn't re-deploy.

A LinkedService is emitted whenever **any** source of that type has
sufficient config. Per-source config validation still runs (and surfaces
warnings) so missing fields don't silently land in the metadata.

Out of scope (later phases):
- ARM template wrapper
- ``az datafactory`` deploy via Python SDK
- Runtime metadata views ``meta.v_enabled_sources`` / ``v_source_config``
  / ``v_enabled_datasets`` (Phase 6)

Determinism rules (load-bearing per CLAUDE.md):
- Sources iterated in alphabetical order.
- Output file paths are stable.
- Inside each artifact, the serializer sorts keys.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import (
    MetadataRepository,
    SourceRow,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import KvRef
from lake_ingestion_framework.realms.adf_adls.generator import (
    datasets as ds_builders,
)
from lake_ingestion_framework.realms.adf_adls.generator import (
    lifecycle,
    naming,
    pipelines,
    triggers,
)
from lake_ingestion_framework.realms.adf_adls.generator import (
    linked_services as ls_builders,
)
from lake_ingestion_framework.realms.adf_adls.generator._security_scan import (
    scan_artifact_for_literal_secrets,
)
from lake_ingestion_framework.realms.adf_adls.generator._serializer import (
    serialize_artifact,
)

# ──────────────────────────────────────────────────────────────────────
# Output dataclass
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Artifact:
    """One ADF resource ready to write to disk."""

    kind: str
    name: str
    relative_path: str
    rendered: str


# ──────────────────────────────────────────────────────────────────────
# Per-source-type wiring
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _SourceTypeWiring:
    """Builders + naming hooks for one source type.

    ``build_linked_services`` returns ``((name, content), ...)`` so a
    type can emit multiple LSs (SFTP basic + SFTP ssh_key, REST basic
    + REST anonymous). ``build_datasets`` and ``build_ingest_pipelines``
    follow the same shape so SFTP can ship two datasets (CSV + JSON
    Lines) and two ingest pipelines.

    ``validate_source`` runs per source row and either records a
    warning (returning False) or accepts the row (returning True).
    The LS/DS/PL artifacts are emitted once per type as long as at
    least one source of that type has a valid config.
    """

    source_type: SourceType
    build_linked_services: Callable[[], tuple[tuple[str, dict[str, object]], ...]]
    build_datasets: Callable[[], tuple[tuple[str, dict[str, object]], ...]]
    build_ingest_pipelines: Callable[[], tuple[tuple[str, dict[str, object]], ...]]
    validate_source: Callable[[SourceRow, dict[str, str]], None]


# ── Per-source-type config validators (raise _MissingConfigError on bad config)


def _validate_azure_sql_db(src: SourceRow, cfg: dict[str, str]) -> None:
    """Azure SQL DB needs server + database (MSI auth — no creds)."""
    _ = src
    if not cfg.get("server"):
        raise _MissingConfigError("server")
    if not cfg.get("database"):
        raise _MissingConfigError("database")


def _validate_basic_auth_rdbms(src: SourceRow, cfg: dict[str, str]) -> None:
    """Postgres / MySQL — need server + database + connection_user + secret."""
    _ = src
    if not cfg.get("server"):
        raise _MissingConfigError("server")
    if not cfg.get("database"):
        raise _MissingConfigError("database")
    if not cfg.get("connection_user"):
        raise _MissingConfigError(
            "connection_user (set this when using basic auth — the secret then holds password only)"
        )
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    KvRef.parse(secret_ref_raw)  # validates shape; raises if malformed


def _validate_bigquery(src: SourceRow, cfg: dict[str, str]) -> None:
    """BigQuery — project_id + connection_secret_name (KV holds the
    service-account JSON key file content).

    auth_method is fixed to 'service_account'. ADF's GoogleBigQueryV2
    LS only supports service-account auth in our setup; OAuth user
    auth is not modelled.
    """
    _ = src
    if not cfg.get("project_id"):
        raise _MissingConfigError("project_id (the GCP project that owns the BigQuery datasets)")
    auth_method = cfg.get("auth_method", "service_account")
    if auth_method != "service_account":
        raise _MissingConfigError(
            f"auth_method must be 'service_account' for BigQuery; got {auth_method!r}"
        )
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError(
            "connection_secret_name (kv://<vault>/<secret>) — KV must hold the "
            "service-account JSON key file content"
        )
    KvRef.parse(secret_ref_raw)


def _validate_snowflake(src: SourceRow, cfg: dict[str, str]) -> None:
    """Snowflake — account + warehouse + database + connection_user + secret.

    Auth method is basic by default; key_pair LS exists but Phase A
    ships only basic-auth dataset binding. Customers using key_pair
    rebind ``DS_SNOWFLAKE_TABLE`` to ``LS_SNOWFLAKE_KEY_PAIR``
    post-deploy.
    """
    _ = src
    if not cfg.get("account"):
        raise _MissingConfigError("account (e.g. 'myorg-myaccount' or 'xy12345.us-east-1')")
    if not cfg.get("warehouse"):
        raise _MissingConfigError("warehouse")
    if not cfg.get("database"):
        raise _MissingConfigError("database")
    if not cfg.get("connection_user"):
        raise _MissingConfigError("connection_user")
    auth_method = cfg.get("auth_method", "basic")
    if auth_method not in {"basic", "key_pair"}:
        raise _MissingConfigError(
            f"auth_method must be 'basic' or 'key_pair' for Snowflake; got {auth_method!r}"
        )
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    KvRef.parse(secret_ref_raw)


def _validate_oracle(src: SourceRow, cfg: dict[str, str]) -> None:
    """Oracle uses service_name (not database) + connection_user + secret."""
    _ = src
    if not cfg.get("server"):
        raise _MissingConfigError("server")
    if not cfg.get("service_name"):
        raise _MissingConfigError("service_name")
    if not cfg.get("connection_user"):
        raise _MissingConfigError("connection_user")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    KvRef.parse(secret_ref_raw)


def _validate_sap_hana(src: SourceRow, cfg: dict[str, str]) -> None:
    """SAP HANA — server + connection_user + connection_secret_name.

    HANA has no per-connection ``database`` concept (schemas are
    referenced inline in the SELECT), so unlike Postgres / MySQL the
    validator doesn't require ``database``. Customers needing the
    multi-tenant SAP_CLIENT (MANDT) filter set ``sap_client`` in
    metadata; the framework injects it into the WHERE clause when
    present.
    """
    _ = src
    if not cfg.get("server"):
        raise _MissingConfigError("server (host:port; e.g. 'hana.example.com:30015')")
    if not cfg.get("connection_user"):
        raise _MissingConfigError("connection_user")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    KvRef.parse(secret_ref_raw)


def _validate_servicenow(src: SourceRow, cfg: dict[str, str]) -> None:
    """ServiceNow — environment_url + connection_user + secret.

    Phase A ships HTTP Basic auth via ADF's legacy ``ServiceNow``
    connector — a service-account user with REST API access on the
    target instance, password in Key Vault. OAuth 2.0 (via the newer
    ``ServiceNowV2`` connector) lands in Phase B.
    """
    _ = src
    if not cfg.get("environment_url"):
        raise _MissingConfigError(
            "environment_url (ServiceNow instance URL — e.g. " "'https://acme.service-now.com')"
        )
    if not cfg.get("connection_user"):
        raise _MissingConfigError("connection_user (ServiceNow REST API service account)")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError(
            "connection_secret_name (kv://<vault>/<secret>) — KV must hold "
            "the service account's password"
        )
    KvRef.parse(secret_ref_raw)


def _validate_salesforce(src: SourceRow, cfg: dict[str, str]) -> None:
    """Salesforce — environment_url + client_id + connection_secret_name.

    auth_method is fixed to OAuth 2.0 Client Credentials in Phase A
    (the SalesforceV2 connector's recommended grant). KV holds the
    Connected App's client secret. JWT bearer flow lands in Phase B.
    """
    _ = src
    if not cfg.get("environment_url"):
        raise _MissingConfigError(
            "environment_url (Salesforce 'My Domain' URL — e.g. "
            "'https://my-org.my.salesforce.com')"
        )
    if not cfg.get("client_id"):
        raise _MissingConfigError("client_id (Connected App's OAuth consumer key)")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError(
            "connection_secret_name (kv://<vault>/<secret>) — KV must hold "
            "the Connected App's client secret"
        )
    KvRef.parse(secret_ref_raw)


def _validate_azure_blob(src: SourceRow, cfg: dict[str, str]) -> None:
    """Azure Blob — storage_account (MSI auth, no secret in v1).

    The ADF MSI must hold ``Storage Blob Data Reader`` on the
    account or specific containers (granted out-of-band). SAS /
    account-key auth variants land in Phase B; until then,
    ``connection_secret_name`` is not required.
    """
    _ = src
    if not cfg.get("storage_account"):
        raise _MissingConfigError(
            "storage_account (Azure Storage account name without " ".blob.core.windows.net suffix)"
        )


def _validate_s3(src: SourceRow, cfg: dict[str, str]) -> None:
    """S3 — access_key_id + connection_secret_name (KV holds the secret access key).

    auth_method is fixed to 'access_key' / unset; STS / role-assumption
    flows are deferred. The Per-Source Driver pulls
    p_access_key_id + p_secret_access_key_secret from v_source_config
    and passes them down to the dataset → LS chain at activity time.
    """
    _ = src
    if not cfg.get("access_key_id"):
        raise _MissingConfigError("access_key_id (the AWS access key for the IAM user)")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError(
            "connection_secret_name (kv://<vault>/<secret>) — KV must hold the "
            "AWS secret access key"
        )
    KvRef.parse(secret_ref_raw)


def _validate_sftp(src: SourceRow, cfg: dict[str, str]) -> None:
    """SFTP — server + connection_user + secret + auth_method."""
    _ = src
    if not cfg.get("server"):
        raise _MissingConfigError("server")
    if not (cfg.get("connection_user") or cfg.get("user")):
        raise _MissingConfigError("connection_user (or user, for ssh_key auth)")
    if not cfg.get("connection_secret_name"):
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    auth_method = cfg.get("auth_method", "basic")
    if auth_method not in {"basic", "ssh_key"}:
        raise _MissingConfigError(
            f"auth_method must be 'basic' or 'ssh_key' for SFTP; got {auth_method!r}"
        )
    KvRef.parse(cfg["connection_secret_name"])


def _validate_rest_api(src: SourceRow, cfg: dict[str, str]) -> None:
    """REST API — base_url + secret + auth_method."""
    _ = src
    if not cfg.get("base_url"):
        raise _MissingConfigError("base_url")
    auth_method = cfg.get("auth_method", "bearer")
    if auth_method not in {"bearer", "basic"}:
        raise _MissingConfigError(
            f"auth_method must be 'bearer' or 'basic' for REST API; got {auth_method!r}"
        )
    if auth_method == "basic" and not cfg.get("connection_user"):
        raise _MissingConfigError("connection_user (required for REST basic auth)")
    secret_ref_raw = cfg.get("connection_secret_name")
    if not secret_ref_raw:
        raise _MissingConfigError("connection_secret_name (kv://<vault>/<secret>)")
    KvRef.parse(secret_ref_raw)


# ── Wiring registry


_WIRINGS: tuple[_SourceTypeWiring, ...] = (
    _SourceTypeWiring(
        source_type=SourceType.AZURE_SQL_DB,
        build_linked_services=lambda: (
            (
                naming.linked_service_azure_sql_db(),
                ls_builders.build_azure_sql_db_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_azure_sql_db_table(),
                ds_builders.build_azure_sql_db_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_azure_sql_db(), pipelines.build_pl_ingest_azure_sql_db()),
        ),
        validate_source=_validate_azure_sql_db,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SYNAPSE_SQL_POOL,
        build_linked_services=lambda: (
            (
                naming.linked_service_synapse_sql_pool(),
                ls_builders.build_synapse_sql_pool_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_synapse_sql_pool_table(),
                ds_builders.build_synapse_sql_pool_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (
                naming.pipeline_ingest_synapse_sql_pool(),
                pipelines.build_pl_ingest_synapse_sql_pool(),
            ),
        ),
        # MSI auth — same shape as Azure SQL DB (server + database).
        validate_source=_validate_azure_sql_db,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SQL_SERVER,
        build_linked_services=lambda: (
            (
                naming.linked_service_sql_server(),
                ls_builders.build_sql_server_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_sql_server_table(),
                ds_builders.build_sql_server_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_sql_server(), pipelines.build_pl_ingest_sql_server()),
        ),
        validate_source=_validate_basic_auth_rdbms,
    ),
    _SourceTypeWiring(
        source_type=SourceType.POSTGRES,
        build_linked_services=lambda: (
            (
                naming.linked_service_postgres(),
                ls_builders.build_postgres_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_postgres_table(),
                ds_builders.build_postgres_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_postgres(), pipelines.build_pl_ingest_postgres()),
        ),
        validate_source=_validate_basic_auth_rdbms,
    ),
    _SourceTypeWiring(
        source_type=SourceType.MYSQL,
        build_linked_services=lambda: (
            (
                naming.linked_service_mysql(),
                ls_builders.build_mysql_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_mysql_table(),
                ds_builders.build_mysql_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_mysql(), pipelines.build_pl_ingest_mysql()),
        ),
        validate_source=_validate_basic_auth_rdbms,
    ),
    _SourceTypeWiring(
        source_type=SourceType.ORACLE,
        build_linked_services=lambda: (
            (
                naming.linked_service_oracle(),
                ls_builders.build_oracle_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_oracle_table(),
                ds_builders.build_oracle_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_oracle(), pipelines.build_pl_ingest_oracle()),
        ),
        validate_source=_validate_oracle,
    ),
    _SourceTypeWiring(
        source_type=SourceType.BIGQUERY,
        build_linked_services=lambda: (
            (
                naming.linked_service_bigquery(),
                ls_builders.build_bigquery_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_bigquery_table(),
                ds_builders.build_bigquery_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_bigquery(), pipelines.build_pl_ingest_bigquery()),
        ),
        validate_source=_validate_bigquery,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SNOWFLAKE,
        build_linked_services=lambda: (
            (
                naming.linked_service_snowflake_basic(),
                ls_builders.build_snowflake_basic_linked_service(),
            ),
            (
                naming.linked_service_snowflake_key_pair(),
                ls_builders.build_snowflake_key_pair_linked_service(),
            ),
        ),
        # Phase A: dataset is bound to LS_SNOWFLAKE_BASIC by default.
        # Customers using key-pair rebind to LS_SNOWFLAKE_KEY_PAIR
        # post-deploy. Phase B may add per-auth datasets if needed.
        build_datasets=lambda: (
            (
                naming.dataset_snowflake_table(),
                ds_builders.build_snowflake_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_snowflake(), pipelines.build_pl_ingest_snowflake()),
        ),
        validate_source=_validate_snowflake,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SAP_HANA,
        build_linked_services=lambda: (
            (
                naming.linked_service_sap_hana(),
                ls_builders.build_sap_hana_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_sap_hana_table(),
                ds_builders.build_sap_hana_table_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_sap_hana(), pipelines.build_pl_ingest_sap_hana()),
        ),
        validate_source=_validate_sap_hana,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SALESFORCE,
        build_linked_services=lambda: (
            (
                naming.linked_service_salesforce(),
                ls_builders.build_salesforce_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_salesforce_object(),
                ds_builders.build_salesforce_object_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_salesforce(), pipelines.build_pl_ingest_salesforce()),
        ),
        validate_source=_validate_salesforce,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SERVICENOW,
        build_linked_services=lambda: (
            (
                naming.linked_service_servicenow(),
                ls_builders.build_servicenow_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (
                naming.dataset_servicenow_object(),
                ds_builders.build_servicenow_object_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_servicenow(), pipelines.build_pl_ingest_servicenow()),
        ),
        validate_source=_validate_servicenow,
    ),
    _SourceTypeWiring(
        source_type=SourceType.AZURE_BLOB_X,
        build_linked_services=lambda: (
            (
                naming.linked_service_azure_blob(),
                ls_builders.build_azure_blob_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (naming.dataset_azure_blob_csv(), ds_builders.build_azure_blob_csv_dataset()),
            (
                naming.dataset_azure_blob_json_lines(),
                ds_builders.build_azure_blob_json_lines_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (
                naming.pipeline_ingest_azure_blob_csv(),
                pipelines.build_pl_ingest_azure_blob_csv(),
            ),
            (
                naming.pipeline_ingest_azure_blob_json_lines(),
                pipelines.build_pl_ingest_azure_blob_json_lines(),
            ),
        ),
        validate_source=_validate_azure_blob,
    ),
    _SourceTypeWiring(
        source_type=SourceType.S3,
        build_linked_services=lambda: (
            (
                naming.linked_service_s3(),
                ls_builders.build_s3_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (naming.dataset_s3_csv(), ds_builders.build_s3_csv_dataset()),
            (
                naming.dataset_s3_json_lines(),
                ds_builders.build_s3_json_lines_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_s3_csv(), pipelines.build_pl_ingest_s3_csv()),
            (
                naming.pipeline_ingest_s3_json_lines(),
                pipelines.build_pl_ingest_s3_json_lines(),
            ),
        ),
        validate_source=_validate_s3,
    ),
    _SourceTypeWiring(
        source_type=SourceType.SFTP,
        build_linked_services=lambda: (
            (
                naming.linked_service_sftp_basic(),
                ls_builders.build_sftp_basic_linked_service(),
            ),
            (
                naming.linked_service_sftp_ssh_key(),
                ls_builders.build_sftp_ssh_key_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (naming.dataset_sftp_csv(), ds_builders.build_sftp_csv_dataset()),
            (
                naming.dataset_sftp_json_lines(),
                ds_builders.build_sftp_json_lines_dataset(),
            ),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_sftp_csv(), pipelines.build_pl_ingest_sftp_csv()),
            (
                naming.pipeline_ingest_sftp_json_lines(),
                pipelines.build_pl_ingest_sftp_json_lines(),
            ),
        ),
        validate_source=_validate_sftp,
    ),
    _SourceTypeWiring(
        source_type=SourceType.REST_API,
        build_linked_services=lambda: (
            (
                naming.linked_service_rest_api_anonymous(),
                ls_builders.build_rest_api_anonymous_linked_service(),
            ),
            (
                naming.linked_service_rest_api_basic(),
                ls_builders.build_rest_api_basic_linked_service(),
            ),
        ),
        build_datasets=lambda: (
            (naming.dataset_rest_resource(), ds_builders.build_rest_resource_dataset()),
        ),
        build_ingest_pipelines=lambda: (
            (naming.pipeline_ingest_rest_api(), pipelines.build_pl_ingest_rest_api()),
        ),
        validate_source=_validate_rest_api,
    ),
)

_SUPPORTED_TYPES: frozenset[SourceType] = frozenset(w.source_type for w in _WIRINGS)


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def plan_artifacts(repo: MetadataRepository) -> tuple[list[Artifact], list[str]]:
    """Walk the metadata DB and return (artifacts, warnings)."""
    platform_config = repo.list_platform_config()
    warnings: list[str] = []
    artifacts: list[Artifact] = []

    sources = sorted(repo.list_sources(), key=lambda s: s.source_name)
    by_type: dict[SourceType, list[SourceRow]] = {}

    for src in sources:
        if src.source_type not in _SUPPORTED_TYPES:
            warnings.append(
                f"source {src.source_name!r} ({src.source_type.value}) "
                "skipped — generator support not yet shipped."
            )
            continue
        by_type.setdefault(src.source_type, []).append(src)

    storage_account = platform_config.get("target_storage_account")
    key_vault_uri = platform_config.get("key_vault_uri")
    metadata_db_server = platform_config.get("metadata_db_server")
    metadata_db_database = platform_config.get("metadata_db_database")

    # ── Validate each source's config (so warnings surface for typos /
    #    missing fields) and remember which types have at least one
    #    valid source. Per-type artifacts are emitted iff at least one
    #    source of that type passes validation.
    valid_types: set[SourceType] = set()
    for wiring in _WIRINGS:
        for src in by_type.get(wiring.source_type, []):
            cfg = repo.get_source_config(src.source_id)
            try:
                wiring.validate_source(src, cfg)
            except _MissingConfigError as exc:
                warnings.append(
                    f"source {src.source_name!r} ({wiring.source_type.value}) "
                    f"missing required config: {exc}; LinkedService skipped."
                )
                continue
            valid_types.add(wiring.source_type)

    # ── Per-type LinkedServices (one or two per type, NOT per source)
    for wiring in _WIRINGS:
        if wiring.source_type not in valid_types:
            continue
        for ls_name, ls_content in wiring.build_linked_services():
            artifacts.append(_to_artifact(kind="linkedService", name=ls_name, content=ls_content))

    # ── Shared LinkedServices
    if storage_account:
        artifacts.append(
            _to_artifact(
                kind="linkedService",
                name=naming.linked_service_adls_gen2(),
                content=ls_builders.build_adls_gen2_linked_service(
                    storage_account=storage_account,
                ),
            )
        )
    else:
        warnings.append(
            "platform_config.target_storage_account not set — ADLS LinkedService skipped."
        )

    if key_vault_uri:
        artifacts.append(
            _to_artifact(
                kind="linkedService",
                name=naming.linked_service_key_vault(),
                content=ls_builders.build_key_vault_linked_service(
                    key_vault_uri=key_vault_uri,
                ),
            )
        )

    if metadata_db_server and metadata_db_database:
        artifacts.append(
            _to_artifact(
                kind="linkedService",
                name=naming.linked_service_metadata_db(),
                content=ls_builders.build_metadata_db_linked_service(
                    server=metadata_db_server,
                    database=metadata_db_database,
                ),
            )
        )
    elif valid_types:
        warnings.append(
            "platform_config.metadata_db_server / metadata_db_database not set — "
            "LS_METADATA_DB skipped. Driver pipelines reference it; they won't "
            "deploy successfully without it."
        )

    # ── Datasets per source type
    for wiring in _WIRINGS:
        if wiring.source_type not in valid_types:
            continue
        for ds_name, ds_content in wiring.build_datasets():
            artifacts.append(_to_artifact(kind="dataset", name=ds_name, content=ds_content))

    # ── Shared datasets
    if storage_account:
        artifacts.append(
            _to_artifact(
                kind="dataset",
                name=naming.dataset_adls_parquet_sink(),
                content=ds_builders.build_adls_parquet_sink_dataset(),
            )
        )

    if metadata_db_server and metadata_db_database:
        artifacts.append(
            _to_artifact(
                kind="dataset",
                name=naming.dataset_metadata_db_query(),
                content=ds_builders.build_metadata_db_query_dataset(),
            )
        )

    # ── Per-source-type ingest Pipelines
    if not storage_account:
        warnings.append(
            "Ingest pipelines skipped — at least one supported source type "
            "and target_storage_account must both be configured."
        )
    else:
        for wiring in _WIRINGS:
            if wiring.source_type not in valid_types:
                continue
            for pl_name, pl_content in wiring.build_ingest_pipelines():
                artifacts.append(_to_artifact(kind="pipeline", name=pl_name, content=pl_content))

    # ── Per-type Per-Source Driver pipelines (one per source TYPE present)
    can_emit_drivers = bool(metadata_db_server and metadata_db_database and storage_account)
    if can_emit_drivers and valid_types:
        for wiring in _WIRINGS:
            if wiring.source_type not in valid_types:
                continue
            artifacts.append(
                _to_artifact(
                    kind="pipeline",
                    name=naming.pipeline_per_source_driver(wiring.source_type.value),
                    content=pipelines.build_pl_per_source_driver(
                        source_type=wiring.source_type.value,
                    ),
                )
            )

        # ── Master driver — one Switch case per type present
        artifacts.append(
            _to_artifact(
                kind="pipeline",
                name=naming.pipeline_master_driver(),
                content=pipelines.build_pl_master_driver(
                    source_types_present=sorted(t.value for t in valid_types),
                ),
            )
        )

    # ── Trigger
    schedule_cron = platform_config.get("default_schedule_cron")
    artifacts.append(
        _to_artifact(
            kind="trigger",
            name=naming.trigger_daily(),
            content=triggers.build_daily_trigger(
                schedule_cron=schedule_cron,
                invoke_master_driver=can_emit_drivers and bool(valid_types),
            ),
        )
    )

    # ── ADLS lifecycle policy (cost concern; see docs/cost.md)
    if storage_account:
        artifacts.append(
            _emit_lifecycle_artifact(
                repo=repo,
                platform_config=platform_config,
                warnings=warnings,
            )
        )

    artifacts.sort(key=lambda a: (a.kind, a.name))

    # ── Defensive secret-leakage scan over every emitted artifact.
    # Catches accidental literals (Password=..., AccountKey=...,
    # Bearer ..., SAS sig=...) before they hit disk. See
    # docs/security.md. Pre-commit gitleaks is the second line of
    # defense; this scanner specifically guards generator output.
    for art in artifacts:
        warnings.extend(scan_artifact_for_literal_secrets(name=art.name, rendered=art.rendered))

    return artifacts, warnings


def _emit_lifecycle_artifact(
    *,
    repo: MetadataRepository,
    platform_config: dict[str, str],
    warnings: list[str],
) -> Artifact:
    """Build the ADLS lifecycle policy artifact + record any override warnings.

    Single platform-level rule targeting the entire target container —
    runtime-rendered prefixes (``{yyyy}/{mm}/{dd}`` etc.) make
    per-dataset prefixMatch unreliable. Per-dataset overrides are
    detected and warned about; operators apply bespoke policies
    manually for those datasets.
    """
    container = platform_config.get("target_container_default", "lake")
    cool_days = int(platform_config.get("lifecycle_cool_days", "30"))
    archive_days = int(platform_config.get("lifecycle_archive_days", "180"))

    override_count = 0
    for ds in repo.list_datasets():
        ds_cfg = repo.get_dataset_config(ds.dataset_id)
        ds_cool = ds_cfg.get("lifecycle_cool_days")
        ds_archive = ds_cfg.get("lifecycle_archive_days")
        ds_delete = ds_cfg.get("lifecycle_delete_days")
        if not any(v is not None for v in (ds_cool, ds_archive, ds_delete)):
            continue
        ds_cool_v = int(ds_cool) if ds_cool is not None else cool_days
        ds_archive_v = int(ds_archive) if ds_archive is not None else archive_days
        ds_delete_v = int(ds_delete) if ds_delete is not None else 0
        if (ds_cool_v, ds_archive_v, ds_delete_v) != (cool_days, archive_days, 0):
            override_count += 1
    if override_count:
        warnings.append(
            f"{override_count} dataset(s) have per-dataset lifecycle overrides; "
            "the emitted ADLS policy uses platform defaults — apply per-dataset "
            "policies manually if their retention requirements differ."
        )

    policy = lifecycle.build_adls_lifecycle_policy(
        container=container,
        cool_days=cool_days,
        archive_days=archive_days,
    )
    return Artifact(
        kind="storagePolicy",
        name="lifecycle",
        relative_path="storagePolicy/lifecycle.json",
        rendered=serialize_artifact(policy),
    )


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


class _MissingConfigError(Exception):
    """Internal — surfaces as a warning rather than crashing the run."""


def _to_artifact(*, kind: str, name: str, content: dict[str, object]) -> Artifact:
    if content.get("name") != name:
        raise ValidationError(
            f"Builder for {kind}/{name} produced content with name="
            f"{content.get('name')!r}; the artifact name + content.name must match."
        )
    return Artifact(
        kind=kind,
        name=name,
        relative_path=f"{kind}/{name}.json",
        rendered=serialize_artifact(content),
    )


__all__ = ["Artifact", "plan_artifacts"]
