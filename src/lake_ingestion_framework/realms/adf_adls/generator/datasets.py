"""ADF Dataset builders.

One dataset per source-type (or per type+file-format for SFTP),
parameterised over **connection details + dataset details**:

- ``schema_name`` / ``table_name`` (or ``folder_path`` / ``file_name``,
  or ``relative_url``) — the per-dataset bits.
- ``p_server`` / ``p_database`` / ``p_user`` / ``p_password_secret``
  (or ``p_base_url``, ``p_private_key_secret``) — the per-source
  bits, forwarded into the parameterised LinkedService.

All these are bound at Copy-Activity invocation time (in
``pipelines.py``) — the Per-Source Driver pipeline supplies the
connection values from a Lookup against ``meta.v_source_config``,
and ``@item().*`` from the Lookup against ``meta.v_enabled_datasets``
supplies the dataset values.

Per-dataset JSON files are config, not data — so they live next to
LinkedServices in ADF's source-control layout.
"""

from __future__ import annotations

from lake_ingestion_framework.realms.adf_adls.generator import naming


def _ls_param_passthrough(*param_names: str) -> dict[str, object]:
    """Build the ``parameters`` payload for a LinkedServiceReference.

    Each named param resolves to ``@dataset().<name>`` so the value
    flows from Copy Activity → dataset → LinkedService.
    """
    return {
        param: {
            "value": f"@dataset().{param}",
            "type": "Expression",
        }
        for param in param_names
    }


def _rdbms_dataset_params(*conn_params: str) -> dict[str, object]:
    """Standard parameter block for an RDBMS table dataset."""
    base: dict[str, object] = {
        "schema_name": {"type": "String"},
        "table_name": {"type": "String"},
    }
    base.update({p: {"type": "String"} for p in conn_params})
    return base


def _rdbms_table_typeproperties() -> dict[str, object]:
    """Standard ``typeProperties`` for an RDBMS table dataset.

    Schema + table forwarded from the dataset's ``schema_name`` /
    ``table_name`` parameters. Uniform across PG / MySQL / Oracle /
    Azure SQL DB.
    """
    return {
        "schema": {
            "value": "@dataset().schema_name",
            "type": "Expression",
        },
        "table": {
            "value": "@dataset().table_name",
            "type": "Expression",
        },
    }


def build_azure_sql_db_table_dataset() -> dict[str, object]:
    """Generic Azure SQL DB table dataset; parameterised over LS conn + table."""
    return {
        "name": naming.dataset_azure_sql_db_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_azure_sql_db(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough("p_server", "p_database"),
            },
            "parameters": _rdbms_dataset_params("p_server", "p_database"),
            "type": "AzureSqlTable",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def build_postgres_table_dataset() -> dict[str, object]:
    """Generic Postgres table dataset; parameterised over LS conn + table."""
    conn_params = ("p_server", "p_database", "p_user", "p_password_secret")
    return {
        "name": naming.dataset_postgres_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_postgres(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "PostgreSqlV2Table",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def build_mysql_table_dataset() -> dict[str, object]:
    """Generic MySQL table dataset; parameterised over LS conn + table."""
    conn_params = ("p_server", "p_database", "p_user", "p_password_secret")
    return {
        "name": naming.dataset_mysql_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_mysql(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "MySqlTable",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def _snowflake_table_typeproperties() -> dict[str, object]:
    """Snowflake-specific typeProperties — uses ``schema`` + ``table``."""
    return {
        "schema": {
            "value": "@dataset().schema_name",
            "type": "Expression",
        },
        "table": {
            "value": "@dataset().table_name",
            "type": "Expression",
        },
    }


def build_snowflake_table_dataset(*, auth_method: str = "basic") -> dict[str, object]:
    """Generic Snowflake table dataset; parameterised over LS conn + table.

    ``auth_method`` selects which of the two Snowflake LSs to bind to
    (``basic`` or ``key_pair``); the dataset declares the matching
    conn-param tuple and forwards values into the LS via the
    parameters block.
    """
    if auth_method == "basic":
        ls_name = naming.linked_service_snowflake_basic()
        secret_param = "p_password_secret"  # noqa: S105  # nosec B105
    elif auth_method == "key_pair":
        ls_name = naming.linked_service_snowflake_key_pair()
        secret_param = "p_private_key_secret"  # noqa: S105  # nosec B105
    else:
        raise ValueError(
            f"Snowflake dataset auth_method must be 'basic' or 'key_pair'; got {auth_method!r}."
        )

    conn_params = ("p_account", "p_database", "p_warehouse", "p_user", secret_param, "p_role")

    return {
        "name": naming.dataset_snowflake_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": ls_name,
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "SnowflakeV2Table",
            "schema": [],
            "typeProperties": _snowflake_table_typeproperties(),
        },
    }


def build_bigquery_table_dataset() -> dict[str, object]:
    """Generic BigQuery table dataset; parameterised over LS conn + (dataset, table).

    schema_name maps to BigQuery's "dataset" (the namespace under
    a project); table_name to the BigQuery table. The LS holds the
    project_id, so SQL queries reference tables as `dataset.table`
    (no project prefix).
    """
    conn_params = ("p_project_id", "p_key_file_secret")
    return {
        "name": naming.dataset_bigquery_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_bigquery(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "GoogleBigQueryV2Object",
            "schema": [],
            "typeProperties": {
                "dataset": {
                    "value": "@dataset().schema_name",
                    "type": "Expression",
                },
                "table": {
                    "value": "@dataset().table_name",
                    "type": "Expression",
                },
            },
        },
    }


def build_servicenow_object_dataset() -> dict[str, object]:
    """Generic ServiceNow object dataset; parameterised over LS conn + tableName.

    Uses ADF's ``ServiceNowObject`` dataset type. The framework's
    ``table_name`` parameter slot carries the ServiceNow table name
    (e.g. ``incident``, ``problem``, ``change_request``, ``cmdb_ci``).
    ``schema_name`` is declared (unused) for symmetry with the per-source
    driver's parameter passing in ``_rdbms_ingest_call``.
    """
    conn_params = (
        "p_environment_url",
        "p_user",
        "p_password_secret",
    )
    parameters: dict[str, object] = {
        "schema_name": {"type": "String"},
        "table_name": {"type": "String"},
    }
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": naming.dataset_servicenow_object(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_servicenow(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": "ServiceNowObject",
            "schema": [],
            "typeProperties": {
                "tableName": {
                    "value": "@dataset().table_name",
                    "type": "Expression",
                },
            },
        },
    }


def build_sap_hana_table_dataset() -> dict[str, object]:
    """Generic SAP HANA table dataset; parameterised over LS conn + table.

    SAP HANA has no per-connection database concept; the schema is
    addressed inline in the SELECT issued by the Copy Activity's
    ``SapHanaSource``. The dataset's ``schema_name`` / ``table_name``
    parameters are forwarded into the query template — the dataset's
    own ``typeProperties`` stay empty (ADF's ``SapHanaTable`` is happy
    with that as long as the source carries the SQL).
    """
    conn_params = ("p_server", "p_user", "p_password_secret")
    return {
        "name": naming.dataset_sap_hana_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_sap_hana(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "SapHanaTable",
            "schema": [],
            "typeProperties": {},
        },
    }


def build_salesforce_object_dataset() -> dict[str, object]:
    """Generic Salesforce SObject dataset; parameterised over LS conn + object_name.

    Uses ADF's ``SalesforceV2Object`` dataset type. ``object_name``
    is the SObject API name (e.g. 'Account', 'Contact',
    'Opportunity__c'); ``schema_name`` and ``table_name`` are not
    used — Salesforce has no schema concept and the Per-Source Driver
    passes object_name via the dataset's parameter.

    The dataset reuses the framework's ``table_name`` parameter slot
    for the SObject API name to stay consistent with the SQL sources'
    parameter passing in the per-source driver. The view's
    `table_name` column projects from dataset_config.object_name —
    same field, different semantic meaning by source type.
    """
    conn_params = (
        "p_environment_url",
        "p_client_id",
        "p_client_secret_secret",
    )
    parameters: dict[str, object] = {
        "table_name": {"type": "String"},
    }
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": naming.dataset_salesforce_object(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_salesforce(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": "SalesforceV2Object",
            "schema": [],
            "typeProperties": {
                "objectApiName": {
                    "value": "@dataset().table_name",
                    "type": "Expression",
                },
            },
        },
    }


def _build_azure_blob_file_dataset(
    *,
    dataset_name: str,
    adf_type: str,
    type_properties_extra: dict[str, object] | None = None,
) -> dict[str, object]:
    """Common shape for Azure Blob file datasets (CSV / JSON Lines).

    Parameterised over ``container`` + ``folder_path`` + ``file_name`` +
    LS conn (``p_storage_account``). The ``container`` parameter
    aliases the framework's per-dataset bucket / container concept —
    one Azure Blob source can read from many containers in the same
    storage account.
    """
    conn_params = ("p_storage_account",)

    type_properties: dict[str, object] = {
        "location": {
            "type": "AzureBlobStorageLocation",
            "container": {
                "value": "@dataset().container_name",
                "type": "Expression",
            },
            "folderPath": {
                "value": "@dataset().folder_path",
                "type": "Expression",
            },
            "fileName": {
                "value": "@dataset().file_name",
                "type": "Expression",
            },
        },
    }
    if type_properties_extra:
        type_properties.update(type_properties_extra)

    parameters: dict[str, object] = {
        "container_name": {"type": "String"},
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
    }
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": dataset_name,
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_azure_blob(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": adf_type,
            "schema": [],
            "typeProperties": type_properties,
        },
    }


def build_azure_blob_csv_dataset() -> dict[str, object]:
    """Azure Blob CSV dataset (DelimitedText) — header row, comma delim, UTF-8."""
    return _build_azure_blob_file_dataset(
        dataset_name=naming.dataset_azure_blob_csv(),
        adf_type="DelimitedText",
        type_properties_extra={
            "columnDelimiter": ",",
            "escapeChar": "\\",
            "firstRowAsHeader": True,
            "quoteChar": '"',
            "encodingName": "UTF-8",
        },
    )


def build_azure_blob_json_lines_dataset() -> dict[str, object]:
    """Azure Blob JSON Lines dataset (Json) — newline-delimited objects, UTF-8."""
    return _build_azure_blob_file_dataset(
        dataset_name=naming.dataset_azure_blob_json_lines(),
        adf_type="Json",
        type_properties_extra={"encodingName": "UTF-8"},
    )


def _build_s3_file_dataset(
    *,
    dataset_name: str,
    adf_type: str,
    type_properties_extra: dict[str, object] | None = None,
) -> dict[str, object]:
    """Common shape for S3 file datasets (CSV / JSON Lines).

    Parameterised over ``bucket`` + ``folder_path`` + ``file_name`` +
    LS conn (``p_access_key_id`` + ``p_secret_access_key_secret``).
    Bucket is per-dataset because one S3 source can read from many
    buckets with the same access key.
    """
    conn_params = ("p_access_key_id", "p_secret_access_key_secret")

    type_properties: dict[str, object] = {
        "location": {
            "type": "AmazonS3Location",
            "bucketName": {
                "value": "@dataset().bucket",
                "type": "Expression",
            },
            "folderPath": {
                "value": "@dataset().folder_path",
                "type": "Expression",
            },
            "fileName": {
                "value": "@dataset().file_name",
                "type": "Expression",
            },
        },
    }
    if type_properties_extra:
        type_properties.update(type_properties_extra)

    parameters: dict[str, object] = {
        "bucket": {"type": "String"},
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
    }
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": dataset_name,
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_s3(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": adf_type,
            "schema": [],
            "typeProperties": type_properties,
        },
    }


def build_s3_csv_dataset() -> dict[str, object]:
    """S3 CSV dataset (DelimitedText) — header row, comma delim, UTF-8."""
    return _build_s3_file_dataset(
        dataset_name=naming.dataset_s3_csv(),
        adf_type="DelimitedText",
        type_properties_extra={
            "columnDelimiter": ",",
            "escapeChar": "\\",
            "firstRowAsHeader": True,
            "quoteChar": '"',
            "encodingName": "UTF-8",
        },
    )


def build_s3_json_lines_dataset() -> dict[str, object]:
    """S3 JSON Lines dataset (Json) — newline-delimited objects, UTF-8."""
    return _build_s3_file_dataset(
        dataset_name=naming.dataset_s3_json_lines(),
        adf_type="Json",
        type_properties_extra={"encodingName": "UTF-8"},
    )


def build_synapse_sql_pool_table_dataset() -> dict[str, object]:
    """Generic Synapse SQL Pool table dataset; parameterised over LS conn + table.

    Uses ADF's ``AzureSqlDWTable`` dataset type — distinct from
    ``AzureSqlTable`` (Azure SQL DB) and ``SqlServerTable``.
    """
    conn_params = ("p_server", "p_database")
    return {
        "name": naming.dataset_synapse_sql_pool_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_synapse_sql_pool(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "AzureSqlDWTable",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def build_sql_server_table_dataset() -> dict[str, object]:
    """Generic SQL Server table dataset; parameterised over LS conn + table."""
    conn_params = ("p_server", "p_database", "p_user", "p_password_secret")
    return {
        "name": naming.dataset_sql_server_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_sql_server(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "SqlServerTable",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def build_oracle_table_dataset() -> dict[str, object]:
    """Generic Oracle table dataset; parameterised over LS conn + table.

    Oracle uses ``p_service_name`` instead of ``p_database``. ADF's
    ``OracleTable`` uses the ``schema`` typeProperty for Oracle's owner
    concept (which is what our ``schema_name`` parameter represents).
    """
    conn_params = ("p_server", "p_service_name", "p_user", "p_password_secret")
    return {
        "name": naming.dataset_oracle_table(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_oracle(),
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": _rdbms_dataset_params(*conn_params),
            "type": "OracleTable",
            "schema": [],
            "typeProperties": _rdbms_table_typeproperties(),
        },
    }


def build_adls_parquet_sink_dataset() -> dict[str, object]:
    """Generic ADLS Gen2 Parquet sink, parameterised over container + folder."""
    name = naming.dataset_adls_parquet_sink()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_adls_gen2(),
                "type": "LinkedServiceReference",
            },
            "parameters": {
                "container": {"type": "String"},
                "folder_path": {"type": "String"},
            },
            "type": "Parquet",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileSystem": {
                        "value": "@dataset().container",
                        "type": "Expression",
                    },
                    "folderPath": {
                        "value": "@dataset().folder_path",
                        "type": "Expression",
                    },
                },
                "compressionCodec": "snappy",
            },
            "schema": [],
        },
    }


def build_metadata_db_query_dataset() -> dict[str, object]:
    """Generic dataset for ``Lookup`` activities against the metadata DB.

    Bound to ``LS_METADATA_DB`` (single-instance per env, no parameters).
    No dataset parameters — Lookup activities carry the SQL inline.
    """
    name = naming.dataset_metadata_db_query()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": naming.linked_service_metadata_db(),
                "type": "LinkedServiceReference",
            },
            "type": "AzureSqlTable",
            "schema": [],
            "typeProperties": {
                # ADF requires *something* here for AzureSqlTable; Lookup
                # overrides via sqlReaderQuery. Pin to a known-safe view.
                "schema": "meta",
                "table": "v_enabled_datasets",
            },
        },
    }


# ── SFTP datasets ──────────────────────────────────────────────────────


def _build_sftp_file_dataset(
    *,
    dataset_name: str,
    adf_type: str,
    auth_method: str,
    type_properties_extra: dict[str, object] | None = None,
) -> dict[str, object]:
    """Common shape for SFTP file datasets (CSV / JSON Lines).

    ``auth_method`` selects which parameterised LS to bind ('basic' or
    'ssh_key'); the dataset declares conn params matching the picked LS.
    Per source the Per-Source Driver picks one auth method per dataset
    via the file_format / auth_method dimensions in metadata.
    """
    # Suppress S105: secret_param holds a *parameter name*, not a credential.
    if auth_method == "basic":
        ls_name = naming.linked_service_sftp_basic()
        secret_param = "p_password_secret"  # noqa: S105  # nosec B105
    elif auth_method == "ssh_key":
        ls_name = naming.linked_service_sftp_ssh_key()
        secret_param = "p_private_key_secret"  # noqa: S105  # nosec B105
    else:
        raise ValueError(
            f"SFTP dataset auth_method must be 'basic' or 'ssh_key'; got {auth_method!r}."
        )
    conn_params = ("p_server", "p_user", secret_param)

    type_properties: dict[str, object] = {
        "location": {
            "type": "SftpLocation",
            "folderPath": {
                "value": "@dataset().folder_path",
                "type": "Expression",
            },
            "fileName": {
                "value": "@dataset().file_name",
                "type": "Expression",
            },
        },
    }
    if type_properties_extra:
        type_properties.update(type_properties_extra)

    parameters: dict[str, object] = {
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
    }
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": dataset_name,
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": ls_name,
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": adf_type,
            "schema": [],
            "typeProperties": type_properties,
        },
    }


def build_sftp_csv_dataset(*, auth_method: str = "basic") -> dict[str, object]:
    """SFTP CSV dataset (DelimitedText), parameterised over folder + file name + LS conn.

    Defaults: header row, comma delimiter, UTF-8. Operators override via
    dataset_config keys when per-dataset format overrides land.
    """
    return _build_sftp_file_dataset(
        dataset_name=naming.dataset_sftp_csv(),
        adf_type="DelimitedText",
        auth_method=auth_method,
        type_properties_extra={
            "columnDelimiter": ",",
            "escapeChar": "\\",
            "firstRowAsHeader": True,
            "quoteChar": '"',
            "encodingName": "UTF-8",
        },
    )


def build_sftp_json_lines_dataset(*, auth_method: str = "basic") -> dict[str, object]:
    """SFTP JSON Lines dataset (Json), parameterised over folder + file name + LS conn.

    Treated as ``Json`` with newline-delimited objects. Operators move
    to ``arrayOfObjects`` for plain JSON arrays via dataset_config.
    """
    return _build_sftp_file_dataset(
        dataset_name=naming.dataset_sftp_json_lines(),
        adf_type="Json",
        auth_method=auth_method,
        type_properties_extra={
            "encodingName": "UTF-8",
        },
    )


# ── REST datasets ──────────────────────────────────────────────────────


def build_rest_resource_dataset(*, auth_method: str = "anonymous") -> dict[str, object]:
    """Generic REST resource dataset, parameterised over relative URL + LS conn.

    ``auth_method`` selects between ``LS_REST_API_ANONYMOUS`` (used for
    anonymous AND bearer — bearer is layered on at activity time via
    ``additionalHeaders``) and ``LS_REST_API_BASIC``.
    """
    conn_params: tuple[str, ...]
    if auth_method in {"anonymous", "bearer"}:
        ls_name = naming.linked_service_rest_api_anonymous()
        conn_params = ("p_base_url",)
    elif auth_method == "basic":
        ls_name = naming.linked_service_rest_api_basic()
        conn_params = ("p_base_url", "p_user", "p_password_secret")
    else:
        raise ValueError(
            "REST dataset auth_method must be 'anonymous', 'bearer', or 'basic'; "
            f"got {auth_method!r}."
        )

    parameters: dict[str, object] = {"relative_url": {"type": "String"}}
    parameters.update({p: {"type": "String"} for p in conn_params})

    return {
        "name": naming.dataset_rest_resource(),
        "properties": {
            "annotations": [],
            "linkedServiceName": {
                "referenceName": ls_name,
                "type": "LinkedServiceReference",
                "parameters": _ls_param_passthrough(*conn_params),
            },
            "parameters": parameters,
            "type": "RestResource",
            "schema": [],
            "typeProperties": {
                "relativeUrl": {
                    "value": "@dataset().relative_url",
                    "type": "Expression",
                },
            },
        },
    }


__all__ = [
    "build_adls_parquet_sink_dataset",
    "build_azure_blob_csv_dataset",
    "build_azure_blob_json_lines_dataset",
    "build_azure_sql_db_table_dataset",
    "build_bigquery_table_dataset",
    "build_metadata_db_query_dataset",
    "build_mysql_table_dataset",
    "build_oracle_table_dataset",
    "build_postgres_table_dataset",
    "build_rest_resource_dataset",
    "build_s3_csv_dataset",
    "build_s3_json_lines_dataset",
    "build_salesforce_object_dataset",
    "build_sap_hana_table_dataset",
    "build_servicenow_object_dataset",
    "build_sftp_csv_dataset",
    "build_sftp_json_lines_dataset",
    "build_snowflake_table_dataset",
    "build_sql_server_table_dataset",
    "build_synapse_sql_pool_table_dataset",
]
