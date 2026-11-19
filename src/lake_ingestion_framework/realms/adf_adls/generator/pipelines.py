"""ADF Pipeline builders.

Three pipeline tiers:

1. **Ingest pipelines** (one per source type) — Copy Activity from a
   parameterised dataset to the shared ADLS Parquet sink. Declares
   per-dataset params (schema/table or folder/file or relative_url),
   per-source connection params (server/database/user/secret-name),
   and target params (container/folder_path). Forwards everything
   through to the dataset reference, which in turn forwards conn
   params into the parameterised LinkedService.

2. **Per-Source Driver pipelines** (one per source type) — orchestrate
   one source through its datasets. Takes ``source_id`` as parameter.
   Two Lookups against the metadata DB:
     - ``meta.v_source_config`` (firstRowOnly=true): one row of
       connection details (server/database/user/secret-name/etc.).
       Captured into pipeline variables via Set Variable activities.
     - ``meta.v_enabled_datasets`` (firstRowOnly=false): one row per
       enabled dataset for this source. Iterated via ForEach.
   Each ForEach iteration ExecutePipelines the matching ingest
   pipeline, passing dataset params from ``@item().*`` and
   connection params from ``@variables('p_*')``.

3. **Master Driver pipeline** (one per env) — fired by the daily
   trigger. Single Lookup against ``meta.v_enabled_sources`` returns
   one row per enabled source. ForEach iterates them; an inner
   Switch dispatches by ``source_type`` into the matching
   per-source-driver pipeline. Adding a new source of an existing
   type requires no deploy — it shows up automatically in the
   Lookup result.

Both ``meta.v_source_config`` and ``meta.v_enabled_sources`` are
created in Phase 6 (the runtime metadata layer). The pipeline JSON
is correctly shaped today; runs will start succeeding once those
views are deployed.

Per CLAUDE.md "No SQL by string": all source queries are parameter-
bound via ADF expressions. Identifiers are bracketed for the dialects
that use brackets, double-quoted for those that don't.
"""

from __future__ import annotations

from dataclasses import dataclass

from lake_ingestion_framework.realms.adf_adls.generator import naming

# ──────────────────────────────────────────────────────────────────────
# Run-log activity helpers (Phase 6b-1b)
# ──────────────────────────────────────────────────────────────────────
#
# Every ingest pipeline wraps its Copy Activity with three run-log
# activities + one watermark advance. The helpers below build those
# activities so each ingest pipeline gets the same observability tier
# without duplicating JSON. See
# docs/realms/adf-adls-runtime-sql.md for the proc API.

# The Copy activity's output exposes these fields (ADF Copy spec):
#   rowsRead, rowsCopied, dataRead, dataWritten, filesWritten,
#   copyDuration (sec), throughput (KB/s), usedParallelCopies,
#   usedDataIntegrationUnits.
# Used by RunLogFinishedSuccess + AdvanceWatermark to capture metrics.
# DIU + parallel-copy outputs feed cost attribution
# (DIU-hours = used_dius * source_duration_ms / 3.6e6).
_COPY_ACTIVITY_NAME = "CopyFromSourceToParquet"


def _run_log_started_activity(*, pipeline_name: str) -> dict[str, object]:
    """Lookup that opens a run_log row and SELECTs run_id for the pipeline.

    Calls ``meta.sp_run_log_started`` with the pipeline's ``dataset_id``
    + ``master_run_id`` parameters. Result row holds ``run_id`` which the
    follow-on SetVariable captures for use by the close + watermark
    activities.
    """
    sql = (
        f"EXEC meta.sp_run_log_started "  # nosec B608
        f"@dataset_id = @{{pipeline().parameters.dataset_id}}, "
        f"@master_run_id = @{{pipeline().parameters.master_run_id}}, "
        f"@run_type = 'INCREMENTAL', "
        f"@pipeline_name = '{pipeline_name}', "
        f"@correlation_id = @{{pipeline().RunId}}"
    )
    return {
        "name": "RunLogStarted",
        "type": "Lookup",
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 2,
            "retryIntervalInSeconds": 30,
        },
        "typeProperties": {
            "source": {
                "type": "AzureSqlSource",
                "sqlReaderQuery": {"value": sql, "type": "Expression"},
                "queryTimeout": "00:10:00",
                "partitionOption": "None",
            },
            "dataset": {
                "referenceName": naming.dataset_metadata_db_query(),
                "type": "DatasetReference",
            },
            "firstRowOnly": True,
        },
    }


def _set_run_id_variable_activity() -> dict[str, object]:
    """Captures ``run_id`` from RunLogStarted into the pipeline variable."""
    return {
        "name": "SetRunId",
        "type": "SetVariable",
        "dependsOn": [
            {"activity": "RunLogStarted", "dependencyConditions": ["Succeeded"]},
        ],
        "typeProperties": {
            "variableName": "run_id",
            "value": {
                "value": "@string(activity('RunLogStarted').output.firstRow.run_id)",
                "type": "Expression",
            },
        },
    }


def _run_log_finished_success_activity(*, target_path_param: str) -> dict[str, object]:
    """SqlServerStoredProcedure that closes run_log with status=SUCCESS.

    Reads metrics from the Copy Activity output. ``target_path_param``
    is the pipeline parameter holding the sink path (RDBMS+REST:
    ``folder_path``; SFTP: ``folder_path_sink``).
    """
    return {
        "name": "RunLogFinishedSuccess",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
            {"activity": _COPY_ACTIVITY_NAME, "dependencyConditions": ["Succeeded"]},
        ],
        "linkedServiceName": {
            "referenceName": naming.linked_service_metadata_db(),
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "storedProcedureName": "[meta].[sp_run_log_finished]",
            "storedProcedureParameters": {
                "run_id": {
                    "value": {"value": "@int(variables('run_id'))", "type": "Expression"},
                    "type": "Int64",
                },
                "status": {"value": "SUCCESS", "type": "String"},
                "rows_extracted": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.rowsRead",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
                "rows_written": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.rowsCopied",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
                "bytes_written": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.dataWritten",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
                "bytes_read": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.dataRead",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
                "used_parallel_copies": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.usedParallelCopies",
                        "type": "Expression",
                    },
                    "type": "Int32",
                },
                "used_dius": {
                    "value": {
                        "value": (
                            f"@activity('{_COPY_ACTIVITY_NAME}')" ".output.usedDataIntegrationUnits"
                        ),
                        "type": "Expression",
                    },
                    "type": "Int32",
                },
                "throughput_kbps": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.throughput",
                        "type": "Expression",
                    },
                    "type": "Decimal",
                },
                "file_count": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.filesWritten",
                        "type": "Expression",
                    },
                    "type": "Int32",
                },
                "target_path": {
                    "value": {
                        "value": f"@pipeline().parameters.{target_path_param}",
                        "type": "Expression",
                    },
                    "type": "String",
                },
                "source_duration_ms": {
                    "value": {
                        "value": (
                            f"@mul(activity('{_COPY_ACTIVITY_NAME}').output.copyDuration, 1000)"
                        ),
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
            },
        },
    }


def _run_log_finished_failure_activity() -> dict[str, object]:
    """SqlServerStoredProcedure that closes run_log with status=FAILED.

    Captures the Copy Activity's error message, redacted to NVARCHAR(MAX)
    by the proc. The framework's redaction layer (see docs/security.md)
    runs on the ADF agent side; this activity just hands the raw error
    over.
    """
    return {
        "name": "RunLogFinishedFailure",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
            {"activity": _COPY_ACTIVITY_NAME, "dependencyConditions": ["Failed"]},
        ],
        "linkedServiceName": {
            "referenceName": naming.linked_service_metadata_db(),
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "storedProcedureName": "[meta].[sp_run_log_finished]",
            "storedProcedureParameters": {
                "run_id": {
                    "value": {"value": "@int(variables('run_id'))", "type": "Expression"},
                    "type": "Int64",
                },
                "status": {"value": "FAILED", "type": "String"},
                "error_message": {
                    "value": {
                        "value": (f"@string(activity('{_COPY_ACTIVITY_NAME}').error.message)"),
                        "type": "Expression",
                    },
                    "type": "String",
                },
            },
        },
    }


def _advance_watermark_activity() -> dict[str, object]:
    """SqlServerStoredProcedure that calls sp_advance_watermark on success.

    Phase 6b-1b passes ``@new_watermark = NULL`` — the proc keeps the
    existing watermark_value but bumps last_loaded_at + rows_loaded.
    Phase 6b-2 adds a max-watermark Lookup before this activity and
    starts passing real values.
    """
    return {
        "name": "AdvanceWatermark",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
            {"activity": "RunLogFinishedSuccess", "dependencyConditions": ["Succeeded"]},
        ],
        "linkedServiceName": {
            "referenceName": naming.linked_service_metadata_db(),
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "storedProcedureName": "[meta].[sp_advance_watermark]",
            "storedProcedureParameters": {
                "dataset_id": {
                    "value": {
                        "value": "@pipeline().parameters.dataset_id",
                        "type": "Expression",
                    },
                    "type": "Int32",
                },
                "rows_loaded": {
                    "value": {
                        "value": f"@activity('{_COPY_ACTIVITY_NAME}').output.rowsCopied",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
            },
        },
    }


def _wrap_copy_with_run_log(
    *,
    pipeline_name: str,
    copy_activity: dict[str, object],
    target_path_param: str,
) -> list[dict[str, object]]:
    """Return the full activity list wrapping a Copy with run-log activities.

    Order:
      RunLogStarted -> SetRunId -> Copy -> RunLogFinishedSuccess
                                       -> AdvanceWatermark
                                  -> RunLogFinishedFailure (on Copy Failed)
    """
    # The bare Copy Activity gets a dependsOn on SetRunId so it only
    # runs once we have a run_id.
    copy_with_dep: dict[str, object] = {
        **copy_activity,
        "dependsOn": [
            {"activity": "SetRunId", "dependencyConditions": ["Succeeded"]},
        ],
    }
    return [
        _run_log_started_activity(pipeline_name=pipeline_name),
        _set_run_id_variable_activity(),
        copy_with_dep,
        _run_log_finished_success_activity(target_path_param=target_path_param),
        _advance_watermark_activity(),
        _run_log_finished_failure_activity(),
    ]


# Run-log pipeline parameters every ingest pipeline now declares.
# The Per-Source Driver supplies dataset_id from @item().dataset_id and
# master_run_id from the captured master_run_log row.
_RUN_LOG_PIPELINE_PARAMS: dict[str, object] = {
    "dataset_id": {"type": "Int"},
    "master_run_id": {"type": "Int"},
}

_RUN_LOG_PIPELINE_VARIABLES: dict[str, object] = {
    "run_id": {"type": "String"},
}


# ──────────────────────────────────────────────────────────────────────
# Ingest pipelines
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _RdbmsShape:
    """Per-source-type bits that vary inside the RDBMS Copy Activity skeleton."""

    activity_source_type: str
    """ADF activity ``source.type`` — e.g. ``AzureSqlSource``."""

    query_field: str
    """Field on the activity source that carries the SELECT statement."""

    dataset_name: str
    """The parameterised dataset to bind the Copy Activity input to."""

    identifier_quote_open: str
    """Identifier-quoting character(s) used in the SELECT — e.g. ``[`` for T-SQL."""

    identifier_quote_close: str

    conn_params: tuple[str, ...]
    """LS connection parameter names this source-type forwards (in order)."""


_AZURE_SQL_SHAPE = _RdbmsShape(
    activity_source_type="AzureSqlSource",
    query_field="sqlReaderQuery",
    dataset_name=naming.dataset_azure_sql_db_table(),
    identifier_quote_open="[",
    identifier_quote_close="]",
    conn_params=("p_server", "p_database"),
)

_POSTGRES_SHAPE = _RdbmsShape(
    activity_source_type="PostgreSqlV2Source",
    query_field="query",
    dataset_name=naming.dataset_postgres_table(),
    identifier_quote_open='"',
    identifier_quote_close='"',
    conn_params=("p_server", "p_database", "p_user", "p_password_secret"),
)

_MYSQL_SHAPE = _RdbmsShape(
    activity_source_type="MySqlSource",
    query_field="query",
    dataset_name=naming.dataset_mysql_table(),
    identifier_quote_open="`",
    identifier_quote_close="`",
    conn_params=("p_server", "p_database", "p_user", "p_password_secret"),
)

_ORACLE_SHAPE = _RdbmsShape(
    activity_source_type="OracleSource",
    query_field="oracleReaderQuery",
    dataset_name=naming.dataset_oracle_table(),
    identifier_quote_open='"',
    identifier_quote_close='"',
    conn_params=("p_server", "p_service_name", "p_user", "p_password_secret"),
)

_SQL_SERVER_SHAPE = _RdbmsShape(
    activity_source_type="SqlServerSource",
    query_field="sqlReaderQuery",
    dataset_name=naming.dataset_sql_server_table(),
    identifier_quote_open="[",
    identifier_quote_close="]",
    conn_params=("p_server", "p_database", "p_user", "p_password_secret"),
)

_SNOWFLAKE_SHAPE = _RdbmsShape(
    activity_source_type="SnowflakeV2Source",
    query_field="query",
    dataset_name=naming.dataset_snowflake_table(),
    identifier_quote_open='"',
    identifier_quote_close='"',
    conn_params=(
        "p_account",
        "p_database",
        "p_warehouse",
        "p_user",
        "p_password_secret",
        "p_role",
    ),
)

_BIGQUERY_SHAPE = _RdbmsShape(
    activity_source_type="GoogleBigQueryV2Source",
    query_field="query",
    dataset_name=naming.dataset_bigquery_table(),
    # BigQuery uses backticks for quoted identifiers (same as MySQL).
    identifier_quote_open="`",
    identifier_quote_close="`",
    conn_params=("p_project_id", "p_key_file_secret"),
)

_SAP_HANA_SHAPE = _RdbmsShape(
    activity_source_type="SapHanaSource",
    query_field="query",
    dataset_name=naming.dataset_sap_hana_table(),
    # SAP HANA uses ANSI SQL with double-quoted identifiers — same
    # quoting as Postgres / Oracle / Snowflake.
    identifier_quote_open='"',
    identifier_quote_close='"',
    # No p_database — HANA has no per-connection database concept;
    # schema is carried in the SELECT.
    conn_params=("p_server", "p_user", "p_password_secret"),
)

_SYNAPSE_SQL_POOL_SHAPE = _RdbmsShape(
    # Synapse Dedicated SQL Pool uses ADF's SqlDWSource activity type
    # (distinct from AzureSqlSource / SqlServerSource). T-SQL syntax,
    # bracket quoting, MSI auth — same shape as Azure SQL DB.
    activity_source_type="SqlDWSource",
    query_field="sqlReaderQuery",
    dataset_name=naming.dataset_synapse_sql_pool_table(),
    identifier_quote_open="[",
    identifier_quote_close="]",
    conn_params=("p_server", "p_database"),
)


def _conn_params_block(conn_params: tuple[str, ...]) -> dict[str, object]:
    """Pipeline ``parameters`` entries for a tuple of conn-param names."""
    return {p: {"type": "String"} for p in conn_params}


def _conn_params_passthrough(conn_params: tuple[str, ...]) -> dict[str, object]:
    """Forward pipeline params into a dataset reference's parameters block."""
    return {
        p: {
            "value": f"@pipeline().parameters.{p}",
            "type": "Expression",
        }
        for p in conn_params
    }


def _build_rdbms_copy_pipeline(
    *,
    pipeline_name: str,
    shape: _RdbmsShape,
) -> dict[str, object]:
    """Build a per-source-type RDBMS ingest pipeline.

    Parameters set at invocation time:
    - Dataset: ``schema_name``, ``table_name``
    - Watermark: ``watermark_column``, ``watermark_value``
    - Sink: ``container``, ``folder_path``
    - Connection (forwarded into the LS via the dataset): per-shape
      ``conn_params`` (e.g. ``p_server``, ``p_database``, ``p_user``,
      ``p_password_secret``)

    The Copy Activity uses the type-specific source with a parameter-
    bound query. Identifiers are quoted per dialect (`[` for T-SQL,
    `\"` for Postgres / Oracle, `` ` `` for MySQL).
    """
    # qo / qc come from a trusted internal frozenset of quote-character
    # constants, not user input. The runtime values from
    # @{pipeline().parameters.*} are bound by ADF; we're not interpolating
    # user-supplied SQL fragments. Suppress S608.
    qo = shape.identifier_quote_open
    qc = shape.identifier_quote_close
    query_value = (
        f"SELECT * FROM {qo}@{{pipeline().parameters.schema_name}}{qc}"  # noqa: S608  # nosec B608
        f".{qo}@{{pipeline().parameters.table_name}}{qc} "
        f"WHERE {qo}@{{pipeline().parameters.watermark_column}}{qc} > "
        f"'@{{pipeline().parameters.watermark_value}}'"
    )

    parameters: dict[str, object] = {
        "schema_name": {"type": "String"},
        "table_name": {"type": "String"},
        "watermark_column": {"type": "String"},
        "watermark_value": {"type": "String"},
        "container": {"type": "String"},
        "folder_path": {"type": "String"},
    }
    parameters.update(_conn_params_block(shape.conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "schema_name": {
            "value": "@pipeline().parameters.schema_name",
            "type": "Expression",
        },
        "table_name": {
            "value": "@pipeline().parameters.table_name",
            "type": "Expression",
        },
    }
    dataset_params.update(_conn_params_passthrough(shape.conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": shape.activity_source_type,
                shape.query_field: {
                    "value": query_value,
                    "type": "Expression",
                },
                "queryTimeout": "02:00:00",
                "partitionOption": "None",
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {
                    "type": "AzureBlobFSWriteSettings",
                },
                "formatSettings": {
                    "type": "ParquetWriteSettings",
                },
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": shape.dataset_name,
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": pipeline_name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=pipeline_name,
                copy_activity=copy_activity,
                target_path_param="folder_path",
            ),
        },
    }


def build_pl_ingest_azure_sql_db() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_azure_sql_db(),
        shape=_AZURE_SQL_SHAPE,
    )


def build_pl_ingest_postgres() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_postgres(),
        shape=_POSTGRES_SHAPE,
    )


def build_pl_ingest_mysql() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_mysql(),
        shape=_MYSQL_SHAPE,
    )


def build_pl_ingest_oracle() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_oracle(),
        shape=_ORACLE_SHAPE,
    )


def build_pl_ingest_sql_server() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_sql_server(),
        shape=_SQL_SERVER_SHAPE,
    )


def build_pl_ingest_snowflake() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_snowflake(),
        shape=_SNOWFLAKE_SHAPE,
    )


def build_pl_ingest_bigquery() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_bigquery(),
        shape=_BIGQUERY_SHAPE,
    )


def build_pl_ingest_synapse_sql_pool() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_synapse_sql_pool(),
        shape=_SYNAPSE_SQL_POOL_SHAPE,
    )


def build_pl_ingest_sap_hana() -> dict[str, object]:
    return _build_rdbms_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_sap_hana(),
        shape=_SAP_HANA_SHAPE,
    )


# ── SFTP ingest pipelines ─────────────────────────────────────────────


# SFTP conn params come from the chosen LS (basic vs ssh_key). We pass
# the same three names through; the dataset routes them to the right LS.
# basic uses p_password_secret; ssh_key uses p_private_key_secret.
# At pipeline level, we declare the SUPER-SET via per-auth pipelines.
# To keep ingest count flat, we ship one ingest pipeline per file format
# regardless of auth — auth is a property of the SOURCE, and the dataset
# binds to one LS; for now we ship the ``basic`` flavour and document
# ``ssh_key`` as a follow-on (datasets.py supports both shapes).


def _sftp_conn_params_basic() -> tuple[str, ...]:
    return ("p_server", "p_user", "p_password_secret")


def _build_sftp_file_copy_pipeline(
    *,
    pipeline_name: str,
    source_dataset_name: str,
    activity_source_type: str,
    format_settings_type: str,
) -> dict[str, object]:
    """Common shape for SFTP file→ADLS Parquet ingest pipelines.

    Parameters set at invocation time:
    - Dataset: ``folder_path``, ``file_name``
    - Sink: ``container``, ``folder_path_sink`` (kept distinct from the
      source ``folder_path`` to avoid a name clash).
    - Connection: ``p_server``, ``p_user``, ``p_password_secret``
      (forwarded into ``LS_SFTP_BASIC`` via the dataset).
    - Run-log: ``dataset_id``, ``master_run_id``.
    """
    conn_params = _sftp_conn_params_basic()

    parameters: dict[str, object] = {
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
        "container": {"type": "String"},
        "folder_path_sink": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "folder_path": {
            "value": "@pipeline().parameters.folder_path",
            "type": "Expression",
        },
        "file_name": {
            "value": "@pipeline().parameters.file_name",
            "type": "Expression",
        },
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": activity_source_type,
                "storeSettings": {
                    "type": "SftpReadSettings",
                    "recursive": False,
                    "enablePartitionDiscovery": False,
                },
                "formatSettings": {
                    "type": format_settings_type,
                },
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {
                    "type": "AzureBlobFSWriteSettings",
                },
                "formatSettings": {
                    "type": "ParquetWriteSettings",
                },
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": source_dataset_name,
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path_sink",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": pipeline_name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=pipeline_name,
                copy_activity=copy_activity,
                target_path_param="folder_path_sink",
            ),
        },
    }


def build_pl_ingest_sftp_csv() -> dict[str, object]:
    return _build_sftp_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_sftp_csv(),
        source_dataset_name=naming.dataset_sftp_csv(),
        activity_source_type="DelimitedTextSource",
        format_settings_type="DelimitedTextReadSettings",
    )


def build_pl_ingest_sftp_json_lines() -> dict[str, object]:
    return _build_sftp_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_sftp_json_lines(),
        source_dataset_name=naming.dataset_sftp_json_lines(),
        activity_source_type="JsonSource",
        format_settings_type="JsonReadSettings",
    )


# ── S3 ingest pipelines ───────────────────────────────────────────────


def _s3_conn_params() -> tuple[str, ...]:
    return ("p_access_key_id", "p_secret_access_key_secret")


def _build_s3_file_copy_pipeline(
    *,
    pipeline_name: str,
    source_dataset_name: str,
    activity_source_type: str,
    format_settings_type: str,
) -> dict[str, object]:
    """Common shape for S3 file→ADLS Parquet ingest pipelines.

    Parameters set at invocation time:
    - Dataset: ``bucket``, ``folder_path``, ``file_name``
    - Sink: ``container``, ``folder_path_sink``
    - Connection: ``p_access_key_id``, ``p_secret_access_key_secret``
    - Run-log: ``dataset_id``, ``master_run_id``
    """
    conn_params = _s3_conn_params()

    parameters: dict[str, object] = {
        "bucket": {"type": "String"},
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
        "container": {"type": "String"},
        "folder_path_sink": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "bucket": {"value": "@pipeline().parameters.bucket", "type": "Expression"},
        "folder_path": {
            "value": "@pipeline().parameters.folder_path",
            "type": "Expression",
        },
        "file_name": {"value": "@pipeline().parameters.file_name", "type": "Expression"},
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": activity_source_type,
                "storeSettings": {
                    "type": "AmazonS3ReadSettings",
                    "recursive": False,
                    "enablePartitionDiscovery": False,
                },
                "formatSettings": {"type": format_settings_type},
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {"type": "ParquetWriteSettings"},
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": source_dataset_name,
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path_sink",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": pipeline_name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=pipeline_name,
                copy_activity=copy_activity,
                target_path_param="folder_path_sink",
            ),
        },
    }


def build_pl_ingest_s3_csv() -> dict[str, object]:
    return _build_s3_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_s3_csv(),
        source_dataset_name=naming.dataset_s3_csv(),
        activity_source_type="DelimitedTextSource",
        format_settings_type="DelimitedTextReadSettings",
    )


def build_pl_ingest_s3_json_lines() -> dict[str, object]:
    return _build_s3_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_s3_json_lines(),
        source_dataset_name=naming.dataset_s3_json_lines(),
        activity_source_type="JsonSource",
        format_settings_type="JsonReadSettings",
    )


# ── Azure Blob ingest pipelines ───────────────────────────────────────


def _azure_blob_conn_params() -> tuple[str, ...]:
    return ("p_storage_account",)


def _build_azure_blob_file_copy_pipeline(
    *,
    pipeline_name: str,
    source_dataset_name: str,
    activity_source_type: str,
    format_settings_type: str,
) -> dict[str, object]:
    """Common shape for Azure Blob file→ADLS Parquet ingest pipelines.

    Parameters set at invocation time:
    - Dataset: ``container_name``, ``folder_path``, ``file_name``
    - Sink: ``container``, ``folder_path_sink``
    - Connection: ``p_storage_account``
    - Run-log: ``dataset_id``, ``master_run_id``
    """
    conn_params = _azure_blob_conn_params()

    parameters: dict[str, object] = {
        "container_name": {"type": "String"},
        "folder_path": {"type": "String"},
        "file_name": {"type": "String"},
        "container": {"type": "String"},
        "folder_path_sink": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "container_name": {
            "value": "@pipeline().parameters.container_name",
            "type": "Expression",
        },
        "folder_path": {
            "value": "@pipeline().parameters.folder_path",
            "type": "Expression",
        },
        "file_name": {"value": "@pipeline().parameters.file_name", "type": "Expression"},
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": activity_source_type,
                "storeSettings": {
                    "type": "AzureBlobStorageReadSettings",
                    "recursive": False,
                    "enablePartitionDiscovery": False,
                },
                "formatSettings": {"type": format_settings_type},
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {"type": "ParquetWriteSettings"},
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": source_dataset_name,
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path_sink",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": pipeline_name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=pipeline_name,
                copy_activity=copy_activity,
                target_path_param="folder_path_sink",
            ),
        },
    }


# ── Salesforce ingest pipeline ────────────────────────────────────────


def _salesforce_conn_params() -> tuple[str, ...]:
    return ("p_environment_url", "p_client_id", "p_client_secret_secret")


def build_pl_ingest_salesforce() -> dict[str, object]:
    """Per-source-type ingest pipeline for Salesforce SObjects.

    SOQL is its own dialect (no identifier quoting; date literals
    are unquoted ISO-8601). Doesn't fit the RDBMS shape, so this is
    a separate builder.

    Parameters set at invocation time:
    - SObject: ``table_name`` (the SObject API name; reused from the
      RDBMS slot for parameter-passing consistency in the Per-Source
      Driver)
    - Watermark: ``watermark_column``, ``watermark_value`` (ISO-8601
      string for DateTime-typed watermarks)
    - Sink: ``container``, ``folder_path``
    - Connection: ``p_environment_url``, ``p_client_id``,
      ``p_client_secret_secret``
    - Run-log: ``dataset_id``, ``master_run_id``

    schema_name is not used — Salesforce has no schema concept — but
    is declared as a parameter so the per-source-driver's generic
    `_rdbms_ingest_call` helper can pass @item().schema_name without
    a special case. The SOQL query ignores it.
    """
    name = naming.pipeline_ingest_salesforce()
    conn_params = _salesforce_conn_params()

    # SOQL: no identifier quoting; watermark value unquoted (ISO date
    # literal). Customer is responsible for storing watermark_value as
    # a SOQL-compatible literal in meta.watermark.watermark_value
    # (typically ISO-8601 DateTime: "2024-01-01T00:00:00Z").
    query_value = (
        "SELECT * FROM @{pipeline().parameters.table_name} "  # nosec B608
        "WHERE @{pipeline().parameters.watermark_column} > "
        "@{pipeline().parameters.watermark_value}"
    )

    parameters: dict[str, object] = {
        "schema_name": {"type": "String"},  # unused; declared for symmetry
        "table_name": {"type": "String"},
        "watermark_column": {"type": "String"},
        "watermark_value": {"type": "String"},
        "container": {"type": "String"},
        "folder_path": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "table_name": {
            "value": "@pipeline().parameters.table_name",
            "type": "Expression",
        },
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": "SalesforceV2Source",
                "query": {"value": query_value, "type": "Expression"},
                "queryTimeout": "02:00:00",
                # readBehavior options: 'query' (active records),
                # 'queryAll' (includes soft-deleted). Default to active.
                "readBehavior": "query",
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {"type": "ParquetWriteSettings"},
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": naming.dataset_salesforce_object(),
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=name,
                copy_activity=copy_activity,
                target_path_param="folder_path",
            ),
        },
    }


# ── ServiceNow ingest pipeline ────────────────────────────────────────


def _servicenow_conn_params() -> tuple[str, ...]:
    return ("p_environment_url", "p_user", "p_password_secret")


def build_pl_ingest_servicenow() -> dict[str, object]:
    """Per-source-type ingest pipeline for ServiceNow tables.

    ServiceNow uses its own encoded-query syntax (not SQL) — column
    names unquoted, value literals unquoted (timestamps in ISO-ish
    form: ``YYYY-MM-DD HH:MM:SS``). Doesn't fit the RDBMS shape, so
    this is a separate builder.

    Parameters set at invocation time:
    - Object: ``table_name`` (the ServiceNow table — e.g. ``incident``,
      ``cmdb_ci``)
    - Watermark: ``watermark_column``, ``watermark_value`` (typically
      ``sys_updated_on`` with a ``YYYY-MM-DD HH:MM:SS`` literal)
    - Sink: ``container``, ``folder_path``
    - Connection: ``p_environment_url``, ``p_user``,
      ``p_password_secret``
    - Run-log: ``dataset_id``, ``master_run_id``

    schema_name is not used — ServiceNow has no schema concept — but
    is declared as a parameter so the per-source-driver's generic
    ``_rdbms_ingest_call`` helper can pass ``@item().schema_name``
    without a special case. The encoded query ignores it.
    """
    name = naming.pipeline_ingest_servicenow()
    conn_params = _servicenow_conn_params()

    # ServiceNow encoded query: <column><op><value>. No quoting around
    # column or value. Customer is responsible for storing
    # watermark_value as a ServiceNow-compatible literal in
    # meta.watermark.watermark_value (typically
    # 'YYYY-MM-DD HH:MM:SS' for sys_updated_on).
    query_value = (
        "@{pipeline().parameters.watermark_column}>"  # nosec B608
        "@{pipeline().parameters.watermark_value}"
    )

    parameters: dict[str, object] = {
        "schema_name": {"type": "String"},  # unused; declared for symmetry
        "table_name": {"type": "String"},
        "watermark_column": {"type": "String"},
        "watermark_value": {"type": "String"},
        "container": {"type": "String"},
        "folder_path": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "schema_name": {
            "value": "@pipeline().parameters.schema_name",
            "type": "Expression",
        },
        "table_name": {
            "value": "@pipeline().parameters.table_name",
            "type": "Expression",
        },
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": "ServiceNowSource",
                "query": {"value": query_value, "type": "Expression"},
                "queryTimeout": "02:00:00",
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                "formatSettings": {"type": "ParquetWriteSettings"},
            },
            "enableStaging": False,
            "translator": {
                "type": "TabularTranslator",
                "typeConversion": True,
            },
        },
        "inputs": [
            {
                "referenceName": naming.dataset_servicenow_object(),
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=name,
                copy_activity=copy_activity,
                target_path_param="folder_path",
            ),
        },
    }


def build_pl_ingest_azure_blob_csv() -> dict[str, object]:
    return _build_azure_blob_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_azure_blob_csv(),
        source_dataset_name=naming.dataset_azure_blob_csv(),
        activity_source_type="DelimitedTextSource",
        format_settings_type="DelimitedTextReadSettings",
    )


def build_pl_ingest_azure_blob_json_lines() -> dict[str, object]:
    return _build_azure_blob_file_copy_pipeline(
        pipeline_name=naming.pipeline_ingest_azure_blob_json_lines(),
        source_dataset_name=naming.dataset_azure_blob_json_lines(),
        activity_source_type="JsonSource",
        format_settings_type="JsonReadSettings",
    )


def build_pl_ingest_rest_api() -> dict[str, object]:
    """REST API → ADLS Parquet ingest pipeline.

    Parameters: ``relative_url``, ``container``, ``folder_path``,
    ``p_base_url`` (forwarded into ``LS_REST_API_ANONYMOUS`` via the
    dataset). Pagination + bearer-header injection are intentionally
    not configured here — operators customise via the dataset's
    ``additionalHeaders`` / ``paginationRules``. Sink is one Parquet
    file per run.
    """
    name = naming.pipeline_ingest_rest_api()
    conn_params = ("p_base_url",)

    parameters: dict[str, object] = {
        "relative_url": {"type": "String"},
        "container": {"type": "String"},
        "folder_path": {"type": "String"},
    }
    parameters.update(_conn_params_block(conn_params))
    parameters.update(_RUN_LOG_PIPELINE_PARAMS)

    dataset_params: dict[str, object] = {
        "relative_url": {
            "value": "@pipeline().parameters.relative_url",
            "type": "Expression",
        },
    }
    dataset_params.update(_conn_params_passthrough(conn_params))

    copy_activity: dict[str, object] = {
        "name": _COPY_ACTIVITY_NAME,
        "type": "Copy",
        "policy": {
            "timeout": "0.12:00:00",
            "retry": 2,
            "retryIntervalInSeconds": 60,
            "secureOutput": True,
            "secureInput": True,
        },
        "typeProperties": {
            "source": {
                "type": "RestSource",
                "httpRequestTimeout": "00:01:40",
                "requestInterval": "00:00:00.010",
                "requestMethod": "GET",
            },
            "sink": {
                "type": "ParquetSink",
                "storeSettings": {
                    "type": "AzureBlobFSWriteSettings",
                },
                "formatSettings": {
                    "type": "ParquetWriteSettings",
                },
            },
            "enableStaging": False,
        },
        "inputs": [
            {
                "referenceName": naming.dataset_rest_resource(),
                "type": "DatasetReference",
                "parameters": dataset_params,
            },
        ],
        "outputs": [
            {
                "referenceName": naming.dataset_adls_parquet_sink(),
                "type": "DatasetReference",
                "parameters": {
                    "container": {
                        "value": "@pipeline().parameters.container",
                        "type": "Expression",
                    },
                    "folder_path": {
                        "value": "@pipeline().parameters.folder_path",
                        "type": "Expression",
                    },
                },
            },
        ],
    }

    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": parameters,
            "variables": dict(_RUN_LOG_PIPELINE_VARIABLES),
            "activities": _wrap_copy_with_run_log(
                pipeline_name=name,
                copy_activity=copy_activity,
                target_path_param="folder_path",
            ),
        },
    }


# ──────────────────────────────────────────────────────────────────────
# Driver pipelines (orchestration)
# ──────────────────────────────────────────────────────────────────────


# Per-source-type connection params in canonical order. Mirrors the
# Lookup column projection from meta.v_source_config (Phase 6 view) and
# the parameter sets declared on the corresponding LinkedServices.
_CONN_PARAMS_BY_SOURCE_TYPE: dict[str, tuple[str, ...]] = {
    "azure_sql_db": ("p_server", "p_database"),
    "sql_server": ("p_server", "p_database", "p_user", "p_password_secret"),
    "postgres": ("p_server", "p_database", "p_user", "p_password_secret"),
    "mysql": ("p_server", "p_database", "p_user", "p_password_secret"),
    "oracle": ("p_server", "p_service_name", "p_user", "p_password_secret"),
    "snowflake": (
        "p_account",
        "p_database",
        "p_warehouse",
        "p_user",
        "p_password_secret",
        "p_role",
    ),
    "bigquery": ("p_project_id", "p_key_file_secret"),
    "synapse_sql_pool": ("p_server", "p_database"),
    "sap_hana": ("p_server", "p_user", "p_password_secret"),
    "sftp": ("p_server", "p_user", "p_password_secret"),
    "s3": ("p_access_key_id", "p_secret_access_key_secret"),
    "azure_blob_x": ("p_storage_account",),
    "salesforce": (
        "p_environment_url",
        "p_client_id",
        "p_client_secret_secret",
    ),
    "servicenow": ("p_environment_url", "p_user", "p_password_secret"),
    "rest_api": ("p_base_url",),
}


# Maps source_type to the ingest pipeline a dataset of that type invokes.
# Keeps the Per-Source Driver builder self-contained.
_INGEST_PIPELINE_BY_SOURCE_TYPE: dict[str, str] = {
    "azure_sql_db": naming.pipeline_ingest_azure_sql_db(),
    "sql_server": naming.pipeline_ingest_sql_server(),
    "postgres": naming.pipeline_ingest_postgres(),
    "mysql": naming.pipeline_ingest_mysql(),
    "oracle": naming.pipeline_ingest_oracle(),
    "snowflake": naming.pipeline_ingest_snowflake(),
    "bigquery": naming.pipeline_ingest_bigquery(),
    "synapse_sql_pool": naming.pipeline_ingest_synapse_sql_pool(),
    "sap_hana": naming.pipeline_ingest_sap_hana(),
    "salesforce": naming.pipeline_ingest_salesforce(),
    "servicenow": naming.pipeline_ingest_servicenow(),
    "rest_api": naming.pipeline_ingest_rest_api(),
}


def _source_config_lookup_query(conn_params: tuple[str, ...]) -> str:
    """Lookup query against ``meta.v_source_config`` (Phase 6 view).

    Returns one row of connection details for the given source_id.
    Column projection mirrors the conn-param tuple — the view exposes
    one column per param so the Lookup result and Set Variable
    activities stay in sync.
    """
    cols = ", ".join(conn_params)
    return (
        f"SELECT {cols} "  # noqa: S608  # nosec B608  -- conn_params is a trusted internal tuple
        "FROM meta.v_source_config "
        "WHERE source_id = @{pipeline().parameters.source_id}"
    )


# Lookup query against ``meta.v_enabled_datasets`` (Phase 6 view).
# Returns one row per enabled dataset for a source. The column set is
# a superset across source types — RDBMS ignores file-related columns,
# REST ignores schema/table, etc. This keeps the view shape stable.
_ENABLED_DATASETS_LOOKUP_QUERY = (
    "SELECT dataset_id, schema_name, table_name, watermark_column, watermark_value, "
    "container, folder_path, file_format, relative_url, file_name, folder_path_sink "
    "FROM meta.v_enabled_datasets WHERE source_id = @{pipeline().parameters.source_id}"
)


def _set_variable_activity(*, var_name: str, source_field: str) -> dict[str, object]:
    """Set Variable activity that captures one Lookup field into a pipeline variable.

    Used to lift connection details out of the Lookup result so the
    ForEach inner activities can reference them via ``@variables(...)``
    instead of ``@activity(...).output.firstRow.*`` (which doesn't
    survive ForEach scope changes cleanly).
    """
    return {
        "name": f"Set_{var_name}",
        "type": "SetVariable",
        "dependsOn": [
            {
                "activity": "GetSourceConfig",
                "dependencyConditions": ["Succeeded"],
            },
        ],
        "typeProperties": {
            "variableName": var_name,
            "value": {
                "value": f"@activity('GetSourceConfig').output.firstRow.{source_field}",
                "type": "Expression",
            },
        },
    }


def _conn_param_variable_refs(conn_params: tuple[str, ...]) -> dict[str, object]:
    """``@variables('p_xxx')`` expressions for each conn param.

    Used inside ForEach to forward connection values into the inner
    ExecutePipeline call.
    """
    return {
        p: {
            "value": f"@variables('{p}')",
            "type": "Expression",
        }
        for p in conn_params
    }


def _run_log_call_params() -> dict[str, object]:
    """Per-iteration run-log params every ingest pipeline expects.

    The dataset list comes from meta.v_enabled_datasets which projects
    ``dataset_id`` per row; ``master_run_id`` is captured from the
    Per-Source Driver's MasterRunStarted Lookup into a pipeline variable.
    """
    return {
        "dataset_id": {"value": "@item().dataset_id", "type": "Expression"},
        "master_run_id": {
            "value": "@int(variables('master_run_id'))",
            "type": "Expression",
        },
    }


def _rdbms_ingest_call(
    *,
    ingest_pipeline_name: str,
    conn_params: tuple[str, ...],
) -> dict[str, object]:
    """ExecutePipeline activity body for an RDBMS ingest call."""
    parameters: dict[str, object] = {
        "schema_name": {"value": "@item().schema_name", "type": "Expression"},
        "table_name": {"value": "@item().table_name", "type": "Expression"},
        "watermark_column": {
            "value": "@item().watermark_column",
            "type": "Expression",
        },
        "watermark_value": {
            "value": "@item().watermark_value",
            "type": "Expression",
        },
        "container": {"value": "@item().container", "type": "Expression"},
        "folder_path": {"value": "@item().folder_path", "type": "Expression"},
    }
    parameters.update(_conn_param_variable_refs(conn_params))
    parameters.update(_run_log_call_params())
    return {
        "type": "ExecutePipeline",
        "typeProperties": {
            "pipeline": {
                "referenceName": ingest_pipeline_name,
                "type": "PipelineReference",
            },
            "parameters": parameters,
            "waitOnCompletion": True,
        },
    }


def _rest_ingest_call(*, conn_params: tuple[str, ...]) -> dict[str, object]:
    """ExecutePipeline activity body for a REST API ingest call."""
    parameters: dict[str, object] = {
        "relative_url": {"value": "@item().relative_url", "type": "Expression"},
        "container": {"value": "@item().container", "type": "Expression"},
        "folder_path": {"value": "@item().folder_path", "type": "Expression"},
    }
    parameters.update(_conn_param_variable_refs(conn_params))
    parameters.update(_run_log_call_params())
    return {
        "type": "ExecutePipeline",
        "typeProperties": {
            "pipeline": {
                "referenceName": naming.pipeline_ingest_rest_api(),
                "type": "PipelineReference",
            },
            "parameters": parameters,
            "waitOnCompletion": True,
        },
    }


def _azure_blob_dispatch_switch(*, conn_params: tuple[str, ...]) -> dict[str, object]:
    """Switch activity that dispatches by ``file_format`` for Azure Blob.

    Mirrors _s3_dispatch_switch / _sftp_dispatch_switch. Per-iteration
    params include `container_name` (the per-dataset Azure Blob
    container) which the v_enabled_datasets view aliases to the
    shared `bucket` column.
    """
    azure_blob_call_params: dict[str, object] = {
        "container_name": {"value": "@item().bucket", "type": "Expression"},
        "folder_path": {"value": "@item().folder_path", "type": "Expression"},
        "file_name": {"value": "@item().file_name", "type": "Expression"},
        "container": {"value": "@item().container", "type": "Expression"},
        "folder_path_sink": {
            "value": "@item().folder_path_sink",
            "type": "Expression",
        },
    }
    azure_blob_call_params.update(_conn_param_variable_refs(conn_params))
    azure_blob_call_params.update(_run_log_call_params())
    return {
        "type": "Switch",
        "typeProperties": {
            "on": {"value": "@item().file_format", "type": "Expression"},
            "cases": [
                {
                    "value": "csv",
                    "activities": [
                        {
                            "name": "InvokeIngestAzureBlobCsv",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_azure_blob_csv(),
                                    "type": "PipelineReference",
                                },
                                "parameters": azure_blob_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
                {
                    "value": "json_lines",
                    "activities": [
                        {
                            "name": "InvokeIngestAzureBlobJsonLines",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_azure_blob_json_lines(),
                                    "type": "PipelineReference",
                                },
                                "parameters": azure_blob_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
            ],
        },
    }


def _s3_dispatch_switch(*, conn_params: tuple[str, ...]) -> dict[str, object]:
    """Switch activity that dispatches by ``file_format`` for S3.

    Mirrors _sftp_dispatch_switch — same file-format dimension, same
    Switch shape, but with S3-specific parameters (bucket added,
    folder_path/file_name reused) and S3-specific conn params
    (access_key_id + secret_access_key_secret).
    """
    s3_call_params: dict[str, object] = {
        "bucket": {"value": "@item().bucket", "type": "Expression"},
        "folder_path": {"value": "@item().folder_path", "type": "Expression"},
        "file_name": {"value": "@item().file_name", "type": "Expression"},
        "container": {"value": "@item().container", "type": "Expression"},
        "folder_path_sink": {
            "value": "@item().folder_path_sink",
            "type": "Expression",
        },
    }
    s3_call_params.update(_conn_param_variable_refs(conn_params))
    s3_call_params.update(_run_log_call_params())
    return {
        "type": "Switch",
        "typeProperties": {
            "on": {"value": "@item().file_format", "type": "Expression"},
            "cases": [
                {
                    "value": "csv",
                    "activities": [
                        {
                            "name": "InvokeIngestS3Csv",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_s3_csv(),
                                    "type": "PipelineReference",
                                },
                                "parameters": s3_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
                {
                    "value": "json_lines",
                    "activities": [
                        {
                            "name": "InvokeIngestS3JsonLines",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_s3_json_lines(),
                                    "type": "PipelineReference",
                                },
                                "parameters": s3_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
            ],
        },
    }


def _sftp_dispatch_switch(*, conn_params: tuple[str, ...]) -> dict[str, object]:
    """Switch activity that dispatches by ``file_format`` for SFTP.

    SFTP datasets' ``file_format`` (a dataset_config key) determines
    which ingest pipeline runs (CSV vs JSON Lines). All other
    SFTP-shape parameters (folder/file/container/conn) pass identically.
    """
    sftp_call_params: dict[str, object] = {
        "folder_path": {"value": "@item().folder_path", "type": "Expression"},
        "file_name": {"value": "@item().file_name", "type": "Expression"},
        "container": {"value": "@item().container", "type": "Expression"},
        "folder_path_sink": {
            "value": "@item().folder_path_sink",
            "type": "Expression",
        },
    }
    sftp_call_params.update(_conn_param_variable_refs(conn_params))
    sftp_call_params.update(_run_log_call_params())
    return {
        "type": "Switch",
        "typeProperties": {
            "on": {"value": "@item().file_format", "type": "Expression"},
            "cases": [
                {
                    "value": "csv",
                    "activities": [
                        {
                            "name": "InvokeIngestSftpCsv",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_sftp_csv(),
                                    "type": "PipelineReference",
                                },
                                "parameters": sftp_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
                {
                    "value": "json_lines",
                    "activities": [
                        {
                            "name": "InvokeIngestSftpJsonLines",
                            "type": "ExecutePipeline",
                            "typeProperties": {
                                "pipeline": {
                                    "referenceName": naming.pipeline_ingest_sftp_json_lines(),
                                    "type": "PipelineReference",
                                },
                                "parameters": sftp_call_params,
                                "waitOnCompletion": True,
                            },
                        },
                    ],
                },
            ],
        },
    }


_MASTER_RUN_LOG_STARTED_QUERY = (
    "EXEC meta.sp_master_run_log_started "
    "@source_id = @{pipeline().parameters.source_id}, "
    "@run_type = 'INCREMENTAL', "
    "@adf_pipeline_run_id = @{pipeline().RunId}"
)


def _master_run_log_started_activity(*, depends_on: list[str]) -> dict[str, object]:
    """Lookup that opens master_run_log row; SELECTs master_run_id back."""
    return {
        "name": "MasterRunStarted",
        "type": "Lookup",
        "dependsOn": [
            {"activity": name, "dependencyConditions": ["Succeeded"]} for name in depends_on
        ],
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 2,
            "retryIntervalInSeconds": 30,
        },
        "typeProperties": {
            "source": {
                "type": "AzureSqlSource",
                "sqlReaderQuery": {
                    "value": _MASTER_RUN_LOG_STARTED_QUERY,
                    "type": "Expression",
                },
                "queryTimeout": "00:10:00",
                "partitionOption": "None",
            },
            "dataset": {
                "referenceName": naming.dataset_metadata_db_query(),
                "type": "DatasetReference",
            },
            "firstRowOnly": True,
        },
    }


def _set_master_run_id_variable_activity() -> dict[str, object]:
    """Captures master_run_id into the pipeline variable."""
    return {
        "name": "SetMasterRunId",
        "type": "SetVariable",
        "dependsOn": [
            {"activity": "MasterRunStarted", "dependencyConditions": ["Succeeded"]},
        ],
        "typeProperties": {
            "variableName": "master_run_id",
            "value": {
                "value": "@string(activity('MasterRunStarted').output.firstRow.master_run_id)",
                "type": "Expression",
            },
        },
    }


def _master_run_log_finished_activity() -> dict[str, object]:
    """SqlServerStoredProcedure that closes master_run_log after ForEach.

    Depends on ForEachDataset Completed (not Succeeded) so the close
    runs even when individual datasets failed — sp_master_run_log_finished
    aggregates per-dataset run_log rows to derive the final
    SUCCESS / PARTIAL / FAILED status.
    """
    return {
        "name": "MasterRunFinished",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
            {"activity": "ForEachDataset", "dependencyConditions": ["Completed"]},
        ],
        "linkedServiceName": {
            "referenceName": naming.linked_service_metadata_db(),
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "storedProcedureName": "[meta].[sp_master_run_log_finished]",
            "storedProcedureParameters": {
                "master_run_id": {
                    "value": {
                        "value": "@int(variables('master_run_id'))",
                        "type": "Expression",
                    },
                    "type": "Int64",
                },
            },
        },
    }


def build_pl_per_source_driver(*, source_type: str) -> dict[str, object]:
    """Per-source-type orchestration pipeline.

    Activities (in order):

    1. ``GetSourceConfig`` — Lookup against ``meta.v_source_config`` for
       the given ``source_id``. Returns one row of connection details
       (firstRowOnly=true).
    2. One ``Set Variable`` per connection field — captures Lookup
       output into pipeline variables so they're addressable from
       inside ForEach.
    3. ``MasterRunStarted`` — Lookup that calls
       ``meta.sp_master_run_log_started``; result row carries the new
       ``master_run_id``.
    4. ``SetMasterRunId`` — captures master_run_id into a variable so
       the ForEach body can pass it down to each ingest pipeline.
    5. ``GetEnabledDatasets`` — Lookup against ``meta.v_enabled_datasets``
       (firstRowOnly=false). Depends on SetMasterRunId.
    6. ``ForEachDataset`` — iterates the dataset list; each iteration
       calls the matching ingest pipeline. Inner activity:
         - RDBMS / REST: a single ``ExecutePipeline``.
         - SFTP: a ``Switch`` on ``file_format``.
    7. ``MasterRunFinished`` — SqlServerStoredProcedure that calls
       ``meta.sp_master_run_log_finished`` (depends on ForEachDataset
       Completed; runs even when individual datasets fail).

    Phase 6 metadata views (``meta.v_source_config``,
    ``meta.v_enabled_datasets``) and Phase 6b-1a procs
    (``sp_master_run_log_*``) are deployed via
    ``lake-ingest deploy --include-procs``.
    """
    if source_type not in _CONN_PARAMS_BY_SOURCE_TYPE:
        raise ValueError(f"Per-source driver not yet supported for source_type={source_type!r}.")
    conn_params = _CONN_PARAMS_BY_SOURCE_TYPE[source_type]

    if source_type == "sftp":
        inner_activity_name = "DispatchByFormat"
        inner_activity = _sftp_dispatch_switch(conn_params=conn_params)
    elif source_type == "s3":
        inner_activity_name = "DispatchByFormat"
        inner_activity = _s3_dispatch_switch(conn_params=conn_params)
    elif source_type == "azure_blob_x":
        inner_activity_name = "DispatchByFormat"
        inner_activity = _azure_blob_dispatch_switch(conn_params=conn_params)
    elif source_type == "rest_api":
        inner_activity_name = "InvokeIngestRestApi"
        inner_activity = _rest_ingest_call(conn_params=conn_params)
    else:
        inner_activity_name = "InvokeIngestPipeline"
        inner_activity = _rdbms_ingest_call(
            ingest_pipeline_name=_INGEST_PIPELINE_BY_SOURCE_TYPE[source_type],
            conn_params=conn_params,
        )

    inner_with_name: dict[str, object] = {"name": inner_activity_name, **inner_activity}

    # Set Variable activities — one per conn param.
    set_var_activities: list[dict[str, object]] = [
        _set_variable_activity(var_name=p, source_field=p) for p in conn_params
    ]
    set_var_names: list[str] = [str(a["name"]) for a in set_var_activities]

    variables: dict[str, object] = {p: {"type": "String"} for p in conn_params}
    variables["master_run_id"] = {"type": "String"}

    return {
        "name": naming.pipeline_per_source_driver(source_type),
        "properties": {
            "annotations": [],
            "parameters": {
                "source_id": {"type": "Int"},
            },
            "variables": variables,
            "activities": [
                {
                    "name": "GetSourceConfig",
                    "type": "Lookup",
                    "policy": {
                        "timeout": "0.00:30:00",
                        "retry": 2,
                        "retryIntervalInSeconds": 30,
                    },
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderQuery": {
                                "value": _source_config_lookup_query(conn_params),
                                "type": "Expression",
                            },
                            "queryTimeout": "00:30:00",
                            "partitionOption": "None",
                        },
                        "dataset": {
                            "referenceName": naming.dataset_metadata_db_query(),
                            "type": "DatasetReference",
                        },
                        "firstRowOnly": True,
                    },
                },
                *set_var_activities,
                _master_run_log_started_activity(depends_on=set_var_names),
                _set_master_run_id_variable_activity(),
                {
                    "name": "GetEnabledDatasets",
                    "type": "Lookup",
                    "dependsOn": [
                        {
                            "activity": "SetMasterRunId",
                            "dependencyConditions": ["Succeeded"],
                        },
                    ],
                    "policy": {
                        "timeout": "0.00:30:00",
                        "retry": 2,
                        "retryIntervalInSeconds": 30,
                    },
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderQuery": {
                                "value": _ENABLED_DATASETS_LOOKUP_QUERY,
                                "type": "Expression",
                            },
                            "queryTimeout": "00:30:00",
                            "partitionOption": "None",
                        },
                        "dataset": {
                            "referenceName": naming.dataset_metadata_db_query(),
                            "type": "DatasetReference",
                        },
                        "firstRowOnly": False,
                    },
                },
                {
                    "name": "ForEachDataset",
                    "type": "ForEach",
                    "dependsOn": [
                        {
                            "activity": "GetEnabledDatasets",
                            "dependencyConditions": ["Succeeded"],
                        },
                    ],
                    "typeProperties": {
                        "items": {
                            "value": "@activity('GetEnabledDatasets').output.value",
                            "type": "Expression",
                        },
                        "isSequential": False,
                        "batchCount": 4,
                        "activities": [inner_with_name],
                    },
                },
                _master_run_log_finished_activity(),
            ],
        },
    }


# Master driver Lookup — returns one row per enabled source. Phase 6
# view ``meta.v_enabled_sources`` projects (source_id, source_type).
_ENABLED_SOURCES_LOOKUP_QUERY = "SELECT source_id, source_type FROM meta.v_enabled_sources"


def build_pl_master_driver(*, source_types_present: list[str]) -> dict[str, object]:
    """Master driver — fans out to every per-type driver dynamically.

    Single Lookup against ``meta.v_enabled_sources`` returns one row
    per enabled source. ForEach iterates them in parallel; an inner
    Switch dispatches by ``source_type`` into the matching
    per-source-driver pipeline.

    ``source_types_present`` is the set of source types the customer
    has at deploy time — only those types get a Switch case (since
    only those have a corresponding per-source driver pipeline). A
    new source of an existing type triggers no deploy. A new source
    of a never-seen-before type requires a deploy to add the LS / DS /
    ingest / per-source-driver / Switch case.
    """
    cases: list[dict[str, object]] = []
    for source_type in sorted(source_types_present):
        cases.append(
            {
                "value": source_type,
                "activities": [
                    {
                        "name": f"Invoke_{source_type.upper()}",
                        "type": "ExecutePipeline",
                        "typeProperties": {
                            "pipeline": {
                                "referenceName": naming.pipeline_per_source_driver(source_type),
                                "type": "PipelineReference",
                            },
                            "parameters": {
                                "source_id": {
                                    "value": "@item().source_id",
                                    "type": "Expression",
                                },
                            },
                            "waitOnCompletion": False,
                        },
                    },
                ],
            }
        )

    return {
        "name": naming.pipeline_master_driver(),
        "properties": {
            "annotations": [],
            "activities": [
                {
                    "name": "GetEnabledSources",
                    "type": "Lookup",
                    "policy": {
                        "timeout": "0.00:30:00",
                        "retry": 2,
                        "retryIntervalInSeconds": 30,
                    },
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderQuery": {
                                "value": _ENABLED_SOURCES_LOOKUP_QUERY,
                                "type": "Expression",
                            },
                            "queryTimeout": "00:30:00",
                            "partitionOption": "None",
                        },
                        "dataset": {
                            "referenceName": naming.dataset_metadata_db_query(),
                            "type": "DatasetReference",
                        },
                        "firstRowOnly": False,
                    },
                },
                {
                    "name": "ForEachSource",
                    "type": "ForEach",
                    "dependsOn": [
                        {
                            "activity": "GetEnabledSources",
                            "dependencyConditions": ["Succeeded"],
                        },
                    ],
                    "typeProperties": {
                        "items": {
                            "value": "@activity('GetEnabledSources').output.value",
                            "type": "Expression",
                        },
                        "isSequential": False,
                        "batchCount": 4,
                        "activities": [
                            {
                                "name": "DispatchByType",
                                "type": "Switch",
                                "typeProperties": {
                                    "on": {
                                        "value": "@item().source_type",
                                        "type": "Expression",
                                    },
                                    "cases": cases,
                                },
                            },
                        ],
                    },
                },
            ],
        },
    }


__all__ = [
    "build_pl_ingest_azure_blob_csv",
    "build_pl_ingest_azure_blob_json_lines",
    "build_pl_ingest_azure_sql_db",
    "build_pl_ingest_bigquery",
    "build_pl_ingest_mysql",
    "build_pl_ingest_oracle",
    "build_pl_ingest_postgres",
    "build_pl_ingest_rest_api",
    "build_pl_ingest_s3_csv",
    "build_pl_ingest_s3_json_lines",
    "build_pl_ingest_salesforce",
    "build_pl_ingest_sap_hana",
    "build_pl_ingest_servicenow",
    "build_pl_ingest_sftp_csv",
    "build_pl_ingest_sftp_json_lines",
    "build_pl_ingest_snowflake",
    "build_pl_ingest_sql_server",
    "build_pl_ingest_synapse_sql_pool",
    "build_pl_master_driver",
    "build_pl_per_source_driver",
]
