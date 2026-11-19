"""Enums for safe-listed values across the metadata model.

Every value here is a contract — adding a member is a minor version
bump (additive); removing or renaming is a breaking change.

Cross-references:
- ``docs/security.md`` → "Enum safe-listing"
- ``docs/metadata_keys.md`` → cross-key validation rules
- ``docs/sources/`` → per-source-type pages enumerate ``SourceType``
"""

from __future__ import annotations

from enum import StrEnum


class Realm(StrEnum):
    """Swappable combination of orchestrator + ingest engine + target + deploy surface.

    Scoped to ingestion only — "compute" (where downstream transforms run)
    is the consumer's choice, not a realm axis. See ``docs/realms_catalog.md``
    for the canonical list of realms (1 built + 5 planned + Tier A+/B
    candidates) and the naming convention.
    """

    ADF_ADLS = "adf-adls"
    # FABRIC_ONELAKE = "fabric-onelake"      # future (realm 2)
    # DATABRICKS_UC = "databricks-uc"        # future (realm 3)
    # DATABRICKS_ADLS = "databricks-adls"    # future (realm 4)
    # GLUE_S3 = "glue-s3"                    # future (realm 5; AWS pilot)
    # DATAFLOW_GCS = "dataflow-gcs"          # future (realm 6; GCP pilot)


class SourceType(StrEnum):
    """The 19 source types supported in v1.

    Grouped by category in ``docs/sources/``. Adding a new source type
    is a minor version bump; the realm registry expands in lockstep.
    """

    # RDBMS
    AZURE_SQL_DB = "azure_sql_db"
    SQL_SERVER = "sql_server"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SAP_HANA = "sap_hana"
    # Cloud DW
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    SYNAPSE_SQL_POOL = "synapse_sql_pool"
    # SaaS APIs
    REST_API = "rest_api"
    SALESFORCE = "salesforce"
    SERVICENOW = "servicenow"
    DATAVERSE = "dataverse"
    # File substrates
    SFTP = "sftp"
    ADLS_GEN2_X = "adls_gen2_x"
    AZURE_BLOB_X = "azure_blob_x"
    S3 = "s3"
    HTTPS_PULL = "https_pull"
    SHAREPOINT = "sharepoint"


class DatasetType(StrEnum):
    """Shape of a dataset's source.

    See ``docs/metadata_keys.md`` → ``dataset_config.dataset_type``.
    """

    TABLE = "TABLE"
    VIEW = "VIEW"
    QUERY = "QUERY"
    DOCUMENT = "DOCUMENT"


class LoadType(StrEnum):
    """How the framework reads from the source on each run."""

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class LoadProfile(StrEnum):
    """Classifier output that picks single-Copy vs. segmented.

    See ``docs/segmented_loads.md``.
    """

    SMALL = "small"
    LARGE = "large"
    HUGE = "huge"
    AUTO = "auto"


class SegmentationStrategy(StrEnum):
    """How a big-table full load is split into parallel segments."""

    NONE = "none"
    RANGE_NUMERIC = "range_numeric"
    RANGE_TEMPORAL = "range_temporal"
    MODULO = "modulo"
    DYNAMIC_RANGE = "dynamic_range"
    FILE_PER_SEGMENT = "file_per_segment"


class RunStatus(StrEnum):
    """Per-dataset run status. ``meta.run_log.status``."""

    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class MasterRunStatus(StrEnum):
    """Per-trigger run status. ``meta.master_run_log.status``."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class RunType(StrEnum):
    """Distinguishes normal runs from re-ingestion / discovery.

    ``meta.run_log.run_type`` and ``meta.master_run_log.run_type``.
    """

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    DISCOVER = "DISCOVER"
    REPLAY = "REPLAY"
    BACKFILL = "BACKFILL"


class TriggerType(StrEnum):
    """How the run was kicked off."""

    SCHEDULED = "scheduled"
    MANUAL = "manual"
    UPSTREAM = "upstream"
    RETRY = "retry"


class FlatteningMode(StrEnum):
    """Per-dataset rule for nested data → Bronze.

    See ``docs/flattening.md``.
    """

    PRESERVE = "preserve"
    STRINGIFY = "stringify"
    FLATTEN_EXPLICIT = "flatten_explicit"


class FlatteningEngine(StrEnum):
    """Where the flattening transformation runs."""

    AUTO = "auto"
    ADF = "adf"
    FUNCTION = "function"
    DATABRICKS = "databricks"


class ArrayStrategy(StrEnum):
    """How arrays are handled when ``flattening_mode != 'preserve'``."""

    PRESERVE = "preserve"
    STRINGIFY = "stringify"
    EXPLODE = "explode"
    TAKE_FIRST_ONLY = "take_first_only"


class AuthMethod(StrEnum):
    """How a source authenticates. Drives the KV-secret shape.

    See ``docs/sources/`` per-source pages for which methods each
    source type supports.
    """

    BASIC = "basic"
    CONNSTR = "connstr"
    WALLET = "wallet"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    BEARER = "bearer"
    AWS_ACCESS_KEY = "aws_access_key"
    MANAGED_IDENTITY = "managed_identity"


class PiiClass(StrEnum):
    """Per-column PII classification. Framework labels; downstream masks.

    See ``docs/security.md`` → "Logging hygiene" + ``docs/discovery.md`` →
    "PII sniffing".
    """

    NONE = "none"
    DIRECT = "direct"
    QUASI = "quasi"
    SENSITIVE = "sensitive"


class FileFormat(StrEnum):
    """Bronze sink + structured-file source format. Default ``PARQUET``."""

    PARQUET = "PARQUET"
    DELTA = "DELTA"  # future
    JSON = "JSON"
    CSV = "csv"
    TSV = "tsv"
    PIPE = "pipe"
    FIXED_WIDTH = "fixed_width"
    JSON_LINES = "json_lines"
    JSON_OBJECT = "json_object"
    XML = "xml"
    EXCEL = "excel"
    AVRO = "avro"


class CompressionCodec(StrEnum):
    """Parquet / structured-file compression."""

    SNAPPY = "snappy"
    GZIP = "gzip"
    ZSTD = "zstd"
    NONE = "none"
