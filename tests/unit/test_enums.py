"""Unit tests for the enum value contracts.

Every enum here is part of the public API. Adding a member is
additive (minor version); removing or renaming is breaking. These
tests pin the exact wire values to catch unintentional renames.
"""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.types.enums import (
    ArrayStrategy,
    AuthMethod,
    CompressionCodec,
    DatasetType,
    FileFormat,
    FlatteningEngine,
    FlatteningMode,
    LoadProfile,
    LoadType,
    MasterRunStatus,
    PiiClass,
    Realm,
    RunStatus,
    RunType,
    SegmentationStrategy,
    SourceType,
    TriggerType,
)


def test_realm_known_values() -> None:
    assert Realm.ADF_ADLS.value == "adf-adls"


def test_source_type_v1_count_is_19() -> None:
    """v1 ships exactly 19 source types — count locked here."""
    assert len(SourceType) == 19


@pytest.mark.parametrize(
    ("member", "wire_value"),
    [
        (SourceType.AZURE_SQL_DB, "azure_sql_db"),
        (SourceType.SQL_SERVER, "sql_server"),
        (SourceType.ORACLE, "oracle"),
        (SourceType.POSTGRES, "postgres"),
        (SourceType.MYSQL, "mysql"),
        (SourceType.SAP_HANA, "sap_hana"),
        (SourceType.SNOWFLAKE, "snowflake"),
        (SourceType.BIGQUERY, "bigquery"),
        (SourceType.SYNAPSE_SQL_POOL, "synapse_sql_pool"),
        (SourceType.REST_API, "rest_api"),
        (SourceType.SALESFORCE, "salesforce"),
        (SourceType.SERVICENOW, "servicenow"),
        (SourceType.DATAVERSE, "dataverse"),
        (SourceType.SFTP, "sftp"),
        (SourceType.ADLS_GEN2_X, "adls_gen2_x"),
        (SourceType.AZURE_BLOB_X, "azure_blob_x"),
        (SourceType.S3, "s3"),
        (SourceType.HTTPS_PULL, "https_pull"),
        (SourceType.SHAREPOINT, "sharepoint"),
    ],
)
def test_source_type_wire_values(member: SourceType, wire_value: str) -> None:
    assert member.value == wire_value


def test_dataset_type_values() -> None:
    assert {t.value for t in DatasetType} == {"TABLE", "VIEW", "QUERY", "DOCUMENT"}


def test_load_type_values() -> None:
    assert {t.value for t in LoadType} == {"FULL", "INCREMENTAL"}


def test_load_profile_values() -> None:
    assert {p.value for p in LoadProfile} == {"small", "large", "huge", "auto"}


def test_segmentation_strategy_values() -> None:
    assert {s.value for s in SegmentationStrategy} == {
        "none",
        "range_numeric",
        "range_temporal",
        "modulo",
        "dynamic_range",
        "file_per_segment",
    }


def test_run_status_values() -> None:
    assert {s.value for s in RunStatus} == {"STARTED", "SUCCESS", "FAILED", "SKIPPED"}


def test_master_run_status_values() -> None:
    assert {s.value for s in MasterRunStatus} == {"RUNNING", "SUCCESS", "PARTIAL", "FAILED"}


def test_run_type_includes_replay_and_backfill() -> None:
    assert {t.value for t in RunType} == {
        "FULL",
        "INCREMENTAL",
        "DISCOVER",
        "REPLAY",
        "BACKFILL",
    }


def test_trigger_type_values() -> None:
    assert {t.value for t in TriggerType} == {"scheduled", "manual", "upstream", "retry"}


def test_flattening_mode_values() -> None:
    assert {m.value for m in FlatteningMode} == {"preserve", "stringify", "flatten_explicit"}


def test_flattening_engine_values() -> None:
    assert {e.value for e in FlatteningEngine} == {"auto", "adf", "function", "databricks"}


def test_array_strategy_values() -> None:
    assert {s.value for s in ArrayStrategy} == {
        "preserve",
        "stringify",
        "explode",
        "take_first_only",
    }


def test_auth_method_values() -> None:
    assert {a.value for a in AuthMethod} == {
        "basic",
        "connstr",
        "wallet",
        "oauth2_client_credentials",
        "bearer",
        "aws_access_key",
        "managed_identity",
    }


def test_pii_class_values() -> None:
    assert {p.value for p in PiiClass} == {"none", "direct", "quasi", "sensitive"}


def test_file_format_includes_parquet() -> None:
    assert FileFormat.PARQUET.value == "PARQUET"


def test_compression_codec_default_is_snappy() -> None:
    """Snappy is the framework's default; ensure it's a valid codec."""
    assert CompressionCodec.SNAPPY.value == "snappy"


def test_str_enum_membership_via_str_value() -> None:
    """StrEnum lookups work both by attribute and by string value."""
    assert SourceType("oracle") is SourceType.ORACLE
    assert SourceType("oracle") == "oracle"


def test_unknown_value_raises() -> None:
    with pytest.raises(ValueError, match="not a valid"):
        SourceType("totally_not_a_real_source")
