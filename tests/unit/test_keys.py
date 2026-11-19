"""Unit tests for the KV-key registry."""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.exceptions import (
    InvalidIdentifierError,
    InvalidKvRefError,
    UnknownKeyError,
    ValidationError,
)
from lake_ingestion_framework.core.metadata.keys import (
    ALL_KEYS,
    KeyTable,
    KeyType,
    lookup_key,
    parse_value,
)
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.types.value_objects import Identifier, KvRef


def test_registry_is_non_empty() -> None:
    """Sanity: at least one key per table."""
    by_table = {table: 0 for table in KeyTable}
    for kd in ALL_KEYS:
        by_table[kd.table] += 1
    for table, count in by_table.items():
        assert count > 0, f"No keys registered for {table}"


def test_no_duplicate_keys_per_table_per_scope() -> None:
    """A (table, key_name, source_type_scope) tuple must be unique."""
    seen: set[tuple[KeyTable, str, tuple[SourceType, ...] | None]] = set()
    for kd in ALL_KEYS:
        key_tuple = (kd.table, kd.name, kd.source_type_scope)
        assert key_tuple not in seen, f"Duplicate key def: {key_tuple}"
        seen.add(key_tuple)


def test_lookup_known_platform_key() -> None:
    kd = lookup_key(KeyTable.PLATFORM_CONFIG, "default_realm")
    assert kd.name == "default_realm"
    assert kd.required is True
    assert kd.type is KeyType.ENUM


def test_lookup_unknown_raises() -> None:
    with pytest.raises(UnknownKeyError):
        lookup_key(KeyTable.PLATFORM_CONFIG, "totally_made_up")


def test_lookup_source_type_scoped() -> None:
    """SAP HANA-specific key only resolves when source_type matches."""
    # sap_client is HANA-only
    kd = lookup_key(KeyTable.SOURCE_CONFIG, "sap_client", source_type=SourceType.SAP_HANA)
    assert kd.name == "sap_client"

    # Without source_type, we should NOT match a scoped key
    with pytest.raises(UnknownKeyError):
        lookup_key(KeyTable.SOURCE_CONFIG, "sap_client", source_type=None)

    # Wrong source_type also fails
    with pytest.raises(UnknownKeyError):
        lookup_key(KeyTable.SOURCE_CONFIG, "sap_client", source_type=SourceType.AZURE_SQL_DB)


def test_lookup_common_source_key_for_any_type() -> None:
    """connection_pattern is common — works without scope."""
    kd = lookup_key(KeyTable.SOURCE_CONFIG, "connection_pattern")
    assert kd.name == "connection_pattern"

    # And also works with any source_type.
    for st in (SourceType.AZURE_SQL_DB, SourceType.ORACLE, SourceType.SHAREPOINT):
        kd = lookup_key(KeyTable.SOURCE_CONFIG, "connection_pattern", source_type=st)
        assert kd.name == "connection_pattern"


# ──────────────────────────────────────────────────────────────────────
# parse_value
# ──────────────────────────────────────────────────────────────────────


class TestParseValue:
    def test_string(self) -> None:
        kd = lookup_key(KeyTable.PLATFORM_CONFIG, "target_storage_account")
        assert parse_value(kd, "myaccount") == "myaccount"

    def test_int(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "segment_count")
        assert parse_value(kd, "8") == 8

    def test_int_invalid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "segment_count")
        with pytest.raises(ValidationError, match="cannot parse"):
            parse_value(kd, "not_an_int")

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("true", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("false", False),
            ("0", False),
            ("no", False),
            ("off", False),
        ],
    )
    def test_bool(self, raw: str, expected: bool) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "enable_staging")
        assert parse_value(kd, raw) is expected

    def test_bool_invalid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "enable_staging")
        with pytest.raises(ValidationError, match="bool"):
            parse_value(kd, "maybe")

    def test_enum_valid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "load_type")
        assert parse_value(kd, "INCREMENTAL") == "INCREMENTAL"

    def test_enum_invalid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "load_type")
        with pytest.raises(ValidationError, match="safe-list"):
            parse_value(kd, "PARTIAL")

    def test_kv_ref_valid(self) -> None:
        kd = lookup_key(KeyTable.SOURCE_CONFIG, "connection_secret_name")
        result = parse_value(kd, "kv://prod-vault/my-secret")
        assert isinstance(result, KvRef)
        assert result.vault == "prod-vault"

    def test_kv_ref_invalid(self) -> None:
        kd = lookup_key(KeyTable.SOURCE_CONFIG, "connection_secret_name")
        with pytest.raises(InvalidKvRefError):
            parse_value(kd, "not-a-kv-ref")

    def test_identifier_valid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "schema_name")
        result = parse_value(kd, "dbo")
        assert isinstance(result, Identifier)
        assert result.name == "dbo"

    def test_identifier_invalid(self) -> None:
        kd = lookup_key(KeyTable.DATASET_CONFIG, "schema_name")
        with pytest.raises(InvalidIdentifierError):
            parse_value(kd, "1invalid")

    def test_json_valid(self) -> None:
        kd = lookup_key(KeyTable.PLATFORM_CONFIG, "redaction_extra_patterns")
        result = parse_value(kd, '["pwd=", "secret="]')
        assert result == ["pwd=", "secret="]

    def test_json_invalid(self) -> None:
        kd = lookup_key(KeyTable.PLATFORM_CONFIG, "redaction_extra_patterns")
        with pytest.raises(ValidationError, match="JSON"):
            parse_value(kd, "[unterminated")
