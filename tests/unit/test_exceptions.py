"""Unit tests for the exception hierarchy.

The hierarchy is part of the public API — catching ``LakeIngestError``
at a boundary should catch every framework-specific error, and not
catch anything else.
"""

from __future__ import annotations

import pytest
from lake_ingestion_framework.core.exceptions import (
    AuthenticationError,
    CrossKeyValidationError,
    InvalidIdentifierError,
    InvalidKvRefError,
    LakeIngestError,
    MetadataConnectionError,
    MetadataError,
    QueryFailedError,
    RuntimeStateError,
    SchemaDriftError,
    SegmentationConfigError,
    SinkError,
    SinkUnavailableError,
    SourceError,
    SourceUnavailableError,
    UnknownKeyError,
    ValidationError,
    WatermarkAdvanceError,
    WriteFailedError,
)


@pytest.mark.parametrize(
    "exc_class",
    [
        ValidationError,
        InvalidKvRefError,
        InvalidIdentifierError,
        UnknownKeyError,
        SegmentationConfigError,
        CrossKeyValidationError,
        SourceError,
        SourceUnavailableError,
        AuthenticationError,
        QueryFailedError,
        SchemaDriftError,
        SinkError,
        SinkUnavailableError,
        WriteFailedError,
        MetadataError,
        MetadataConnectionError,
        WatermarkAdvanceError,
        RuntimeStateError,
    ],
)
def test_all_exceptions_inherit_lakeingest_error(exc_class: type[Exception]) -> None:
    """Catching LakeIngestError must catch every framework error."""
    assert issubclass(exc_class, LakeIngestError)


def test_validation_subtypes_inherit_validation_error() -> None:
    for sub in (
        InvalidKvRefError,
        InvalidIdentifierError,
        UnknownKeyError,
        SegmentationConfigError,
        CrossKeyValidationError,
    ):
        assert issubclass(sub, ValidationError)


def test_source_subtypes_inherit_source_error() -> None:
    for sub in (
        SourceUnavailableError,
        AuthenticationError,
        QueryFailedError,
        SchemaDriftError,
    ):
        assert issubclass(sub, SourceError)


def test_sink_subtypes_inherit_sink_error() -> None:
    for sub in (SinkUnavailableError, WriteFailedError):
        assert issubclass(sub, SinkError)


def test_metadata_subtypes_inherit_metadata_error() -> None:
    for sub in (MetadataConnectionError, WatermarkAdvanceError):
        assert issubclass(sub, MetadataError)


def test_invalid_kv_ref_carries_raw() -> None:
    err = InvalidKvRefError("not_a_kv_ref")
    assert err.raw == "not_a_kv_ref"
    assert "not_a_kv_ref" in str(err)


def test_invalid_identifier_carries_role() -> None:
    err = InvalidIdentifierError("123bad", role="schema_name")
    assert err.identifier == "123bad"
    assert err.role == "schema_name"
    assert "schema_name" in str(err)


def test_unknown_key_carries_table_and_key() -> None:
    err = UnknownKeyError(table="dataset_config", key="rmrf")
    assert err.table == "dataset_config"
    assert err.key == "rmrf"
    assert "rmrf" in str(err)


def test_query_failed_carries_query_hash_not_query_text() -> None:
    """We never log the raw query — only the hash. Test enforces that
    the message uses the hash, not query text."""
    err = QueryFailedError(query_hash="sha256:7f3e", message="ORA-00942")
    assert "sha256:7f3e" in str(err)
    assert "ORA-00942" in str(err)


def _raises_keyboard_interrupt_under_lakeingest_handler() -> None:
    """Helper: raise KeyboardInterrupt while a LakeIngestError handler is active.

    The handler MUST NOT catch the interrupt — KeyboardInterrupt is
    not a LakeIngestError.
    """
    try:
        raise KeyboardInterrupt
    except LakeIngestError:
        pytest.fail("LakeIngestError should not catch KeyboardInterrupt")


def test_lakeingest_error_does_not_catch_keyboard_interrupt() -> None:
    """The framework's catch-at-boundary pattern must not swallow interactive interrupts."""
    with pytest.raises(KeyboardInterrupt):
        _raises_keyboard_interrupt_under_lakeingest_handler()
