"""Unit tests for ``LocalDeltaWriter`` (Phase 0b).

Tests run without ``deltalake`` or ``pyarrow`` installed — both are
mocked at the lazy-import boundary. Real-DB / real-Delta behaviour is
covered by integration tests gated on ``[local]`` extra.

What's covered:

- Row enrichment with ``_run_id`` / ``_loaded_at`` / ``_record_hash``
- ``_record_hash`` recipe (sentinel for nulls; deterministic SHA256)
- Custom ``hash_columns`` (subset participates)
- Empty batches skipped (no write call)
- Multiple batches accumulated in stats
- Missing extra surfaces a setup-friendly error
- Write error mapped to ``WriteFailedError``
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from lake_ingestion_framework.core.exceptions import (
    SinkUnavailableError,
    WriteFailedError,
)
from lake_ingestion_framework.core.source_connectors.protocol import QueryBatch
from lake_ingestion_framework.core.writers.local_delta import (
    LocalDeltaWriter,
)

# ──────────────────────────────────────────────────────────────────────
# Pyarrow / deltalake fakes
# ──────────────────────────────────────────────────────────────────────


class _FakeArray:
    def __init__(self, values: list[Any], type_hint: Any = None) -> None:
        self.values = list(values)
        self.type_hint = type_hint


class _FakeArrowTable:
    def __init__(self, arrays: list[_FakeArray], names: list[str]) -> None:
        self.arrays = list(arrays)
        self.names = list(names)


class _FakeTimestampType:
    def __init__(self, unit: str, tz: str | None) -> None:
        self.unit = unit
        self.tz = tz


class _FakePyArrowModule:
    @staticmethod
    def array(values: list[Any], type: Any = None) -> _FakeArray:
        return _FakeArray(values, type_hint=type)

    @staticmethod
    def string() -> str:
        return "string"

    @staticmethod
    def timestamp(unit: str, tz: str | None = None) -> _FakeTimestampType:
        return _FakeTimestampType(unit, tz)

    class Table:
        @staticmethod
        def from_arrays(arrays: list[_FakeArray], names: list[str]) -> _FakeArrowTable:
            return _FakeArrowTable(arrays, names)


class _FakeDeltalakeModule:
    """Records every write_deltalake call."""

    def __init__(self, raise_on_write: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._raise = raise_on_write

    def write_deltalake(
        self,
        table_or_uri: str,
        data: Any,
        *,
        mode: str = "append",
        partition_by: list[str] | None = None,
    ) -> None:
        if self._raise is not None:
            raise self._raise
        self.calls.append(
            {
                "uri": table_or_uri,
                "data": data,
                "mode": mode,
                "partition_by": partition_by,
            }
        )


def _patch_pyarrow(monkeypatch: pytest.MonkeyPatch) -> _FakePyArrowModule:
    fake = _FakePyArrowModule()
    monkeypatch.setattr(
        "lake_ingestion_framework.core.writers.local_delta._lazy_import_pyarrow",
        lambda: fake,
    )
    return fake


def _patch_deltalake(
    monkeypatch: pytest.MonkeyPatch, *, raise_on_write: Exception | None = None
) -> _FakeDeltalakeModule:
    fake = _FakeDeltalakeModule(raise_on_write=raise_on_write)
    monkeypatch.setattr(
        "lake_ingestion_framework.core.writers.local_delta._lazy_import_deltalake",
        lambda: fake,
    )
    return fake


# ──────────────────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────────────────


def test_writes_one_batch_and_returns_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/tmp/lakes/orders"))
    batch = QueryBatch(
        columns=("id", "email"),
        rows=[(1, "a@x.com"), (2, "b@x.com")],
    )
    stats = writer.write_batched([batch], run_id="run-123")

    assert stats.rows_written == 2
    assert stats.batches_written == 1
    assert stats.columns == ("id", "email", "_run_id", "_loaded_at", "_record_hash")
    assert len(fake_dl.calls) == 1
    call = fake_dl.calls[0]
    assert call["uri"] == "/tmp/lakes/orders"
    assert call["mode"] == "append"
    assert call["partition_by"] is None


def test_writes_multiple_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/tmp/lakes/orders"))
    batches = [
        QueryBatch(columns=("id",), rows=[(1,), (2,)]),
        QueryBatch(columns=("id",), rows=[(3,)]),
    ]
    stats = writer.write_batched(batches, run_id="run-1")

    assert stats.rows_written == 3
    assert stats.batches_written == 2
    assert len(fake_dl.calls) == 2


def test_partition_by_passed_through(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(
        table_path=Path("/lakes/x"),
        partition_by=["_partition_date"],
    )
    batch = QueryBatch(columns=("id",), rows=[(1,)])
    writer.write_batched([batch], run_id="r")

    assert fake_dl.calls[0]["partition_by"] == ["_partition_date"]


def test_empty_batch_does_not_call_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    empty = QueryBatch(columns=("id",), rows=[])
    stats = writer.write_batched([empty], run_id="r")

    assert stats.rows_written == 0
    assert stats.batches_written == 0
    assert fake_dl.calls == []


def test_zero_batch_iterator_returns_empty_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    stats = writer.write_batched([], run_id="r")

    assert stats.rows_written == 0
    assert stats.batches_written == 0
    assert stats.columns == ()
    assert fake_dl.calls == []


# ──────────────────────────────────────────────────────────────────────
# Metadata column injection
# ──────────────────────────────────────────────────────────────────────


def test_metadata_columns_injected_into_arrow_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    batch = QueryBatch(columns=("id",), rows=[(1,), (2,)])
    writer.write_batched([batch], run_id="run-x")

    table: _FakeArrowTable = fake_dl.calls[0]["data"]
    assert table.names == ["id", "_run_id", "_loaded_at", "_record_hash"]
    # _run_id column has run-x for every row
    run_id_array = table.arrays[1]
    assert run_id_array.values == ["run-x", "run-x"]


def test_loaded_at_is_utc_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    before = datetime.now(UTC)
    writer.write_batched([QueryBatch(columns=("id",), rows=[(1,)])], run_id="r")
    after = datetime.now(UTC)

    table: _FakeArrowTable = fake_dl.calls[0]["data"]
    loaded_at_array = table.arrays[2]  # 0=id, 1=_run_id, 2=_loaded_at
    timestamps = loaded_at_array.values
    assert all(before <= ts <= after for ts in timestamps)


# ──────────────────────────────────────────────────────────────────────
# Hash recipe
# ──────────────────────────────────────────────────────────────────────


def _expected_hash(parts: list[Any]) -> str:
    """Re-implement the hash recipe in the test for cross-checking."""
    sentinel = "\0NULL\0"
    serialized = sentinel.join(sentinel if p is None else str(p) for p in parts)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def test_record_hash_default_uses_all_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    batch = QueryBatch(columns=("id", "email"), rows=[(1, "a@x.com")])
    writer.write_batched([batch], run_id="r")

    table: _FakeArrowTable = fake_dl.calls[0]["data"]
    hash_array = table.arrays[3]  # 0=id, 1=email, 2=_run_id, 3=_loaded_at, 4=_record_hash
    # Wait: arrays = [id, email, _run_id, _loaded_at, _record_hash]
    # So _record_hash is at index 4
    hash_array = table.arrays[4]
    assert hash_array.values == [_expected_hash([1, "a@x.com"])]


def test_record_hash_with_nulls(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    batch = QueryBatch(columns=("a", "b"), rows=[(None, "x"), ("y", None)])
    writer.write_batched([batch], run_id="r")

    table: _FakeArrowTable = fake_dl.calls[0]["data"]
    hash_array = table.arrays[4]  # 0=a, 1=b, 2=_run_id, 3=_loaded_at, 4=_record_hash
    assert hash_array.values == [
        _expected_hash([None, "x"]),
        _expected_hash(["y", None]),
    ]


def test_record_hash_subset_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    fake_dl = _patch_deltalake(monkeypatch)

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    # 'audit_at' is excluded from the hash so two rows differing only in
    # audit_at produce the same _record_hash
    batch = QueryBatch(
        columns=("id", "email", "audit_at"),
        rows=[(1, "a@x.com", "t1"), (1, "a@x.com", "t2")],
    )
    writer.write_batched([batch], run_id="r", hash_columns=["id", "email"])

    table: _FakeArrowTable = fake_dl.calls[0]["data"]
    # arrays = [id, email, audit_at, _run_id, _loaded_at, _record_hash]
    hash_array = table.arrays[5]
    assert hash_array.values[0] == hash_array.values[1]
    expected = _expected_hash([1, "a@x.com"])
    assert hash_array.values == [expected, expected]


# ──────────────────────────────────────────────────────────────────────
# Error paths
# ──────────────────────────────────────────────────────────────────────


def test_missing_deltalake_extra_raises_setup_error() -> None:
    """Without the [local] extra, the writer surfaces a clean error."""
    import importlib.util

    if importlib.util.find_spec("deltalake") is not None:
        pytest.skip("deltalake is installed; this test only runs without the extra")
    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    with pytest.raises(SinkUnavailableError, match="deltalake not installed"):
        writer.write_batched([QueryBatch(columns=("id",), rows=[(1,)])], run_id="r")


def test_write_error_mapped_to_write_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pyarrow(monkeypatch)
    _patch_deltalake(monkeypatch, raise_on_write=RuntimeError("disk full or whatever"))

    writer = LocalDeltaWriter(table_path=Path("/lakes/x"))
    with pytest.raises(WriteFailedError, match="Delta write failed"):
        writer.write_batched([QueryBatch(columns=("id",), rows=[(1,)])], run_id="r")
