"""Unit tests for the Bronze → Silver pipeline.

The pipeline orchestrates: read Bronze (deltalake) → drop Bronze metadata
→ append Silver metadata → write Silver (deltalake). Tests mock the
deltalake module at the lazy-import boundary so we don't need real Delta
files; pyarrow is real (installed via [local]) so we get realistic Table
operations.

What's covered:

- Bronze metadata cols are dropped before Silver write
- Silver metadata cols are appended (_silver_run_id, _silver_loaded_at,
  _record_hash) with the correct types
- The same _silver_run_id appears on every row of one run
- _record_hash is deterministic + recomputed at Silver (not inherited
  from Bronze)
- Custom business_keys narrow the hash input
- append strategy → write_deltalake called with mode='append'
- Unsupported strategies raise UnsupportedSilverStrategyError
- Read errors surface as SinkUnavailableError
- Write errors surface as WriteFailedError
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from lake_ingestion_framework.core.exceptions import (
    SinkUnavailableError,
    ValidationError,
    WriteFailedError,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_LOADED_AT,
    BRONZE_RECORD_HASH,
    BRONZE_RUN_ID,
    SILVER_LOADED_AT,
    SILVER_RECORD_HASH,
    SILVER_RUN_ID,
    compute_record_hash,
)
from lake_ingestion_framework.core.silver import (
    SilverConfig,
    SilverPipeline,
    SilverStrategy,
)
from lake_ingestion_framework.core.silver.pipeline import (
    UnsupportedSilverStrategyError,
)

# ──────────────────────────────────────────────────────────────────────
# deltalake fakes
# ──────────────────────────────────────────────────────────────────────


class _FakeDeltaTable:
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_pyarrow_table(self) -> pa.Table:
        return self._table


class _FakeDeltalake:
    """Records every write_deltalake call; serves a configured DeltaTable."""

    def __init__(
        self,
        bronze_table: pa.Table | None = None,
        *,
        read_raises: Exception | None = None,
        write_raises: Exception | None = None,
    ) -> None:
        self._bronze = bronze_table
        self._read_raises = read_raises
        self._write_raises = write_raises
        self.write_calls: list[dict[str, Any]] = []

    def DeltaTable(self, path: str) -> _FakeDeltaTable:
        if self._read_raises is not None:
            raise self._read_raises
        if self._bronze is None:
            raise AssertionError("Test forgot to provide a bronze_table to _FakeDeltalake.")
        return _FakeDeltaTable(self._bronze)

    def write_deltalake(
        self,
        path: str,
        data: pa.Table,
        *,
        mode: str = "append",
        partition_by: list[str] | None = None,
    ) -> None:
        if self._write_raises is not None:
            raise self._write_raises
        self.write_calls.append(
            {"path": path, "data": data, "mode": mode, "partition_by": partition_by}
        )


def _patch_deltalake(monkeypatch: pytest.MonkeyPatch, fake: _FakeDeltalake) -> _FakeDeltalake:
    monkeypatch.setattr(
        "lake_ingestion_framework.core.silver.pipeline._lazy_import_deltalake",
        lambda: fake,
    )
    return fake


# ──────────────────────────────────────────────────────────────────────
# Test data
# ──────────────────────────────────────────────────────────────────────


def _bronze_with_metadata() -> pa.Table:
    """A Bronze-shaped table: business cols + Bronze metadata."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
            BRONZE_RUN_ID: ["bronze-run-1", "bronze-run-1", "bronze-run-1"],
            BRONZE_LOADED_AT: pa.array(
                [datetime(2026, 5, 1, tzinfo=UTC)] * 3,
                type=pa.timestamp("us", tz="UTC"),
            ),
            BRONZE_RECORD_HASH: ["bh1", "bh2", "bh3"],
        }
    )


def _config(tmp_path: Path, **overrides: Any) -> SilverConfig:
    base: dict[str, Any] = {
        "bronze_path": tmp_path / "bronze",
        "silver_path": tmp_path / "silver",
        "run_id": "silver-run-42",
        "strategy": SilverStrategy.APPEND,
    }
    base.update(overrides)
    return SilverConfig(**base)


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


def test_drops_bronze_run_id_and_loaded_at(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Bronze tracking columns are dropped; Silver substitutes its own.

    Note: Bronze ``_record_hash`` *is* removed too, but the column name
    is reused by Silver (with a recomputed value) — that's covered by
    ``test_record_hash_is_recomputed_not_inherited_from_bronze``.
    """
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path))

    written: pa.Table = fake.write_calls[0]["data"]
    assert BRONZE_RUN_ID not in written.column_names
    assert BRONZE_LOADED_AT not in written.column_names


def test_adds_silver_metadata_columns(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path))

    written: pa.Table = fake.write_calls[0]["data"]
    assert SILVER_RUN_ID in written.column_names
    assert SILVER_LOADED_AT in written.column_names
    assert SILVER_RECORD_HASH in written.column_names

    # _silver_run_id is the same value on every row
    run_ids = set(written.column(SILVER_RUN_ID).to_pylist())
    assert run_ids == {"silver-run-42"}


def test_silver_loaded_at_is_utc_timestamp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    before = datetime.now(UTC)
    SilverPipeline().run(_config(tmp_path))
    after = datetime.now(UTC)

    written: pa.Table = fake.write_calls[0]["data"]
    timestamps = written.column(SILVER_LOADED_AT).to_pylist()
    assert all(before <= ts <= after for ts in timestamps)
    # All rows share one batch-level timestamp
    assert len(set(timestamps)) == 1


def test_record_hash_is_recomputed_not_inherited_from_bronze(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Silver hashes the surviving business cols; Bronze _record_hash is dropped."""
    bronze = _bronze_with_metadata()
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(bronze))
    SilverPipeline().run(_config(tmp_path))

    written: pa.Table = fake.write_calls[0]["data"]
    silver_hashes = written.column(SILVER_RECORD_HASH).to_pylist()
    bronze_hashes = bronze.column(BRONZE_RECORD_HASH).to_pylist()
    assert silver_hashes != bronze_hashes  # different recipe + different inputs

    # Silver hash for row 0 = SHA256 over (id=1, email="a@x.com")
    expected = compute_record_hash((1, "a@x.com"))
    assert silver_hashes[0] == expected


def test_hash_columns_narrow_hash_input(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Hashing only ``id`` makes ``_record_hash`` independent of ``email``."""
    bronze = _bronze_with_metadata()
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(bronze))
    SilverPipeline().run(_config(tmp_path, hash_columns=("id",)))

    written: pa.Table = fake.write_calls[0]["data"]
    silver_hashes = written.column(SILVER_RECORD_HASH).to_pylist()
    assert silver_hashes[0] == compute_record_hash((1,))
    assert silver_hashes[1] == compute_record_hash((2,))
    assert silver_hashes[2] == compute_record_hash((3,))


def test_hash_columns_unknown_columns_are_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A hash column that isn't in the table is dropped silently."""
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path, hash_columns=("id", "unknown_col")))

    written: pa.Table = fake.write_calls[0]["data"]
    # Hash should ignore the missing col; falls back to ('id',) only
    assert written.column(SILVER_RECORD_HASH).to_pylist()[0] == compute_record_hash((1,))


def test_append_strategy_uses_append_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path, strategy=SilverStrategy.APPEND))

    assert fake.write_calls[0]["mode"] == "append"


def test_partition_by_is_passed_through(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path, partition_by=("id",)))

    assert fake.write_calls[0]["partition_by"] == ["id"]


def test_returns_run_stats(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bronze = _bronze_with_metadata()
    _patch_deltalake(monkeypatch, _FakeDeltalake(bronze))
    stats = SilverPipeline().run(_config(tmp_path))

    assert stats.rows_read == bronze.num_rows
    assert stats.rows_written == bronze.num_rows
    assert stats.strategy == SilverStrategy.APPEND


# ──────────────────────────────────────────────────────────────────────
# Error paths
# ──────────────────────────────────────────────────────────────────────


def test_unsupported_strategy_value_at_construction(tmp_path: Path) -> None:
    """SilverStrategy is a StrEnum — a non-enum value can't be constructed."""
    with pytest.raises(ValueError, match="not a valid"):
        SilverConfig(
            bronze_path=tmp_path / "b",
            silver_path=tmp_path / "s",
            run_id="x",
            strategy=SilverStrategy("not_a_real_strategy"),  # type: ignore[arg-type]
        )


def test_strategy_not_implemented_raises_during_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If a future enum value lacks a write-mode mapping, fail loud at runtime."""
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    config = _config(tmp_path)
    # Bypass the StrEnum validation by hand-installing a fictional value
    object.__setattr__(config, "strategy", "scd1_future")  # type: ignore[arg-type]

    with pytest.raises(UnsupportedSilverStrategyError):
        SilverPipeline().run(config)
    assert fake.write_calls == []  # no write attempted


def test_read_error_surfaces_as_sink_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeDeltalake(read_raises=FileNotFoundError("no such Delta table"))
    _patch_deltalake(monkeypatch, fake)
    with pytest.raises(SinkUnavailableError, match="Cannot read Bronze"):
        SilverPipeline().run(_config(tmp_path))


def test_write_error_surfaces_as_write_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeDeltalake(_bronze_with_metadata(), write_raises=RuntimeError("disk full"))
    _patch_deltalake(monkeypatch, fake)
    with pytest.raises(WriteFailedError, match="Silver write failed"):
        SilverPipeline().run(_config(tmp_path))


# ──────────────────────────────────────────────────────────────────────
# SCD1 — preconditions + initial-load + MERGE
# ──────────────────────────────────────────────────────────────────────


class _FakeMergeBuilder:
    """Records merge configuration; chainable like deltalake's Merger."""

    def __init__(self, recorder: dict[str, Any]) -> None:
        self._rec = recorder

    def when_matched_update_all(self) -> _FakeMergeBuilder:
        self._rec["when_matched_update_all_called"] = True
        return self

    def when_not_matched_insert_all(self) -> _FakeMergeBuilder:
        self._rec["when_not_matched_insert_all_called"] = True
        return self

    def execute(self) -> dict[str, str]:
        """Mirror deltalake's merge: returns a metrics dict (string-typed)."""
        self._rec["executed"] = True
        return self._rec.get("execute_returns", {})


class _FakeDeltaTableForMerge(_FakeDeltaTable):
    """DeltaTable stand-in that records merge() calls."""

    def __init__(self, table: pa.Table, merge_record: dict[str, Any]) -> None:
        super().__init__(table)
        self._merge_record = merge_record

    def merge(
        self,
        source: pa.Table,
        predicate: str,
        *,
        source_alias: str,
        target_alias: str,
    ) -> _FakeMergeBuilder:
        self._merge_record.update(
            source=source,
            predicate=predicate,
            source_alias=source_alias,
            target_alias=target_alias,
        )
        return _FakeMergeBuilder(self._merge_record)


def test_scd1_requires_business_keys(tmp_path: Path) -> None:
    """SCD1 has no MERGE predicate without business_keys → fail at validate."""
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.SCD1,
        business_keys=None,
    )
    with pytest.raises(ValidationError, match="requires non-empty business_keys"):
        SilverPipeline().run(config)


def test_scd1_rejects_unsafe_business_key_identifiers(tmp_path: Path) -> None:
    """Identifier-validation guards SQL injection through MERGE predicate."""
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.SCD1,
        business_keys=("id; DROP TABLE customers --",),
    )
    with pytest.raises(ValidationError, match="not a safe SQL identifier"):
        SilverPipeline().run(config)


def test_scd1_initial_load_uses_write_deltalake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """First run for a Silver path: no existing table → write_deltalake (append mode)."""
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path, strategy=SilverStrategy.SCD1, business_keys=("id",)))

    # No _delta_log/ in tmp_path → existence check fails → falls back to append
    assert fake.write_calls[0]["mode"] == "append"


def test_scd1_subsequent_run_invokes_merge(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When the Silver path already has a _delta_log/, run() does a MERGE."""
    silver_path = tmp_path / "silver"
    (silver_path / "_delta_log").mkdir(parents=True)  # simulate existing table

    merge_record: dict[str, Any] = {}
    merger_table = _FakeDeltaTableForMerge(_bronze_with_metadata(), merge_record)

    class _MergeAwareFake(_FakeDeltalake):
        """DeltaTable() returns the merge-recorder for the silver path; bronze read also works."""

        def DeltaTable(self, path: str) -> _FakeDeltaTable:
            if path == str(silver_path):
                return merger_table
            return super().DeltaTable(path)

    fake = _patch_deltalake(monkeypatch, _MergeAwareFake(_bronze_with_metadata()))

    SilverPipeline().run(
        _config(
            tmp_path,
            silver_path=silver_path,
            strategy=SilverStrategy.SCD1,
            business_keys=("id",),
        )
    )

    # No write_deltalake call — MERGE was used instead
    assert fake.write_calls == []
    # MERGE recorded with correct predicate
    assert merge_record["predicate"] == "target.id = source.id"
    assert merge_record["source_alias"] == "source"
    assert merge_record["target_alias"] == "target"
    assert merge_record["when_matched_update_all_called"] is True
    assert merge_record["when_not_matched_insert_all_called"] is True
    assert merge_record["executed"] is True


def test_scd1_merge_predicate_joins_multiple_keys_with_and(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    silver_path = tmp_path / "silver"
    (silver_path / "_delta_log").mkdir(parents=True)
    merge_record: dict[str, Any] = {}
    merger_table = _FakeDeltaTableForMerge(_bronze_with_metadata(), merge_record)

    class _MergeAwareFake(_FakeDeltalake):
        def DeltaTable(self, path: str) -> _FakeDeltaTable:
            if path == str(silver_path):
                return merger_table
            return super().DeltaTable(path)

    _patch_deltalake(monkeypatch, _MergeAwareFake(_bronze_with_metadata()))
    SilverPipeline().run(
        _config(
            tmp_path,
            silver_path=silver_path,
            strategy=SilverStrategy.SCD1,
            business_keys=("id", "email"),
        )
    )
    assert merge_record["predicate"] == "target.id = source.id AND target.email = source.email"


# ──────────────────────────────────────────────────────────────────────
# SCD2 — preconditions + initial-load + classification + close
# ──────────────────────────────────────────────────────────────────────


def test_scd2_requires_business_keys(tmp_path: Path) -> None:
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.SCD2,
        business_keys=None,
        scd2_order_by="updated_at",
    )
    with pytest.raises(ValidationError, match="non-empty business_keys"):
        SilverPipeline().run(config)


def test_scd2_requires_scd2_order_by(tmp_path: Path) -> None:
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.SCD2,
        business_keys=("id",),
        scd2_order_by=None,
    )
    with pytest.raises(ValidationError, match="scd2_order_by"):
        SilverPipeline().run(config)


def test_scd2_rejects_unsafe_scd2_order_by(tmp_path: Path) -> None:
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.SCD2,
        business_keys=("id",),
        scd2_order_by="updated_at; DROP TABLE--",
    )
    with pytest.raises(ValidationError, match="not a safe SQL identifier"):
        SilverPipeline().run(config)


def _bronze_with_order_by() -> pa.Table:
    """Bronze rows with an order-by column the SCD2 path can use."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
            "updated_at": pa.array(
                [
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 2, 1, tzinfo=UTC),
                    datetime(2026, 3, 1, tzinfo=UTC),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )


def test_scd2_initial_load_stamps_scd2_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No existing target → write_deltalake with SCD2 cols (_is_current=true, _valid_from set, _valid_to NULL)."""  # noqa: E501
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_order_by()))
    SilverPipeline().run(
        SilverConfig(
            bronze_path=tmp_path / "b",
            silver_path=tmp_path / "s",
            run_id="r",
            strategy=SilverStrategy.SCD2,
            business_keys=("id",),
            scd2_order_by="updated_at",
        )
    )

    written: pa.Table = fake.write_calls[0]["data"]
    assert "_is_current" in written.column_names
    assert "_valid_from" in written.column_names
    assert "_valid_to" in written.column_names
    # All initial-load rows are current
    assert all(written.column("_is_current").to_pylist())
    # _valid_from = source updated_at column
    assert written.column("_valid_from").to_pylist() == written.column("updated_at").to_pylist()
    # _valid_to is NULL on every row
    assert all(v is None for v in written.column("_valid_to").to_pylist())


def test_scd2_order_by_missing_from_source_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bronze_no_order = pa.table({"id": [1], "email": ["a@x.com"]})
    _patch_deltalake(monkeypatch, _FakeDeltalake(bronze_no_order))
    with pytest.raises(ValidationError, match="not present in source"):
        SilverPipeline().run(
            SilverConfig(
                bronze_path=tmp_path / "b",
                silver_path=tmp_path / "s",
                run_id="r",
                strategy=SilverStrategy.SCD2,
                business_keys=("id",),
                scd2_order_by="updated_at",
            )
        )


# ──────────────────────────────────────────────────────────────────────
# UPSERT — same write semantics as SCD1; verify the alias works
# ──────────────────────────────────────────────────────────────────────


def test_upsert_requires_business_keys(tmp_path: Path) -> None:
    """UPSERT routes through _write_scd1 → same precondition."""
    config = SilverConfig(
        bronze_path=tmp_path / "b",
        silver_path=tmp_path / "s",
        run_id="x",
        strategy=SilverStrategy.UPSERT,
        business_keys=None,
    )
    with pytest.raises(ValidationError, match="non-empty business_keys"):
        SilverPipeline().run(config)


def test_upsert_initial_load_uses_write_deltalake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No existing target → write_deltalake (same as SCD1)."""
    fake = _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    SilverPipeline().run(_config(tmp_path, strategy=SilverStrategy.UPSERT, business_keys=("id",)))
    assert fake.write_calls[0]["mode"] == "append"


def test_upsert_subsequent_run_invokes_merge(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Existing target → MERGE (same as SCD1)."""
    silver_path = tmp_path / "silver"
    (silver_path / "_delta_log").mkdir(parents=True)
    merge_record: dict[str, Any] = {}
    merger_table = _FakeDeltaTableForMerge(_bronze_with_metadata(), merge_record)

    class _MergeAwareFake(_FakeDeltalake):
        def DeltaTable(self, path: str) -> _FakeDeltaTable:
            if path == str(silver_path):
                return merger_table
            return super().DeltaTable(path)

    fake = _patch_deltalake(monkeypatch, _MergeAwareFake(_bronze_with_metadata()))
    SilverPipeline().run(
        _config(
            tmp_path,
            silver_path=silver_path,
            strategy=SilverStrategy.UPSERT,
            business_keys=("id",),
        )
    )

    assert fake.write_calls == []
    assert merge_record["predicate"] == "target.id = source.id"
    assert merge_record["when_matched_update_all_called"] is True
    assert merge_record["when_not_matched_insert_all_called"] is True
    assert merge_record["executed"] is True


# ──────────────────────────────────────────────────────────────────────
# Instrumentation — counts + timing + version (no extra full-table scans)
# ──────────────────────────────────────────────────────────────────────


def test_append_stats_report_inserted_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """APPEND populates rows_inserted from the input table; updated/unchanged are 0."""
    _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    stats = SilverPipeline().run(_config(tmp_path))
    assert stats.rows_inserted == 3  # bronze has 3 rows
    assert stats.rows_updated == 0
    assert stats.rows_unchanged == 0


def test_scd1_initial_load_stats(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """SCD1 first-run falls through to write_deltalake → counts as inserts."""
    _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    stats = SilverPipeline().run(
        _config(tmp_path, strategy=SilverStrategy.SCD1, business_keys=("id",))
    )
    assert stats.rows_inserted == 3
    assert stats.rows_updated == 0


def test_scd1_merge_stats_from_returned_metrics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """SCD1 MERGE captures inserted/updated counts from merge.execute()'s return value."""
    silver_path = tmp_path / "silver"
    (silver_path / "_delta_log").mkdir(parents=True)

    merge_record: dict[str, Any] = {
        "execute_returns": {
            "num_target_rows_inserted": "1",
            "num_target_rows_updated": "2",
        }
    }
    merger_table = _FakeDeltaTableForMerge(_bronze_with_metadata(), merge_record)

    class _MergeAwareFake(_FakeDeltalake):
        def DeltaTable(self, path: str) -> _FakeDeltaTable:
            if path == str(silver_path):
                return merger_table
            return super().DeltaTable(path)

    _patch_deltalake(monkeypatch, _MergeAwareFake(_bronze_with_metadata()))
    stats = SilverPipeline().run(
        _config(
            tmp_path,
            silver_path=silver_path,
            strategy=SilverStrategy.SCD1,
            business_keys=("id",),
        )
    )
    assert stats.rows_inserted == 1
    assert stats.rows_updated == 2
    assert stats.rows_unchanged == 0


def test_run_duration_is_populated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Wall-clock duration is captured (non-negative even on a fast mock run)."""
    _patch_deltalake(monkeypatch, _FakeDeltalake(_bronze_with_metadata()))
    stats = SilverPipeline().run(_config(tmp_path))
    assert stats.duration_seconds >= 0
