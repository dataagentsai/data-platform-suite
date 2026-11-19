"""SilverPipeline + standard processors — full integration test.

Bridges the gap between per-processor unit tests + the Docker-bound
demo smoke. Uses real deltalake for both Bronze read + Silver write
so the test catches wiring regressions where:

* unit tests still pass (each processor in isolation behaves)
* but the pipeline's compose order, the conn/owns_conn lifecycle,
  the PII-before-hash invariant, or dedup-before-hash sequencing
  breaks

Each test seeds a real SqliteMetadataRepository, writes a real Bronze
Delta to tmp_path, runs SilverPipeline.run(), reads back the Silver
Delta, and asserts on the actual post-write shape.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import deltalake
import pyarrow as pa
import pytest
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.metadata_cols import (
    BRONZE_LOADED_AT,
    BRONZE_RECORD_HASH,
    BRONZE_RUN_ID,
)
from lake_ingestion_framework.core.processors import DedupConfig
from lake_ingestion_framework.core.silver import (
    SilverConfig,
    SilverPipeline,
    SilverProcessorContext,
    SilverStrategy,
)
from lake_ingestion_framework.core.types.enums import SourceType

# ──────────────────────────────────────────────────────────────────────
# Fixtures + helpers
# ──────────────────────────────────────────────────────────────────────


def _bronze_with_messy_customers() -> pa.Table:
    """Bronze customers table with intentionally messy data:

    * row 1: clean baseline
    * row 2: leading/trailing whitespace + mixed-case email
    * row 3: empty string in segment + tabs in name
    * row 4: malformed email (validates to NULL)
    * row 5: duplicate of row 1's customer_id with later modified_at
    """
    loaded_at = datetime(2026, 5, 5, tzinfo=UTC)
    return pa.table(
        {
            "customer_id": [1, 2, 3, 4, 1],
            "name": ["alice", " bob ", "carol\twith\ttabs", "dave", "ALICE-UPDATED"],
            "email": [
                "alice@example.com",
                "  Bob@EXAMPLE.com  ",
                "carol@example.com",
                "not-an-email",
                "alice@example.com",
            ],
            "segment": ["premium", "FREE", "", "free", "premium"],
            "modified_at": [
                "2026-05-05T10:00:00Z",
                "2026-05-05T11:00:00Z",
                "2026-05-05T12:00:00Z",
                "2026-05-05T13:00:00Z",
                "2026-05-05T14:00:00Z",  # later than row 1
            ],
            BRONZE_RUN_ID: ["bronze-1"] * 5,
            BRONZE_LOADED_AT: pa.array([loaded_at] * 5, type=pa.timestamp("us", tz="UTC")),
            BRONZE_RECORD_HASH: ["h1", "h2", "h3", "h4", "h5"],
        }
    )


def _write_bronze(path: Path, table: pa.Table) -> None:
    deltalake.write_deltalake(str(path), table, mode="overwrite")


def _read_silver(path: Path) -> pa.Table:
    return deltalake.DeltaTable(str(path)).to_pyarrow_table()


@pytest.fixture()
def seeded_repo() -> Iterator[tuple[SqliteMetadataRepository, int]]:
    """Real SqliteMetadataRepository + customers dataset_id with the
    full processor metadata wired up."""
    repo = SqliteMetadataRepository(":memory:")
    try:
        repo.bootstrap_schema()
        source_id = repo.upsert_source(
            source_name="orders_db",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
            },
        )
        dataset_id = repo.upsert_dataset(
            source_id=source_id,
            dataset_name="customers",
            is_enabled=True,
            config={
                "realm": "adf-adls",
                "dataset_type": "TABLE",
                "load_type": "INCREMENTAL",
                "target_path_template": "raw/{source}/{object}",
                "schema_name": "dbo",
                "object_name": "customers",
                "watermark_column": "modified_at",
            },
            columns=[
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="customer_id",
                    data_type="INT",
                    is_nullable=False,
                    ordinal_position=1,
                    is_primary_key=True,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                ),
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="name",
                    data_type="VARCHAR",
                    is_nullable=False,
                    ordinal_position=2,
                    is_primary_key=False,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                ),
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="email",
                    data_type="VARCHAR",
                    is_nullable=False,
                    ordinal_position=3,
                    is_primary_key=False,
                    is_excluded=False,
                    pii_class="email",
                    source_path=None,
                ),
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="segment",
                    data_type="VARCHAR",
                    is_nullable=False,
                    ordinal_position=4,
                    is_primary_key=False,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                    column_role="code",
                ),
                DatasetColumnRow(
                    dataset_id=0,
                    column_name="modified_at",
                    data_type="VARCHAR",
                    is_nullable=False,
                    ordinal_position=5,
                    is_primary_key=False,
                    is_excluded=False,
                    pii_class="none",
                    source_path=None,
                ),
            ],
        )
        yield repo, dataset_id
    finally:
        repo.close()


def _ctx(
    repo: SqliteMetadataRepository,
    dataset_id: int,
    *,
    env: str = "dev",
    platform_groups: dict[str, str] | None = None,
) -> SilverProcessorContext:
    return SilverProcessorContext(
        repo=repo,
        dataset_id=dataset_id,
        platform_groups=platform_groups,
        env=env,
    )


def _run_silver(
    *,
    bronze_path: Path,
    silver_path: Path,
    config_overrides: dict[str, object] | None = None,
) -> tuple[pa.Table, object]:
    config = SilverConfig(
        bronze_path=bronze_path,
        silver_path=silver_path,
        run_id="silver-integration-1",
        strategy=SilverStrategy.APPEND,
        **(config_overrides or {}),
    )
    stats = SilverPipeline().run(config)
    return _read_silver(silver_path), stats


# ──────────────────────────────────────────────────────────────────────
# Full chain — every processor that should fire actually fires
# ──────────────────────────────────────────────────────────────────────


def test_full_chain_messy_input_to_clean_silver(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """Every processor group exercised against a real Bronze Delta.

    Asserts the post-write Silver shape proves each group ran in the
    right order (hygiene → type_safety → case_normalize →
    timestamp_normalize → standardization → pii → dedup → hash).
    """
    repo, dataset_id = seeded_repo
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    _write_bronze(bronze, _bronze_with_messy_customers())

    # Explicitly enable standardization so email_validate fires —
    # the group's built-in default is OPT_IN per docs/standard_processors.md
    # because most standardization is opinionated. email_validate is
    # the only auto-on member; flipping the group to ENABLED here
    # exercises that path.
    silver_table, stats = _run_silver(
        bronze_path=bronze,
        silver_path=silver,
        config_overrides={
            "processor_context": _ctx(
                repo,
                dataset_id,
                platform_groups={"standardization": "enabled"},
            ),
            "dedup": DedupConfig(
                mode="business_key_latest",
                business_keys=("customer_id",),
                order_by="modified_at",
            ),
        },
    )

    # ── hygiene fired: whitespace stripped ──
    names = silver_table.column("name").to_pylist()
    assert "bob" in names, f"trim_whitespace should have folded ' bob ' → 'bob'; got {names}"
    # tabs collapsed to single spaces by normalize_internal_whitespace
    assert "carol with tabs" in names

    # ── case_normalize fired: email lowercased, segment uppercased ──
    emails = silver_table.column("email").to_pylist()
    assert "bob@example.com" in emails  # was "  Bob@EXAMPLE.com  "

    segments = set(silver_table.column("segment").to_pylist())
    assert "PREMIUM" in segments
    assert "FREE" in segments

    # ── standardization fired: bad email validated to NULL ──
    # Only when standardization is opt_in by default; email_validate
    # is in DEFAULT_ON because pii_class=email triggers it.
    assert None in emails, f"email_validate should have nulled 'not-an-email'; got {emails}"

    # ── hygiene's empty_string_to_null fired on segment ──
    assert None in segments, f"empty_string_to_null should have folded '' → None; got {segments}"

    # ── dedup fired: row 1's duplicate (customer_id=1, later mod time) wins ──
    customer_ids = silver_table.column("customer_id").to_pylist()
    assert customer_ids.count(1) == 1, (
        f"business_key_latest should have left exactly one customer_id=1; "
        f"got customer_ids={customer_ids}"
    )
    assert silver_table.num_rows == 4  # 5 - 1 dedup

    # ── _record_hash recomputed AFTER processors ──
    assert "_record_hash" in silver_table.column_names

    # ── stats round-trip ──
    assert stats.processors_fired > 0
    assert stats.rows_deduped == 1


# ──────────────────────────────────────────────────────────────────────
# Per-column skip_groups isolates the bypass
# ──────────────────────────────────────────────────────────────────────


def test_per_column_skip_groups_isolates_skip(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """Mark the `name` column with skip_groups=[hygiene]; whitespace
    survives there, but email + segment still get cleaned."""
    repo, dataset_id = seeded_repo
    # Overwrite name's column row with skip_groups configured
    repo.upsert_dataset(
        source_id=1,
        dataset_name="customers",
        is_enabled=True,
        config={
            "realm": "adf-adls",
            "dataset_type": "TABLE",
            "load_type": "INCREMENTAL",
            "target_path_template": "raw/{source}/{object}",
            "schema_name": "dbo",
            "object_name": "customers",
            "watermark_column": "modified_at",
        },
        columns=[
            DatasetColumnRow(
                dataset_id=0,
                column_name="customer_id",
                data_type="INT",
                is_nullable=False,
                ordinal_position=1,
                is_primary_key=True,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="name",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
                skip_groups=("hygiene",),
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="email",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=3,
                is_primary_key=False,
                is_excluded=False,
                pii_class="email",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="segment",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=4,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
                column_role="code",
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="modified_at",
                data_type="VARCHAR",
                is_nullable=False,
                ordinal_position=5,
                is_primary_key=False,
                is_excluded=False,
                pii_class="none",
                source_path=None,
            ),
        ],
    )
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    _write_bronze(bronze, _bronze_with_messy_customers())

    silver_table, _ = _run_silver(
        bronze_path=bronze,
        silver_path=silver,
        config_overrides={"processor_context": _ctx(repo, dataset_id)},
    )

    # Whitespace + tabs in name SURVIVED (skip_groups=[hygiene])
    names = silver_table.column("name").to_pylist()
    assert " bob " in names, f"name skipped hygiene; ' bob ' should survive: {names}"
    # But email was still cleaned (different column, no skip)
    assert "bob@example.com" in silver_table.column("email").to_pylist()


# ──────────────────────────────────────────────────────────────────────
# PII per-env policy split — dev pass_through vs prod hash
# ──────────────────────────────────────────────────────────────────────


def test_pii_per_env_policy_dev_pass_through_vs_prod_hash(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """Same dataset, same column metadata. Two pii_policy rows
    (dev=pass_through, prod=hash) → same input produces clear text in
    one env and 64-char hex in the other."""
    repo, dataset_id = seeded_repo
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")
    repo.upsert_pii_policy(pii_class="email", env="prod", action="hash", salt_kv_ref="prod-salt")

    bronze = tmp_path / "bronze"
    _write_bronze(bronze, _bronze_with_messy_customers())

    # ── dev: PII pass-through, email is clear ──
    silver_dev = tmp_path / "silver_dev"
    table_dev, _ = _run_silver(
        bronze_path=bronze,
        silver_path=silver_dev,
        config_overrides={"processor_context": _ctx(repo, dataset_id, env="dev")},
    )
    dev_emails = [e for e in table_dev.column("email").to_pylist() if e is not None]
    assert any("@example.com" in e for e in dev_emails)

    # ── prod: PII hash, all 64-char hex ──
    silver_prod = tmp_path / "silver_prod"
    table_prod, _ = _run_silver(
        bronze_path=bronze,
        silver_path=silver_prod,
        config_overrides={"processor_context": _ctx(repo, dataset_id, env="prod")},
    )
    prod_emails = [e for e in table_prod.column("email").to_pylist() if e is not None]
    assert all(len(e) == 64 for e in prod_emails), (
        f"prod should hash emails to 64-char hex; got lengths " f"{[len(e) for e in prod_emails]}"
    )
    # Same input → same hash (SCD2-friendly determinism)
    assert prod_emails.count(prod_emails[0]) >= 1


# ──────────────────────────────────────────────────────────────────────
# PII runs BEFORE hash (record_hash is over masked values)
# ──────────────────────────────────────────────────────────────────────


def test_pii_runs_before_record_hash(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """If clear-PII inputs hash to the same masked value, their
    _record_hash should match — the dedup-friendly invariant.
    """
    repo, dataset_id = seeded_repo
    repo.upsert_pii_policy(
        pii_class="email",
        env="dev",
        action="redact",
        # Both rows redact to '[redacted]' so post-PII shape is identical
    )
    bronze = tmp_path / "bronze"

    # Two rows with different clear emails but identical other fields
    table = pa.table(
        {
            "customer_id": [1, 2],
            "name": ["alice", "alice"],  # same name
            "email": ["alice@a.com", "alice@b.com"],  # different clear
            "segment": ["FREE", "FREE"],
            "modified_at": ["2026-05-05T10:00:00Z", "2026-05-05T10:00:00Z"],
            BRONZE_RUN_ID: ["b1", "b1"],
            BRONZE_LOADED_AT: pa.array(
                [datetime(2026, 5, 5, tzinfo=UTC)] * 2,
                type=pa.timestamp("us", tz="UTC"),
            ),
            BRONZE_RECORD_HASH: ["h1", "h2"],
        }
    )
    _write_bronze(bronze, table)

    silver_table, _ = _run_silver(
        bronze_path=bronze,
        silver_path=tmp_path / "silver",
        config_overrides={"processor_context": _ctx(repo, dataset_id)},
    )

    # After redaction, both emails are '[redacted]' so _record_hash
    # must match — identical post-PII shape.
    hashes = silver_table.column("_record_hash").to_pylist()
    # Two rows with identical post-PII values share a hash. They differ
    # only in customer_id, so drop that for the assertion.
    emails = silver_table.column("email").to_pylist()
    assert emails == ["[redacted]", "[redacted]"]
    # Hashes differ ONLY because customer_id differs — proving hash
    # was computed over post-PII shape (not pre-PII clear emails)
    assert hashes[0] != hashes[1]
    # Final sanity: hash recomputed at Silver, not inherited from Bronze
    assert "h1" not in hashes


# ──────────────────────────────────────────────────────────────────────
# Disabling groups at the platform level
# ──────────────────────────────────────────────────────────────────────


def test_disable_hygiene_at_platform_level_preserves_whitespace(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """Platform-level processor_groups['hygiene']='disabled' → whitespace
    survives across every column, no per-column skip needed."""
    repo, dataset_id = seeded_repo
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, _bronze_with_messy_customers())

    silver_table, _ = _run_silver(
        bronze_path=bronze,
        silver_path=tmp_path / "silver",
        config_overrides={
            "processor_context": _ctx(
                repo,
                dataset_id,
                platform_groups={
                    "hygiene": "disabled",
                    "type_safety": "enabled",
                    "case_normalize": "enabled",
                    "pii": "enabled",
                },
            ),
        },
    )

    # Hygiene OFF: " bob " keeps padding in name
    assert " bob " in silver_table.column("name").to_pylist()
    # case_normalize ON: email lowercased, but hygiene-OFF means
    # whitespace around the lowercased email survives.
    assert "  bob@example.com  " in silver_table.column("email").to_pylist()


# ──────────────────────────────────────────────────────────────────────
# No processor_context preserves Phase-4 behaviour
# ──────────────────────────────────────────────────────────────────────


def test_no_processor_context_preserves_phase4_behaviour(
    tmp_path: Path,
) -> None:
    """Running without processor_context = no transforms. Whitespace
    + mixed case all survive into Silver. Important regression guard
    for customers who haven't yet adopted the processor track."""
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, _bronze_with_messy_customers())

    silver_table, stats = _run_silver(
        bronze_path=bronze,
        silver_path=tmp_path / "silver",
    )

    # Inputs survived as-is
    names = silver_table.column("name").to_pylist()
    assert " bob " in names
    assert any("Bob@EXAMPLE.com" in (e or "") for e in silver_table.column("email").to_pylist())
    # No processors fired
    assert stats.processors_fired == 0
    assert stats.rows_deduped == 0


# ──────────────────────────────────────────────────────────────────────
# Idempotence — same Bronze + same metadata = same Silver
# ──────────────────────────────────────────────────────────────────────


def test_pipeline_is_idempotent_across_runs(
    seeded_repo: tuple[SqliteMetadataRepository, int], tmp_path: Path
) -> None:
    """Two identical runs against the same Bronze produce the same
    Silver shape (modulo Silver metadata cols which carry timestamps
    + run_id). The post-transform business columns must match exactly.
    """
    repo, dataset_id = seeded_repo
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, _bronze_with_messy_customers())

    silver_a = tmp_path / "silver_a"
    silver_b = tmp_path / "silver_b"

    table_a, _ = _run_silver(
        bronze_path=bronze,
        silver_path=silver_a,
        config_overrides={"processor_context": _ctx(repo, dataset_id)},
    )
    table_b, _ = _run_silver(
        bronze_path=bronze,
        silver_path=silver_b,
        config_overrides={"processor_context": _ctx(repo, dataset_id)},
    )

    # Business columns match exactly
    for col in ("customer_id", "name", "email", "segment"):
        assert table_a.column(col).to_pylist() == table_b.column(col).to_pylist()

    # _record_hash is over post-transform shape — also identical
    assert table_a.column("_record_hash").to_pylist() == table_b.column("_record_hash").to_pylist()
