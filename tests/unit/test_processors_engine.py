"""ProcessorEngine integration tests — config resolution + group
ordering + per-column overrides — against a real SQLite metadata DB
+ pyarrow tables.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.processors import (
    GroupMode,
    ProcessorEngine,
    ProcessorGroup,
)
from lake_ingestion_framework.core.types.enums import SourceType


@pytest.fixture()
def repo_with_columns() -> tuple[SqliteMetadataRepository, int]:
    """Repo seeded with one dataset + 3 columns (one excluded)."""
    repo = SqliteMetadataRepository(":memory:")
    repo.bootstrap_schema()
    source_id = repo.upsert_source(
        source_name="orders",
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"connection_pattern": "azure_basic", "auth_method": "managed_identity"},
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
                column_name="email",
                data_type="VARCHAR",
                is_nullable=True,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="email",
                source_path=None,
            ),
            DatasetColumnRow(
                dataset_id=0,
                column_name="excluded_col",
                data_type="VARCHAR",
                is_nullable=True,
                ordinal_position=3,
                is_primary_key=False,
                is_excluded=True,
                pii_class="none",
                source_path=None,
            ),
        ],
    )
    return repo, dataset_id


# ──────────────────────────────────────────────────────────────────────
# Config resolution
# ──────────────────────────────────────────────────────────────────────


def test_resolve_config_excludes_is_excluded_columns(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    config = ProcessorEngine().resolve_config(repo, dataset_id=dataset_id)
    names = [c.bronze_column_name for c in config.columns]
    assert names == ["customer_id", "email"]
    assert "excluded_col" not in names


def test_resolve_config_uses_built_in_defaults_when_no_overrides(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    config = ProcessorEngine().resolve_config(repo, dataset_id=dataset_id)
    by_group = {g.group: g for g in config.groups}
    assert by_group[ProcessorGroup.HYGIENE].mode == GroupMode.ENABLED
    assert by_group[ProcessorGroup.TYPE_SAFETY].mode == GroupMode.ENABLED
    assert by_group[ProcessorGroup.STANDARDIZATION].mode == GroupMode.OPT_IN
    assert by_group[ProcessorGroup.DEDUP].mode == GroupMode.DISABLED


def test_dataset_overrides_platform_default(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    config = ProcessorEngine().resolve_config(
        repo,
        dataset_id=dataset_id,
        platform_groups={"hygiene": "enabled"},
        dataset_groups={"hygiene": "disabled"},
    )
    by_group = {g.group: g for g in config.groups}
    assert by_group[ProcessorGroup.HYGIENE].mode == GroupMode.DISABLED


def test_dataset_decides_at_platform_falls_to_disabled_without_override(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    config = ProcessorEngine().resolve_config(
        repo,
        dataset_id=dataset_id,
        platform_groups={"dedup": "dataset_decides"},
        dataset_groups=None,
    )
    by_group = {g.group: g for g in config.groups}
    assert by_group[ProcessorGroup.DEDUP].mode == GroupMode.DISABLED


# ──────────────────────────────────────────────────────────────────────
# Group application — hygiene end-to-end
# ──────────────────────────────────────────────────────────────────────


def test_hygiene_runs_on_enabled_columns(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    """Email column gets trim + lowercase via the hygiene defaults."""
    repo, dataset_id = repo_with_columns
    table = pa.table(
        {
            "customer_id": pa.array([1, 2, 3], type=pa.int32()),
            "email": pa.array(["  alice@x.com", "  bob@x.com  ", "carol@x.com"]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(repo, dataset_id=dataset_id)
    new_table, report = engine.apply_pre_hash_groups(table, config)
    # email values trimmed
    assert new_table.column("email").to_pylist() == [
        "alice@x.com",
        "bob@x.com",
        "carol@x.com",
    ]
    # int column promoted to int64
    assert new_table.column("customer_id").type == pa.int64()
    assert report.processors_fired >= 1


def test_disable_hygiene_group_skips_string_processors(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    table = pa.table(
        {
            "customer_id": pa.array([1, 2, 3], type=pa.int32()),
            "email": pa.array(["  alice@x.com  ", "  bob@x.com  ", "carol@x.com"]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(
        repo,
        dataset_id=dataset_id,
        platform_groups={"hygiene": "disabled"},
    )
    new_table, _ = engine.apply_pre_hash_groups(table, config)
    # email is unchanged (whitespace preserved)
    assert "  alice@x.com  " in new_table.column("email").to_pylist()
    # type_safety still runs — int promoted
    assert new_table.column("customer_id").type == pa.int64()


def test_per_column_skip_groups_isolates_skip(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    """email column has skip_groups=[hygiene] → only that column
    bypasses hygiene; other columns still get cleaned."""
    repo, dataset_id = repo_with_columns
    # Re-upsert dataset with skip_groups on email
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
                column_name="email",
                data_type="VARCHAR",
                is_nullable=True,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="email",
                source_path=None,
                skip_groups=("hygiene",),
            ),
        ],
    )

    table = pa.table(
        {
            "customer_id": pa.array([1, 2], type=pa.int32()),
            "email": pa.array(["  alice@x.com  ", "  bob@x.com  "]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(repo, dataset_id=dataset_id)
    new_table, _ = engine.apply_pre_hash_groups(table, config)
    # email retains its whitespace (skip_groups=[hygiene])
    assert new_table.column("email").to_pylist() == [
        "  alice@x.com  ",
        "  bob@x.com  ",
    ]


# ──────────────────────────────────────────────────────────────────────
# describe (CLI helper)
# ──────────────────────────────────────────────────────────────────────


def test_describe_lists_active_processors_per_group(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    engine = ProcessorEngine()
    config = engine.resolve_config(repo, dataset_id=dataset_id)
    described = engine.describe(config)
    assert "trim_whitespace" in described[ProcessorGroup.HYGIENE]
    assert "safe_cast" in described[ProcessorGroup.TYPE_SAFETY]


def test_describe_reports_disabled_groups_as_empty(
    repo_with_columns: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_columns
    engine = ProcessorEngine()
    config = engine.resolve_config(
        repo,
        dataset_id=dataset_id,
        platform_groups={"hygiene": "disabled"},
    )
    described = engine.describe(config)
    assert described[ProcessorGroup.HYGIENE] == []
