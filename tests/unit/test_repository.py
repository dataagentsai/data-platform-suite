"""Unit tests for the SQLite metadata repository."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from lake_ingestion_framework.core.exceptions import (
    UnknownKeyError,
    ValidationError,
)
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType


@pytest.fixture()
def repo() -> Iterator[SqliteMetadataRepository]:
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    try:
        yield r
    finally:
        r.close()


def test_bootstrap_is_idempotent() -> None:
    r = SqliteMetadataRepository(":memory:")
    try:
        r.bootstrap_schema()
        r.bootstrap_schema()
        assert r.schema_version() == 4  # bumped when column-transform fields landed (Wave 1)
    finally:
        r.close()


def test_schema_version_default(repo: SqliteMetadataRepository) -> None:
    assert repo.schema_version() == 4


def test_upsert_source_inserts_then_updates(repo: SqliteMetadataRepository) -> None:
    sid = repo.upsert_source(
        source_name="orders_db",
        source_env="prod",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={
            "connection_pattern": "azure_basic",
            "auth_method": "managed_identity",
        },
    )
    assert sid > 0

    # Update — same identity, different config
    sid2 = repo.upsert_source(
        source_name="orders_db",
        source_env="prod",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=False,
        config={
            "connection_pattern": "azure_basic",
            "auth_method": "managed_identity",
            "connect_timeout_seconds": "60",
        },
    )
    assert sid2 == sid

    row = repo.get_source("orders_db", "prod")
    assert row is not None
    assert row.is_enabled is False


def test_upsert_source_distinct_envs(repo: SqliteMetadataRepository) -> None:
    sid_dev = repo.upsert_source(
        source_name="orders_db",
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"connection_pattern": "azure_basic", "auth_method": "managed_identity"},
    )
    sid_prod = repo.upsert_source(
        source_name="orders_db",
        source_env="prod",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"connection_pattern": "azure_basic", "auth_method": "managed_identity"},
    )
    assert sid_dev != sid_prod
    assert len(repo.list_sources()) == 2


def test_upsert_source_rejects_unknown_key(repo: SqliteMetadataRepository) -> None:
    with pytest.raises(UnknownKeyError):
        repo.upsert_source(
            source_name="x",
            source_env="prod",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"made_up_key": "whatever"},
        )


def test_upsert_source_rejects_bad_value(repo: SqliteMetadataRepository) -> None:
    with pytest.raises(ValidationError):
        repo.upsert_source(
            source_name="x",
            source_env="prod",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"connection_pattern": "x", "auth_method": "telepathy"},
        )


def test_upsert_source_rejects_invalid_source_env(repo: SqliteMetadataRepository) -> None:
    with pytest.raises(ValidationError):
        repo.upsert_source(
            source_name="x",
            source_env="PROD!",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={"connection_pattern": "x", "auth_method": "managed_identity"},
        )


def test_get_source_missing_returns_none(repo: SqliteMetadataRepository) -> None:
    assert repo.get_source("nope", "prod") is None


def test_get_source_config_roundtrips(repo: SqliteMetadataRepository) -> None:
    repo.upsert_source(
        source_name="orders_db",
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={
            "connection_pattern": "azure_basic",
            "auth_method": "managed_identity",
            "connect_timeout_seconds": "60",
        },
    )
    source = repo.get_source("orders_db", "dev")
    assert source is not None
    config = repo.get_source_config(source.source_id)
    assert config == {
        "connection_pattern": "azure_basic",
        "auth_method": "managed_identity",
        "connect_timeout_seconds": "60",
    }


def test_upsert_dataset_with_columns(repo: SqliteMetadataRepository) -> None:
    sid = repo.upsert_source(
        source_name="orders_db",
        source_env="prod",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"connection_pattern": "azure_basic", "auth_method": "managed_identity"},
    )

    dsid = repo.upsert_dataset(
        source_id=sid,
        dataset_name="customers",
        is_enabled=True,
        config={
            "realm": "adf-adls",
            "dataset_type": "TABLE",
            "load_type": "INCREMENTAL",
            "target_path_template": "raw/{source}/{schema}/{object}/{yyyy}/{mm}/{dd}",
            "schema_name": "dbo",
            "object_name": "customers",
            "watermark_column": "modified_at",
        },
        columns=[
            DatasetColumnRow(
                dataset_id=0,
                column_name="id",
                data_type="int",
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
                data_type="varchar(255)",
                is_nullable=True,
                ordinal_position=2,
                is_primary_key=False,
                is_excluded=False,
                pii_class="direct",
                source_path=None,
            ),
        ],
    )
    assert dsid > 0
    assert repo.get_dataset(sid, "customers") is not None
    assert len(repo.list_datasets(source_id=sid)) == 1
    assert len(repo.list_datasets()) == 1


def test_upsert_dataset_replaces_columns(repo: SqliteMetadataRepository) -> None:
    sid = repo.upsert_source(
        source_name="db",
        source_env="prod",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={"connection_pattern": "x", "auth_method": "managed_identity"},
    )
    base_config = {
        "realm": "adf-adls",
        "dataset_type": "TABLE",
        "load_type": "FULL",
        "target_path_template": "raw/{source}/{object}",
        "schema_name": "dbo",
        "object_name": "t",
    }
    repo.upsert_dataset(
        source_id=sid,
        dataset_name="t",
        is_enabled=True,
        config=base_config,
        columns=[
            DatasetColumnRow(0, "a", "int", False, 1, True, False, "none", None),
            DatasetColumnRow(0, "b", "int", True, 2, False, False, "none", None),
        ],
    )
    repo.upsert_dataset(
        source_id=sid,
        dataset_name="t",
        is_enabled=True,
        config=base_config,
        columns=[
            DatasetColumnRow(0, "a", "int", False, 1, True, False, "none", None),
        ],
    )
    # Hitting the underlying SQLite directly to verify the wholesale replace
    conn = repo._connect()
    rows = conn.execute("SELECT column_name FROM dataset_config_columns").fetchall()
    assert [r["column_name"] for r in rows] == ["a"]


def test_upsert_platform_config_validates(repo: SqliteMetadataRepository) -> None:
    repo.upsert_platform_config(
        {
            "default_realm": "adf-adls",
            "target_storage_account": "mysa",
            "default_file_format": "PARQUET",
        }
    )
    # Bad value
    with pytest.raises(ValidationError):
        repo.upsert_platform_config({"default_file_format": "WHATEVER"})
    # Unknown key
    with pytest.raises(UnknownKeyError):
        repo.upsert_platform_config({"made_up_key": "x"})


def test_repository_persists_to_disk(tmp_path: Path) -> None:
    db = tmp_path / "meta.sqlite"
    r1 = SqliteMetadataRepository(db)
    r1.bootstrap_schema()
    r1.upsert_platform_config({"default_realm": "adf-adls"})
    r1.close()

    r2 = SqliteMetadataRepository(db)
    assert r2.schema_version() == 4
    r2.close()
