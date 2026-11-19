"""Unit tests for the pii group (Wave 2)."""

from __future__ import annotations

import pyarrow as pa
import pytest
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.processors import (
    ProcessorEngine,
)
from lake_ingestion_framework.core.processors.pii import (
    PiiPolicy,
    PiiProcessor,
    _hash_value,
    _mask_value,
    _tokenize_value,
    build_pii_group,
)
from lake_ingestion_framework.core.processors.types import ColumnTransformContext
from lake_ingestion_framework.core.types.enums import SourceType


def _ctx(pii_class: str = "email", *, name: str = "email") -> ColumnTransformContext:
    return ColumnTransformContext(
        bronze_column_name=name,
        silver_column_name=name,
        bronze_data_type="VARCHAR",
        silver_data_type=None,
        cast_safety="strict",
        pii_class=pii_class,
        column_role=None,
        null_replacement=None,
    )


# ──────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────


def test_hash_is_deterministic_with_salt() -> None:
    a = _hash_value("alice@x.com", "salt-A")
    b = _hash_value("alice@x.com", "salt-A")
    assert a == b


def test_hash_differs_for_different_salts() -> None:
    assert _hash_value("alice@x.com", "salt-A") != _hash_value("alice@x.com", "salt-B")


def test_hash_differs_for_different_inputs() -> None:
    salt = "common"
    assert _hash_value("a", salt) != _hash_value("b", salt)


def test_mask_keeps_last_n_chars() -> None:
    assert _mask_value("alice@x.com", 4) == "*******.com"
    assert _mask_value("hi", 4) == "**"  # short input → all stars
    assert _mask_value("", 4) == ""


def test_tokenize_is_deterministic_with_salt() -> None:
    salt = "tok-salt"
    assert _tokenize_value("alice@x.com", salt) == _tokenize_value("alice@x.com", salt)


def test_tokenize_does_not_look_like_a_hash() -> None:
    """Tokens use base-36 (lowercase letters + digits) and are short
    relative to a 64-char SHA hash."""
    out = _tokenize_value("alice@x.com", "salt")
    assert all(c.isalnum() and c.islower() or c.isdigit() for c in out)
    assert len(out) < 32


# ──────────────────────────────────────────────────────────────────────
# PiiProcessor — per strategy
# ──────────────────────────────────────────────────────────────────────


def _processors(
    action: str, pii_class: str = "email", **policy_kwargs: object
) -> tuple[PiiProcessor, ...]:
    """Build a single-class PII group for one action."""
    policy = PiiPolicy(
        pii_class=pii_class,
        env="dev",
        action=action,
        salt=str(policy_kwargs.get("salt", "salt-1")),
        redact_value=str(policy_kwargs.get("redact_value", "[redacted]")),
        mask_visible=int(policy_kwargs.get("mask_visible", 4)),
    )
    return build_pii_group({pii_class: policy}, env="dev")


def test_pass_through_does_not_transform() -> None:
    procs = _processors("pass_through")
    table = pa.table({"email": pa.array(["alice@x.com", "bob@x.com"])})
    # Find the pass_through processor and run it
    proc = next(p for p in procs if p.name == "pii_pass_through")
    new_table, result = proc.run_arrow(table, _ctx())
    # No transform; describe-CLI sees fired=False
    assert result.fired is False
    assert new_table.column("email").to_pylist() == ["alice@x.com", "bob@x.com"]


def test_hash_replaces_values_with_hex_digest() -> None:
    procs = _processors("hash", salt="my-salt")
    proc = next(p for p in procs if p.name == "pii_hash")
    table = pa.table({"email": pa.array(["alice@x.com", "alice@x.com", None, "bob@x.com"])})
    new_table, result = proc.run_arrow(table, _ctx())
    out = new_table.column("email").to_pylist()
    assert result.fired is True
    # Same input → same hash (SCD2-friendly)
    assert out[0] == out[1]
    assert out[0] != out[3]
    assert out[2] is None  # NULLs preserved
    # 64-char hex
    assert len(out[0]) == 64


def test_mask_keeps_last_4_by_default() -> None:
    procs = _processors("mask")
    proc = next(p for p in procs if p.name == "pii_mask")
    # "alice@x.com" (11) → 7 stars + ".com"
    # "abcd" (4 = visible) → all stars (don't reveal whole short value)
    # "short" (5 > visible) → 1 star + "hort"
    table = pa.table({"email": pa.array(["alice@x.com", "abcd", "short", None])})
    new_table, _ = proc.run_arrow(table, _ctx())
    out = new_table.column("email").to_pylist()
    assert out[0] == "*******.com"
    assert out[1] == "****"  # len == visible → all stars
    assert out[2] == "*hort"  # len > visible → mask prefix
    assert out[3] is None


def test_tokenize_is_deterministic_across_runs() -> None:
    procs = _processors("tokenize", salt="tok-salt")
    proc = next(p for p in procs if p.name == "pii_tokenize")
    table = pa.table({"email": pa.array(["alice@x.com", "alice@x.com", "bob@x.com"])})
    new_table, _ = proc.run_arrow(table, _ctx())
    out = new_table.column("email").to_pylist()
    assert out[0] == out[1]
    assert out[0] != out[2]


def test_redact_replaces_with_literal() -> None:
    procs = _processors("redact", redact_value="[hidden]")
    proc = next(p for p in procs if p.name == "pii_redact")
    table = pa.table({"email": pa.array(["alice@x.com", None])})
    new_table, _ = proc.run_arrow(table, _ctx())
    assert new_table.column("email").to_pylist() == ["[hidden]", None]


def test_processor_only_fires_when_pii_class_matches_policy() -> None:
    procs = _processors("hash", pii_class="email")
    proc = next(p for p in procs if p.name == "pii_hash")
    # column with pii_class='ssn' (no policy) → applies_to=False
    assert proc.applies_to(_ctx(pii_class="ssn")) is False
    # column with pii_class='email' → applies_to=True
    assert proc.applies_to(_ctx(pii_class="email")) is True


def test_processor_skips_pii_class_none() -> None:
    procs = _processors("hash")
    proc = next(p for p in procs if p.name == "pii_hash")
    assert proc.applies_to(_ctx(pii_class="none")) is False


# ──────────────────────────────────────────────────────────────────────
# Engine integration — pii policy resolution
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def repo_with_pii_dataset() -> tuple[SqliteMetadataRepository, int]:
    """Repo with one dataset that has an email column flagged as PII."""
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
                column_name="id",
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
        ],
    )
    return repo, dataset_id


def test_engine_skips_pii_when_no_env_supplied(
    repo_with_pii_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """Without env, engine doesn't load policies → no PII transforms."""
    repo, dataset_id = repo_with_pii_dataset
    repo.upsert_pii_policy(pii_class="email", env="dev", action="hash", salt_kv_ref="dev-salt")
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "email": pa.array(["alice@x.com", "bob@x.com"]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(repo, dataset_id=dataset_id)  # no env
    new_table, report = engine.apply_pre_hash_groups(table, config)
    # email unchanged; PII skipped because env=None
    assert "alice@x.com" in new_table.column("email").to_pylist()


def test_engine_applies_pii_hash_when_env_set(
    repo_with_pii_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_pii_dataset
    repo.upsert_pii_policy(pii_class="email", env="dev", action="hash", salt_kv_ref="my-salt")
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int32()),
            "email": pa.array(["alice@x.com", "bob@x.com"]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(repo, dataset_id=dataset_id, env="dev")
    new_table, report = engine.apply_pre_hash_groups(table, config)
    out = new_table.column("email").to_pylist()
    assert all(len(v) == 64 for v in out)  # all SHA-256 hex
    assert out[0] != out[1]


def test_engine_pii_per_env_policy_picks_right_action(
    repo_with_pii_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    """Same column; dev → pass_through, prod → hash."""
    repo, dataset_id = repo_with_pii_dataset
    repo.upsert_pii_policy(pii_class="email", env="dev", action="pass_through")
    repo.upsert_pii_policy(pii_class="email", env="prod", action="hash", salt_kv_ref="prod-salt")
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "email": pa.array(["alice@x.com"]),
        }
    )

    engine_dev = ProcessorEngine()
    cfg_dev = engine_dev.resolve_config(repo, dataset_id=dataset_id, env="dev")
    out_dev, _ = engine_dev.apply_pre_hash_groups(table, cfg_dev)
    assert out_dev.column("email").to_pylist() == ["alice@x.com"]

    engine_prod = ProcessorEngine()
    cfg_prod = engine_prod.resolve_config(repo, dataset_id=dataset_id, env="prod")
    out_prod, _ = engine_prod.apply_pre_hash_groups(table, cfg_prod)
    assert len(out_prod.column("email").to_pylist()[0]) == 64  # hashed


def test_pii_group_disabled_skips_processors(
    repo_with_pii_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, dataset_id = repo_with_pii_dataset
    repo.upsert_pii_policy(pii_class="email", env="dev", action="hash", salt_kv_ref="x")
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "email": pa.array(["alice@x.com"]),
        }
    )
    engine = ProcessorEngine()
    config = engine.resolve_config(
        repo,
        dataset_id=dataset_id,
        env="dev",
        platform_groups={"pii": "disabled"},
    )
    new_table, _ = engine.apply_pre_hash_groups(table, config)
    # PII group disabled → email passes through clear
    assert new_table.column("email").to_pylist() == ["alice@x.com"]


def test_pii_policy_table_round_trip(
    repo_with_pii_dataset: tuple[SqliteMetadataRepository, int],
) -> None:
    repo, _ = repo_with_pii_dataset
    repo.upsert_pii_policy(pii_class="email", env="dev", action="hash", salt_kv_ref="x")
    repo.upsert_pii_policy(pii_class="ssn", env="prod", action="redact", redact_value="***-**-****")
    rows = repo.list_pii_policy()
    assert len(rows) == 2
    by_class = {(r.pii_class, r.env): r for r in rows}
    assert by_class[("email", "dev")].action == "hash"
    assert by_class[("ssn", "prod")].redact_value == "***-**-****"


def test_pii_policy_upsert_replaces_existing() -> None:
    repo = SqliteMetadataRepository(":memory:")
    repo.bootstrap_schema()
    p1 = repo.upsert_pii_policy(pii_class="email", env="dev", action="hash", salt_kv_ref="a")
    p2 = repo.upsert_pii_policy(pii_class="email", env="dev", action="mask", mask_visible=2)
    assert p1 == p2  # same policy_id (UPDATE)
    [row] = repo.list_pii_policy(env="dev")
    assert row.action == "mask"
    assert row.mask_visible == 2
