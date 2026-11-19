"""Closes the YAML→metadata loop for the standard-processors track.

Customer authors:

* ``processor_groups:`` + ``pii_policy:`` blocks in platform_config
* ``column_transforms`` fields on each ``columns:`` entry
* ``dedup:`` block + ``processor_groups:`` overrides at dataset level

``lake-ingest config sync`` parses, validates, persists. These tests
cover the full round-trip — YAML → plan → apply → metadata DB →
read-back via the repository methods the engine consumes at runtime.
"""

from __future__ import annotations

from pathlib import Path

from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
from lake_ingestion_framework.core.use_cases.config_sync import (
    apply_config_sync,
    plan_config_sync,
)


def _write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _seed_repo_with_processor_yaml(root: Path) -> Path:
    """Customer-repo skeleton with processor_groups + pii_policy +
    column_transforms + dedup all wired up."""
    _write(
        root / "config" / "platform_config" / "dev.yaml",
        """
default_realm: adf-adls
target_storage_account: mysa
processor_groups:
  hygiene: enabled
  type_safety: enabled
  case_normalize: enabled
  timestamp_normalize: enabled
  standardization: opt_in
  pii: enabled
  dedup: dataset_decides
  provenance: enabled
pii_policy:
  - pii_class: email
    action: pass_through
  - pii_class: ssn
    action: hash
    salt_kv_ref: kv://my-vault/pii-salt
  - pii_class: phone
    action: mask
    mask_visible: 4
        """.strip(),
    )
    _write(
        root / "config" / "sources" / "orders_db.yaml",
        """
source_name: orders_db
source_env: dev
source_type: azure_sql_db
config:
  connection_pattern: azure_basic
  auth_method: managed_identity
        """.strip(),
    )
    _write(
        root / "config" / "datasets" / "orders_db" / "customers.yaml",
        """
source_name: orders_db
source_env: dev
dataset_name: customers
is_enabled: true
config:
  realm: adf-adls
  dataset_type: TABLE
  load_type: INCREMENTAL
  target_path_template: raw/{source}/{object}
  schema_name: dbo
  object_name: customers
  watermark_column: modified_at
processor_groups:
  case_normalize: disabled
dedup:
  mode: business_key_latest
  business_keys: [customer_id]
  order_by: modified_at
columns:
  - column_name: cust_id
    data_type: INT
    ordinal_position: 1
    is_primary_key: true
    silver_column_name: customer_id
    silver_data_type: BIGINT
    cast_safety: try_cast
  - column_name: email
    data_type: VARCHAR
    ordinal_position: 2
    pii_class: email
    transforms: [decode_html_entities]
  - column_name: notes
    data_type: VARCHAR
    ordinal_position: 3
    skip_groups: [hygiene]
        """.strip(),
    )
    return root


# ──────────────────────────────────────────────────────────────────────
# Plan-side
# ──────────────────────────────────────────────────────────────────────


def test_plan_picks_up_platform_processor_groups(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert plan.platform_config.processor_groups["hygiene"] == "enabled"
    assert plan.platform_config.processor_groups["dedup"] == "dataset_decides"
    assert len(plan.platform_config.processor_groups) == 8


def test_plan_picks_up_pii_policy(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert len(plan.platform_config.pii_policy) == 3
    by_class = {e.pii_class: e for e in plan.platform_config.pii_policy}
    assert by_class["email"].action == "pass_through"
    assert by_class["ssn"].action == "hash"
    assert by_class["ssn"].salt_kv_ref == "kv://my-vault/pii-salt"
    assert by_class["phone"].mask_visible == 4


def test_plan_picks_up_dataset_processor_overrides(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    [ds] = plan.datasets
    assert ds.processor_groups == {"case_normalize": "disabled"}
    assert ds.dedup is not None
    assert ds.dedup.mode == "business_key_latest"
    assert ds.dedup.business_keys == ("customer_id",)
    assert ds.dedup.order_by == "modified_at"


def test_plan_picks_up_column_transforms(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    [ds] = plan.datasets
    by_name = {c.column_name: c for c in ds.columns}
    assert by_name["cust_id"].silver_column_name == "customer_id"
    assert by_name["cust_id"].silver_data_type == "BIGINT"
    assert by_name["cust_id"].cast_safety == "try_cast"
    assert by_name["email"].transforms == ("decode_html_entities",)
    assert by_name["notes"].skip_groups == ("hygiene",)


# ──────────────────────────────────────────────────────────────────────
# Apply-side — round-trip via the repository
# ──────────────────────────────────────────────────────────────────────


def test_apply_persists_platform_processor_groups_as_kv(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)
        # 8 platform-level + 1 dataset-level (case_normalize override) = 9
        assert result.processor_group_keys_upserted == 9
        kv = repo.list_platform_config()
        assert kv["processor_groups.hygiene"] == "enabled"
        assert kv["processor_groups.dedup"] == "dataset_decides"
        assert kv["default_realm"] == "adf-adls"
    finally:
        repo.close()


def test_apply_persists_pii_policy_rows(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)
        assert result.pii_policies_upserted == 3
        rows = repo.list_pii_policy(env="dev")
        assert len(rows) == 3
        by_class = {r.pii_class: r for r in rows}
        assert by_class["email"].action == "pass_through"
        assert by_class["ssn"].action == "hash"
        assert by_class["ssn"].salt_kv_ref == "kv://my-vault/pii-salt"
    finally:
        repo.close()


def test_apply_persists_dataset_processor_overrides_and_dedup(tmp_path: Path) -> None:
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        apply_config_sync(plan, repo)
        ds = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        ds_kv = repo.get_dataset_config(ds.dataset_id)
        # Per-dataset processor_groups override
        assert ds_kv["processor_groups.case_normalize"] == "disabled"
        # Dedup config flat-keyed
        assert ds_kv["dedup.mode"] == "business_key_latest"
        assert ds_kv["dedup.business_keys"] == "customer_id"
        assert ds_kv["dedup.order_by"] == "modified_at"
    finally:
        repo.close()


def test_apply_persists_column_transforms_into_dataset_config_columns(
    tmp_path: Path,
) -> None:
    """The processor engine reads dataset_config_columns at runtime —
    so customer YAML transforms need to land there or processors have
    nothing to act on."""
    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        apply_config_sync(plan, repo)
        ds = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        cols = repo.list_dataset_columns(ds.dataset_id)
        by_name = {c.column_name: c for c in cols}

        assert by_name["cust_id"].silver_column_name == "customer_id"
        assert by_name["cust_id"].silver_data_type == "BIGINT"
        assert by_name["cust_id"].cast_safety == "try_cast"

        assert by_name["email"].pii_class == "email"
        assert by_name["email"].transforms == ("decode_html_entities",)

        assert by_name["notes"].skip_groups == ("hygiene",)
    finally:
        repo.close()


def test_apply_processor_engine_round_trip(tmp_path: Path) -> None:
    """End-to-end proof that authoring in YAML produces an
    EngineConfig the runtime can use without any further wiring."""
    from lake_ingestion_framework.core.processors import ProcessorEngine

    root = _seed_repo_with_processor_yaml(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        apply_config_sync(plan, repo)
        ds = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        ds_kv = repo.get_dataset_config(ds.dataset_id)

        engine = ProcessorEngine()
        config = engine.resolve_config(
            repo,
            dataset_id=ds.dataset_id,
            platform_groups={
                k.removeprefix("processor_groups."): v
                for k, v in repo.list_platform_config().items()
                if k.startswith("processor_groups.")
            },
            dataset_groups={
                k.removeprefix("processor_groups."): v
                for k, v in ds_kv.items()
                if k.startswith("processor_groups.")
            },
            env="dev",
        )

        # Dataset disabled case_normalize → engine respects it
        from lake_ingestion_framework.core.processors import GroupMode, ProcessorGroup

        by_group = {g.group: g for g in config.groups}
        assert by_group[ProcessorGroup.CASE_NORMALIZE].mode == GroupMode.DISABLED
        assert by_group[ProcessorGroup.HYGIENE].mode == GroupMode.ENABLED

        # column-level skip_groups round-trip into the engine's view
        assert "hygiene" in config.column_skip_groups.get("notes", set())
        assert "decode_html_entities" in config.column_opt_in_processors.get("email", set())
    finally:
        repo.close()


# ──────────────────────────────────────────────────────────────────────
# Validation paths
# ──────────────────────────────────────────────────────────────────────


def test_invalid_processor_group_name_rejected(tmp_path: Path) -> None:
    import pytest
    from lake_ingestion_framework.core.exceptions import ValidationError

    _write(
        tmp_path / "config" / "platform_config" / "dev.yaml",
        """
default_realm: adf-adls
processor_groups:
  not_a_real_group: enabled
        """.strip(),
    )
    with pytest.raises(ValidationError, match="not_a_real_group"):
        plan_config_sync(customer_repo=tmp_path, env="dev")


def test_invalid_pii_action_rejected(tmp_path: Path) -> None:
    import pytest
    from lake_ingestion_framework.core.exceptions import ValidationError

    _write(
        tmp_path / "config" / "platform_config" / "dev.yaml",
        """
default_realm: adf-adls
pii_policy:
  - pii_class: email
    action: encrypt
        """.strip(),
    )
    with pytest.raises(ValidationError, match="action"):
        plan_config_sync(customer_repo=tmp_path, env="dev")


def test_invalid_dedup_mode_rejected(tmp_path: Path) -> None:
    import pytest
    from lake_ingestion_framework.core.exceptions import ValidationError

    _write(tmp_path / "config" / "platform_config" / "dev.yaml", "default_realm: adf-adls")
    _write(
        tmp_path / "config" / "sources" / "orders_db.yaml",
        """
source_name: orders_db
source_env: dev
source_type: azure_sql_db
config:
  connection_pattern: azure_basic
  auth_method: managed_identity
        """.strip(),
    )
    _write(
        tmp_path / "config" / "datasets" / "orders_db" / "customers.yaml",
        """
source_name: orders_db
source_env: dev
dataset_name: customers
is_enabled: true
config:
  realm: adf-adls
  dataset_type: TABLE
  load_type: INCREMENTAL
  target_path_template: raw/{source}/{object}
  schema_name: dbo
  object_name: customers
  watermark_column: modified_at
dedup:
  mode: invent_a_mode
        """.strip(),
    )
    with pytest.raises(ValidationError, match="dedup.mode"):
        plan_config_sync(customer_repo=tmp_path, env="dev")
