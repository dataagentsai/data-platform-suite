"""Unit tests for the config-sync use case."""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
from lake_ingestion_framework.core.use_cases.config_sync import (
    apply_config_sync,
    plan_config_sync,
)


def _write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _seed_minimal_repo(root: Path) -> Path:
    """Create a customer-repo skeleton with one source + one dataset."""
    _write(
        root / "config" / "platform_config" / "dev.yaml",
        """
default_realm: adf-adls
target_storage_account: mysa
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
        """.strip(),
    )
    return root


def test_plan_config_sync_minimal(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert plan.env == "dev"
    assert len(plan.sources) == 1
    assert len(plan.datasets) == 1
    assert plan.warnings == ()


def test_plan_config_sync_warns_on_orphan_dataset(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _write(
        root / "config" / "datasets" / "ghost" / "x.yaml",
        """
source_name: ghost
source_env: dev
dataset_name: x
        """.strip(),
    )
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert any("ghost" in w for w in plan.warnings)


def test_plan_config_sync_missing_platform_config(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    with pytest.raises(ValidationError, match="platform_config"):
        plan_config_sync(customer_repo=tmp_path, env="dev")


def test_plan_config_sync_missing_config_dir(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="config/"):
        plan_config_sync(customer_repo=tmp_path, env="dev")


def test_apply_config_sync_writes_rows(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)

        assert result.platform_config_keys == 2
        assert result.sources_upserted == 1
        assert result.datasets_upserted == 1

        assert repo.schema_version() == 4
        sources = repo.list_sources()
        assert [(s.source_name, s.source_env) for s in sources] == [("orders_db", "dev")]

        datasets = repo.list_datasets()
        assert [d.dataset_name for d in datasets] == ["customers"]
    finally:
        repo.close()


def test_apply_skips_orphan_dataset(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _write(
        root / "config" / "datasets" / "ghost" / "x.yaml",
        """
source_name: ghost
source_env: dev
dataset_name: x
        """.strip(),
    )
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)
        # The orphan was skipped; only the legit dataset landed.
        assert result.datasets_upserted == 1
    finally:
        repo.close()


# ──────────────────────────────────────────────────────────────────────
# DQ rules — config sync end-to-end (DQ engine v1, Phase 1)
# ──────────────────────────────────────────────────────────────────────


def _seed_dq_yaml(root: Path) -> None:
    """Drop a couple of DQ rule files into the seeded customer repo."""
    _write(
        root / "config" / "dq" / "silver_customers.yaml",
        """
dataset: customers
layer: silver
rules:
  - name: customer_id_not_null
    predicate: customer_id IS NULL
    severity: error
    action: stop_pipeline
    injection_point: post_processor

  - name: email_format
    predicate: email IS NOT NULL AND email NOT LIKE '%@%'
    severity: warning
    action: quarantine_record
    quarantine_strategy: side_table
    injection_point: post_processor
        """.strip(),
    )


def test_plan_picks_up_dq_files(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _seed_dq_yaml(root)
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert len(plan.dq_rule_sets) == 1
    rule_set = plan.dq_rule_sets[0]
    assert rule_set.dataset_name == "customers"
    assert len(rule_set.rules) == 2
    # No cheap-eval warnings on these (they're all fine)
    assert plan.dq_warnings == ()


def test_plan_warns_when_dq_references_unknown_dataset(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _write(
        root / "config" / "dq" / "silver_orphan.yaml",
        """
dataset: orphan_table_not_in_metadata
layer: silver
rules:
  - name: x_not_null
    predicate: x IS NULL
    severity: info
    action: continue_log
    injection_point: post_processor
        """.strip(),
    )
    plan = plan_config_sync(customer_repo=root, env="dev")
    assert any("orphan_table_not_in_metadata" in w for w in plan.warnings)


def test_apply_writes_dq_rules_to_db(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _seed_dq_yaml(root)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)
        assert result.dq_rules_inserted == 2
        assert result.dq_rules_updated == 0
        assert result.dq_rules_unchanged == 0

        # Read back via the repository — proves the round-trip works
        # the same way the SilverPipeline runtime will at evaluate time.
        from lake_ingestion_framework.core.dq import Layer

        dataset = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        rules = repo.list_dq_rules(dataset_id=dataset.dataset_id, layer=Layer.SILVER)
        assert {r.rule_name for r in rules} == {"customer_id_not_null", "email_format"}
    finally:
        repo.close()


def test_apply_dq_rules_is_idempotent(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _seed_dq_yaml(root)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        first = apply_config_sync(plan, repo)
        assert first.dq_rules_inserted == 2
        # Re-plan + re-apply with no YAML change → all unchanged
        plan2 = plan_config_sync(customer_repo=root, env="dev")
        second = apply_config_sync(plan2, repo)
        assert second.dq_rules_inserted == 0
        assert second.dq_rules_updated == 0
        assert second.dq_rules_unchanged == 2
        assert second.dq_rules_soft_deleted == 0
    finally:
        repo.close()


def test_apply_soft_deletes_rule_dropped_from_yaml(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _seed_dq_yaml(root)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        apply_config_sync(plan, repo)
        # Rewrite the YAML to drop one rule
        _write(
            root / "config" / "dq" / "silver_customers.yaml",
            """
dataset: customers
layer: silver
rules:
  - name: customer_id_not_null
    predicate: customer_id IS NULL
    severity: error
    action: stop_pipeline
    injection_point: post_processor
            """.strip(),
        )
        plan2 = plan_config_sync(customer_repo=root, env="dev")
        result = apply_config_sync(plan2, repo)
        # Default behavior is soft-delete (is_enabled=0)
        assert result.dq_rules_soft_deleted == 1
        assert result.dq_rules_hard_deleted == 0

        from lake_ingestion_framework.core.dq import Layer

        dataset = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        enabled = repo.list_dq_rules(dataset_id=dataset.dataset_id, layer=Layer.SILVER)
        all_rules = repo.list_dq_rules(
            dataset_id=dataset.dataset_id, layer=Layer.SILVER, only_enabled=False
        )
        assert {r.rule_name for r in enabled} == {"customer_id_not_null"}
        # Audit trail preserved — the dropped rule is still there
        assert {r.rule_name for r in all_rules} == {"customer_id_not_null", "email_format"}
    finally:
        repo.close()


def test_apply_hard_deletes_with_flag(tmp_path: Path) -> None:
    root = _seed_minimal_repo(tmp_path)
    _seed_dq_yaml(root)
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        apply_config_sync(plan, repo)
        _write(
            root / "config" / "dq" / "silver_customers.yaml",
            """
dataset: customers
layer: silver
rules:
  - name: customer_id_not_null
    predicate: customer_id IS NULL
    severity: error
    action: stop_pipeline
    injection_point: post_processor
            """.strip(),
        )
        plan2 = plan_config_sync(customer_repo=root, env="dev")
        result = apply_config_sync(plan2, repo, delete_missing_dq_rules=True)
        assert result.dq_rules_hard_deleted == 1
        assert result.dq_rules_soft_deleted == 0

        from lake_ingestion_framework.core.dq import Layer

        dataset = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        all_rules = repo.list_dq_rules(
            dataset_id=dataset.dataset_id, layer=Layer.SILVER, only_enabled=False
        )
        assert {r.rule_name for r in all_rules} == {"customer_id_not_null"}
    finally:
        repo.close()


def test_apply_skips_dq_rules_when_dataset_unknown(tmp_path: Path) -> None:
    """Orphan DQ files (referencing a dataset not in metadata) are
    warned in plan; apply skips them silently — matches the
    dataset/source orphan posture."""
    root = _seed_minimal_repo(tmp_path)
    _write(
        root / "config" / "dq" / "silver_ghost.yaml",
        """
dataset: ghost_dataset_not_registered
layer: silver
rules:
  - name: x_check
    predicate: x IS NULL
    severity: info
    action: continue_log
    injection_point: post_processor
        """.strip(),
    )
    plan = plan_config_sync(customer_repo=root, env="dev")
    repo = SqliteMetadataRepository(":memory:")
    try:
        result = apply_config_sync(plan, repo)
        # The orphan rule set was silently skipped at apply time.
        assert result.dq_rules_inserted == 0
    finally:
        repo.close()
