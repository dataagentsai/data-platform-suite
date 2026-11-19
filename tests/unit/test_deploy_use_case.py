"""Unit tests for the ``deploy`` use case (plan + apply)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
from lake_ingestion_framework.core.types.enums import SourceType
from lake_ingestion_framework.core.use_cases.deploy import (
    apply_deploy,
    plan_deploy,
)


@pytest.fixture()
def seeded_repo() -> Iterator[SqliteMetadataRepository]:
    r = SqliteMetadataRepository(":memory:")
    r.bootstrap_schema()
    r.upsert_platform_config(
        {
            "target_storage_account": "dummycustomeradevsa",
            "target_container_default": "lake",
            "default_schedule_cron": "0 2 * * *",
        }
    )
    sid = r.upsert_source(
        source_name="orders_db",
        source_env="dev",
        source_type=SourceType.AZURE_SQL_DB,
        is_enabled=True,
        config={
            "connection_pattern": "azure_basic",
            "auth_method": "managed_identity",
            "server": "orders-server.database.windows.net",
            "database": "orders",
        },
    )
    r.upsert_dataset(
        source_id=sid,
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
    )
    try:
        yield r
    finally:
        r.close()


class TestPlanDeploy:
    def test_plan_returns_files_with_target_paths(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        plan = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        assert plan.realm == "adf-adls"
        assert plan.env == "dev"
        assert plan.target_dir == tmp_path / "generated" / "adf-adls" / "dev"
        # Each planned file has a target path under target_dir
        for pf in plan.files:
            assert pf.target_path.is_relative_to(plan.target_dir)
            # Filename matches kind subdirectory
            assert pf.target_path.parent.name == pf.kind

    def test_plan_unknown_realm_raises(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(ValidationError, match="Unknown realm"):
            plan_deploy(
                realm="aws-glue-stepfunctions",
                env="dev",
                repo=seeded_repo,
                customer_repo=tmp_path,
            )


class TestApplyDeploy:
    def test_apply_writes_files(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        plan = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        result = apply_deploy(plan)
        assert result.files_written == len(plan.files)
        for pf in plan.files:
            assert pf.target_path.exists()
            assert pf.target_path.read_text(encoding="utf-8") == pf.rendered

    def test_apply_overwrites_by_default(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        plan = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        # Pre-create one file with stale content
        first_file = plan.files[0]
        first_file.target_path.parent.mkdir(parents=True, exist_ok=True)
        first_file.target_path.write_text("STALE", encoding="utf-8")
        # Apply should overwrite — that's what deploy is for
        apply_deploy(plan)
        assert first_file.target_path.read_text(encoding="utf-8") == first_file.rendered

    def test_apply_strict_no_overwrite_refuses(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        plan = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        # Pre-create
        first_file = plan.files[0]
        first_file.target_path.parent.mkdir(parents=True, exist_ok=True)
        first_file.target_path.write_text("STALE", encoding="utf-8")
        with pytest.raises(ValidationError, match="already exists"):
            apply_deploy(plan, overwrite=False)


class TestRoundTripDeterminism:
    """Golden-diff invariant: plan → write → re-plan → re-write → bytes equal."""

    def test_two_full_runs_produce_identical_bytes(
        self,
        seeded_repo: SqliteMetadataRepository,
        tmp_path: Path,
    ) -> None:
        plan_a = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        apply_deploy(plan_a)
        snapshot_a = {
            pf.target_path.relative_to(plan_a.target_dir): pf.target_path.read_bytes()
            for pf in plan_a.files
        }

        plan_b = plan_deploy(
            realm="adf-adls",
            env="dev",
            repo=seeded_repo,
            customer_repo=tmp_path,
        )
        apply_deploy(plan_b)
        snapshot_b = {
            pf.target_path.relative_to(plan_b.target_dir): pf.target_path.read_bytes()
            for pf in plan_b.files
        }

        assert snapshot_a == snapshot_b
