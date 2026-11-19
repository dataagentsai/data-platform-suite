"""Tests for the `lake-ingest init` use case — generation determinism."""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.use_cases.init import (
    apply_init,
    plan_init,
    profile_summary,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_ROOT = REPO_ROOT / "templates" / "customer_repo_template"
PROFILES_DIR = REPO_ROOT / "templates" / "profiles"


def test_plan_dummycustomera(tmp_path: Path) -> None:
    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )

    assert plan.profile.customer.name == "dummycustomera"
    assert plan.target_dir == tmp_path / "out"

    file_names = {pf.target_path.relative_to(plan.target_dir).as_posix() for pf in plan.files}
    expected = {
        "README.md",
        "pyproject.toml",
        ".gitignore",
        "customer-profile.yaml",
        "config/platform_config/dev.yaml",
        "config/platform_config/uat.yaml",
        "config/platform_config/prod.yaml",
    }
    assert file_names == expected


def test_plan_is_deterministic(tmp_path: Path) -> None:
    """Same profile + same template + same paths → byte-identical output."""
    p1 = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out1",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    p2 = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out2",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    p1_relpath_content = sorted((pf.target_path.name, pf.rendered) for pf in p1.files)
    p2_relpath_content = sorted((pf.target_path.name, pf.rendered) for pf in p2.files)
    assert p1_relpath_content == p2_relpath_content


def test_apply_writes_files(tmp_path: Path) -> None:
    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    apply_init(plan)

    out = tmp_path / "out"
    assert (out / "README.md").exists()
    assert (out / "pyproject.toml").exists()
    assert (out / "config" / "platform_config" / "dev.yaml").exists()
    assert (out / "config" / "platform_config" / "uat.yaml").exists()
    assert (out / "config" / "platform_config" / "prod.yaml").exists()


def test_apply_refuses_existing_dir(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "stuff").write_text("existing")

    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=out,
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    with pytest.raises(ValidationError, match="already non-empty"):
        apply_init(plan)


def test_apply_overwrite_into_existing_dir(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "stuff").write_text("existing")

    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=out,
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    apply_init(plan, overwrite=True)
    assert (out / "README.md").exists()
    # Existing unrelated file is preserved
    assert (out / "stuff").exists()


def test_pyproject_pins_correct_extras(tmp_path: Path) -> None:
    """The customer's pyproject.toml extras come from declared sources."""
    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    pyproject = next(pf for pf in plan.files if pf.target_path.name == "pyproject.toml")
    # dummycustomera has sql_server + sftp; expect those extras + adf realm.
    assert "lake-ingest[adf,sftp,sql_server]" in pyproject.rendered


def test_per_env_platform_config_uses_env_values(tmp_path: Path) -> None:
    """Each environment block produces a platform_config/<env>.yaml that
    contains the env's own storage account."""
    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=tmp_path / "out",
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    dev_yaml = next(
        pf
        for pf in plan.files
        if pf.target_path.name == "dev.yaml" and pf.target_path.parent.name == "platform_config"
    )
    assert "dummycustomeradevsa" in dev_yaml.rendered
    assert "runtime_environment: dev" in dev_yaml.rendered

    prod_yaml = next(
        pf
        for pf in plan.files
        if pf.target_path.name == "prod.yaml" and pf.target_path.parent.name == "platform_config"
    )
    assert "dummycustomeraprodsa" in prod_yaml.rendered
    assert "runtime_environment: prod" in prod_yaml.rendered


def test_profile_summary() -> None:
    plan = plan_init(
        profile_path=PROFILES_DIR / "dummycustomera.yaml",
        target_dir=Path("/tmp/x"),
        template_root=TEMPLATE_ROOT,
        framework_local_path=REPO_ROOT,
    )
    summary = profile_summary(plan.profile)
    assert summary["customer"] == "dummycustomera"
    assert summary["environments"] == ["dev", "uat", "prod"]
    assert "sql_server" in summary["source_types"]  # type: ignore[operator]
    assert "sftp" in summary["source_types"]  # type: ignore[operator]
