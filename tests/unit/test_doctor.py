"""Unit tests for ``lake-ingest doctor``."""

from __future__ import annotations

from pathlib import Path

from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
from lake_ingestion_framework.core.probes import ProbeStatus
from lake_ingestion_framework.core.use_cases.doctor import run_doctor


def _seed_repo(root: Path) -> Path:
    config_dir = root / "config"
    (config_dir / "platform_config").mkdir(parents=True)
    (config_dir / "platform_config" / "dev.yaml").write_text(
        "default_realm: adf-adls\n",
        encoding="utf-8",
    )
    return root


def test_doctor_pass_against_clean_repo(tmp_path: Path) -> None:
    root = _seed_repo(tmp_path)
    repo = SqliteMetadataRepository(":memory:")
    try:
        repo.bootstrap_schema()
        report = run_doctor(customer_repo=root, env="dev", metadata_repo=repo)
    finally:
        repo.close()

    assert report.overall is ProbeStatus.PASS
    assert {r.name for r in report.results} == {
        "framework-version",
        "config-schema",
        "metadata-db",
    }


def test_doctor_warns_on_missing_config_dir(tmp_path: Path) -> None:
    repo = SqliteMetadataRepository(":memory:")
    try:
        repo.bootstrap_schema()
        report = run_doctor(customer_repo=tmp_path, env="dev", metadata_repo=repo)
    finally:
        repo.close()

    assert report.overall is ProbeStatus.WARN
    config_probe = next(r for r in report.results if r.name == "config-schema")
    assert config_probe.status is ProbeStatus.WARN


def test_doctor_fails_on_missing_platform_config(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    repo = SqliteMetadataRepository(":memory:")
    try:
        repo.bootstrap_schema()
        report = run_doctor(customer_repo=tmp_path, env="dev", metadata_repo=repo)
    finally:
        repo.close()

    assert report.overall is ProbeStatus.FAIL
    config_probe = next(r for r in report.results if r.name == "config-schema")
    assert config_probe.status is ProbeStatus.FAIL


def test_doctor_fails_on_unbootstrapped_db(tmp_path: Path) -> None:
    root = _seed_repo(tmp_path)
    repo = SqliteMetadataRepository(":memory:")
    try:
        # Note: no bootstrap_schema()
        report = run_doctor(customer_repo=root, env="dev", metadata_repo=repo)
    finally:
        repo.close()

    assert report.overall is ProbeStatus.FAIL
    db_probe = next(r for r in report.results if r.name == "metadata-db")
    assert db_probe.status is ProbeStatus.FAIL
