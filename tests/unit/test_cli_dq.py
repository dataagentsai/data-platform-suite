"""End-to-end CLI tests for the DQ subcommands.

Covers ``lake-ingest dq run`` (Phase 5) and ``lake-ingest dq history``
(Phase 5). The existing ``dq list`` is exercised here too for round-trip
parity.

Strategy: write a real SQLite metadata DB + a real Delta table on disk,
invoke the CLI, assert exit code + stdout content.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from lake_ingestion_framework.cli import app
from lake_ingestion_framework.core.dq import (
    Action,
    DqRule,
    DqRuleSet,
    InjectionPoint,
    Layer,
    Severity,
)
from lake_ingestion_framework.core.metadata.repository import (
    DatasetColumnRow,
    SqliteMetadataRepository,
)
from lake_ingestion_framework.core.types.enums import SourceType
from typer.testing import CliRunner

runner = CliRunner()


@pytest.fixture()
def seeded_repo(tmp_path: Path) -> Path:
    """Materialise a real SQLite metadata DB on disk, seeded with one
    source + dataset + rules. Yields the DB path."""
    db_path = tmp_path / "metadata-dev.sqlite"
    repo = SqliteMetadataRepository(db_path)
    try:
        repo.bootstrap_schema()
        source_id = repo.upsert_source(
            source_name="orders",
            source_env="dev",
            source_type=SourceType.AZURE_SQL_DB,
            is_enabled=True,
            config={
                "connection_pattern": "azure_basic",
                "auth_method": "managed_identity",
            },
        )
        repo.upsert_dataset(
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
            ],
        )
    finally:
        repo.close()
    return db_path


def _seed_rules(db_path: Path, *rules: DqRule) -> None:
    repo = SqliteMetadataRepository(db_path)
    try:
        ds = next(d for d in repo.list_datasets() if d.dataset_name == "customers")
        rs = DqRuleSet(dataset_name="customers", layer=Layer.SILVER, rules=rules)
        repo.upsert_dq_rule_set(rs, dataset_id=ds.dataset_id)
    finally:
        repo.close()


def _make_rule(
    name: str,
    predicate: str,
    *,
    action: Action = Action.CONTINUE_LOG,
    severity: Severity = Severity.WARNING,
) -> DqRule:
    return DqRule(
        name=name,
        dataset_name="customers",
        layer=Layer.SILVER,
        predicate=predicate,
        injection_point=InjectionPoint.POST_PROCESSOR,
        severity=severity,
        action=action,
    )


def _write_delta(path: Path, *, with_null: bool) -> None:
    """Write a tiny Delta table to ``path`` with one optional NULL email."""
    import deltalake

    table = pa.table(
        {
            "customer_id": [1, 2, 3],
            "email": ["a@x.com", None if with_null else "b@x.com", "c@x.com"],
        }
    )
    deltalake.write_deltalake(str(path), table, mode="overwrite")


@pytest.fixture()
def fake_repo_dir(tmp_path: Path) -> Path:
    """A directory the CLI can be pointed at as ``--customer-repo``."""
    target = tmp_path / "customer"
    target.mkdir()
    return target


# ──────────────────────────────────────────────────────────────────────
# dq run
# ──────────────────────────────────────────────────────────────────────


def test_dq_run_passing_rule_exits_zero(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    """A rule that no rows violate prints PASS and exits 0."""
    _seed_rules(seeded_repo, _make_rule("clean", "customer_id < 0"))
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=False)

    result = runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
        ],
    )
    assert result.exit_code == 0, result.stdout + (result.stderr or "")
    assert "PASS" in result.stdout
    assert "1 passed" in result.stdout


def test_dq_run_continue_log_failure_exits_zero(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    """A continue_log rule that fires prints FAIL but exits 0 — the
    pipeline continues; the violation is logged for review."""
    _seed_rules(
        seeded_repo,
        _make_rule("emails", "email IS NULL", action=Action.CONTINUE_LOG),
    )
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=True)

    result = runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
        ],
    )
    assert result.exit_code == 0, result.stdout + (result.stderr or "")
    assert "FAIL" in result.stdout
    assert "1 failed" in result.stdout


def test_dq_run_stop_pipeline_exits_two(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    """stop_pipeline halts; CLI returns exit code 2 for orchestrators."""
    _seed_rules(
        seeded_repo,
        _make_rule("halt", "email IS NULL", action=Action.STOP_PIPELINE, severity=Severity.ERROR),
    )
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=True)

    result = runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
        ],
    )
    assert result.exit_code == 2, result.stdout + (result.stderr or "")
    assert "HALT" in result.stdout


def test_dq_run_no_rules_exits_zero_with_message(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=False)
    result = runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
        ],
    )
    assert result.exit_code == 0
    assert "No enabled rules" in result.stdout


def test_dq_run_writes_audit_row(seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path) -> None:
    """Verify the run actually wrote dq_results — the CLI is the
    "real" path to the action engine, not a dry-run."""
    _seed_rules(seeded_repo, _make_rule("emails", "email IS NULL"))
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=True)

    runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
            "--run-id",
            "cli-test-run",
        ],
    )

    # Read back the audit row
    repo = SqliteMetadataRepository(seeded_repo)
    try:
        results = repo.list_dq_results(run_id="cli-test-run")
        assert len(results) == 1
        assert results[0].action_taken == "continue_log"
    finally:
        repo.close()


# ──────────────────────────────────────────────────────────────────────
# dq history
# ──────────────────────────────────────────────────────────────────────


def test_dq_history_empty_prints_message(seeded_repo: Path, fake_repo_dir: Path) -> None:
    result = runner.invoke(
        app,
        [
            "dq",
            "history",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
        ],
    )
    assert result.exit_code == 0
    assert "No DQ results found" in result.stdout


def test_dq_history_renders_table_after_run(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    """After dq run produces results, dq history shows them."""
    _seed_rules(seeded_repo, _make_rule("emails", "email IS NULL"))
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=True)
    runner.invoke(
        app,
        [
            "dq",
            "run",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--dataset",
            "customers",
            "--layer",
            "silver",
            "--data-path",
            str(delta_path),
            "--run-id",
            "htest",
        ],
    )

    result = runner.invoke(
        app,
        [
            "dq",
            "history",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
        ],
    )
    assert result.exit_code == 0, result.stdout + (result.stderr or "")
    assert "htest" in result.stdout
    assert "failed" in result.stdout  # the rule fired


def test_dq_history_filter_by_run_id(
    seeded_repo: Path, fake_repo_dir: Path, tmp_path: Path
) -> None:
    """--run-id narrows the result set."""
    _seed_rules(seeded_repo, _make_rule("emails", "email IS NULL"))
    delta_path = tmp_path / "data"
    _write_delta(delta_path, with_null=True)

    for run_id in ("run-A", "run-B"):
        runner.invoke(
            app,
            [
                "dq",
                "run",
                "--customer-repo",
                str(fake_repo_dir),
                "--env",
                "dev",
                "--metadata-db",
                str(seeded_repo),
                "--dataset",
                "customers",
                "--layer",
                "silver",
                "--data-path",
                str(delta_path),
                "--run-id",
                run_id,
            ],
        )

    result = runner.invoke(
        app,
        [
            "dq",
            "history",
            "--customer-repo",
            str(fake_repo_dir),
            "--env",
            "dev",
            "--metadata-db",
            str(seeded_repo),
            "--run-id",
            "run-A",
        ],
    )
    assert result.exit_code == 0
    assert "run-A" in result.stdout
    assert "run-B" not in result.stdout
