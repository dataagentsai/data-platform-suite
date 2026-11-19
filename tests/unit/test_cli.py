"""Unit tests for the CLI skeleton.

Asserts the subcommand surface exists and `--version` works. Real
behavior tests land alongside each subcommand's implementation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from lake_ingestion_framework._version import __version__
from lake_ingestion_framework.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_version_flag() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_help_lists_subcommands() -> None:
    """Every documented subcommand must appear in --help output."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in (
        "init",
        "config",
        "discover",
        "deploy",
        "doctor",
        "reingest",
        "backfill",
        "reset-watermark",
    ):
        assert command in result.stdout


@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("reingest", ["--dataset", "x"]),
        ("backfill", ["--dataset", "x", "--data-from", "2024-01-01", "--data-to", "2024-12-31"]),
        ("reset-watermark", ["--dataset", "x", "--to", "0", "--reason", "test reason for v0.x"]),
    ],
)
def test_unimplemented_subcommands_raise_pointing_to_doc(command: str, args: list[str]) -> None:
    """Stub commands surface NotImplementedError with a doc pointer.

    Each command moves out of this list as it gets implemented.
    """
    result = runner.invoke(app, [command, *args])
    assert result.exit_code != 0
    assert isinstance(result.exception, NotImplementedError)
    assert "docs/" in str(result.exception)


def test_init_dry_run_against_dummycustomera(tmp_path: Path) -> None:
    """`lake-ingest init` runs end-to-end against the bundled profile."""
    profile_path = (
        Path(__file__).resolve().parents[2] / "templates" / "profiles" / "dummycustomera.yaml"
    )
    target = tmp_path / "out"
    result = runner.invoke(
        app, ["init", "--profile", str(profile_path), "--target-dir", str(target)]
    )
    assert result.exit_code == 0, result.stdout + (result.stderr or "")
    assert "dummycustomera" in result.stdout
    assert "Dry run" in result.stdout
    # Dry run wrote nothing
    assert not target.exists()


def test_init_apply_writes_files(tmp_path: Path) -> None:
    profile_path = (
        Path(__file__).resolve().parents[2] / "templates" / "profiles" / "dummycustomera.yaml"
    )
    target = tmp_path / "out"
    result = runner.invoke(
        app,
        [
            "init",
            "--profile",
            str(profile_path),
            "--target-dir",
            str(target),
            "--apply",
        ],
    )
    assert result.exit_code == 0
    assert (target / "README.md").exists()
    assert (target / "pyproject.toml").exists()
    assert (target / "config" / "platform_config" / "dev.yaml").exists()
