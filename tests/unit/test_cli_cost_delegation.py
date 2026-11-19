"""Smoke tests for `lake-ingest cost ...` → costtool-core delegation.

The `cost` subcommand was extracted to the standalone `costtool-core`
package in Phase 2 Item 1 (see ADR 0009). This module verifies that
`lake-ingest cost lint` and `lake-ingest cost inspect` still work
end-to-end via the delegator in `cli.py`. The full Phase-1 rule
behavior is tested in `costtool-core/tests/`.
"""

from __future__ import annotations

from lake_ingestion_framework.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_lake_ingest_cost_lint_delegates() -> None:
    """`lake-ingest cost lint` invokes the costtool-core engine."""
    result = runner.invoke(
        app,
        ["cost", "lint", "--sql", "SELECT * FROM x WHERE id = 1"],
    )
    assert result.exit_code == 0  # WARNING-only
    assert "select_star" in result.stdout


def test_lake_ingest_cost_lint_error_exits_two() -> None:
    """ERROR-severity findings propagate through the delegator."""
    result = runner.invoke(app, ["cost", "lint", "--sql", "DELETE FROM customers"])
    assert result.exit_code == 2
    assert "delete_without_where" in result.stdout


def test_lake_ingest_cost_lint_clean_sql() -> None:
    """Clean SQL still exits 0 with a friendly message."""
    result = runner.invoke(
        app,
        ["cost", "lint", "--sql", "SELECT id FROM x WHERE id = 1 LIMIT 10"],
    )
    assert result.exit_code == 0
    assert "No SQL anti-patterns found" in result.stdout


def test_lake_ingest_cost_help_lists_subcommands() -> None:
    """`lake-ingest cost --help` shows lint + inspect as subcommands."""
    result = runner.invoke(app, ["cost", "--help"])
    assert result.exit_code == 0
    assert "lint" in result.stdout
    assert "inspect" in result.stdout
