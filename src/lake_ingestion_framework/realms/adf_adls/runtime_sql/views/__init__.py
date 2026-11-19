"""Loader for the Phase 6a metadata DB views.

Two dialects ship:

- ``"azure_sql_db"`` — canonical T-SQL DDL applied at customer site
  (see ``docs/realms/adf-adls-runtime-sql.md`` for the
  install command). Each file uses ``CREATE OR ALTER VIEW`` so
  re-running the deploy is idempotent.
- ``"sqlite"`` — same logical projection in SQLite syntax for unit
  tests. Each file is ``DROP VIEW IF EXISTS`` + ``CREATE VIEW``.

The view files live in dialect subdirectories alongside this module
so they're packaged with the wheel and reachable via
``importlib.resources``.
"""

from __future__ import annotations

import sqlite3
from importlib import resources
from typing import Literal

Dialect = Literal["azure_sql_db", "sqlite"]

# Order matters when applying: views with no inter-dependencies but a
# stable order keeps deployments deterministic and diffable.
VIEW_NAMES: tuple[str, ...] = (
    # Runtime dispatch views — the ADF runtime Lookups against these
    # to figure out what to ingest.
    "v_enabled_sources",
    "v_source_config",
    "v_enabled_datasets",
    # Observability views — Power BI / Grafana / Synapse Studio
    # consume these for ops dashboards. See docs/operability.md.
    "v_dataset_health",
    "v_recent_failures",
    "v_master_run_summary",
    "v_data_volume_trend",
    "v_slow_runs",
    # Cost attribution — finance-facing; bridges observability and
    # cost concerns. See docs/cost.md.
    "v_cost_attribution",
    # Data quality — flags runs where source and sink row counts
    # disagree, and days where volume drifts > 25% from a 7-day
    # baseline. See docs/operability.md.
    "v_row_count_anomalies",
    "v_volume_drift",
    # Reliability — current watermark + time since advance + last
    # operator-driven reset. See docs/operability.md.
    "v_watermark_status",
    # Reliability — master-run retry chains (length >= 2). Audits
    # the L4 auto-retry layer; see docs/recovery.md.
    "v_retry_chains",
)


def view_sql(view_name: str, *, dialect: Dialect) -> str:
    """Return the DDL for a single view in the chosen dialect."""
    if view_name not in VIEW_NAMES:
        raise ValueError(
            f"unknown view {view_name!r}; expected one of {VIEW_NAMES}",
        )
    return (
        resources.files(
            "lake_ingestion_framework.realms.adf_adls.runtime_sql.views",
        )
        .joinpath(dialect)
        .joinpath(f"{view_name}.sql")
        .read_text()
    )


def all_views(*, dialect: Dialect) -> list[tuple[str, str]]:
    """Return ``[(view_name, ddl), ...]`` in deploy order."""
    return [(name, view_sql(name, dialect=dialect)) for name in VIEW_NAMES]


def bootstrap_sqlite_views(conn: sqlite3.Connection) -> None:
    """Apply the SQLite view DDLs to an open connection.

    Used by tests that need to exercise the views against the
    in-memory ``SqliteMetadataRepository``. Production deploys go
    through the T-SQL files via ``sqlcmd`` (or whatever the customer's
    DBA uses); this helper is test-only.
    """
    for _, ddl in all_views(dialect="sqlite"):
        conn.executescript(ddl)


__all__ = [
    "VIEW_NAMES",
    "Dialect",
    "all_views",
    "bootstrap_sqlite_views",
    "view_sql",
]
