"""Loader for Phase 6b-1 metadata DB stored procedures.

Stored procs ship **T-SQL only** — SQLite has no stored-proc support,
so unit tests against the SQLite metadata DB exercise the same logic
in Python (see ``tests/unit/test_runtime_sql_procs.py``). Integration
tests in Phase 8 apply the canonical T-SQL to a real Azure SQL DB.

Procs that ship in 6b-1:
- ``sp_master_run_log_started`` / ``sp_master_run_log_finished``
- ``sp_run_log_started`` / ``sp_run_log_finished``
- ``sp_advance_watermark``

Each proc uses ``CREATE OR ALTER PROCEDURE`` for idempotent deploy.
"""

from __future__ import annotations

from importlib import resources

# Deploy order is open (procs don't reference each other in their
# bodies — they all reference the underlying tables, which exist by
# the time procs are deployed). Listed in observability-flow order
# (master open → child open → child close → master close → watermark)
# for readability of generated deploy scripts.
PROC_NAMES: tuple[str, ...] = (
    "sp_master_run_log_started",
    "sp_run_log_started",
    "sp_run_log_finished",
    "sp_master_run_log_finished",
    "sp_advance_watermark",
)


def proc_sql(proc_name: str) -> str:
    """Return the canonical T-SQL DDL for a single stored procedure."""
    if proc_name not in PROC_NAMES:
        raise ValueError(
            f"unknown proc {proc_name!r}; expected one of {PROC_NAMES}",
        )
    return (
        resources.files(
            "lake_ingestion_framework.realms.adf_adls.runtime_sql.procs",
        )
        .joinpath("azure_sql_db")
        .joinpath(f"{proc_name}.sql")
        .read_text()
    )


def all_procs() -> list[tuple[str, str]]:
    """Return ``[(proc_name, ddl), ...]`` in deploy order."""
    return [(name, proc_sql(name)) for name in PROC_NAMES]


__all__ = [
    "PROC_NAMES",
    "all_procs",
    "proc_sql",
]
