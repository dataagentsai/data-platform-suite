"""Runtime SQL artifacts for the Azure Databricks ADF realm.

Three artifact families ship under this package:

- **Views** (Phase 6a): the projections Phase 5d ADF Lookups query —
  ``meta.v_enabled_sources``, ``meta.v_source_config``,
  ``meta.v_enabled_datasets``.
- **Tables** (Phase 6b-1): typed log tables ADF writes to via stored
  procedures — ``meta.master_run_log``, ``meta.run_log``.
  ``dataset_segments``, ``segment_run_log``, ``watermark_audit`` land
  in 6b-2 / 6b-3.
- **Procs** (Phase 6b-1): the stored procedures the ADF runtime calls
  to record runs and advance watermarks atomically.

Two dialects ship for views and tables:
- ``"azure_sql_db"`` — canonical T-SQL, deployed at customer site.
- ``"sqlite"`` — same logical shape for unit tests against the
  in-memory ``SqliteMetadataRepository``.

Procs are T-SQL only (SQLite has no stored-proc support); their
behavior is unit-tested via Python equivalents and integration-
tested against a real Azure SQL DB in Phase 8.
"""

from __future__ import annotations

from .procs import PROC_NAMES, all_procs, proc_sql
from .tables import TABLE_NAMES, all_tables, bootstrap_sqlite_tables, table_sql
from .views import VIEW_NAMES, all_views, bootstrap_sqlite_views, view_sql

__all__ = [
    "PROC_NAMES",
    "TABLE_NAMES",
    "VIEW_NAMES",
    "all_procs",
    "all_tables",
    "all_views",
    "bootstrap_sqlite_tables",
    "bootstrap_sqlite_views",
    "proc_sql",
    "table_sql",
    "view_sql",
]
