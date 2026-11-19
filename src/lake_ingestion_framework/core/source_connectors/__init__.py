"""Source-side connectors used by ``lake-ingest discover``.

Read-only connectors that talk to source systems for catalog
introspection (INFORMATION_SCHEMA reads, sample-row reads). They
*never* mutate the source.

Per ``docs/code_design.md``: ``core/`` doesn't import realm-specific
SDKs (``azure-storage-blob`` / ``pyspark``), but generic database
drivers (``psycopg``, ``pyodbc``) are allowed here because each
driver applies across multiple realms — a Postgres source feeds
``adf-adls``, ``aws-glue-stepfunctions``, *and*
``airflow-spark-anylake``.
"""

from __future__ import annotations

from lake_ingestion_framework.core.source_connectors.protocol import (
    ColumnInfo,
    SourceConnector,
    TableMeta,
)

__all__ = ["ColumnInfo", "SourceConnector", "TableMeta"]
