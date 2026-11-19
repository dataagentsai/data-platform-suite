"""Adapters: one package per product family. Only adapters import a product.

    files.py        landing_store on any folder: a local path, a Databricks volume (/Volumes/...), a mounted share.
    memory.py       metadata_service, table_writer, run_log and check sink in memory, for tests.
    memory_meta.py  the metadata store port (dataplatform.meta.port) in memory, for tests.
    postgres_meta.py the metadata store on any PostgreSQL through its procedures (sql/postgres_meta.sql);
                    the engine's managed PostgreSQL is the same adapter with another DSN (DP_META_DSN).
    spark_delta.py  the same ports on Spark + Delta tables (Databricks, or open-source Spark locally).
"""
