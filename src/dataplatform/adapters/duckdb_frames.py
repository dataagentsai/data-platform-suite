"""DuckDB as a frame engine for the pure stage and build code, through SQLFrame (the PySpark DataFrame API compiled to
SQL by sqlglot): one logic, several engines (docs/DUCKDB-SPIKE.md). For laptop and CI runs only; nothing here ever
writes a governed table on Databricks, where Spark (and Photon) stay the engine.

SQLFrame takes over the `pyspark` imports for the whole process (`activate`), so the stages (`dataplatform.stages`)
and the dataframe builds (`models/`) run unchanged: call `activate()` before anything imports pyspark, then use the
session from `local_session()` wherever a SparkSession is expected.

The session is small like the local Spark (laptop rule): 2 threads, 1 GB, and UTC as the session zone (timestamps stay
instants in UTC, as on Spark).
"""

from __future__ import annotations

from typing import Any

THREADS, MEMORY = 2, "1GB"


def connection(threads: int = THREADS, memory: str = MEMORY) -> Any:
    import duckdb

    return duckdb.connect(config={"threads": threads, "memory_limit": memory, "TimeZone": "UTC"})


def activate(conn: Any = None) -> Any:
    """Point every later `import pyspark...` at SQLFrame on DuckDB (process-wide). Returns the connection."""
    import logging

    import sqlframe

    # SQLFrame and sqlglot warn on every to_json (nulls kept) and every hint (ignored); both are known and recorded
    # in docs/DUCKDB-SPIKE.md, so the run's log keeps only errors from them
    for name in ("sqlframe", "sqlglot"):
        logging.getLogger(name).setLevel(logging.ERROR)
    conn = conn or connection()
    sqlframe.activate(engine="duckdb", conn=conn)
    return conn


def local_session(conn: Any = None) -> Any:
    """A SQLFrame DuckDB session (the SparkSession stand-in) on the activated connection, or a new one."""
    from sqlframe.duckdb import DuckDBSession

    return DuckDBSession.builder.getOrCreate() if conn is None else DuckDBSession(conn=conn)


def checkpoint(df: Any) -> Any:
    """localCheckpoint for a SQLFrame frame: run its query once into a DuckDB table and read that table (a plain
    table in the session's in-memory database: SQLFrame finds a temporary table's columns in no catalog it reads).
    Without it a frame built from earlier frames is one ever-growing query that DuckDB recomputes from the start (the
    history tests' third batch took minutes)."""
    import uuid

    session = df.session
    name = f"_hold_{uuid.uuid4().hex}"
    query = df.sql(dialect="duckdb", optimize=False, pretty=False)
    # the table name is generated here and the query is SQLFrame's own compiled text: nothing from data
    # shiftleft: allow SL-SEC-01 -- a uuid4 table name and SQLFrame's own compiled query; no value from data
    session._conn.execute(f'CREATE TABLE "{name}" AS {query}')
    return session.table(name)
