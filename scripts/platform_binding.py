"""The adapters a run picks, shared by the day loop (run_platform.py) and by one hop running as its own process
(run_hop.py), so both bind the same engine, tables and metadata store from the same options."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUN_META_DSN = os.environ.get("DP_RUN_META_DSN", "dbname=dp_meta_run")


@dataclass
class Engine:
    """What silver and gold run on. Every field is None when the run stops before that layer."""

    spark: Any = None
    table_store: Any = None
    bronze_writer: Any = None
    tables: Any = None
    stages: Any = None
    gold: Any = None  # (gold tables, gold engine, gold reader, judge)
    local_session: Callable[[], Any] | None = None


def attach_meta(kind: str) -> Any:
    """The metadata store a run already filled, opened by another process. Nothing is created or emptied."""
    if kind == "memory":
        raise SystemExit("--meta memory lives in one process; a hop in its own process needs postgres or lakebase")
    if kind == "lakebase":
        from dataplatform.adapters.lakebase import connect_lakebase

        return connect_lakebase(os.environ["DP_META_INSTANCE"], os.environ.get("DP_META_DATABASE", "dp_meta_proof"))
    from dataplatform.adapters.postgres_meta import PostgresMetaStore

    return PostgresMetaStore.connect(RUN_META_DSN)


def build_engine(a: Any, tz: str, on_store: Callable[[Any], None] | None = None, *, dwd: Any) -> Engine:
    """Silver and gold adapters for the options `a` (silver, gold, engine, table_format, spark, out, env). The domain's
    gold models folder and gold judge come from the world `dwd` (never from a fixed domain).
    `on_store(table_store)` runs once the table store exists and before anything is built on it (a proof run empties
    its catalogs there)."""
    if not a.silver:
        return Engine()
    if a.engine == "duckdb":
        return _duckdb(a, tz, dwd)
    if a.table_format != "legacy":
        return _table_port(a, tz, on_store, dwd)
    return _legacy(a, tz, dwd)


def _duckdb(a: Any, tz: str, dwd: Any) -> Engine:
    from dataplatform.adapters import duckdb_frames

    duckdb_frames.activate()  # before anything imports pyspark: the stages and builds then run on DuckDB
    from dataplatform.adapters.duckdb_gold import DeltaRsGoldTables, DuckGoldEngine, DuckGoldReader
    from dataplatform.adapters.duckdb_silver import DeltaRsSilverTables, DuckStageRunner
    from dataplatform.world.domains import gold_judge_for, models_folder

    def local_session() -> Any:
        return duckdb_frames.local_session(duckdb_frames.connection())

    spark = local_session()
    e = Engine(spark=spark, tables=DeltaRsSilverTables(spark, a.out / "bronze", a.out / "silver"),
               stages=DuckStageRunner(), local_session=local_session)
    if a.gold:
        e.gold = (DeltaRsGoldTables(spark, a.out / "silver", a.out / "gold"),
                  DuckGoldEngine(spark, ROOT / models_folder(dwd)), DuckGoldReader(spark, a.out / "gold"), gold_judge_for(dwd, tz))
    return e


def _table_port(a: Any, tz: str, on_store: Callable[[Any], None] | None, dwd: Any) -> Engine:
    from dataplatform.adapters.spark_gold import SparkGoldEngine, StoreGoldReader, StoreGoldTables
    from dataplatform.adapters.spark_silver import SparkStageRunner, StoreSilverTables
    from dataplatform.adapters.spark_silver import local_session as spark_session
    from dataplatform.adapters.table_store import StoreBronzeWriter, make_store
    from dataplatform.world.domains import gold_judge_for, models_folder

    warehouse = a.out / "iceberg" if a.table_format == "iceberg" else None

    def local_session() -> Any:
        if a.spark == "existing":  # Databricks: the job's own session (Spark Connect; it is never stopped)
            from pyspark.sql import SparkSession

            return SparkSession.builder.getOrCreate()
        return spark_session(iceberg_warehouse=warehouse)

    spark = local_session()
    table_store = make_store(spark, a.table_format, a.out, a.env if a.table_format == "iceberg-uc" else "ice")
    if on_store is not None:
        on_store(table_store)
    # serverless refuses cache() (probed 13 Nov)
    e = Engine(spark=spark, table_store=table_store, bronze_writer=StoreBronzeWriter(table_store),
               tables=StoreSilverTables(spark, table_store), stages=SparkStageRunner(cache=a.spark == "local"),
               local_session=local_session)
    if a.gold:
        e.gold = (StoreGoldTables(spark, table_store), SparkGoldEngine(spark, ROOT / models_folder(dwd)),
                  StoreGoldReader(spark, table_store), gold_judge_for(dwd, tz))
    return e


def _legacy(a: Any, tz: str, dwd: Any) -> Engine:
    from dataplatform.adapters.spark_silver import DeltaSilverTables, SparkStageRunner, local_session

    spark = local_session()
    e = Engine(spark=spark, tables=DeltaSilverTables(spark, a.out / "bronze", a.out / "silver"),
               stages=SparkStageRunner(), local_session=local_session)
    if a.gold:
        from dataplatform.adapters.spark_gold import DeltaGoldTables, SparkGoldEngine, SparkGoldReader
        from dataplatform.world.domains import gold_judge_for, models_folder

        e.gold = (DeltaGoldTables(spark, a.out / "silver", a.out / "gold"), SparkGoldEngine(spark, ROOT / models_folder(dwd)),
                  SparkGoldReader(spark, a.out / "gold"), gold_judge_for(dwd, tz))
    return e
