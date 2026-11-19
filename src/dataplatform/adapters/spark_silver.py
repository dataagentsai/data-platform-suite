"""Silver on Spark and Delta tables: the stage runner (dataplatform.stages on PySpark) and the read and write edges.

Locally: open-source Spark from the `pyspark` package with `delta-spark`, local[2] and a small driver (laptop rule),
Delta tables in folders: <root>/<source>__<dataset> (bronze written by delta-rs is read as it is), silver and
<source>__<dataset>__quarantine under the silver root. On Databricks the same classes run against the cluster's
session; only the paths change.

Write edge (one Delta commit per table):
    append kinds   an idempotent append (txnAppId = silver:<table>, txnVersion = the run's file-log mark), so writing
                   the same result twice changes nothing
    other kinds    MERGE on _row_key: upserts update or insert, stale rows are deleted; new columns are added first
"""

from __future__ import annotations

import contextlib
import os
import re
import sys
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dataplatform.silver.plan import Measure, SilverPlan
from dataplatform.silver.ports import StageError, StageResult
from dataplatform.stages import run_stages

_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
ASCII_NINE = ord("9")  # hex digits 0-9 are ascii 48-57, a-f 97-102


def local_session(app: str = "dataplatform-silver", cores: int = 2, memory: str = "1g",
                  iceberg_warehouse: str | Path | None = None, iceberg_catalog: str = "ice") -> SparkSession:
    """A small local Spark with Delta. Uses the Spark inside the installed `pyspark` package (a SPARK_HOME pointing
    at another Spark install would mismatch it) and this Python for the workers. With `iceberg_warehouse`, an Iceberg
    folder catalog named `iceberg_catalog` is added beside Delta (ADR-027); the Iceberg runtime jar must be present
    (dataplatform.adapters.table_store.iceberg_jar)."""
    import pyspark
    from delta import configure_spark_with_delta_pip

    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    extensions = "io.delta.sql.DeltaSparkSessionExtension"
    extra: dict[str, str] = {}
    if iceberg_warehouse is not None:
        from dataplatform.adapters.table_store import iceberg_jar  # noqa: PLC0415 -- only when Iceberg is asked for

        jar = iceberg_jar()
        if jar is None:
            raise RuntimeError("Iceberg runtime jar not found: set DP_ICEBERG_JAR or put it in ~/.dataplatform/jars/")
        extensions += ",org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        extra = {
            "spark.jars": jar,
            f"spark.sql.catalog.{iceberg_catalog}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{iceberg_catalog}.type": "hadoop",
            f"spark.sql.catalog.{iceberg_catalog}.warehouse": str(iceberg_warehouse),
            # no in-process table metadata cache: a long run on a small driver must not keep every snapshot's metadata
            f"spark.sql.catalog.{iceberg_catalog}.cache-enabled": "false",
        }
    builder = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app)
        .config("spark.driver.memory", memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("DP_SHUFFLE_PARTITIONS", "1"))  # laptop data is small
        .config("spark.default.parallelism", str(cores))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", extensions)
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    for k, v in extra.items():
        builder = builder.config(k, v)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark



def _quoted(name: str) -> str:
    """A Spark SQL identifier in backticks, any backtick inside doubled (Spark's escape), so a column name that came
    from a source cannot end the identifier and add SQL of its own."""
    if "\x00" in name or "\n" in name:
        raise ValueError(f"not a usable identifier: {name!r}")
    return "`" + name.replace("`", "``") + "`"

class SparkStageRunner:
    """stage_runner on PySpark: the stages' frames are checkpointed once so the counts and the write read the same
    rows (and a merge into the table the stages read from sees a fixed input)."""

    def __init__(self, cache: bool = True):
        self.cache = cache  # serverless refuses cache() (PERSIST TABLE, probed 13 Nov): off there

    def run(self, plan: SilverPlan, bronze: DataFrame, current: DataFrame | None, run_id: str,
            as_of: str) -> StageResult:
        if self.cache:
            bronze = bronze.cache()
        rows, quarantine, tally = run_stages(plan, bronze, current, run_id, as_of, datetime.now(UTC))
        try:
            rows = rows.localCheckpoint(eager=True)
        except Exception as exc:  # an engine error on this batch's data (e.g. a cast the plan did not foresee)
            if self.cache:
                bronze.unpersist()
            raise StageError(f"{plan.table}: {type(exc).__name__}: {str(exc)[:500]}") from exc
        counts = {"rows_in": bronze.count()}
        by_action = {r["_action"]: r["n"] for r in rows.groupBy("_action").agg(F.count(F.lit(1)).alias("n")).collect()}
        counts["rows_out"] = int(by_action.get("upsert", 0))
        counts["rows_removed"] = int(by_action.get("delete", 0))
        # everything else counted in one job: quarantine per stage and each tally (dropped, replayed, emptied, ...)
        parts = [f.select(F.lit(name).alias("_what")) for name, f in tally.items()]
        if quarantine is not None:
            parts.append(quarantine.select(F.concat(F.col("_stage"), F.lit(".quarantined")).alias("_what")))
        counts.update({name: 0 for name in tally})
        if parts:
            for r in reduce(lambda x, y: x.unionByName(y), parts).groupBy("_what").count().collect():
                counts[str(r["_what"])] = int(r["count"])
        if self.cache:
            bronze.unpersist()
        return StageResult(rows, quarantine, counts)

    def release(self, result: StageResult) -> None:
        with contextlib.suppress(Exception):  # a checkpoint has nothing to free under Spark Connect
            result.rows.unpersist()


class DeltaSilverTables:
    """The read and write edges on Delta folders."""

    def __init__(self, spark: SparkSession, bronze_root: str | Path, silver_root: str | Path):
        self.spark, self.bronze_root, self.silver_root = spark, Path(bronze_root), Path(silver_root)

    @staticmethod
    def _folder(source: str, dataset: str, suffix: str = "") -> str:
        if not (_NAME.match(source) and _NAME.match(dataset)):
            raise ValueError(f"not a safe table name: {source}.{dataset}")
        return f"{source}__{dataset}{suffix}"

    def silver_path(self, source: str, dataset: str, suffix: str = "") -> Path:
        return self.silver_root / self._folder(source, dataset, suffix)

    @staticmethod
    def _exists(path: Path) -> bool:
        return (path / "_delta_log").exists()

    def _read(self, path: Path) -> DataFrame | None:
        return self.spark.read.format("delta").load(str(path)) if self._exists(path) else None

    def reset(self, plan: SilverPlan) -> None:
        import shutil  # noqa: PLC0415 -- only a rebuild removes tables

        for suffix in ("", "__quarantine"):
            shutil.rmtree(self.silver_path(plan.source, plan.dataset, suffix), ignore_errors=True)

    # ---- read edge
    def bronze(self, plan: SilverPlan, batch_keys: list[str]) -> DataFrame:
        path = self.bronze_root / self._folder(plan.source, plan.dataset)
        df = self._read(path)
        if df is None:
            raise StageError(f"{plan.table}: no bronze table at {path}")
        return df.where(F.col("_batch_key").isin(batch_keys))

    def current(self, plan: SilverPlan) -> DataFrame | None:
        return self._read(self.silver_path(plan.source, plan.dataset))

    # ---- write edge
    def write(self, plan: SilverPlan, result: StageResult) -> int:
        path = self.silver_path(plan.source, plan.dataset)
        txn = str(result.txn)
        held = sum(v for k, v in result.counts.items() if k.endswith(".quarantined"))
        if result.quarantine is not None and held:
            self._append(result.quarantine, self.silver_path(plan.source, plan.dataset, "__quarantine"),
                         f"quarantine:{plan.table}", txn)
        written = result.counts.get("rows_out", 0) + result.counts.get("rows_removed", 0)
        if not written:
            return 0
        if plan.history == "append" or not self._exists(path):
            self._append(result.rows.where(F.col("_action") == "upsert").drop("_action"), path,
                         f"silver:{plan.table}", txn)
        else:
            self._merge(result.rows, path)
        return written

    def _append(self, df: DataFrame, path: Path, app: str, txn: str) -> None:
        (df.write.format("delta").mode("append").option("txnAppId", app).option("txnVersion", txn)
         .option("mergeSchema", "true").save(str(path)))

    # shiftleft: allow SL-PURE-02 -- the write and read edge of silver (ADR-013): IO belongs here
    def _merge(self, rows: DataFrame, path: Path) -> None:
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(self.spark, str(path))
        have = {f.name for f in target.toDF().schema.fields}
        new = [f for f in rows.schema.fields if f.name not in have and f.name != "_action"]
        if new:  # new columns reach silver without a release (ADR-008)
            added = ", ".join(f"{_quoted(f.name)} {f.dataType.simpleString()}" for f in new)
            # DDL takes no bind parameters, so the identifiers are quoted instead: _quoted() escapes every column name
            # and the table path, and the types come from Spark's own schema, never from data.
            # shiftleft: allow SL-SEC-01 -- identifiers only, each escaped by _quoted(); no values in the statement
            self.spark.sql(f"ALTER TABLE delta.{_quoted(str(path))} ADD COLUMNS ({added})")
            target = DeltaTable.forPath(self.spark, str(path))
        cols = [c for c in rows.columns if c != "_action"]
        values: dict[str, str | Column] = {f"`{c}`": f"s.`{c}`" for c in cols}
        (
            target.alias("t")
            .merge(rows.alias("s"), "t._row_key = s._row_key")
            .whenMatchedDelete(condition="s._action = 'delete'")
            .whenMatchedUpdate(condition="s._action = 'upsert'", set=values)
            .whenNotMatchedInsert(condition="s._action = 'upsert'", values=values)
            .execute()
        )

    # ---- what silver holds
    # shiftleft: allow SL-PURE-06 -- the write and read edge of silver (ADR-013): IO belongs here
    def measures(self, plan: SilverPlan, measures: tuple[Measure, ...]) -> dict[str, float]:
        df = self.current(plan)
        names = ["row_count", "versions", "deleted_marked", *[m.name for m in measures if m.name != "row_count"]]
        if df is None:
            return {n: 0.0 for n in names}
        live = F.col("_is_current") & ~F.col("_is_deleted")
        aggs = [
            F.sum(F.when(live, 1).otherwise(0)).alias("row_count"),
            F.count(F.lit(1)).alias("versions"),
            F.sum(F.when(F.col("_is_current") & F.col("_is_deleted"), 1).otherwise(0)).alias("deleted_marked"),
            *[F.sum(F.when(live, F.col(m.column))).alias(m.name) for m in measures if m.agg == "sum"],
        ]
        row = df.agg(*aggs).first()
        return {n: round(float(row[n] or 0), 2) for n in names if row is not None}

    # shiftleft: allow SL-PURE-06 -- the write and read edge of silver (ADR-013): IO belongs here
    def bucket_totals(self, plan: SilverPlan, measures: dict[str, str | None],
                      buckets: int) -> dict[int, dict[str, float]]:
        cur = self.current(plan)
        return {} if cur is None else bucket_totals_frame(cur, plan.keys, measures, buckets)

    def keys_in_buckets(self, plan: SilverPlan, buckets: int, wanted: set[int]) -> set[tuple[str, ...]]:
        cur = self.current(plan)
        return set() if cur is None or not wanted else bucket_keys_frame(cur, plan.keys, buckets, wanted)

    def current_keys(self, source: str, dataset: str, keys: tuple[str, ...]) -> set[tuple[Any, ...]] | None:
        df = self._read(self.silver_path(source, dataset))
        if df is None:
            return None
        live = df.where(F.col("_is_current") & ~F.col("_is_deleted")).select(*keys).distinct()
        return {tuple(r[k] for k in keys) for r in live.collect()}

    # shiftleft: allow SL-PURE-06 -- the write and read edge of silver (ADR-013): IO belongs here
    def versions(self, plan: SilverPlan, key: dict[str, Any]) -> list[dict[str, Any]]:
        df = self.current(plan)
        if df is None:
            return []
        for k, v in key.items():
            df = df.where(F.col(k) == F.lit(v))
        order = "_seq_from" if plan.history == "scd2" else "_silver_loaded_at"
        return [r.asDict() for r in df.orderBy(order).collect()]

    # shiftleft: allow SL-PURE-06 -- the write and read edge of silver (ADR-013): IO belongs here
    def count_by_hour(self, plan: SilverPlan, column: str, start: str, end: str) -> dict[str, int]:
        df = self.current(plan)
        if df is None:
            return {}
        live = df.where(F.col("_is_current") & ~F.col("_is_deleted"))
        t, t0 = F.col(column), F.to_timestamp(F.lit(start))
        secs = F.unix_timestamp(t) - F.unix_timestamp(t0)
        bucket = F.timestamp_seconds(F.unix_timestamp(t0) + F.floor(secs / 3600) * 3600)
        rows = (live.where((t >= t0) & (t < F.to_timestamp(F.lit(end))))
                .groupBy(F.date_format(bucket, "yyyy-MM-dd'T'HH:mm:ss'+00:00'").alias("h"))
                .agg(F.count(F.lit(1)).alias("n")).collect())
        return {str(r["h"]): int(r["n"]) for r in rows}  # rendered by the engine in UTC (the session's zone)

    def frame(self, source: str, dataset: str, suffix: str = "") -> DataFrame | None:
        """The silver (or quarantine) table as a frame, for checks and tests."""
        return self._read(self.silver_path(source, dataset, suffix))


class StoreSilverTables(DeltaSilverTables):
    """The read and write edges of silver through the table port (ADR-027): the same behaviour as DeltaSilverTables
    on any format the store speaks. Bronze is <source>__<dataset> in the bronze layer; silver and its quarantine
    (<source>__<dataset>__quarantine) in the silver layer."""

    def __init__(self, spark: SparkSession, store: Any):
        self.spark, self.store = spark, store

    @staticmethod
    def _table(source: str, dataset: str, suffix: str = "") -> str:
        return DeltaSilverTables._folder(source, dataset, suffix)

    def bronze(self, plan: SilverPlan, batch_keys: list[str]) -> DataFrame:
        df = self.store.read("bronze", self._table(plan.source, plan.dataset))
        if df is None:
            raise StageError(f"{plan.table}: no bronze table for {plan.source}.{plan.dataset}")
        return df.where(F.col("_batch_key").isin(batch_keys))

    def current(self, plan: SilverPlan) -> DataFrame | None:
        return self.store.read("silver", self._table(plan.source, plan.dataset))

    def reset(self, plan: SilverPlan) -> None:
        for suffix in ("", "__quarantine"):
            self.store.drop("silver", self._table(plan.source, plan.dataset, suffix))

    # shiftleft: allow SL-PURE-02 -- the write edge of silver (ADR-013): IO belongs here
    def write(self, plan: SilverPlan, result: StageResult) -> int:
        table, txn = self._table(plan.source, plan.dataset), str(result.txn)
        held = sum(v for k, v in result.counts.items() if k.endswith(".quarantined"))
        if result.quarantine is not None and held:
            self.store.append("silver", self._table(plan.source, plan.dataset, "__quarantine"), result.quarantine,
                              f"quarantine:{plan.table}", txn)
        written = result.counts.get("rows_out", 0) + result.counts.get("rows_removed", 0)
        if not written:
            return 0
        if plan.history == "append" or not self.store.exists("silver", table):
            self.store.append("silver", table, result.rows.where(F.col("_action") == "upsert").drop("_action"),
                              f"silver:{plan.table}", txn)
        else:
            self.store.merge("silver", table, result.rows)
        return written

    # shiftleft: allow SL-PURE-06 -- the read edge of silver (ADR-013): IO belongs here
    def current_keys(self, source: str, dataset: str, keys: tuple[str, ...]) -> set[tuple[Any, ...]] | None:
        df = self.store.read("silver", self._table(source, dataset))
        if df is None:
            return None
        live = df.where(F.col("_is_current") & ~F.col("_is_deleted")).select(*keys).distinct()
        return {tuple(r[k] for k in keys) for r in live.collect()}

    def frame(self, source: str, dataset: str, suffix: str = "") -> DataFrame | None:
        return self.store.read("silver", self._table(source, dataset, suffix))


def _bucket_col(keys: tuple[str, ...], buckets: int) -> Column:
    """The bucket of each row's business key, as dataplatform.reconcile.bucket_of computes it: sha256 of the key
    parts as text, first 8 hex digits, mod buckets."""
    sha = F.sha2(F.concat_ws("\x1f", *[F.coalesce(F.col(k).cast("string"), F.lit("")) for k in keys]), 256)

    # the first 8 hex digits as a number, digit by digit (no hex conversion function exists on every engine)
    def digit(i: int) -> Column:  # '0'-'9' are ascii 48-57, 'a'-'f' 97-102 (sha2 gives lower-case hex)
        code = F.ascii(F.substring(sha, i + 1, 1))
        return F.when(code <= ASCII_NINE, code - 48).otherwise(code - 87).cast("long") * (16 ** (7 - i))

    return reduce(lambda a, b: a + b, [digit(i) for i in range(8)]) % F.lit(buckets)


def bucket_totals_frame(df: DataFrame, keys: tuple[str, ...], measures: dict[str, str | None],
                        buckets: int = 16) -> dict[int, dict[str, float]]:
    """dataplatform.reconcile.bucket_totals computed in the engine over silver's current, not deleted rows: the same
    bucket function (sha256 of the key parts as text, first 8 hex digits, mod buckets), only the totals come back."""
    live = df.where(F.col("_is_current") & ~F.col("_is_deleted"))
    bucket = _bucket_col(keys, buckets).alias("_bucket")
    aggs = [(F.count(F.lit(1)) if col is None else F.sum(F.col(col))).cast("double").alias(m)
            for m, col in measures.items()]
    return {int(r["_bucket"]): {m: float(r[m] or 0) for m in measures}
            for r in live.groupBy(bucket).agg(*aggs).collect()}


def bucket_keys_frame(df: DataFrame, keys: tuple[str, ...], buckets: int, wanted: set[int]) -> set[tuple[str, ...]]:
    """The business keys (as text) of silver's current, not deleted rows in the wanted buckets only: the drill-down
    after a bucket differs, so only a few buckets' keys ever reach the driver."""
    live = df.where(F.col("_is_current") & ~F.col("_is_deleted")).withColumn("_bucket", _bucket_col(keys, buckets))
    rows = live.where(F.col("_bucket").isin(sorted(wanted))).select(*[F.col(k).cast("string") for k in keys])
    return {tuple("" if v is None else str(v) for v in r) for r in rows.collect()}


def hop_delays_frame(df: DataFrame) -> list[Any]:
    """dataplatform.timing.BATCH_HOPS on a silver frame, computed in the engine: seconds between the stamps of each
    hop, then the percentiles; only hops whose stamps the frame has, and only rows that carry both stamps."""
    from dataplatform.timing import BATCH_HOPS, PERCENTILES, HopDelay  # noqa: PLC0415 -- adapters import the kernel lazily

    out = []
    for hop, start, end in BATCH_HOPS:
        if start not in df.columns or end not in df.columns:
            continue
        secs = (F.unix_timestamp(F.col(end)) - F.unix_timestamp(F.col(start))).cast("double")
        both = df.where(F.col(start).isNotNull() & F.col(end).isNotNull()).select(secs.alias("s"))
        row = both.agg(F.count(F.lit(1)).alias("n"),
                       *[F.percentile_approx("s", p / 100).alias(f"p{p}") for p in PERCENTILES]).first()
        if row is not None and row["n"]:
            out.append(HopDelay(hop, int(row["n"]), *(float(row[f"p{p}"]) for p in PERCENTILES)))
    return out
