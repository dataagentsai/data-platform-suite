"""The slice 1 ports on Spark + Delta tables: Databricks (serverless, Spark Connect) or open-source Spark locally.

Metadata DB tables (one schema per environment, decision D10 shape kept small for slice 1):
    dataset_config   one row per dataset: the compiled manifest as JSON + its fingerprint
    quality_rules    one row per row-level rule, compiled from the contract; the silver pipeline reads these
    batch_registry   one row per batch key and per file seen, with status and run id
    run_log          one row per step per run
    check_results    one row per verdict, keyed by run id
    evidence         a view joining run log and checks per run: "is this number right, and how do we know?"
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable

from dataplatform.model import CheckResult, Delivery, RunRecord

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}(\.[A-Za-z_][A-Za-z0-9_]{0,127}){0,2}$")


def ident(name: str) -> str:
    """Validate a (catalog.)(schema.)table name before it goes into SQL (the repo's no-SQL-by-string rule)."""
    if not _IDENT.match(name):
        raise ValueError(f"not a safe identifier: {name!r}")
    return ".".join(f"`{p}`" for p in name.split("."))


DDL = {
    "dataset_config": "source_name STRING, dataset_name STRING, config_json STRING, contract_json STRING, "
                      "config_fingerprint STRING, registered_at TIMESTAMP",
    "quality_rules": "source_name STRING, dataset_name STRING, rule STRING, predicate STRING, contract_version STRING",
    "batch_registry": "batch_key STRING, source_name STRING, dataset_name STRING, window STRING, file STRING, "
                      "status STRING, run_id STRING, reason STRING, recorded_at TIMESTAMP",
    "run_log": "run_id STRING, day INT, step STRING, source_name STRING, dataset_name STRING, status STRING, "
               "config_fingerprint STRING, counts STRING, message STRING, logged_at TIMESTAMP",
    "check_results": "run_id STRING, day INT, check STRING, obligation STRING, passed BOOLEAN, expected STRING, "
                     "observed STRING, detail STRING, checked_at TIMESTAMP",
}


def now() -> datetime:
    return datetime.now(timezone.utc)


def create_meta(spark: Any, schema: str) -> None:
    for t, cols in DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ident(schema + '.' + t)} ({cols})")
    s = ident(schema)
    spark.sql(f"""CREATE OR REPLACE VIEW {s}.`evidence` AS
        SELECT r.run_id, r.day, r.status AS land_status, r.config_fingerprint, r.counts AS land_counts,
               r.message AS land_message, c.checks, c.failed, c.failed_checks
        FROM {s}.`run_log` r
        LEFT JOIN (SELECT run_id, day, COUNT(*) AS checks, COUNT_IF(NOT passed) AS failed,
                          ARRAY_SORT(COLLECT_LIST(CASE WHEN NOT passed THEN check END)) AS failed_checks
                   FROM {s}.`check_results` GROUP BY run_id, day) c
          ON r.run_id = c.run_id AND r.day = c.day
        WHERE r.step = 'land'""")


def _append(spark: Any, table: str, rows: list[tuple], ddl: str) -> None:
    if rows:
        spark.createDataFrame(rows, ddl).write.mode("append").saveAsTable(table)


class SparkMeta:
    """metadata_service on Delta tables. Reads all batch statuses once per run (no per-file queries)."""

    def __init__(self, spark: Any, schema: str):
        self.spark, self.schema = spark, schema
        rows = spark.table(f"{schema}.batch_registry").select("batch_key", "status").collect()
        self._status = {r["batch_key"]: r["status"] for r in rows}

    def dataset_config(self, source: str, dataset: str) -> dict[str, Any]:
        from pyspark.sql import functions as F

        row = (self.spark.table(f"{self.schema}.dataset_config")
               .where((F.col("source_name") == source) & (F.col("dataset_name") == dataset))
               .orderBy("registered_at", ascending=False).first())
        if row is None:
            raise LookupError(f"{source}.{dataset} is not registered in {self.schema}.dataset_config")
        return {"dataset": json.loads(row["config_json"]), "contract": json.loads(row["contract_json"]),
                "fingerprint": row["config_fingerprint"]}

    def batch_status(self, batch_key: str) -> str | None:
        return self._status.get(batch_key)

    def record_batch(self, batch_key: str, delivery: Delivery, status: str, run_id: str, reason: str = "") -> None:
        self._status[batch_key] = status
        _append(self.spark, f"{self.schema}.batch_registry",
                [(batch_key, delivery.source, delivery.dataset, delivery.window, delivery.name, status, run_id,
                  reason, now())], DDL["batch_registry"])


class DeltaWriter:
    """table_writer for bronze: typed append to a Delta table."""

    CAST = {"string": str, "double": float, "bigint": int, "boolean": bool,
            "timestamp": lambda v: datetime.fromisoformat(v) if isinstance(v, str) else v}

    def __init__(self, spark: Any):
        self.spark = spark

    def append(self, table: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> None:
        ident(table)
        ddl = ", ".join(f"{c} {t.upper()}" for c, t in columns)
        typed = [tuple(None if r.get(c) is None else self.CAST[t](r[c]) for c, t in columns) for r in rows]
        _append(self.spark, table, typed, ddl)


class SparkRunLog:
    def __init__(self, spark: Any, schema: str):
        self.spark, self.table = spark, f"{schema}.run_log"

    def write(self, r: RunRecord) -> None:
        _append(self.spark, self.table, [(r.run_id, r.day, r.step, r.source, r.dataset, r.status,
                                          r.config_fingerprint, json.dumps(r.counts), r.message[:4000], now())],
                DDL["run_log"])


class SparkChecks:
    def __init__(self, spark: Any, schema: str):
        self.spark, self.table = spark, f"{schema}.check_results"

    def write(self, results: Iterable[CheckResult]) -> None:
        _append(self.spark, self.table,
                [(c.run_id, c.day, c.check, c.obligation, c.passed, json.dumps(c.expected, default=str),
                  json.dumps(c.observed, default=str), c.detail[:4000], now()) for c in results],
                DDL["check_results"])


class SparkBronzeWriter:
    """The bronze writer (dataplatform.ingest.ports.BronzeWriter) on Delta tables through Spark: Databricks, or
    open-source Spark with delta-spark locally. One write replaces the rows of its batch key (replaceWhere), so a
    batch written twice leaves the table unchanged and a failed write leaves nothing. Tables: <catalog>.<source>.<dataset>
    (ADR-012: one catalog per environment and layer, e.g. dev_bronze)."""

    TYPES = {"string": "STRING", "integer": "BIGINT", "number": "DOUBLE", "boolean": "BOOLEAN",
             "timestamp": "TIMESTAMP", "date": "DATE"}

    def __init__(self, spark: Any, catalog: str = ""):
        self.spark, self.catalog = spark, catalog

    def _name(self, table: str) -> str:
        return ident(f"{self.catalog}.{table}" if self.catalog else table)

    def write(self, table: str, batch_key: str, rows: list[dict[str, Any]], columns: list[tuple[str, str]],
              replace: bool = False) -> int:
        from dataplatform.ingest.ports import check_batch_key

        check_batch_key(batch_key)  # it goes into replaceWhere
        name = self._name(table)
        ddl = ", ".join(f"`{c}` {self.TYPES[t]}" for c, t in columns)
        df = self.spark.createDataFrame([tuple(r.get(c) for c, _ in columns) for r in rows], ddl)
        writer = df.write.format("delta")
        if self.spark.catalog.tableExists(name.replace("`", "")):
            writer = writer.mode("overwrite").option("replaceWhere", f"_batch_key = '{batch_key}'")
        else:
            writer = writer.mode("append")
        writer.saveAsTable(name)
        return len(rows)

    def write_many(self, table: str, batches: list[tuple[str, list[dict[str, Any]]]],
                   columns: list[tuple[str, str]]) -> int:
        """New batches in one append (one commit); the lander logs them only after it returns."""
        from dataplatform.ingest.ports import check_batch_key

        if not batches:
            return 0
        for k, _ in batches:
            check_batch_key(k)
        ddl = ", ".join(f"`{c}` {self.TYPES[t]}" for c, t in columns)
        rows = [tuple(r.get(c) for c, _ in columns) for _, part in batches for r in part]
        self.spark.createDataFrame(rows, ddl).write.format("delta").mode("append").saveAsTable(self._name(table))
        return len(rows)

    def _rows(self, table: str, where: dict[str, str] | None) -> Any:
        from pyspark.sql import functions as F

        name = self._name(table)
        if not self.spark.catalog.tableExists(name.replace("`", "")):
            return None
        df = self.spark.table(name)
        for c, v in (where or {}).items():
            df = df.where(F.col(c).cast("string") == F.lit(v))
        return df

    def fingerprint(self, table: str, where: dict[str, str] | None = None,
                    sums: tuple[str, ...] = ()) -> dict[str, float]:
        from pyspark.sql import functions as F

        df = self._rows(table, where)
        if df is None:
            return {"row_count": 0.0, **{c: 0.0 for c in sums}}
        row = df.agg(F.count(F.lit(1)).alias("row_count"), *[F.sum(c).alias(c) for c in sums]).first()
        return {k: float(row[k] or 0) for k in ("row_count", *sums)}

    def keys(self, table: str, columns: tuple[str, ...], where: dict[str, str]) -> set[tuple[Any, ...]]:
        df = self._rows(table, where)
        if df is None:
            return set()
        return {tuple(r[c] for c in columns) for r in df.select(*columns).distinct().collect()}
