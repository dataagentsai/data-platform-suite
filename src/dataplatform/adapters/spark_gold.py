"""Gold on Spark and Delta tables: the harness's engine operations (PySpark) and the gold read and write edges.

Locally: the same small Spark as silver (adapters/spark_silver.local_session), Delta tables in folders:
<gold root>/<gold schema>__<table> (gold_sales__fact_otif). On Databricks the same classes run on the cluster's
session; only the paths change.

Builds (D18): a SQL model is a file of one query that reads its inputs as views named <source>__<dataset> and takes
named parameters (:business_tz); a dataframe build is `module.path:function`, a pure function
(inputs: dict[str, DataFrame], params: dict[str, str]) -> DataFrame. Both are resolved under the models root.

Write edges (one Delta commit per table):
    dimension   overwrite (keys are deterministic: a rebuild changes nothing that did not change)
    fact        replace the changed windows: overwrite with replaceWhere on the window column (ADR-006)
"""

from __future__ import annotations

import contextlib
import importlib.util
import re
from collections.abc import Callable
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from dataplatform.gold.keys import HEX_DIGITS, NOT_APPLICABLE, SEP, UNKNOWN
from dataplatform.gold.plan import GoldPlan
from dataplatform.gold.windows import Numbers
from dataplatform.meta.port import Correction

_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
_WINDOW = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}Z)?$")
BEGINNING, END_OF_TIME = "1900-01-01 00:00:00", "9999-12-31 00:00:00"
# keys.TS_FORMAT and the hour window's name, as the engine writes them (the session is UTC). Built from pieces around
# literal text rather than quoted letters in one pattern: quoted letters are not portable (docs/DUCKDB-SPIKE.md).
TS_PIECES = ("yyyy-MM-dd", "T", "HH:mm:ss.SSSSSS", "Z")
HOUR_PIECES = ("yyyy-MM-dd", "T", "HH:mm:ss", "Z")
RESERVED = {UNKNOWN: "unknown", NOT_APPLICABLE: "not applicable"}
Build = Callable[[dict[str, DataFrame], dict[str, str]], DataFrame]


def _safe(name: str) -> str:
    if not _NAME.match(name):
        raise ValueError(f"not a safe table name: {name}")
    return name


def as_text(col: Column, pieces: tuple[str, ...]) -> Column:
    """A timestamp as text: date_format patterns at even places, literal text at odd places."""
    return F.concat(*[F.date_format(col, p) if i % 2 == 0 else F.lit(p) for i, p in enumerate(pieces)])


def key_text(dataset: str, key: list[Column], version_start: Column | None) -> Column:
    """keys.key_text in the engine: the same text the Python recipe hashes."""
    parts = [F.lit(dataset), *[F.coalesce(c.cast("string"), F.lit("")) for c in key]]
    if version_start is not None:
        parts.append(as_text(version_start, TS_PIECES))
    return F.concat_ws(SEP, *parts)


def surrogate(dataset: str, key: list[Column], version_start: Column | None) -> Column:
    """keys.surrogate_key in Spark: the same text, the same SHA-256, the first 15 hex digits."""
    text = key_text(dataset, key, version_start)
    return F.conv(F.substring(F.sha2(text, 256), 1, HEX_DIGITS), 16, 10).cast("long")


def _free(df: Any) -> None:
    """Release a cached or checkpointed frame where the engine allows it (serverless refuses UNPERSIST)."""
    with contextlib.suppress(Exception):
        df.unpersist()


class SparkGoldEngine:
    """The harness's frame operations on PySpark. Pure: no reads or writes (ADR-013).

    The engine-specific pieces are the methods `surrogate`, `row_hash`, `sql`, `hold` and `release`; everything else is
    the PySpark DataFrame API in forms other engines behind the same API run too (adapters/duckdb_gold.py)."""

    surrogate = staticmethod(surrogate)

    def __init__(self, spark: SparkSession, models_root: str | Path):
        self.spark, self.models_root = spark, Path(models_root)

    # ---- the window column as text (windows.py names windows by text)
    @staticmethod
    def window_text(plan: GoldPlan) -> Column:
        assert plan.window is not None
        col = F.col(plan.window.column)
        return as_text(col, HOUR_PIECES) if plan.window.unit == "hour" else F.date_format(col, "yyyy-MM-dd")

    # ---- the common one-to-one pipeline
    def dimension(self, plan: GoldPlan, silver: DataFrame, run_id: str) -> DataFrame:
        bk = list(plan.business_key)
        # nulls first stated (Spark's default): a window order on SQLFrame leaves it to the engine
        by_key = Window.partitionBy(*bk).orderBy(F.col("_valid_from").asc_nulls_first(),
                                                 F.col("_row_key").asc_nulls_first())
        rows = silver.withColumn("_n", F.row_number().over(by_key))
        sk = self.surrogate(plan.inputs[0].dataset, [F.col(c) for c in bk], F.col("_valid_from"))
        own = {plan.surrogate: sk,
               "valid_from": F.when(F.col("_n") == 1, F.to_timestamp(F.lit(BEGINNING))).otherwise(F.col("_valid_from")),
               "valid_to": F.coalesce(F.col("_valid_to"), F.to_timestamp(F.lit(END_OF_TIME))),
               "is_current": F.col("_is_current")}
        cols = [own[c].alias(c) if c in own else F.col(c).cast(t).alias(c) for c, t in plan.columns]
        extra = [F.col("_valid_from").alias("_source_valid_from"), F.col("_is_deleted"), F.lit(run_id).alias("_run_id")]
        members = rows.select(*cols, *extra)
        return members.unionByName(self._reserved(plan, run_id, members.schema))

    def _reserved(self, plan: GoldPlan, run_id: str, schema: T.StructType) -> DataFrame:
        rows = []
        for sk, text in RESERVED.items():
            row: dict[str, Any] = {}
            for f in schema.fields:
                if f.name == plan.surrogate:
                    row[f.name] = sk
                elif f.name == "valid_from":
                    row[f.name] = datetime(1900, 1, 1, tzinfo=UTC)
                elif f.name == "valid_to":
                    row[f.name] = datetime(9999, 12, 31, tzinfo=UTC)
                elif f.name == "is_current":
                    row[f.name] = True
                elif f.name == "_is_deleted":
                    row[f.name] = False
                elif f.name == "_run_id":
                    row[f.name] = run_id
                elif isinstance(f.dataType, T.StringType):
                    row[f.name] = text
                elif isinstance(f.dataType, T.LongType | T.IntegerType):
                    row[f.name] = 0
                else:
                    row[f.name] = None
            rows.append(row)
        df = self.spark.createDataFrame(rows, schema=T.StructType([T.StructField(f.name, f.dataType, True)
                                                                   for f in schema.fields]))
        # set the two bounds as the session (UTC) reads them, the same literals the members use
        return df.withColumn("valid_from", F.to_timestamp(F.lit(BEGINNING))).withColumn(
            "valid_to", F.to_timestamp(F.lit(END_OF_TIME)))

    # ---- corrections over silver (ADR-017)
    def overlay(self, frame: DataFrame, corrections: list[Correction], keys: tuple[str, ...]) -> DataFrame:
        types = {f.name: f.dataType.simpleString() for f in frame.schema.fields}  # not .dtypes: not on every engine
        for fld in sorted({c.field for c in corrections}):
            mine = [c for c in corrections if c.field == fld]
            fields = {f.name: f.dataType for f in frame.schema.fields}  # StructType[name] is not on every engine
            schema = T.StructType([*[T.StructField(k, fields[k]) for k in keys],
                                   T.StructField("_fix", T.StringType())])
            fixes = self.spark.createDataFrame([(*[c.key[k] for k in keys], c.value) for c in mine], schema)
            # shiftleft: allow SL-COST-CODE-14 -- joined on the business key columns `keys` (a USING join)
            frame = (frame.join(fixes.hint("broadcast"), list(keys), "left")
                     .withColumn(fld, F.coalesce(F.col("_fix").cast(types[fld]), F.col(fld))).drop("_fix"))
        return frame

    # ---- the build (D18)
    def query_text(self, plan: GoldPlan, params: dict[str, str]) -> str:
        if plan.build_kind != "sql":
            return f"-- dataframe function {plan.build_ref}\n-- params: {dict(sorted(params.items()))}"
        text = (self.models_root / plan.build_ref).read_text()
        used = {k: v for k, v in sorted(params.items()) if f":{k}" in text}
        return f"-- {plan.build_ref}; parameters bound by the engine: {used}\n{text}"

    def build(self, plan: GoldPlan, inputs: dict[str, DataFrame], params: dict[str, str]) -> DataFrame:
        if plan.build_kind == "sql":
            text = (self.models_root / plan.build_ref).read_text()
            for name, df in inputs.items():
                df.createOrReplaceTempView(_safe(name))
            return self.sql(text, {k: v for k, v in params.items() if f":{k}" in text})
        fn = self._function(plan.build_ref)
        return fn(dict(inputs), dict(params))

    def sql(self, text: str, params: dict[str, str]) -> DataFrame:
        """One SQL query with named parameters bound by the engine (never pasted into the text)."""
        return self.spark.sql(text, args=params)

    def _function(self, ref: str) -> Build:
        module, _, name = ref.partition(":")
        path = self.models_root.joinpath(*module.split(".")).with_suffix(".py")
        spec = importlib.util.spec_from_file_location(f"gold_model_{module.replace('.', '_')}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"build {ref}: no module at {path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        fn: Build = getattr(mod, name)
        return fn

    # ---- keys as of the event (ADR-004, ADR-005)
    def add_keys(self, plan: GoldPlan, facts: DataFrame, dimensions: dict[str, DataFrame]) -> DataFrame:
        out = facts
        for i, d in enumerate(plan.dimensions):
            dim = dimensions[d.name].where(F.col(d.key) >= 0)
            names = {c: f"_d{i}_{c}" for c in (d.key, *d.business_key, "valid_from", "valid_to")}
            dim = dim.select(*[F.col(c).alias(a) for c, a in names.items()])
            t = F.col(d.as_of).cast("timestamp")
            on = [F.col(b) == F.col(names[b]) for b in d.business_key]
            on += [F.col(names["valid_from"]) <= t, t < F.col(names["valid_to"])]
            no_key = reduce(lambda a, b: a & b, [F.col(b).isNull() for b in d.business_key])
            # shiftleft: allow SL-COST-CODE-14 -- `on` is the business key equality plus the validity range (ADR-005)
            out = (out.join(dim, reduce(lambda a, b: a & b, on), "left")
                   .withColumn(d.key, F.when(no_key, F.lit(NOT_APPLICABLE))
                               .otherwise(F.coalesce(F.col(names[d.key]), F.lit(UNKNOWN))).cast("long"))
                   .drop(*names.values()))
        return out

    def finish(self, plan: GoldPlan, facts: DataFrame, complete: dict[str, bool], run_id: str) -> DataFrame:
        flags = F.create_map(*[x for w, ok in sorted(complete.items()) for x in (F.lit(w), F.lit(ok))]) \
            if complete else F.create_map().cast("map<string,boolean>")
        done = F.coalesce(flags[self.window_text(plan)], F.lit(False))
        if "_input_complete" in facts.columns:  # a row still waiting for its own input is flagged on its own
            done = done & F.coalesce(F.col("_input_complete"), F.lit(True))
        rows = facts.withColumn("_is_complete", done).withColumn("_run_id", F.lit(run_id))
        return rows.select(*[F.col(c).cast(t).alias(c) for c, t in plan.columns])

    def only_windows(self, plan: GoldPlan, facts: DataFrame, first: str, also: set[str]) -> DataFrame:
        w = self.window_text(plan)
        keep = w >= F.lit(first)
        if also:
            keep = keep | w.isin(sorted(also))
        return facts.where(keep)

    def window_numbers(self, plan: GoldPlan, facts: DataFrame | None, first: str,
                       also: set[str]) -> dict[str, tuple[Numbers, str]]:
        if facts is None:
            return {}
        cols = tuple(c for _, c in plan.reconcile.measures) if plan.reconcile else plan.measure_columns
        content = [c for c in facts.columns if c != "_run_id"]
        digest = F.sum(self.row_hash(content).cast("decimal(38,0)")).cast("string")
        done = F.min(F.col("_is_complete").cast("int")) if "_is_complete" in facts.columns else F.lit(0)
        rows = (self.only_windows(plan, facts, first, also).groupBy(self.window_text(plan).alias("_w"))
                .agg(F.count(F.lit(1)).alias("_rows"), *[F.sum(c).cast("double").alias(c) for c in cols],
                     done.alias("_done"), digest.alias("_digest"))
                .collect())
        return {str(r["_w"]): (Numbers(int(r["_rows"]), tuple((c, round(float(r[c] or 0), 6)) for c in cols),
                                       bool(r["_done"])), str(r["_digest"])) for r in rows}

    @staticmethod
    def row_hash(columns: list[str]) -> Column:
        """A 64-bit hash of a row's values (window digests compare one engine's runs with each other only)."""
        return F.xxhash64(*[F.col(c) for c in columns])

    # ---- checks and small numbers
    def hold(self, frame: DataFrame) -> DataFrame:
        return frame.localCheckpoint(eager=True)

    def release(self, *frames: DataFrame) -> None:
        for f in frames:
            _free(f)  # serverless refuses unpersist (13 Nov)

    def count(self, frame: DataFrame) -> int:
        return int(frame.count())

    def duplicates(self, frame: DataFrame, columns: tuple[str, ...]) -> int:
        row = frame.groupBy(*columns).count().where(F.col("count") > 1) \
            .agg(F.sum(F.col("count") - 1).alias("n")).first()
        return int(row["n"] or 0) if row else 0

    def unknown_rows(self, plan: GoldPlan, facts: DataFrame) -> dict[str, int]:
        if not plan.dimensions:
            return {}
        row = facts.agg(*[F.sum(F.when(F.col(d.key) == UNKNOWN, 1).otherwise(0)).alias(d.key)
                          for d in plan.dimensions]).first()
        return {d.key: int(row[d.key] or 0) for d in plan.dimensions} if row else {}

    def key_tally(self, plan: GoldPlan, facts: DataFrame) -> dict[str, dict[str, int]]:
        if not plan.dimensions:
            return {}
        cols = []
        for d in plan.dimensions:
            k = F.col(d.key)
            cols += [F.sum(F.when(k >= 0, 1).otherwise(0)).alias(f"{d.key}__matched"),
                     F.sum(F.when(k == UNKNOWN, 1).otherwise(0)).alias(f"{d.key}__unknown"),
                     F.sum(F.when(k == NOT_APPLICABLE, 1).otherwise(0)).alias(f"{d.key}__not_applicable")]
        row = facts.agg(*cols).first()
        return {d.key: {w: int((row[f"{d.key}__{w}"] if row else 0) or 0) for w in ("matched", "unknown",
                                                                                   "not_applicable")}
                for d in plan.dimensions}

    def windows_with_unknown(self, plan: GoldPlan, facts: DataFrame | None) -> set[str]:
        if facts is None or not plan.dimensions:
            return set()
        hit = reduce(lambda a, b: a | b, [F.col(d.key) == UNKNOWN for d in plan.dimensions])
        return {str(r[0]) for r in facts.where(hit).select(self.window_text(plan)).distinct().collect()}

    def rows_incomplete(self, plan: GoldPlan, facts: DataFrame) -> set[str]:
        if "_input_complete" not in facts.columns:
            return set()
        return {str(r[0]) for r in facts.where(~F.col("_input_complete")).select(self.window_text(plan))
                .distinct().collect()}

    def first_seen(self, dimension: DataFrame, column: str) -> dict[str, datetime]:
        rows = (dimension.where(F.col("_source_valid_from").isNotNull()).groupBy(column)
                .agg(as_text(F.min("_source_valid_from"), (*TS_PIECES[:3], "+00:00")).alias("t"))
                .collect())
        return {str(r[column]): datetime.fromisoformat(str(r["t"])) for r in rows}

    def totals(self, frame: DataFrame, columns: tuple[str, ...]) -> dict[str, float]:
        if not columns:
            return {}
        row = frame.agg(*[F.sum(c).cast("double").alias(c) for c in columns]).first()
        return {c: float((row[c] if row else 0) or 0) for c in columns}


class DeltaGoldTables:
    """The read and write edges of gold on Delta folders (silver read as W1c wrote it)."""

    def __init__(self, spark: SparkSession, silver_root: str | Path, gold_root: str | Path):
        self.spark, self.silver_root, self.gold_root = spark, Path(silver_root), Path(gold_root)

    def path(self, plan: GoldPlan) -> Path:
        return self.gold_root / f"{_safe(plan.schema)}__{_safe(plan.name)}"

    def _read(self, path: Path) -> DataFrame | None:
        return self.spark.read.format("delta").load(str(path)) if (path / "_delta_log").exists() else None

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver(self, dataset: str, columns: list[tuple[str, str]]) -> DataFrame:
        source, name = dataset.split(".", 1)
        df = self._read(self.silver_root / f"{_safe(source)}__{_safe(name)}")
        if df is not None:
            return df
        own = [("_valid_from", "timestamp"), ("_valid_to", "timestamp"), ("_is_current", "boolean"),
               ("_is_deleted", "boolean"), ("_seq_from", "bigint"), ("_row_key", "string")]
        schema = ", ".join(f"`{c}` {t}" for c, t in [*columns, *own])
        return self.spark.createDataFrame([], schema)

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def gold(self, plan: GoldPlan) -> DataFrame | None:
        return self._read(self.path(plan))

    def _silver_path(self, dataset: str) -> Path:
        source, name = dataset.split(".", 1)
        return self.silver_root / f"{_safe(source)}__{_safe(name)}"

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver_version(self, dataset: str) -> int | None:
        path = self._silver_path(dataset)
        if not (path / "_delta_log").exists():
            return None
        from delta.tables import DeltaTable  # noqa: PLC0415 -- delta-spark only where this adapter runs

        row = DeltaTable.forPath(self.spark, str(path)).history(1).select("version").first()
        return int(row["version"]) if row else None

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver_at(self, dataset: str, version: int, columns: list[tuple[str, str]]) -> DataFrame:
        return self.spark.read.format("delta").option("versionAsOf", int(version)).load(str(self._silver_path(dataset)))

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def write_dimension(self, plan: GoldPlan, rows: DataFrame) -> int:
        rows = rows.localCheckpoint(eager=True)
        rows.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(self.path(plan)))
        n = int(rows.count())
        _free(rows)  # serverless refuses unpersist (13 Nov)
        return n

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def replace_windows(self, plan: GoldPlan, rows: DataFrame, windows: list[str]) -> int:
        assert plan.window is not None
        bad = [w for w in windows if not _WINDOW.match(w)]
        if bad:
            raise ValueError(f"not window names: {bad[:3]}")
        path = self.path(plan)
        if not (path / "_delta_log").exists():
            rows.write.format("delta").mode("overwrite").save(str(path))
            return int(self.spark.read.format("delta").load(str(path)).count())
        col = _safe(plan.window.column)
        if plan.window.unit == "hour":
            values = ", ".join(f"TIMESTAMP'{w[:10]} {w[11:19]}'" for w in windows)
        else:
            values = ", ".join(f"DATE'{w}'" for w in windows)
        (rows.write.format("delta").mode("overwrite").option("replaceWhere", f"`{col}` IN ({values})")
         .save(str(path)))
        return int(rows.count())


class SparkGoldReader:
    """GoldReader on the gold folders: each table visible as a view by its own name."""

    def __init__(self, spark: SparkSession, gold_root: str | Path):
        self.spark, self.gold_root = spark, Path(gold_root)
        self.views: set[str] = set()

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def rows(self, sql: str, params: dict[str, str] | None = None,
             time_zone: str | None = None) -> list[dict[str, Any]]:
        seen = set()
        for folder in sorted(self.gold_root.glob("*__*")):
            if (folder / "_delta_log").exists():
                name = _safe(folder.name.split("__", 1)[1])
                self.spark.read.format("delta").load(str(folder)).createOrReplaceTempView(name)
                seen.add(name)
        for name in self.views - seen:  # a table this reader showed before and that is gone is not shown again
            self.spark.catalog.dropTempView(name)
        self.views = seen
        before = self.spark.conf.get("spark.sql.session.timeZone")
        if time_zone:  # metric views are read in the business time zone (a day is a business day)
            self.spark.conf.set("spark.sql.session.timeZone", time_zone)
        try:
            return [r.asDict() for r in self.spark.sql(sql, args=dict(params or {})).collect()]
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", before or "UTC")


class StoreGoldTables(DeltaGoldTables):
    """The read and write edges of gold through the table port (ADR-027): silver read from the silver layer, gold
    tables <schema>__<name> in the gold layer, on any format the store speaks."""

    def __init__(self, spark: SparkSession, store: Any):
        self.spark, self.store = spark, store

    @staticmethod
    def table(plan: GoldPlan) -> str:
        return f"{_safe(plan.schema)}__{_safe(plan.name)}"

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def silver(self, dataset: str, columns: list[tuple[str, str]]) -> DataFrame:
        source, name = dataset.split(".", 1)
        df = self.store.read("silver", f"{_safe(source)}__{_safe(name)}")
        if df is not None:
            return df
        own = [("_valid_from", "timestamp"), ("_valid_to", "timestamp"), ("_is_current", "boolean"),
               ("_is_deleted", "boolean"), ("_seq_from", "bigint"), ("_row_key", "string")]
        schema = ", ".join(f"`{c}` {t}" for c, t in [*columns, *own])
        return self.spark.createDataFrame([], schema)

    def silver_version(self, dataset: str) -> int | None:
        source, name = dataset.split(".", 1)
        return self.store.version("silver", f"{_safe(source)}__{_safe(name)}")

    def silver_at(self, dataset: str, version: int, columns: list[tuple[str, str]]) -> DataFrame:
        source, name = dataset.split(".", 1)
        df = self.store.read_at("silver", f"{_safe(source)}__{_safe(name)}", version)
        if df is None:
            raise ValueError(f"{dataset}: no silver table to read at version {version}")
        return df

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def gold(self, plan: GoldPlan) -> DataFrame | None:
        return self.store.read("gold", self.table(plan))

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def write_dimension(self, plan: GoldPlan, rows: DataFrame) -> int:
        rows = rows.localCheckpoint(eager=True)
        self.store.overwrite("gold", self.table(plan), rows)
        n = int(rows.count())
        _free(rows)  # serverless refuses unpersist (13 Nov)
        return n

    # shiftleft: allow SL-PURE-02 -- the write edge of gold (ADR-013): IO belongs here
    def replace_windows(self, plan: GoldPlan, rows: DataFrame, windows: list[str]) -> int:
        assert plan.window is not None
        bad = [w for w in windows if not _WINDOW.match(w)]
        if bad:
            raise ValueError(f"not window names: {bad[:3]}")
        table = self.table(plan)
        if not self.store.exists("gold", table):
            self.store.overwrite("gold", table, rows)
            return int(self.store.read("gold", table).count())
        col = _safe(plan.window.column)
        if plan.window.unit == "hour":
            values = ", ".join(f"TIMESTAMP'{w[:10]} {w[11:19]}'" for w in windows)
        else:
            values = ", ".join(f"DATE'{w}'" for w in windows)
        self.store.replace_where("gold", table, rows, f"`{col}` IN ({values})")
        return int(rows.count())


class StoreGoldReader(SparkGoldReader):
    """GoldReader through the table port: each gold table visible as a view by its own name."""

    def __init__(self, spark: SparkSession, store: Any):
        self.spark, self.store = spark, store
        self.views: set[str] = set()

    # shiftleft: allow SL-PURE-06 -- the read edge of gold (ADR-013): IO belongs here
    def rows(self, sql: str, params: dict[str, str] | None = None,
             time_zone: str | None = None) -> list[dict[str, Any]]:
        seen = set()
        for table in self.store.tables("gold"):
            if "__" not in table:
                continue
            name = _safe(table.split("__", 1)[1])
            df = self.store.read("gold", table)
            if df is not None:
                df.createOrReplaceTempView(name)
                seen.add(name)
        for name in self.views - seen:
            self.spark.catalog.dropTempView(name)
        self.views = seen
        before = self.spark.conf.get("spark.sql.session.timeZone")
        if time_zone:
            self.spark.conf.set("spark.sql.session.timeZone", time_zone)
        try:
            return [r.asDict() for r in self.spark.sql(sql, args=dict(params or {})).collect()]
        finally:
            self.spark.conf.set("spark.sql.session.timeZone", before or "UTC")
