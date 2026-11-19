"""What every stage shares: its inputs, its output, and the quarantine record shape.

A stage is a pure function `(frame, ctx) -> Out`: no reads, no writes, no clock (ADR-013). `ctx.current` is silver as
the read edge handed it over; a stage may join with it but never loads anything itself.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from dataplatform.silver.plan import SilverPlan

SEP = "\u001f"  # between key parts in a hash: never in a business value
QUARANTINE_COLUMNS = ("_stage", "_reason", "_batch_key", "_source_file", "_record")


@dataclass(frozen=True)
class StageCtx:
    plan: SilverPlan
    current: DataFrame | None  # silver as it stands; None before the first write
    run_id: str
    as_of: str  # the business date of the run (ISO)
    loaded_at: datetime | None = None  # when the write edge takes the rows (passed in: no clock inside a stage)


@dataclass
class Out:
    df: DataFrame
    quarantine: DataFrame | None = None  # QUARANTINE_COLUMNS
    tally: dict[str, DataFrame] = field(default_factory=dict)  # name -> rows to count (dropped, emptied, ...)


Stage = Callable[[DataFrame, StageCtx], Out]


def quarantine_rows(
    df: DataFrame, reason: Column, stage: str, record: list[str] | None = None
) -> tuple[DataFrame, DataFrame]:
    """Split a frame on `reason` (null = good row): (good rows, quarantine records keeping every original value of
    the `record` columns, all columns by default)."""
    marked = df.withColumn("_q_reason", reason)
    good = marked.where(F.col("_q_reason").isNull()).drop("_q_reason")
    data = list(record if record is not None else df.columns)
    bad = marked.where(F.col("_q_reason").isNotNull()).select(
        F.lit(stage).alias("_stage"),
        F.col("_q_reason").alias("_reason"),
        (F.col("_batch_key") if "_batch_key" in df.columns else F.lit(None).cast("string")).alias("_batch_key"),
        (F.col("_source_file") if "_source_file" in df.columns else F.lit(None).cast("string")).alias("_source_file"),
        F.to_json(F.struct(*[F.col(c) for c in data])).alias("_record"),
    )
    return good, bad


def reasons(parts: list[tuple[Column, str]]) -> Column:
    """One text naming every failed check, or null when none failed."""
    if not parts:
        return F.lit(None).cast("string")
    texts = [F.when(cond, F.lit(text)) for cond, text in parts]
    joined = F.concat_ws("; ", *texts)
    return F.when(joined == "", F.lit(None)).otherwise(joined)


def null_mark() -> Column:
    """The text standing for a null key part in a hash: one NUL character. Built by chr(0) rather than a literal so
    every engine reads the same character (a NUL inside a SQL string literal does not survive on all of them;
    docs/DUCKDB-SPIKE.md). The hash is the same SHA-256 of the same bytes on Spark and DuckDB (tested)."""
    return F.expr("chr(0)")


def text_parts(columns: list[str] | tuple[str, ...]) -> Column:
    """The columns as one text, SEP between parts, a null part as null_mark()."""
    return F.concat_ws(SEP, *[F.coalesce(F.col(c).cast("string"), null_mark()) for c in columns])


def key_hash(columns: tuple[str, ...]) -> Column:
    return F.sha2(text_parts(columns), 256)


def column_types(df: DataFrame) -> dict[str, str]:
    """Each column's type in Spark's simple form (string, bigint, timestamp, ...), read from the schema (portable:
    `DataFrame.dtypes` is not on every engine)."""
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def no_flags() -> Column:
    """An empty list of flag names. Written as SQL text because an empty F.array() does not compile on every engine
    the stages run on (docs/DUCKDB-SPIKE.md); Spark reads it the same way."""
    return F.expr("CAST(array() AS array<string>)")


def add_flags(df: DataFrame, flags: list[tuple[Column, str]]) -> DataFrame:
    """Append flag names to _dq_flags where their condition holds."""
    if not flags:
        return df
    new = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for cond, name in flags]))
    return df.withColumn("_dq_flags", F.concat(F.coalesce(F.col("_dq_flags"), no_flags()), new))


ARRIVAL = ("_feed_pos", "_seq", "_arrived_at", "_committed_at", "_source_file", "_batch_key")


def arrival_order(df: DataFrame) -> list[Column]:
    """How rows arrived, oldest first: feed position, stream sequence, arrival time, file name, batch."""
    return [F.col(c).asc_nulls_last() for c in ARRIVAL if c in df.columns] or [F.lit(0)]


def arrival_desc(df: DataFrame) -> list[Column]:
    """Newest arrival first: the latest delivery of the same change wins a tie."""
    return [F.col(c).desc_nulls_last() for c in ARRIVAL if c in df.columns] or [F.lit(0)]
