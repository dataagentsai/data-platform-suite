"""Stages 1-9: contract check, projection, hygiene, parse and type, time, standardise, personal data, row filter,
quality. Each is a pure function over a DataFrame (ADR-013); the settings come from the plan (ADR-007)."""

from __future__ import annotations

import re
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from dataplatform.silver.plan import ColumnPlan
from dataplatform.silver.ports import StageError
from dataplatform.stages.common import Out, StageCtx, add_flags, column_types, no_flags, quarantine_rows, reasons

SPARK_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "integer": LongType(),
    "number": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}
_CONTROL = "[\\x00-\\x08\\x0b\\x0c\\x0e-\\x1f\\x7f]"
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def _source_name(c: ColumnPlan) -> str:
    return c.source or c.name


# ---------------------------------------------------------------------------------------------------- 1 contract check
def contract_check(df: DataFrame, ctx: StageCtx) -> Out:
    """Every contract column the plan keeps is in the batch (DPC-0008). A missing one stops the batch, naming it."""
    have = set(df.columns)
    missing = sorted(_source_name(c) for c in ctx.plan.kept if not c.path and _source_name(c) not in have)
    if ctx.plan.kept and any(c.path for c in ctx.plan.kept) and "_extra" not in have:
        missing.append("_extra")
    if missing:
        raise StageError(f"{ctx.plan.table}: contract column(s) {', '.join(missing)} missing from bronze (DPC-0008)")
    return Out(df)


# ---------------------------------------------------------------------------------------------------- 2 projection
def project(df: DataFrame, ctx: StageCtx) -> Out:
    """All columns by default (ADR-008): contract columns under their contract names (renamed or extracted from the
    held columns), every other source column too unless the projection is 'declared'; excluded columns go."""
    plan = ctx.plan
    for c in plan.columns:
        if not _NAME.match(c.name) or not _NAME.match(_source_name(c)):
            raise StageError(f"{plan.table}: {c.name!r} is not a safe column name")
    sources = {_source_name(c) for c in plan.columns} | {c.name for c in plan.columns}
    picked = [
        (F.get_json_object(F.col("_extra"), c.path) if c.path else F.col(_source_name(c))).alias(c.name)
        for c in plan.kept
    ]
    others = [F.col(x) for x in df.columns if x not in sources and not x.startswith("_")]
    meta = [F.col(x) for x in df.columns if x.startswith("_") and x not in sources and x != "_dq_flags"]
    flags = F.col("_dq_flags") if "_dq_flags" in df.columns else no_flags()
    return Out(df.select(*picked, *(others if plan.projection == "all" else []), *meta, flags.alias("_dq_flags")))


# ---------------------------------------------------------------------------------------------------- 3 hygiene
def clean_text(col: Column) -> Column:
    """LakeIngestionFramework's default-on hygiene processors, as one column expression: byte-order mark, control
    characters, line endings, leading and trailing whitespace, runs of spaces, and empty text to null."""
    x = F.regexp_replace(col, "^﻿", "")
    x = F.regexp_replace(x, _CONTROL, "")
    x = F.regexp_replace(x, "\r\n?", "\n")
    x = F.regexp_replace(x, "^\\s+|\\s+$", "")
    x = F.regexp_replace(x, "[ \\t]{2,}", " ")
    return F.when(x == "", F.lit(None)).otherwise(x)


def hygiene(df: DataFrame, ctx: StageCtx) -> Out:
    types = column_types(df)
    out = df
    for c in ctx.plan.kept:
        if c.hygiene and types.get(c.name) == "string":
            out = out.withColumn(c.name, clean_text(F.col(c.name)))
    return Out(out)


# ---------------------------------------------------------------------------------------------------- 4 parse and type
def _try_cast(name: str, sql_type: str) -> Column:
    """CAST that gives null when the value does not convert, on every engine (Spark with ANSI on or off, DuckDB):
    try_cast. The column name is checked and quoted; the type is one of SQL_TYPES, never data."""
    if not _NAME.match(name):
        raise StageError(f"{name!r} is not a safe column name")
    return F.expr(f"try_cast(`{name}` AS {sql_type})")


SQL_TYPES = {"string": "string", "integer": "bigint", "number": "double", "boolean": "boolean",
             "timestamp": "timestamp", "date": "date"}


def cast_to(name: str, logical: str, fmt: str = "") -> Column:
    """The contract type, or null when the value does not convert. A fraction never becomes a whole number.
    try_cast and try_to_timestamp: a value that does not convert is null, never an error, whatever the engine."""
    col = F.col(name)
    if logical in ("date", "timestamp") and fmt:
        parsed = F.try_to_timestamp(col.cast("string"), F.lit(fmt))
        return parsed.cast("date") if logical == "date" else parsed
    if logical == "integer":
        d = _try_cast(name, "double")
        return F.when(d == F.floor(d), d.cast("long"))
    if logical == "boolean":
        t = F.lower(col.cast("string"))
        return F.when(t.isin("true", "1"), F.lit(True)).when(t.isin("false", "0"), F.lit(False))
    if logical == "string":
        return col.cast("string")
    return _try_cast(name, SQL_TYPES[logical])


def parse_type(df: DataFrame, ctx: StageCtx) -> Out:
    """Cast each column to its contract type. A value that does not convert is quarantined, or emptied, flagged and
    counted when its column says on_cast_failure: empty (DQ-02): never silently emptied."""
    types = {f.name: f.dataType for f in df.schema.fields}
    work = df
    checks: list[tuple[Column, str]] = []
    emptied: list[tuple[Column, str]] = []
    for c in ctx.plan.kept:
        target = SPARK_TYPES[c.logical_type]
        if types.get(c.name) == target and not c.fmt:
            continue
        work = work.withColumn(f"_cast_{c.name}", cast_to(c.name, c.logical_type, c.fmt))
        failed = F.col(c.name).isNotNull() & F.col(f"_cast_{c.name}").isNull()
        if c.on_cast_failure == "empty":
            emptied.append((failed, f"emptied:{c.name}"))
        else:
            checks.append((failed, f"{c.name} is not a {c.logical_type}"))
    good, bad = quarantine_rows(work, reasons(checks), "parse_type", df.columns)
    good = add_flags(good, emptied)
    tally = {"emptied": good.where(F.size(F.filter("_dq_flags", lambda f: f.startswith("emptied:"))) > 0)}
    for c in ctx.plan.kept:
        if f"_cast_{c.name}" in work.columns:
            good = good.withColumn(c.name, F.col(f"_cast_{c.name}")).drop(f"_cast_{c.name}")
    return Out(good, bad, tally if emptied else {})


# ---------------------------------------------------------------------------------------------------- 5 time
def time(df: DataFrame, ctx: StageCtx) -> Out:
    """Timestamps stay instants in UTC (the session's zone). A column with local_tz gets <name>_local beside it: the
    wall-clock time in that zone, as text (ISO, no offset)."""
    out = df
    for c in ctx.plan.kept:
        if c.logical_type == "timestamp" and c.local_tz:
            # convert_timezone from UTC (the session zone) and the text in two pieces around a literal T: the same
            # wall time on every engine (from_utc_timestamp and quoted letters in a pattern are not portable,
            # docs/DUCKDB-SPIKE.md)
            wall = F.convert_timezone(F.lit("UTC"), F.lit(c.local_tz), F.col(c.name))
            local = F.concat(F.date_format(wall, "yyyy-MM-dd"), F.lit("T"), F.date_format(wall, "HH:mm:ss"))
            out = out.withColumn(f"{c.name}_local", local)
    return Out(out)


# ---------------------------------------------------------------------------------------------------- 6 standardise
def standardise(df: DataFrame, ctx: StageCtx) -> Out:
    """Declared value maps (a value not in the map is kept as it is) and letter case."""
    out = df
    for c in ctx.plan.kept:
        if c.logical_type != "string":
            continue
        col = F.col(c.name)
        if c.values:
            mapping = F.create_map(*[F.lit(x) for kv in c.values for x in kv])
            col = F.coalesce(mapping[col], col)
        if c.case == "upper":
            col = F.upper(col)
        elif c.case == "lower":
            col = F.lower(col)
        if c.values or c.case:
            out = out.withColumn(c.name, col)
    return Out(out)


# ---------------------------------------------------------------------------------------------------- 7 personal data
def personal_data(df: DataFrame, ctx: StageCtx) -> Out:
    """A restricted column must hold tokens, never the plain value (DPC-0070): a row with a plain value is quarantined.
    The token format comes from the binding's tokenizer."""
    restricted = [c for c in ctx.plan.kept if c.restricted]
    if not restricted:
        return Out(df)
    if not ctx.plan.token_pattern:
        raise StageError(f"{ctx.plan.table}: restricted column(s) and no token format bound to check them")
    checks = [
        (F.col(c.name).isNotNull() & ~F.col(c.name).rlike(ctx.plan.token_pattern), f"{c.name} is not tokenised")
        for c in restricted
    ]
    good, bad = quarantine_rows(df, reasons(checks), "personal_data")
    return Out(good, bad)


# ---------------------------------------------------------------------------------------------------- 8 row filter
def row_filter(df: DataFrame, ctx: StageCtx) -> Out:
    """Declared filters: rows that do not satisfy one are dropped and counted (not quarantined: they are not wrong)."""
    if not ctx.plan.row_filters:
        return Out(df)
    keep = F.lit(True)
    for r in ctx.plan.row_filters:
        keep = keep & F.coalesce(F.expr(r.predicate), F.lit(False))
    marked = df.withColumn("_keep", keep)
    return Out(marked.where("_keep").drop("_keep"), tally={"dropped": marked.where(~F.col("_keep"))})


# ---------------------------------------------------------------------------------------------------- 9 quality
def quality(df: DataFrame, ctx: StageCtx) -> Out:
    """Row rules; severity sets the action (ADR-009): quarantine the row, or flag it in _dq_flags and keep it."""
    unknown = sorted({r.action for r in ctx.plan.rules if r.action not in ("quarantine", "flag", "none")})
    if unknown:  # a rule whose action the stage does not know must never be silently skipped
        raise StageError(f"{ctx.plan.table}: rule action(s) {', '.join(unknown)} are not quarantine, flag or none")
    fails = [(r, ~F.coalesce(F.expr(r.predicate), F.lit(False))) for r in ctx.plan.rules if r.action != "none"]
    held = [(cond, r.name) for r, cond in fails if r.action == "quarantine"]
    flagged = [(cond, r.name) for r, cond in fails if r.action == "flag"]
    critical = [cond for r, cond in fails if r.action == "quarantine" and r.severity == "critical"]
    good, bad = quarantine_rows(df, reasons(held), "quality")
    good = add_flags(good, flagged)
    tally = {"flagged": good.where(F.size("_dq_flags") > 0)} if flagged else {}
    if critical:  # ADR-009: any row that breaks a critical rule blocks the batch (the runner holds it)
        tally["critical"] = df.where(reduce(lambda a, b: a | b, critical))
    return Out(good, bad, tally)
