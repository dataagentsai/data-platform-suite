"""The input gate for a silver we do not build (silver_mode: virtual or conform): what gold checks on a provider's table before it
reads it. Read-only and engine-neutral (a frame in, findings out); it never changes the table.

The contract is the CONSUMER's: it lists only what gold depends on, so a provider change that does not touch it does not stop us.
A finding is an error (the run must not publish) or a warning (publish, and say so). The gate runs before the gold build, and
the version it was shown is pinned in the run log (see docs/VIRTUAL-SILVER-PROOF.md).

Checks, each with a code the run log can carry:
  COLUMN-MISSING / TYPE-CHANGED   the columns and types gold reads are there and unchanged
  KEY-EMPTY / KEY-DUPLICATE       a business key is never empty; a key has one current version
  VERSION-NO-START / -ENDS-BEFORE-START / -OVERLAP / -GAP   the provider's own history is sound (the rules of
                                  silver's source_versioned kind, checked here without rebuilding anything)
  STALE                           the newest version is no older than the contract allows
  VOLUME-DROP                     the table did not shrink by more than the contract allows since the last run
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# the engine's type names that satisfy each logical type of the contract
TYPES: dict[str, tuple[str, ...]] = {
    "string": ("string",),
    "integer": ("tinyint", "smallint", "int", "bigint"),
    "number": ("float", "double", "decimal"),
    "boolean": ("boolean",),
    "timestamp": ("timestamp",),
    "date": ("date",),
}
ERROR, WARN = "error", "warn"


@dataclass(frozen=True)
class InputContract:
    dataset: str
    columns: tuple[tuple[str, str], ...]  # (name, logical type) gold depends on
    keys: tuple[str, ...]
    valid_from: str = ""  # the provider's own history columns; empty = no history to check
    valid_to: str = ""
    gaps_allowed: bool = False
    fresh_column: str = ""  # a timestamp column whose maximum says how new the data is
    max_age: timedelta | None = None
    max_volume_drop: float = 0.2  # a drop larger than this share of the last run's rows is an error
    warn_only: frozenset[str] = field(default_factory=frozenset)  # codes this dataset may publish through


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    detail: str


@dataclass(frozen=True)
class GateResult:
    status: str  # pass | warn | fail
    findings: tuple[Finding, ...]
    rows: int


def _kind(data_type: Any) -> str:
    return str(data_type.simpleString()).split("(")[0].lower()


def check(df: DataFrame, c: InputContract, *, now: datetime, last_rows: int | None = None) -> GateResult:
    """Check `df`, the provider's table as read at the version being pinned."""
    found: list[Finding] = []

    def add(code: str, detail: str, error: bool = True) -> None:
        found.append(Finding(code, WARN if (not error or code in c.warn_only) else ERROR, detail))

    types = {f.name: _kind(f.dataType) for f in df.schema.fields}
    usable = True
    for name, logical in c.columns:
        if name not in types:
            add("COLUMN-MISSING", f"{c.dataset}: column {name} is not in the table")
            usable = False
        elif types[name] not in TYPES[logical]:
            add("TYPE-CHANGED", f"{c.dataset}: column {name} is {types[name]}, the contract says {logical}")
    rows = df.count()
    if usable:
        found += _keys(df, c)
        if c.valid_from and c.valid_from in types:
            found += _history(df, c)
        found += _freshness(df, c, now)
    if last_rows and rows < last_rows * (1 - c.max_volume_drop):
        add("VOLUME-DROP", f"{c.dataset}: {rows} rows, {last_rows} at the last run")
    found = [f if f.code not in c.warn_only else Finding(f.code, WARN, f.detail) for f in found]
    status = "fail" if any(f.severity == ERROR for f in found) else "warn" if found else "pass"
    return GateResult(status, tuple(found), rows)


def _keys(df: DataFrame, c: InputContract) -> list[Finding]:
    out: list[Finding] = []
    empty = df.where(F.lit(False))
    for k in c.keys:
        empty = df.where(F.col(k).isNull()) if k == c.keys[0] else empty.unionByName(df.where(F.col(k).isNull()))
    if c.keys and empty.count():
        out.append(Finding("KEY-EMPTY", ERROR, f"{c.dataset}: {empty.count()} row(s) with an empty business key"))
    if c.keys and c.valid_to:
        current = df.where(F.col(c.valid_to).isNull())
        dup = current.groupBy(*c.keys).count().where(F.col("count") > 1).count()
        if dup:
            out.append(Finding("KEY-DUPLICATE", ERROR, f"{c.dataset}: {dup} key(s) with more than one current version"))
    return out


def _history(df: DataFrame, c: InputContract) -> list[Finding]:
    """The provider's versions per key: a start, an end that is not before it, no overlap, and no gap unless allowed."""
    vf, vt = F.col(c.valid_from).cast("timestamp"), (F.col(c.valid_to).cast("timestamp") if c.valid_to else F.lit(None).cast("timestamp"))
    work = df.select(*[F.col(k) for k in c.keys], vf.alias("_vf"), vt.alias("_vt"))
    per_key = Window.partitionBy(*c.keys).orderBy(F.col("_vf").asc_nulls_first())
    prev_vt, is_first = F.lag("_vt").over(per_key), F.lag("_vf").over(per_key).isNull()
    reason = (
        F.when(F.col("_vf").isNull(), F.lit("VERSION-NO-START"))
        .when(F.col("_vt").isNotNull() & (F.col("_vt") < F.col("_vf")), F.lit("VERSION-ENDS-BEFORE-START"))
        .when(~is_first & (prev_vt.isNull() | (prev_vt > F.col("_vf"))), F.lit("VERSION-OVERLAP"))
    )
    if not c.gaps_allowed:
        reason = reason.when(~is_first & (prev_vt < F.col("_vf")), F.lit("VERSION-GAP"))
    bad = work.withColumn("_r", reason).where(F.col("_r").isNotNull()).groupBy("_r").agg(F.count("*").alias("n"))
    return [Finding(r["_r"], ERROR, f"{c.dataset}: {r['n']} version(s) with {r['_r'].lower().replace('-', ' ')}") for r in bad.collect()]


def _freshness(df: DataFrame, c: InputContract, now: datetime) -> list[Finding]:
    if not (c.fresh_column and c.max_age):
        return []
    newest = df.agg(F.max(F.col(c.fresh_column).cast("timestamp")).alias("m")).collect()[0]["m"]
    if newest is None:
        return [Finding("STALE", ERROR, f"{c.dataset}: no value in {c.fresh_column}")]
    newest = newest.replace(tzinfo=None)
    age = now.replace(tzinfo=None) - newest
    return [Finding("STALE", ERROR, f"{c.dataset}: newest {c.fresh_column} is {age} old, the contract allows {c.max_age}")] if age > c.max_age else []
