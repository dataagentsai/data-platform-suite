"""The privacy scan (B20 C15, SEC-01): restricted values in clear. Pure PySpark (Spark, DuckDB, Databricks).

A restricted column must hold only tokens (or nothing) from bronze on: the lander tokenises before bronze. The scan
counts, per column, the values that are present and do not look like a token, in one pass over the frame.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clear_counts(df: DataFrame, columns: list[str], pattern: str) -> dict[str, int]:
    """Values present in each column that do not match the token pattern."""
    cols = [c for c in columns if c in df.columns]
    if not cols:
        return {}
    row = df.agg(*[F.sum(F.when(F.col(c).isNotNull() & ~F.col(c).cast("string").rlike(pattern), 1).otherwise(0))
                   .alias(c) for c in cols]).first()
    return {c: int((row[c] if row else 0) or 0) for c in cols}
