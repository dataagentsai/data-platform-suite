"""fact_scrap build (D18, ADR-003): tyres cured and scrapped per press, material and hour. A pure dataframe function:
frames in, one frame out, no reads or writes (ADR-013); the harness adds press_sk and material_sk as of cure_hour,
the window flags and the run id, then replaces the changed hours.

cure_hour is the hour of the curing time (an instant, UTC). `_input_complete` says whether every production row of
the hour has its inspection result yet: the lab files arrive after the hour, so the harness keeps the hour flagged
incomplete until they do (ADR-018). Rework is not scrap (contract): only tyres_scrapped counts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _current(df: DataFrame) -> DataFrame:
    return df.where(F.col("_is_current") & ~F.col("_is_deleted"))


def build(inputs: dict[str, DataFrame], params: dict[str, str]) -> DataFrame:
    """The build signature every dataframe build shares (D18); scrap needs no parameter (cure_hour is an instant)."""
    del params
    production = _current(inputs["mes__production"])
    results = (
        _current(inputs["lims__inspection_result"])
        .groupBy("production_id")
        .agg(F.sum("tyres_scrapped").alias("scrapped"))
    )
    rows = production.join(results, "production_id", "left")
    return (
        rows.groupBy("press_id", "material_code", F.date_trunc("hour", "cured_ts").alias("cure_hour"))
        .agg(
            F.sum("tyres_cured").cast("long").alias("tyres_cured"),
            F.sum(F.coalesce(F.col("scrapped"), F.lit(0))).cast("long").alias("tyres_scrapped"),
            F.min(F.col("scrapped").isNotNull().cast("int")).cast("boolean").alias("_input_complete"),
        )
    )
