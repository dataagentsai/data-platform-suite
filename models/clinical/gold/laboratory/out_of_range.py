"""fact_lab_out_of_range build (D18, ADR-003): laboratory results ordered, reported and out of range per site and day of
sample collection. A pure dataframe function: frames in, one frame out, no reads or writes (ADR-013); the harness adds
site_sk as of collection_date, the window flags and the run id, then replaces the changed days.

The results ordered come from the visits (completed, with analytes ordered); the results reported from the laboratory's
files. `_input_complete` says whether every ordered result of the site-day has been reported: a late file keeps the
day flagged incomplete until it arrives (ADR-018).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _current(df: DataFrame) -> DataFrame:
    return df.where(F.col("_is_current") & ~F.col("_is_deleted"))


def build(inputs: dict[str, DataFrame], params: dict[str, str]) -> DataFrame:
    """The build signature every dataframe build shares (D18); collection_date is a date and needs no parameter."""
    del params
    visits = _current(inputs["edc__visit"]).where(
        (F.col("visit_status") == "completed") & (F.col("analytes_ordered") > 0)
    )
    results = (
        _current(inputs["lab__lab_result"])
        .groupBy("visit_id")
        .agg(F.count(F.lit(1)).alias("reported"), F.sum("out_of_range").alias("out_of_range"))
    )
    rows = visits.join(results, "visit_id", "left")
    return (
        rows.groupBy("site_id", F.col("actual_date").alias("collection_date"))
        .agg(
            F.sum("analytes_ordered").cast("long").alias("results_expected"),
            F.sum(F.coalesce(F.col("reported"), F.lit(0))).cast("long").alias("results_reported"),
            F.sum(F.coalesce(F.col("out_of_range"), F.lit(0))).cast("long").alias("results_out_of_range"),
        )
        .withColumn("_input_complete", F.col("results_reported") >= F.col("results_expected"))
    )
