"""The thirteen standard bronze-to-silver stages (ADR-007) as pure PySpark functions: DataFrames in, DataFrames out,
no reads or writes (ADR-013). This is the in-house part of the stage_runner port the engine binding names
(dbs/engine/databricks.yaml: gap_filled_by dataplatform.stages); the kernel owns the order and the settings
(dataplatform.silver.plan), the engine adapter owns reading and writing (dataplatform.adapters.spark_silver).

    run_stages(plan, bronze, current, run_id, as_of) -> (rows to write, quarantine, frames to count)
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame

from dataplatform.silver.plan import STAGES, SilverPlan, check_stage_order
from dataplatform.stages.clean import (
    contract_check,
    hygiene,
    parse_type,
    personal_data,
    project,
    quality,
    row_filter,
    standardise,
    time,
)
from dataplatform.stages.common import Out, Stage, StageCtx
from dataplatform.stages.history import history
from dataplatform.stages.rows import duplicates, keys, provenance

FUNCTIONS: dict[str, Stage] = {
    "contract_check": contract_check,
    "project": project,
    "hygiene": hygiene,
    "parse_type": parse_type,
    "time": time,
    "standardise": standardise,
    "personal_data": personal_data,
    "row_filter": row_filter,
    "quality": quality,
    "duplicates": duplicates,
    "keys": keys,
    "history": history,
    "provenance": provenance,
}
check_stage_order(list(FUNCTIONS))  # the engine supplies exactly the standard stages, in order (CFG-01)

__all__ = ["FUNCTIONS", "STAGES", "Out", "StageCtx", "run_stages"]


def run_stages(
    plan: SilverPlan,
    bronze: DataFrame,
    current: DataFrame | None,
    run_id: str,
    as_of: str,
    loaded_at: datetime | None = None,
    hold: Callable[[DataFrame], DataFrame] | None = None,
) -> tuple[DataFrame, DataFrame | None, dict[str, DataFrame]]:
    """Every stage in order; a stage the plan switches off is skipped. Returns the rows to write, the quarantined
    rows of every stage, and the frames to count, named <stage>.<what>. `hold`, when the engine adapter passes one,
    fixes each stage's output before the next stage reads it (an engine that compiles a frame's whole lineage into
    one query, as SQLFrame does, otherwise recompiles every earlier stage at every step; docs/DUCKDB-SPIKE.md)."""
    ctx = StageCtx(plan, current, run_id, as_of, loaded_at)
    df = bronze
    held: list[DataFrame] = []
    tally: dict[str, DataFrame] = {}
    for name in STAGES:
        if not plan.runs(name):
            continue
        out = FUNCTIONS[name](df, ctx)
        df = out.df if hold is None else hold(out.df)
        if out.quarantine is not None:
            held.append(out.quarantine)
        tally.update({f"{name}.{k}": v for k, v in out.tally.items()})
    quarantine = reduce(lambda x, y: x.unionByName(y), held) if held else None
    return df, quarantine, tally
