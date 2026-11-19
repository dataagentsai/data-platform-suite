"""The declarative silver engine's recipe for one dataset (ADR-035, DAC ENG-02). Pure and product-free.

A declarative flow is streaming, so the two stages that need the whole table cannot run inside it: `duplicates`
(window functions, and a look at silver already loaded) and `history` (ordering and versions against current silver).
The declarative engine runs the other eleven stages inside the flow, unchanged, and hands ordering, latest-per-key,
deletes and SCD Type 1 or 2 to the engine's managed change capture, described here as ChangeCapture. Append datasets
get a streaming de-duplication on their keys instead. Anything the managed change capture cannot express is refused
with the reason (the loader refuses the same through engines.engine_problems).
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from dataplatform.silver.plan import PlanError, SilverPlan

OUTSIDE_THE_FLOW = frozenset({"duplicates", "history"})
OP_COLUMN = "_op"  # the change-feed operation the lander keeps (silver/plan.py TRANSPORT_COLUMNS)


@dataclass(frozen=True)
class ChangeCapture:
    keys: tuple[str, ...]
    sequence_by: str
    scd_type: int  # 1 or 2
    delete_when: str | None  # a SQL condition on the change row, None when deletes are not applied
    track: tuple[str, ...]  # columns that open a new version (SCD2); empty = every column
    exclude: tuple[str, ...]  # change-feed columns not stored in silver


@dataclass(frozen=True)
class Recipe:
    flow_plan: SilverPlan  # the plan the flow runs: every stage except those outside the flow
    capture: ChangeCapture | None  # history datasets
    dedupe_keys: tuple[str, ...]  # append datasets: keys for streaming de-duplication; empty = none
    dedupe_within: tuple[str, int] | None = None  # (event-time column, days): bounded de-duplication state
    notes: tuple[str, ...] = ()  # what the engine cannot bound or keep, for the run log


def recipe(plan: SilverPlan) -> Recipe:
    flow = replace(plan, disabled=plan.disabled | OUTSIDE_THE_FLOW)
    if plan.history in ("scd1", "scd2"):
        if not plan.keys:
            raise PlanError(f"{plan.table}: the declarative engine needs business keys for {plan.history}")
        if not plan.order_by:
            raise PlanError(f"{plan.table}: the declarative engine needs a change order (load.history_order_by) "
                            f"for {plan.history}; arrival order is not a sequence")
        if plan.delete_detect == "key_compare":
            raise PlanError(f"{plan.table}: deletes found by comparing full copies are not supported by the "
                            "declarative engine; use silver_engine 'job'")
        deletes = plan.delete_detect == "change_feed" and plan.delete_apply in ("mark_deleted", "remove")
        capture = ChangeCapture(
            keys=plan.keys,
            sequence_by=plan.order_by,
            scd_type=2 if plan.history == "scd2" else 1,
            delete_when=f"{OP_COLUMN} = 'D'" if deletes else None,
            track=plan.track,
            exclude=(OP_COLUMN,) if plan.ingest_read == "change_feed" else (),  # only a change feed carries it
        )
        return Recipe(flow, capture, ())
    if plan.history == "append":
        keys = (plan.dedup_keys or plan.keys) if plan.dedup_mode != "none" else ()
        if not keys:
            return Recipe(flow, None, ())
        if plan.event_time and plan.lookback_days is not None:
            return Recipe(flow, None, tuple(keys), (plan.event_time, plan.lookback_days))
        return Recipe(flow, None, tuple(keys), None, (f"{plan.table}: no event time and late lookback declared, so "
                                                      "streaming de-duplication keeps every key it has seen",))
    raise PlanError(f"{plan.table}: history {plan.history!r} is not supported by the declarative engine "
                    "(ADR-035); use silver_engine 'job'")
