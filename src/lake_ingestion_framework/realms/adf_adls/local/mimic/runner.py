"""ADFMimicRunner — top-level orchestrator.

Loads a pipeline JSON from the registry, walks its activity DAG honouring
``dependsOn`` edges, and dispatches each activity to its executor. Per
``docs/adf_mimic_design.md`` §3.

Lifecycle:

    runner = ADFMimicRunner(registry)
    result = runner.run_pipeline(
        pipeline_name="PL_INGEST_POSTGRES",
        parameters={"p_server": "localhost", ...},
        ctx=ExecutionContext(...),
    )
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from lake_ingestion_framework.core.exceptions import LakeIngestError
from lake_ingestion_framework.realms.adf_adls.local.mimic.activities import (
    ActivityResult,
    ExecutionContext,
    UnsupportedActivityError,
    build_activity_executors,
)
from lake_ingestion_framework.realms.adf_adls.local.mimic.expressions import evaluate
from lake_ingestion_framework.realms.adf_adls.local.mimic.run_state import RunState


@dataclass(frozen=True)
class PipelineResult:
    """Outcome of one pipeline run."""

    pipeline_name: str
    run_id: str
    status: str  # "Succeeded" | "Failed"
    activity_results: list[ActivityResult]
    error: str | None = None


class ADFMimicRunner:
    """Walks ADF pipelines + dispatches their activities to executors."""

    def __init__(self) -> None:
        self._executors = build_activity_executors()

    # ── Public API

    def run_pipeline(
        self,
        *,
        pipeline_name: str,
        parameters: dict[str, Any],
        ctx: ExecutionContext,
        parent_run_id: str | None = None,
    ) -> PipelineResult:
        """Run one pipeline by name. Recursive entry for ExecutePipeline."""
        ctx_with_self = ctx if ctx.runner is self else _replace_runner(ctx, self)
        pipeline_def = ctx_with_self.registry.pipeline(pipeline_name)
        properties = pipeline_def.get("properties", {})

        run_id = str(uuid.uuid4())
        run_state = RunState(
            run_id=run_id,
            pipeline_name=pipeline_name,
            parameters=_apply_parameter_defaults(properties, parameters),
            variables=_initialise_variables(properties),
            parent_run_id=parent_run_id,
        )

        activities = properties.get("activities", [])
        results = self.dispatch_activities(activities, run_state, ctx_with_self)

        any_failure = any(r.status == "Failed" for r in results)
        return PipelineResult(
            pipeline_name=pipeline_name,
            run_id=run_id,
            status="Failed" if any_failure else "Succeeded",
            activity_results=results,
            error=next((r.error for r in results if r.status == "Failed" and r.error), None),
        )

    def dispatch_activities(
        self,
        activities: list[dict[str, Any]],
        run_state: RunState,
        ctx: ExecutionContext,
    ) -> list[ActivityResult]:
        """Walk the activity DAG honouring ``dependsOn`` edges sequentially.

        Activities are run in dependency order. An activity whose
        dependencies failed is skipped (cloud ADF default: dependent
        activities are blocked on failure).
        """
        ordered = _topological_order(activities)
        completed: dict[str, ActivityResult] = {}
        results: list[ActivityResult] = []

        for activity in ordered:
            name = activity["name"]
            if not _dependencies_satisfied(activity, completed):
                skipped = ActivityResult(status="Skipped", output={}, error="upstream failed")
                completed[name] = skipped
                results.append(skipped)
                continue

            activity_type = str(activity.get("type", ""))
            executor = self._executors.get(activity_type) if activity_type else None
            if executor is None:
                err = (
                    f"UnsupportedActivityInLocalMode: {activity_type!r} is not supported by "
                    "ADF Mimic v1. Either extend the activity factory or check why the "
                    "generator emits this activity."
                )
                result = ActivityResult(status="Failed", output={}, error=err)
                completed[name] = result
                results.append(result)
                run_state.record_activity(name, {})
                continue

            try:
                result = executor.execute(activity, run_state, ctx)
            except UnsupportedActivityError as exc:
                result = ActivityResult(status="Failed", output={}, error=str(exc))
            except LakeIngestError as exc:
                result = ActivityResult(status="Failed", output={}, error=str(exc))
            except Exception as exc:  # surface unexpected failures with type
                result = ActivityResult(
                    status="Failed",
                    output={},
                    error=f"{type(exc).__name__}: {exc}",
                )

            completed[name] = result
            results.append(result)
            run_state.record_activity(name, result.output)

        return results


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _replace_runner(ctx: ExecutionContext, runner: ADFMimicRunner) -> ExecutionContext:
    return ExecutionContext(
        registry=ctx.registry,
        metadata_db_path=ctx.metadata_db_path,
        lakes_root=ctx.lakes_root,
        runner=runner,
    )


def _topological_order(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort activities so each appears after its `dependsOn` predecessors."""
    by_name = {a["name"]: a for a in activities}
    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _visit(activity: dict[str, Any]) -> None:
        name = activity["name"]
        if name in seen:
            return
        for dep in activity.get("dependsOn", []) or []:
            dep_name = dep.get("activity")
            if dep_name and dep_name in by_name:
                _visit(by_name[dep_name])
        seen.add(name)
        ordered.append(activity)

    for activity in activities:
        _visit(activity)
    return ordered


def _dependencies_satisfied(activity: dict[str, Any], completed: dict[str, ActivityResult]) -> bool:
    """True when every `dependsOn` prerequisite has matching condition met."""
    for dep in activity.get("dependsOn", []) or []:
        dep_name = dep.get("activity")
        if dep_name is None or dep_name not in completed:
            return False
        prior = completed[dep_name]
        conditions = dep.get("dependencyConditions") or ["Succeeded"]
        # Match either ADF-style condition or our own status names
        ok = False
        for condition in conditions:
            if condition == "Succeeded" and prior.status == "Succeeded":
                ok = True
                break
            if condition == "Failed" and prior.status == "Failed":
                ok = True
                break
            if condition == "Completed" and prior.status in {"Succeeded", "Failed"}:
                ok = True
                break
            if condition == "Skipped" and prior.status == "Skipped":
                ok = True
                break
        if not ok:
            return False
    return True


def _apply_parameter_defaults(
    properties: dict[str, Any], overrides: dict[str, Any]
) -> dict[str, Any]:
    """Merge declared parameter defaults with caller-supplied overrides."""
    declared = properties.get("parameters", {}) or {}
    resolved: dict[str, Any] = {}
    for name, spec in declared.items():
        if isinstance(spec, dict) and "defaultValue" in spec:
            resolved[name] = spec["defaultValue"]
    resolved.update(overrides)
    return resolved


def _initialise_variables(properties: dict[str, Any]) -> dict[str, Any]:
    """Initialise every declared variable to its default (or None)."""
    declared = properties.get("variables", {}) or {}
    out: dict[str, Any] = {}
    for name, spec in declared.items():
        if isinstance(spec, dict) and "defaultValue" in spec:
            out[name] = spec["defaultValue"]
        else:
            out[name] = None
    return out


# Re-export evaluate so callers that only import the runner can resolve
# expressions without importing expressions directly. Trivial convenience.
_evaluate = evaluate


__all__ = ["ADFMimicRunner", "PipelineResult"]
