"""In-memory run state for one ADF Mimic pipeline run.

Holds parameters, variables, and per-activity outputs. Activity executors
read/write this state via the API rather than passing dicts around. Per
``docs/adf_mimic_design.md`` §9.1.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any


@dataclass
class RunState:
    """Mutable state carried through one pipeline execution.

    A single ``RunState`` accompanies the pipeline as activities run.
    ForEach forks a child state per iteration via ``fork_for_iteration``
    (the original is unchanged; the child carries the iteration item via
    ``iteration``).
    """

    run_id: str
    pipeline_name: str
    parameters: dict[str, Any] = field(default_factory=dict)
    variables: dict[str, Any] = field(default_factory=dict)
    activity_outputs: dict[str, Any] = field(default_factory=dict)
    iteration: tuple[int, Any] | None = None
    parent_run_id: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def fork_for_iteration(self, idx: int, item: Any) -> RunState:
        """Return a copy with ``iteration`` set; same outputs/variables refs.

        ForEach iterations share the parent's variables + outputs (writing
        in a child ForEach activity is visible to outer activities — same
        as cloud ADF). Only the loop bookkeeping is forked.
        """
        return replace(self, iteration=(idx, item))

    def record_activity(self, name: str, output: Any) -> None:
        """Record an activity's output for use in later expressions."""
        self.activity_outputs[name] = output

    def set_variable(self, name: str, value: Any) -> None:
        self.variables[name] = value

    def get_variable(self, name: str) -> Any:
        if name not in self.variables:
            raise KeyError(f"Variable {name!r} not declared on pipeline {self.pipeline_name!r}.")
        return self.variables[name]

    def get_parameter(self, name: str) -> Any:
        if name not in self.parameters:
            raise KeyError(f"Parameter {name!r} missing on pipeline {self.pipeline_name!r}.")
        return self.parameters[name]


__all__ = ["RunState"]
