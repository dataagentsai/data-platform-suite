"""The ports silver talks to (data-platform-catalog dpc/ports.yaml: stage_runner, and table_writer for silver).

A frame is whatever dataframe the engine adapter works on; the kernel never looks inside one (R1, ADR-013): it hands
frames from the read edge to the stage runner and from the stage runner to the write edge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from dataplatform.silver.plan import Measure, SilverPlan

Frame = Any  # the engine's dataframe, opaque to the kernel


@dataclass
class StageResult:
    """What one run of the stages gives back (stage_runner operation run)."""

    rows: Frame  # silver rows to write: upserts (and stale versions to remove) for history kinds, new rows for append
    quarantine: Frame | None  # rows set aside, each with the stage and the reason
    counts: dict[str, int] = field(default_factory=dict)  # rows_in, rows_out, <stage>.quarantined, <stage>.dropped
    txn: int = 0  # the run's file-log mark: an append written twice with the same mark is written once


class StageRunner(Protocol):
    """stage_runner: the thirteen standard stages, in order, on one dataset's new bronze batches."""

    def run(self, plan: SilverPlan, bronze: Frame, current: Frame | None, run_id: str, as_of: str) -> StageResult:
        """Run every stage. `current` is silver as it stands (None before the first write). No reads or writes
        happen here: the frames come in and go out (ADR-013). A batch that breaks its contract raises StageError."""

    def release(self, result: StageResult) -> None:
        """Free what the run held (cached frames) once the result is written."""


class SilverTables(Protocol):
    """The read and write edges of silver: bronze batches in, silver and quarantine out."""

    def bronze(self, plan: SilverPlan, batch_keys: list[str]) -> Frame:
        """The bronze rows of these batches."""

    def current(self, plan: SilverPlan) -> Frame | None:
        """Silver as it stands, or None if the table was never written."""

    def reset(self, plan: SilverPlan) -> None:
        """Remove the dataset's silver table and its quarantine, before a rebuild from bronze (ADR-025)."""

    def write(self, plan: SilverPlan, result: StageResult) -> int:
        """Write the result in one transaction per table: append for append kinds, otherwise a merge on the row key
        that applies upserts and removes stale versions. Writing the same result twice changes nothing. Quarantined
        rows go to the dataset's quarantine table. Returns the rows written."""

    def measures(self, plan: SilverPlan, measures: tuple[Measure, ...]) -> dict[str, float]:
        """Current rows (current version, not deleted): row_count and each measure; plus versions (every row) and
        deleted_marked (current rows marked deleted). A table never written gives zeros."""

    def current_keys(self, source: str, dataset: str, keys: tuple[str, ...]) -> set[tuple[Any, ...]] | None:
        """The business keys of the current, not deleted rows (the mass-delete guard's comparison, DPC-0054), or
        None if the table was never written."""


    def bucket_totals(self, plan: SilverPlan, measures: dict[str, str | None],
                      buckets: int) -> dict[int, dict[str, float]]:
        """Totals per business-key bucket over current, not deleted rows (dataplatform.reconcile.bucket_of)."""

    def keys_in_buckets(self, plan: SilverPlan, buckets: int, wanted: set[int]) -> set[tuple[str, ...]]:
        """The business keys, as text, of current, not deleted rows in the wanted buckets only."""

    def versions(self, plan: SilverPlan, key: dict[str, Any]) -> list[dict[str, Any]]:
        """Every row silver holds for one business key, oldest version first (for checks)."""

    def count_by_hour(self, plan: SilverPlan, column: str, start: str, end: str) -> dict[str, int]:
        """Current rows per hour of a timestamp column in [start, end), hours counted from `start` (ISO, UTC; a
        business day may start on the half hour): {hour start ISO: rows}."""


class StageError(RuntimeError):
    """A batch the stages cannot take: it stops (DPC-0008), the dataset run fails, nothing is written."""
