"""The ports gold talks to (dpc/ports.yaml: table_writer for gold, and the engine operations the harness asks for).

A frame is the engine's dataframe, opaque to the kernel (R1, ADR-013). The read and write edges (GoldTables) are the
only places that read or write; the engine operations (GoldEngine) take frames and give frames or small numbers back,
and the build a person writes is called only through `GoldEngine.build`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from dataplatform.gold.plan import GoldPlan
from dataplatform.gold.windows import Numbers
from dataplatform.meta.port import Correction

Frame = Any


class GoldTables(Protocol):
    """The read and write edges of gold."""

    def silver(self, dataset: str, columns: list[tuple[str, str]]) -> Frame:
        """Every row silver holds for `source.dataset` (every version, deleted ones marked). A table never written
        (an input that has delivered nothing yet) is an empty frame of its contract `columns` (name, engine type)
        and silver's own columns: an input with no rows is not an error."""

    def silver_version(self, dataset: str) -> int | None:
        """The silver table's current version (commit or snapshot), recorded per gold run (INSP-01); None when
        the table was never written or the format cannot say."""

    def silver_at(self, dataset: str, version: int, columns: list[tuple[str, str]]) -> Frame:
        """The silver table as it was at `version` (time travel), to rebuild a past run's window (INSP-01)."""

    def gold(self, plan: GoldPlan) -> Frame | None:
        """The gold table as published, or None before its first write."""

    def write_dimension(self, plan: GoldPlan, rows: Frame) -> int:
        """Replace the whole dimension in one transaction (its keys are deterministic, so a rebuild changes nothing
        that did not change). Returns the rows written."""

    def replace_windows(self, plan: GoldPlan, rows: Frame, windows: list[str]) -> int:
        """Replace exactly these windows of a fact with `rows` in one transaction: rows of other windows are never
        touched, rows of these windows not in `rows` are removed (ADR-006: replace, never append). Writing the same
        rows twice changes nothing. Returns the rows written."""


class GoldEngine(Protocol):
    """Frame operations of the harness, pure: frames in, frames or numbers out."""

    def dimension(self, plan: GoldPlan, silver: Frame, run_id: str) -> Frame:
        """The common one-to-one pipeline: contract columns, hash surrogate keys per version (ADR-004), valid from and
        to (the first version from the beginning of time so an early fact finds it, the current one to the end of
        time), and the reserved members."""

    def overlay(self, frame: Frame, corrections: list[Correction], keys: tuple[str, ...]) -> Frame:
        """Lay active corrections over silver rows (ADR-017): each correction's field takes its value on every row
        of its business key. Silver itself is never changed."""

    def query_text(self, plan: GoldPlan, params: dict[str, str]) -> str:
        """What the build runs, for people (INSP-01): the SQL model with its parameter values shown, or the name of
        the dataframe function. Never executed: the build binds the parameters itself."""

    def build(self, plan: GoldPlan, inputs: dict[str, Frame], params: dict[str, str]) -> Frame:
        """Call the fact's declared build() (SQL model or dataframe function) on its input frames."""

    def add_keys(self, plan: GoldPlan, facts: Frame, dimensions: dict[str, Frame]) -> Frame:
        """Each dimension's surrogate key by a left join as of the event (ADR-005): valid_from <= t < valid_to; no
        member -> -1, no business key -> -2. Never drops or multiplies a row."""

    def finish(self, plan: GoldPlan, facts: Frame, complete: dict[str, bool], run_id: str) -> Frame:
        """Harness columns (_is_complete: the window's flag, and false on a row whose `_input_complete` is false;
        _run_id) and the contract's column order."""

    def only_windows(self, plan: GoldPlan, facts: Frame, first: str, also: set[str]) -> Frame:
        """The rows of windows from `first` on, plus the windows named in `also`."""

    def window_numbers(self, plan: GoldPlan, facts: Frame | None, first: str,
                       also: set[str]) -> dict[str, tuple[Numbers, str]]:
        """Per window from `first` on (plus `also`): its numbers and a digest of every row's content but _run_id."""

    def hold(self, frame: Frame) -> Frame:
        """The same rows, computed once and kept for the operations that follow (released with `release`)."""

    def count(self, frame: Frame) -> int: ...

    def duplicates(self, frame: Frame, columns: tuple[str, ...]) -> int:
        """Rows beyond the first per value of `columns` (0 when unique)."""

    def key_tally(self, plan: GoldPlan, facts: Frame) -> dict[str, dict[str, int]]:
        """Per dimension key: rows matched to a member, on the unknown member (-1), not applicable (-2) (LEDG-01)."""

    def unknown_rows(self, plan: GoldPlan, facts: Frame) -> dict[str, int]:
        """Rows on the unknown member, per dimension key (orphans, LATE-02)."""

    def windows_with_unknown(self, plan: GoldPlan, facts: Frame | None) -> set[str]:
        """Published windows still holding rows on the unknown member: re-keyed on the next recompute (ADR-004)."""

    def rows_incomplete(self, plan: GoldPlan, facts: Frame) -> set[str]:
        """Windows where a build said a row's inputs have not all arrived (its `_input_complete` column is false)."""

    def first_seen(self, dimension: Frame, column: str) -> dict[str, datetime]:
        """Each real member's first version start in its source (for which feeds a window expects)."""

    def totals(self, frame: Frame, columns: tuple[str, ...]) -> dict[str, float]:
        """Sum of each column (for reconciliation)."""

    def release(self, *frames: Frame) -> None:
        """Free what the operations held for these frames (cached rows) once the table is written."""


class GoldReader(Protocol):
    """Read-only questions to published gold (checks, the oracle comparison, known answers): one SQL query over the
    gold tables, each visible by its table name (fact_otif, dim_dealer, ...). Rows come back as plain values."""

    def rows(self, sql: str, params: dict[str, str] | None = None,
             time_zone: str | None = None) -> list[dict[str, Any]]:
        """`time_zone`: the session time zone the query runs in (default: the engine's, UTC)."""
