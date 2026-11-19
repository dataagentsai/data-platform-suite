"""Plain records shared by the kernel and the adapters."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Delivery:
    """One delivery in landing: a data file plus the manifest the mover wrote beside it (DPC-0004)."""
    source: str
    dataset: str
    window: str            # the business window the delivery covers, e.g. 2026-11-03
    name: str              # file name as received
    checksum: str          # sha256 of the file bytes
    declared_rows: int     # row count the source declared in the delivery manifest


@dataclass
class CheckResult:
    """One verdict, always tied to a run id so quality, reconciliation and run log join up (connect the dots)."""
    run_id: str
    day: int
    check: str             # e.g. reconcile.row_count.bronze, oracle.silver-agrees-with-world
    obligation: str        # the DAC id this check proves, e.g. DUP-01
    passed: bool
    expected: Any = None
    observed: Any = None
    detail: str = ""


@dataclass
class LandOutcome:
    delivery: Delivery
    batch_key: str
    status: str            # loaded | skipped_duplicate | rejected_contract | rejected_volume
    rows: int = 0
    reason: str = ""


@dataclass
class RunRecord:
    """One row of the run log (DPC-0020): what ran, with which config, and what happened."""
    run_id: str
    day: int
    step: str
    source: str
    dataset: str
    status: str
    config_fingerprint: str
    counts: dict[str, int] = field(default_factory=dict)
    message: str = ""
