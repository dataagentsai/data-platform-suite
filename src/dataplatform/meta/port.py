"""The metadata store port (ADR-010; dpc/ports.yaml metadata_service and run_log). The kernel imports only these shapes.

One store holds two things:
    configuration   datasets, their columns and business keys, settings with defaults (the settings registry),
                    written only by the loader from the manifest (REL-01), read through one merged view (DPC-0002);
    run state       the run / dataset run / step / file / error log keyed by run id, watermarks and bookmarks.
                    A watermark or bookmark moves only when its dataset run finishes as succeeded, in the same
                    transaction as the run record (HIST-04, "watermarks are sacred").

An adapter reaches its database only through the store's procedures (DPC-0033). Names here say what the store is,
never which product holds it (R1).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Protocol

SCOPES = ("platform", "source", "dataset")  # settings registry scopes, narrowest last
ORIGINS = ("dataset", "domain", "default")  # where a merged value came from, narrowest first
RUN_TYPES = ("normal", "reload", "backfill")
LOAD_TYPES = ("incremental", "full", "baseline")  # baseline = written by the platform from silver (housekeeping)
FILE_STATUSES = ("loaded", "skipped_duplicate", "rejected_contract", "rejected_volume", "held_mass_delete")
VOID = "void"  # a batch voided after it loaded (ADR-025): every read skips it; set by void_batch, never logged
DATASET_RUN_END = ("succeeded", "failed", "skipped")
STEP_STATUSES = ("succeeded", "failed", "skipped", "warned")
KEY_KINDS = ("primary", "dedup")
GOLD_KINDS = ("gold_dimension", "gold_fact")  # gold datasets sit beside source datasets; ingestion and silver skip them
ERROR_CAP = 4000  # characters kept of an error message
MIN_REASON = 10  # a watermark reset needs a real reason


class MetaError(ValueError):
    """Base for every refusal by the metadata store."""


class UnknownSetting(MetaError):
    """A setting key not in the registry for its scope (DPC-0034)."""


class NotFound(MetaError):
    """A dataset, run or dataset run the store does not have."""


class InvalidArgument(MetaError):
    """A value outside its allowed set (status, run type, reason too short, ...)."""


class RunStateError(MetaError):
    """A run step out of order: finishing a run never started, starting one twice, finishing with open datasets."""


@dataclass(frozen=True)
class SettingDef:
    key: str
    scope: str  # platform | source | dataset
    value_type: str
    default: str | None = None
    allowed: tuple[str, ...] | None = None
    description: str = ""
    status: str = "registered"  # registered | proposed


@dataclass(frozen=True)
class Setting:
    key: str
    value: str | None
    origin: str  # dataset | domain | default


@dataclass(frozen=True)
class Column:
    name: str
    logical_type: str
    required: bool = False
    primary_key_position: int | None = None
    description: str = ""
    classification: str = ""  # the contract's personal-data class; restricted columns are tokenised before bronze


@dataclass(frozen=True)
class DatasetRef:
    domain: str
    source: str
    dataset: str
    is_enabled: bool
    kind: str
    ingest_mode: str
    fingerprint: str


@dataclass(frozen=True)
class StepRecord:
    run_id: str
    source: str
    dataset: str
    step: str
    status: str
    counts: dict[str, int] = field(default_factory=dict)
    message: str = ""


@dataclass(frozen=True)
class FileRecord:
    run_id: str
    source: str
    dataset: str
    batch_key: str
    file_name: str
    window: str
    checksum: str
    declared_rows: int | None
    rows: int
    status: str  # one of FILE_STATUSES
    reason: str = ""
    load_type: str = "incremental"  # one of LOAD_TYPES: a full copy is a rebase point for rebuilds and housekeeping


@dataclass(frozen=True)
class BackfillPlan:
    """A plan that activates corrections on a date and names the gold windows to rebuild (ADR-017, DPC-0082)."""

    plan_id: str
    source: str
    dataset: str
    windows: tuple[str, ...]  # the windows the recompute rebuilds when the plan activates
    activate_on: str  # business date (ISO) from which its corrections are active
    reason: str
    recorded_by: str


@dataclass(frozen=True)
class Correction:
    """One corrected value (DPC-0082: entity, field, value, applies from and to, who, why, status). The entity is the
    plan's dataset; who and why are the plan's. `key` holds the business key columns and their values."""

    key: dict[str, Any]
    field: str
    value: str | None
    was: str | None = None
    applies_from: str | None = None  # business dates; None = always
    applies_to: str | None = None
    plan_id: str = ""  # filled by the store
    status: str = "recorded"  # recorded until the plan's date, then active (filled by the store)


DQ_SEVERITIES = ("critical", "major", "minor")  # ADR-009
DQ_ACTION_OF = {"critical": "stop", "major": "quarantine", "minor": "warn"}  # ADR-009: severity sets the action
DQ_ACTIONS = ("quarantine", "stop", "warn")


@dataclass(frozen=True)
class DqRule:
    """One quality rule of a dataset (DPC-0010, ADR-009: severity sets the action). `column` is empty for a rule on
    the whole row or batch; `check` names the check (not_null, range, allowed, unique, freshness, ...)."""

    rule_id: str
    column: str
    check: str
    params: dict[str, Any] = field(default_factory=dict)
    severity: str = "major"  # one of DQ_SEVERITIES
    action: str = "quarantine"  # one of DQ_ACTIONS
    enabled: bool = True


@dataclass(frozen=True)
class DqResult:
    """What one rule found in one dataset run: rows checked, rows failed, the action taken (DPC-0042)."""

    run_id: str
    source: str
    dataset: str
    rule_id: str
    checked: int
    failed: int
    action: str
    detail: str = ""


class MetadataStore(Protocol):
    # ---- configuration: written only by the loader ----
    def register_settings(self, settings: list[SettingDef]) -> None: ...

    def load_domain(self, doc: dict[str, Any]) -> None:
        """doc: domain, business_timezone, environments, settings {key: value}, manifest, fingerprint.
        Domain settings are defaults for dataset keys (or platform keys); unknown keys are refused."""

    def load_dataset(self, doc: dict[str, Any]) -> None:
        """doc: domain, source_name, source_env, dataset_name, is_enabled, kind, archetypes, ingest_mode, ingest_read,
        history_kind, load_pattern, contract_id, contract_version, fingerprint, settings {key: value},
        columns [Column fields], business_keys {kind: [columns]}, manifest. Replaces the dataset's rows whole."""

    # ---- configuration: readers ----
    def settings_registry(self) -> list[SettingDef]: ...

    def datasets(self, domain: str) -> list[DatasetRef]: ...

    def settings(self, source: str, dataset: str) -> list[Setting]:
        """Every registered dataset key, merged: dataset over domain over the registry default. (DPC-0002 also names a
        source level; DPM has no source manifest yet, so nothing feeds it.)"""

    def columns(self, source: str, dataset: str) -> list[Column]: ...

    def business_keys(self, source: str, dataset: str, kind: str = "primary") -> list[str]: ...

    def dataset_view(self, source: str, dataset: str) -> dict[str, Any]:
        """The merged configuration for one dataset with its fingerprint (port operation dataset_view)."""

    # ---- run log ----
    def start_run(self, run_id: str, domain: str, env: str, trigger: str = "manual", run_type: str = "normal") -> None:
        ...

    def start_dataset_run(self, run_id: str, source: str, dataset: str, config_fingerprint: str,
                          config: dict[str, Any]) -> str | None:
        """Open the dataset's run record with its config snapshot; returns the watermark before the run."""

    def log_step(self, step: StepRecord) -> None: ...

    def log_file(self, file: FileRecord) -> None: ...

    def batch_status(self, source: str, dataset: str, batch_key: str) -> str | None:
        """The latest status recorded for a batch key of this dataset, or None if never seen."""

    def log_error(self, run_id: str, source: str | None, dataset: str | None, step: str, error_class: str,
                  message: str) -> None:
        """Message is redacted and capped by the store, whoever calls."""

    def finish_dataset_run(self, run_id: str, source: str, dataset: str, status: str,
                           counts: dict[str, int] | None = None, watermark: str | None = None,
                           bookmarks: dict[str, str] | None = None, message: str = "") -> None:
        """Close the dataset run. Watermark and bookmarks move only when status is succeeded, in one transaction."""

    def finish_run(self, run_id: str) -> str:
        """Close the run; its status follows its dataset runs. Returns the status."""

    def watermark(self, source: str, dataset: str) -> str | None: ...

    def bookmarks(self, source: str, dataset: str) -> dict[str, str]: ...

    def reset_watermark(self, source: str, dataset: str, value: str | None, reason: str, changed_by: str) -> None:
        """An operator reset (backfill, correction): audited, with a reason."""

    def run_report(self, run_id: str) -> dict[str, Any]:
        """Everything recorded for one run: run, datasets, steps, files, errors."""

    def dataset_files(self, source: str, dataset: str, window: str | None = None) -> list[FileRecord]:
        """Every batch recorded for a dataset (optionally one window), oldest first: what the lander has handled,
        and where a reload finds the batches of a window again."""

    # ---- corrections (ADR-017): rows in their own table, activated by a backfill plan ----
    def record_corrections(self, plan: BackfillPlan, corrections: list[Correction]) -> None:
        """Record a plan and its corrections in one transaction. A plan id is recorded once; the key columns must be
        the dataset's primary key and the field one of its columns."""

    def corrections(self, source: str, dataset: str, as_of: str | None = None) -> list[Correction]:
        """The dataset's corrections with their status on a business date (active once the plan's date is reached);
        as_of None lists every correction as recorded."""

    def backfill_plans(self, source: str, dataset: str) -> list[BackfillPlan]: ...

    def acquire_lease(self, table: str, run_id: str, ttl_seconds: int) -> str | None:
        """ORCH-04: take the writer lease on a table. None when this run holds it (new, re-entered, or taken over
        from a run whose lease expired); otherwise the run id holding it, and this run must not write."""

    def release_lease(self, table: str, run_id: str) -> None:
        """Give the lease back (only the holder's release counts)."""

    def void_batch(self, source: str, dataset: str, batch_key: str, reason: str, approved_by: str) -> None:
        """Void a loaded batch (ADR-025): from now on batch_status and dataset_files report it as `void`, so silver
        skips it and a resend of the same bytes is not loaded again. Refused: a batch that never loaded, one already
        void, no approver or a reason under MIN_REASON characters."""

    def save_profile(self, run_id: str, source: str, dataset: str, profile: dict[str, Any]) -> None:
        """Keep a dataset run's profile (B9, DPC-0051). Refused: a dataset run that was never started."""

    def last_profile(self, source: str, dataset: str) -> dict[str, Any] | None:
        """The most recent profile kept for the dataset, or None."""

    def set_dq_rules(self, source: str, dataset: str, rules: list[DqRule]) -> None:
        """Replace a dataset's quality rules (the loader writes them from the contract). Refused: a rule id twice, a
        column the dataset does not have, a severity or action outside DQ_SEVERITIES / DQ_ACTIONS."""

    def dq_rules(self, source: str, dataset: str) -> list[DqRule]:
        """The dataset's rules, by rule id."""

    def log_dq_results(self, results: list[DqResult]) -> None:
        """Record what each rule found in a dataset run. Refused: a run or dataset run that was never started, a
        rule the dataset does not have, failed above checked or below zero."""

    def dq_results(self, run_id: str, source: str | None = None, dataset: str | None = None) -> list[DqResult]:
        """The results of a run (optionally of one dataset), in the order they were logged."""


# ---- rules both adapters apply the same way (the procedures mirror them) ----

_SECRETS = [
    (re.compile(r"(password|pwd|accountkey|sharedaccesskey|sig|token|secret)=[^;&\s]+", re.IGNORECASE), r"\1=***"),
    (re.compile(r"bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE), "Bearer ***"),
]


def redact(message: str) -> str:
    """Secrets out, then cap (the repo's rule: logs never contain secrets; error messages at most 4 KB)."""
    for pattern, repl in _SECRETS:
        message = pattern.sub(repl, message)
    return message[:ERROR_CAP]


def run_status(dataset_statuses: list[str]) -> str:
    """A run's status from its dataset runs (kept from LakeIngestionFramework's sp_master_run_log_finished)."""
    if not dataset_statuses:
        return "failed"
    failed = sum(s == "failed" for s in dataset_statuses)
    if failed == 0:
        return "succeeded"
    return "failed" if failed == len(dataset_statuses) else "partial"
