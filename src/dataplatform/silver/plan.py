"""The silver plan: what the thirteen standard stages (ADR-007) do for one dataset, read from the metadata store's
merged view of the dataset (manifest + contract columns). Pure and product-free (R1): no dataframe here, only settings.

The stages, in their fixed order (DPC-0045):

     1 contract_check   every contract column is in the batch (DPC-0008); a missing one stops the batch
     2 project          projection (all columns by default, ADR-008), extract from held columns, rename, exclude
     3 hygiene          string clean-up: control characters, byte-order mark, line endings, whitespace, empty -> null
     4 parse_type       cast to the contract's logical type; a value that cannot be cast is quarantined or emptied and
                        counted, as its column says (DQ-02), never silently emptied
     5 time             timestamps kept in UTC; a column may add its local time beside it
     6 standardise      declared value maps and letter case
     7 personal_data    a restricted column must hold tokens (DPC-0070): a plain value is quarantined
     8 row_filter       declared row filters drop rows, counted
     9 quality          row rules; severity sets the action (ADR-009): quarantine or flag
    10 duplicates       dedup by business key (first, latest, exact change) and against silver already loaded (DUP-04)
    11 keys             business key present, key hash added
    12 history          by dataset kind (ADR-016): SCD type 2 ordered by the source sequence (SCD-02), type 1, append,
                        snapshot; deletes are soft (DEL-01)
    13 provenance       batch, file, bronze run and silver run on every row, record hash

Per-column settings live in the manifest's `columns` list (LakeIngestionFramework's dataset_config_columns shape, which
DPM keeps): {name, from, path, exclude, hygiene, on_cast_failure, format, local_tz, case, values}. Stage switches are
the registered `processor_groups.<group>` keys. A setting outside this list is refused (ADR-007 confirmation).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STAGES = (
    "contract_check",
    "project",
    "hygiene",
    "parse_type",
    "time",
    "standardise",
    "personal_data",
    "row_filter",
    "quality",
    "duplicates",
    "keys",
    "history",
    "provenance",
)
# The registered processor_groups.<group> keys that switch an optional stage off ("disabled"); the others always run.
SWITCHES = {
    "hygiene": "hygiene",
    "parse_type": "type_safety",
    "time": "timestamp_normalize",
    "standardise": "standardization",
    "personal_data": "pii",
    "duplicates": "dedup",
}
COLUMN_SETTINGS = frozenset(
    {"name", "from", "path", "exclude", "hygiene", "on_cast_failure", "format", "local_tz", "case", "values"}
)
LOGICAL_TYPES = ("string", "integer", "number", "boolean", "timestamp", "date")
HISTORY_BY_KIND = {"master": "scd2", "transaction": "scd1", "event": "append", "snapshot": "snapshot"}  # ADR-016
HISTORY_TYPES = ("scd1", "scd2", "append", "snapshot", "source_versioned")  # ADR-016, ADR-032
DEDUP_MODES = ("business_key_latest", "business_key_first", "full_row", "none")
CAST_FAILURE = ("quarantine", "empty")
DELETE_APPLY = ("mark_deleted", "remove", "none")
# ADR-009: severity sets the action. The platform manifest's quality_defaults say the same; the dataset view does not
# carry the domain's defaults yet (gap), so these are the defaults a dataset's quality.actions overrides.
QUALITY_DEFAULTS = {"critical": "quarantine", "major": "quarantine", "minor": "flag"}
ROW_ACTIONS = ("quarantine", "flag", "none")

# Columns the lander stamps on every bronze row, and the transport columns some source types add.
LOAD_COLUMNS = ("_batch_key", "_run_id", "_source_file", "_window", "_extra", "_bronze_at")
TRANSPORT_COLUMNS = ("_op", "_feed_pos", "_committed_at", "_shard", "_seq", "_arrived_at")
# Silver's own columns (names kept from LakeIngestionFramework's metadata_cols where they existed).
SILVER_COLUMNS = (
    "_key_hash",
    "_record_hash",
    "_seq_from",
    "_valid_from",
    "_valid_to",
    "_is_current",
    "_is_deleted",
    "_deleted_at",
    "_dq_flags",
    "_row_key",
    "_bronze_run_id",
    "_silver_run_id",
    "_silver_loaded_at",
)


class PlanError(ValueError):
    """A dataset's silver settings the engine cannot run: an unknown setting or stage, a missing key or order."""


@dataclass(frozen=True)
class ColumnPlan:
    name: str
    logical_type: str
    required: bool = False
    classification: str = ""
    source: str = ""  # the bronze column it comes from when renamed ("" = same name)
    path: str = ""  # a JSON path into the held columns (_extra) to extract it from
    exclude: bool = False
    hygiene: bool = True
    on_cast_failure: str = "quarantine"
    fmt: str = ""  # a date or timestamp pattern for text values
    local_tz: str = ""  # adds <name>_local, the time in this zone
    case: str = ""  # upper | lower
    values: tuple[tuple[str, str], ...] = ()  # declared value map

    @property
    def restricted(self) -> bool:
        return self.classification == "restricted"


@dataclass(frozen=True)
class Rule:
    """A row rule: rows must satisfy `predicate` (an engine expression); failing rows get `action`."""

    name: str
    predicate: str
    severity: str
    action: str


@dataclass(frozen=True)
class Measure:
    name: str
    agg: str  # count | sum
    column: str = ""


@dataclass(frozen=True)
class SilverPlan:
    source: str
    dataset: str
    columns: tuple[ColumnPlan, ...]
    keys: tuple[str, ...]
    history: str  # scd1 | scd2 | append | snapshot | source_versioned
    kind: str
    ingest_read: str
    dedup_mode: str = "none"
    dedup_keys: tuple[str, ...] = ()
    order_by: str = ""  # history order (load.history_order_by), else dedup.order_by
    track: tuple[str, ...] = ()  # columns that open a new version (SCD2); empty = every non-key column
    effective_time: str = ""  # the column that says when a change took effect (validity of versions)
    event_time: str = ""  # the event-time column (the watermark column when it is a timestamp): appended rows' time
    delete_detect: str = "none"
    delete_apply: str = "none"
    snapshot_by: tuple[str, ...] = ()
    valid_from: str = ""  # source_versioned (ADR-032): the source's own validity columns
    valid_to: str = ""
    gaps_allowed: bool = False
    lookback_days: int | None = None
    projection: str = "all"
    rules: tuple[Rule, ...] = ()
    quality_actions: tuple[tuple[str, str], ...] = ()  # the dataset's own severity -> action overrides (ADR-009)
    row_filters: tuple[Rule, ...] = ()
    disabled: frozenset[str] = field(default_factory=frozenset)
    token_pattern: str = ""
    measures: tuple[Measure, ...] = ()

    @property
    def table(self) -> str:
        return f"{self.source}.{self.dataset}"

    def column(self, name: str) -> ColumnPlan:
        for c in self.columns:
            if c.name == name:
                return c
        raise PlanError(f"{self.table}: no column {name}")

    @property
    def kept(self) -> tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if not c.exclude)

    def runs(self, stage: str) -> bool:
        return stage not in self.disabled


# ---------------------------------------------------------------------------------------------------- building
def _column(c: dict[str, Any], override: dict[str, Any]) -> ColumnPlan:
    unknown = sorted(set(override) - COLUMN_SETTINGS)
    if unknown:
        raise PlanError(f"column {c['name']}: setting(s) {', '.join(unknown)} are not stage settings (ADR-007)")
    if c["logical_type"] not in LOGICAL_TYPES:
        raise PlanError(f"column {c['name']}: logical type {c['logical_type']!r} is not one of {LOGICAL_TYPES}")
    failure = str(override.get("on_cast_failure", "quarantine"))
    if failure not in CAST_FAILURE:
        raise PlanError(f"column {c['name']}: on_cast_failure {failure!r} is not one of {CAST_FAILURE}")
    if failure == "empty" and (c.get("required") or c.get("primary_key_position")):
        raise PlanError(f"column {c['name']}: a required or key column cannot be emptied on a failed cast")
    case = str(override.get("case", ""))
    if case not in ("", "upper", "lower"):
        raise PlanError(f"column {c['name']}: case {case!r} is not upper or lower")
    return ColumnPlan(
        name=c["name"],
        logical_type=c["logical_type"],
        required=bool(c.get("required")),
        classification=str(c.get("classification") or ""),
        source=str(override.get("from", "")),
        path=str(override.get("path", "")),
        exclude=bool(override.get("exclude", False)),
        hygiene=bool(override.get("hygiene", True)),
        on_cast_failure=failure,
        fmt=str(override.get("format", "")),
        local_tz=str(override.get("local_tz", "")),
        case=case,
        values=tuple(sorted((str(k), str(v)) for k, v in (override.get("values") or {}).items())),
    )


def _rules(columns: tuple[ColumnPlan, ...], actions: dict[str, Any]) -> tuple[Rule, ...]:
    """Required columns are present (critical). The contract's other row rules reach this stage with the DQ engine
    (W1e); the stage applies whatever rules the plan holds."""
    action = {**QUALITY_DEFAULTS, **{k: str((v or {}).get("rows", QUALITY_DEFAULTS.get(k, "flag"))) for k, v in
                                     actions.items()}}
    bad = sorted(a for a in action.values() if a not in ROW_ACTIONS)
    if bad:
        raise PlanError(f"quality action(s) {', '.join(bad)} are not one of {ROW_ACTIONS}")
    return tuple(
        Rule(f"{c.name}_present", f"{c.name} IS NOT NULL", "critical", action["critical"])
        for c in columns
        if c.required and not c.exclude
    )


def _measures(manifest: dict[str, Any]) -> tuple[Measure, ...]:
    rec = manifest.get("reconciliation") or {}
    out: dict[str, Measure] = {}
    for m in rec.get("measures") or []:
        out[m["name"]] = Measure(m["name"], "sum" if m.get("column") else "count", str(m.get("column", "")))
    for entry in rec.get("entries") or []:
        for m in entry.get("measures") or []:
            if m.get("agg") == "sum" and m["name"] not in out:
                out[m["name"]] = Measure(m["name"], "sum", str(m["column"]))
    return tuple(out.values())


def _validity(name: str, history: str, hist: dict[str, Any], settings: dict[str, Any],
              names: set[str]) -> tuple[str, str, bool]:
    """(valid_from column, valid_to column, gaps allowed) for history the source keeps (ADR-032): from the manifest's
    history block, else the dataset settings. source_versioned needs both columns, and both must be contract columns."""
    valid_from = str(hist.get("valid_from_column") or settings.get("valid_from_column") or "")
    valid_to = str(hist.get("valid_to_column") or settings.get("valid_to_column") or "")
    if history == "source_versioned" and any(not c or c not in names for c in (valid_from, valid_to)):
        raise PlanError(f"{name}: source_versioned needs valid_from_column and valid_to_column that are contract "
                        "columns (ADR-032)")
    gaps = str(hist.get("gaps_allowed", settings.get("gaps_allowed", "false"))).lower() == "true"
    return valid_from, valid_to, gaps


def plan_from_view(
    view: dict[str, Any],
    token_pattern: str = "",
    projection: str = "all",
    row_filters: tuple[Rule, ...] = (),
) -> SilverPlan:
    """The plan for one dataset from its merged view (MetadataStore.dataset_view). `token_pattern` is the binding's
    tokenizer format; `projection` 'declared' keeps only the contract's columns (ADR-008: DPM has no key for it yet)."""
    m = view["manifest"]
    name = f"{view['source']}.{view['dataset']}"
    overrides = {str(c["name"]): {k: v for k, v in c.items()} for c in (m.get("columns") or [])}
    contract_names = {c["name"] for c in view["columns"]}
    stray = sorted(set(overrides) - contract_names)
    if stray:
        raise PlanError(f"{name}: column settings for {', '.join(stray)}, which the contract does not have")
    columns = tuple(_column(c, overrides.get(c["name"], {})) for c in view["columns"])
    keys = tuple(view["business_keys"]["primary"])
    if not keys:
        raise PlanError(f"{name}: the contract names no primary key; silver needs one")
    hist = m.get("history") or {}
    kind = str(hist.get("kind") or view.get("history_kind") or "")
    if kind not in HISTORY_BY_KIND:
        raise PlanError(f"{name}: dataset kind {kind!r} is not one of {tuple(HISTORY_BY_KIND)} (ADR-016)")
    history = str(hist.get("type") or HISTORY_BY_KIND[kind])
    if history not in HISTORY_TYPES:
        raise PlanError(f"{name}: history type {history!r} is not one of {HISTORY_TYPES}")
    load, dedup = m.get("load") or {}, m.get("dedup") or {}
    order_by = str(load.get("history_order_by") or dedup.get("order_by") or "")
    if history in ("scd1", "scd2") and not order_by:
        raise PlanError(f"{name}: {history} needs an order (load.history_order_by or dedup.order_by)")
    names = {c.name for c in columns}
    track = tuple(hist.get("track_columns") or ())
    missing = sorted((set(track) | ({order_by} - {""})) - names)
    if missing:
        raise PlanError(f"{name}: history column(s) {', '.join(missing)} are not contract columns")
    valid_from, valid_to, gaps = _validity(name, history, hist, view.get("settings") or {}, names)
    deletes = load.get("deletes") or {}
    if str(deletes.get("apply", "none")) not in DELETE_APPLY:
        raise PlanError(f"{name}: deletes.apply {deletes.get('apply')!r} is not one of {DELETE_APPLY}")
    mode = str(dedup.get("mode", "none"))
    if mode not in DEDUP_MODES:
        raise PlanError(f"{name}: dedup.mode {mode!r} is not one of {DEDUP_MODES}")
    settings = view.get("settings") or {}
    disabled = frozenset(s for s, group in SWITCHES.items() if settings.get(f"processor_groups.{group}") == "disabled")
    if projection not in ("all", "declared"):
        raise PlanError(f"{name}: projection {projection!r} is not all or declared")
    by_type = {c.name: c.logical_type for c in columns}
    read = str(view.get("ingest_read") or (m.get("ingest") or {}).get("read") or "")
    effective = "_committed_at" if read == "change_feed" else (order_by if by_type.get(order_by) == "timestamp" else "")
    lookback = load.get("late_lookback_days")
    wm = str(settings.get("watermark_column") or "")
    event = wm if by_type.get(wm) == "timestamp" else effective
    return SilverPlan(
        source=view["source"],
        dataset=view["dataset"],
        columns=columns,
        keys=keys,
        history=history,
        kind=kind,
        ingest_read=read,
        dedup_mode=mode,
        dedup_keys=tuple(dedup.get("business_keys") or keys),
        order_by=order_by,
        track=track,
        effective_time=effective,
        event_time=event,
        delete_detect=str(deletes.get("detect", "none")),
        delete_apply=str(deletes.get("apply", "none")),
        snapshot_by=keys[:-1] if history == "snapshot" else (),
        valid_from=valid_from,
        valid_to=valid_to,
        gaps_allowed=gaps,
        lookback_days=int(lookback) if lookback is not None else None,
        projection=projection,
        rules=_rules(columns, dict((m.get("quality") or {}).get("actions") or {})),
        quality_actions=tuple(sorted((str(k), str(v["rows"])) for k, v in
                                     ((m.get("quality") or {}).get("actions") or {}).items()
                                     if isinstance(v, dict) and v.get("rows"))),
        row_filters=row_filters,
        disabled=disabled,
        token_pattern=token_pattern,
        measures=_measures(m),
    )


def check_stage_order(names: tuple[str, ...] | list[str]) -> None:
    """An engine must supply exactly the standard stages, in the standard order (DPC-0045, CFG-01)."""
    if tuple(names) != STAGES:
        raise PlanError(f"the stages must be exactly {STAGES} in this order; got {tuple(names)}")
