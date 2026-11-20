"""The lander steps: one library every way into bronze calls (ING-01, ADR-002, DPC-0037). A pushed file, a pulled copy,
a page from an interface and a chunk of a stream all pass the same steps in the same order:

    1. batch key       a content checksum; a batch already loaded is skipped as a duplicate (DUP-01, DPC-0024)
    2. parse           CSV or JSON lines, as the dataset's settings say (pulled batches arrive as rows)
    3. columns         a contract column missing stops the batch, naming it (SCH-02); a new column is kept in
                       bronze (`_extra`) or stops the batch, as the manifest's load.new_columns says (SCH-01)
    4. derive          contract columns the lander fills from the delivery (file_name)
    5. types           every value cast to its contract type; a value that does not cast stops the batch (SCH-02)
    6. volume          declared rows and the manifest's expected band (DPC-0013); an empty batch is held
    7. mass delete     a full copy missing more keys than load.mass_delete_max_share is held (DEL-04, DPC-0054)
    8. protect         restricted columns tokenised before bronze (DPC-0070)
    9. stamp, write    load metadata on every row (DPC-0022), one idempotent write by batch key
   10. log             one file record in the metadata store, whatever the outcome

Pure functions, except `enter`, which runs them and does the two writes (bronze, the file log).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from dataplatform.ingest.ports import BronzeWriter, Columns, CurrentKeys, Row, Tokenizer
from dataplatform.meta.port import Column, FileRecord, MetadataStore
from dataplatform.profiling import profile_rows

LOAD_COLUMNS: Columns = [
    ("_batch_key", "string"),
    ("_run_id", "string"),
    ("_source_file", "string"),
    ("_window", "string"),
    ("_extra", "string"),
    ("_bronze_at", "timestamp"),  # when the lander wrote the row (ADR-034: every hop stamps its own time)
]
POSITION_COLUMNS: dict[str, Columns] = {  # where a row came from, by read type; bronze keeps them beside the row
    "change_feed": [("_op", "string"), ("_feed_pos", "integer"), ("_committed_at", "timestamp")],
    "events": [("_shard", "string"), ("_seq", "integer"), ("_arrived_at", "timestamp")],
}
DERIVED = frozenset({"file_name"})  # contract columns the lander fills from the delivery when the source does not
RESTRICTED = frozenset({"restricted"})  # personal-data classes tokenised before bronze


class CastError(ValueError):
    pass


def content_key(data: bytes) -> str:
    """The batch key: the same bytes give the same key whatever the file is called or when it came (DUP-01)."""
    return hashlib.sha256(data).hexdigest()[:32]


@dataclass(frozen=True)
class Spec:
    """One dataset as the lander needs it, from the metadata store's merged view (never from files)."""

    source: str
    dataset: str
    mode: str
    read: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    settings: dict[str, str]
    load: dict[str, Any]
    band: tuple[int, int | None]
    fingerprint: str

    @property
    def table(self) -> str:
        return f"{self.source}.{self.dataset}"

    @property
    def bronze_columns(self) -> Columns:
        return [(c.name, c.logical_type) for c in self.columns] + POSITION_COLUMNS.get(self.read, []) + LOAD_COLUMNS

    @classmethod
    def from_view(cls, view: dict[str, Any]) -> Spec:
        m = view["manifest"]
        band = (m.get("volume") or {}).get("expected_rows_per_delivery") or {}
        return cls(
            source=view["source"],
            dataset=view["dataset"],
            mode=view["ingest_mode"],
            read=view["ingest_read"],
            columns=tuple(Column(**c) for c in view["columns"]),
            primary_key=tuple(view["business_keys"]["primary"]),
            settings=dict(view["settings"]),
            load=dict(m.get("load") or {}),
            band=(int(band.get("min", 1)), band.get("max")),
            fingerprint=view["fingerprint"],
        )


@dataclass(frozen=True)
class Batch:
    """One batch on its way in: a delivery's bytes (push) or rows read from a source (pull, stream)."""

    window: str  # the business date it belongs to
    name: str  # file name, or the source position for pulled batches (e.g. feed:1-250, shard-000:1-9000)
    data: bytes  # what the batch key is computed from: a delivery's bytes exactly as delivered, or a pulled batch's
    # source batch id (manifest load.batch_key: source_batch_id), e.g. b"press_reading/shard-000:1-9000"
    rows: list[Row] | None = None  # already parsed (pull, stream); None = parse `data`
    declared_rows: int | None = None  # what the delivery manifest says


@dataclass
class Outcome:
    batch: Batch
    batch_key: str
    status: str  # one of meta.port.FILE_STATUSES
    rows: int = 0
    reason: str = ""
    notes: list[str] = field(default_factory=list)
    feeds: dict[str, Any] = field(default_factory=dict)  # feed -> newest data time in the batch (feed_marks)
    profile: dict[str, Any] | None = None  # the loaded batch's profile (B9, DPC-0051)


@dataclass
class Ctx:
    spec: Spec
    store: MetadataStore
    writer: BronzeWriter
    tokenizer: Tokenizer
    run_id: str
    rewrite: bool = False  # reload: rewrite a batch already loaded (the write replaces it, so nothing changes)
    previous_keys: set[tuple[Any, ...]] | None = None  # the keys a full copy is compared with (mass-delete guard)
    silver: CurrentKeys | None = None  # silver's current keys: what the guard compares with once silver exists
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))  # the clock port (DPC-0032)
    # this run's outcomes waiting for the one bronze commit (flush), in order: (outcome, stamped rows for a new batch,
    # else None). None = write and log each batch at once (a reload, or a caller that does not flush)
    pending: list[tuple[Outcome, list[Row] | None]] | None = None


# ---------------------------------------------------------------------------------------------------- pure steps
def parse(data: bytes, settings: dict[str, str]) -> list[Row]:
    """A delivery's bytes as rows, by the dataset's file_format_data (ADR-031): json_lines (default), csv/tsv/pipe
    with a header, or parquet (change-capture and buffered-stream deliveries; types and nulls kept)."""
    fmt = settings.get("file_format_data", "json_lines")
    if fmt == "parquet":
        return _parquet_rows(data)
    text = data.decode(settings.get("file_encoding", "utf-8") or "utf-8")
    if fmt in ("csv", "tsv", "pipe"):
        delimiter = {"tsv": "\t", "pipe": "|"}.get(fmt, settings.get("csv_delimiter", ",") or ",")
        if settings.get("csv_has_header", "true") != "true":
            raise CastError("a CSV without a header row cannot be matched to the contract's columns")
        return [dict(r) for r in csv.DictReader(io.StringIO(text), delimiter=delimiter)]
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _parquet_rows(data: bytes) -> list[Row]:
    """Rows of a columnar file; needs pyarrow (the `local` extra). Timestamps come back as datetimes, so the cast step
    sees the same types as from text."""
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415 -- optional: only datasets delivered as parquet need it
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise CastError("a parquet delivery needs pyarrow installed") from exc
    return list(pq.read_table(io.BytesIO(data)).to_pylist())


def check_columns(spec: Spec, rows: list[Row]) -> tuple[str, list[str]]:
    """(reason the batch stops, or ''; new columns seen). An empty batch has no columns to judge: the volume step
    holds it."""
    if not rows:
        return "", []
    observed = {k for r in rows for k in r if not k.startswith("_")}
    names = {c.name for c in spec.columns}
    missing = sorted(names - observed - DERIVED)
    extra = sorted(observed - names)
    if missing:
        return (
            "missing contract column(s) "
            + ", ".join(missing)
            + " (renamed, removed or never sent)"
            + (f"; new column(s) seen: {', '.join(extra)}" if extra else "")
        ), extra
    if extra and spec.load.get("new_columns", "hold_in_bronze") == "stop":
        return f"new column(s) {', '.join(extra)} and the manifest says new_columns: stop", extra
    return "", extra


def _cast_ts(v: Any) -> datetime:
    t = v if isinstance(v, datetime) else datetime.fromisoformat(str(v))
    return t if t.tzinfo else t.replace(tzinfo=UTC)


def _cast_int(v: Any) -> int:
    if isinstance(v, bool):
        raise CastError("a boolean is not a whole number")
    if isinstance(v, float) and not v.is_integer():
        raise CastError(f"{v} is not a whole number")
    return int(v)


def _cast_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if str(v).lower() in ("true", "false"):
        return str(v).lower() == "true"
    raise CastError(f"{v!r} is not true or false")


CASTS: dict[str, Callable[[Any], Any]] = {
    "string": str,
    "integer": _cast_int,
    "number": float,
    "timestamp": _cast_ts,
    "date": lambda v: v if isinstance(v, date) and not isinstance(v, datetime) else date.fromisoformat(str(v)),
    "boolean": _cast_bool,
}


def cast_rows(columns: Columns, rows: list[Row]) -> str:
    """Cast every value to its column's type, in place. Returns why the batch stops (naming the column), or ''.
    An empty text value is null for every type but string (CSV has no null)."""
    casts = [(name, CASTS[t], t == "string") for name, t in columns]
    for i, r in enumerate(rows):
        for name, cast, is_text in casts:
            v = r.get(name)
            if v is None or (v == "" and not is_text):
                r[name] = None
                continue
            try:
                r[name] = cast(v)
            except (ValueError, TypeError, CastError) as exc:
                return f"column {name} has {v!r} in row {i + 1}, which is not a {dict(columns)[name]} ({exc})"
    return ""


def volume_problem(spec: Spec, batch: Batch, n: int) -> str:
    if batch.declared_rows is not None and n != batch.declared_rows:
        return f"the delivery manifest says {batch.declared_rows} rows, the batch has {n}"
    lo, hi = spec.band
    if n == 0 or n < lo or (hi is not None and n > int(hi)):
        return f"{n} rows is outside the expected {lo}-{hi if hi is not None else 'any'} (DPC-0013)"
    return ""


def mass_delete_problem(spec: Spec, rows: list[Row], previous: set[tuple[Any, ...]] | None,
                        tokenizer: Tokenizer | None = None) -> str:
    """A full copy that would delete more than the declared share of the last accepted copy's keys (DEL-04).
    `previous` holds the keys as stored (a restricted key column holds its token), so the copy's keys are compared as
    stored too: with a `tokenizer`, a restricted key column is tokenised the way `protect` will."""
    limit = spec.load.get("mass_delete_max_share")
    if spec.read != "full_copy" or limit is None or not previous:
        return ""
    classes = {c.name: c.classification for c in spec.columns if c.classification in RESTRICTED}

    def stored(r: Row, k: str) -> Any:
        if tokenizer is not None and k in classes and r[k] not in (None, ""):
            return tokenizer.token(classes[k], str(r[k]))
        return r[k]

    now = {tuple(stored(r, k) for k in spec.primary_key) for r in rows}
    gone = len(previous - now) / len(previous)
    if gone > float(limit):
        return (
            f"mass delete held: {len(previous - now)} of {len(previous)} keys ({gone:.1%}) are missing, "
            f"more than the {float(limit):.0%} allowed (DEL-04); nothing appended, dataset blocked"
        )
    return ""


def feed_marks(spec: Spec, rows: list[Row]) -> dict[str, Any]:
    """A file that feeds one member (a dealer's monthly file): every row shares one value of a text column of the
    primary key, and the key holds a date or time column. Returns {that value: the newest date or time in the
    file}, else {}. One bookmark per feed lets a feed that has not delivered be seen as behind its siblings (LATE-01)."""
    if spec.read != "files" or not rows:
        return {}
    types = {c.name: c.logical_type for c in spec.columns}
    times = [k for k in spec.primary_key if types.get(k) in ("date", "timestamp")]
    texts = [k for k in spec.primary_key if types.get(k) == "string" and len({r.get(k) for r in rows}) == 1]
    if not times or not texts or rows[0].get(texts[0]) in (None, ""):
        return {}
    values = [r[times[0]] for r in rows if r.get(times[0]) is not None]
    return {str(rows[0][texts[0]]): max(values)} if values else {}


def protect(spec: Spec, rows: list[Row], tokenizer: Tokenizer) -> int:
    """Tokenise restricted columns in place; returns how many values were replaced."""
    restricted = [(c.name, c.classification) for c in spec.columns if c.classification in RESTRICTED]
    n = 0
    for r in rows:
        for name, cls in restricted:
            if r.get(name) not in (None, ""):
                r[name] = tokenizer.token(cls, str(r[name]))
                n += 1
    return n


def stamp(spec: Spec, rows: list[Row], key: str, run_id: str, batch: Batch, extra: list[str],  # noqa: PLR0917
          at: datetime | None = None) -> list[Row]:
    names = [c for c, _ in spec.bronze_columns if c not in ("_extra",)]
    out = []
    for r in rows:
        row = {c: r.get(c) for c in names}
        row.update(
            _batch_key=key,
            _run_id=run_id,
            _source_file=batch.name,
            _window=batch.window,
            _extra=json.dumps({c: r[c] for c in extra if c in r}, sort_keys=True, default=str) if extra else None,
            _bronze_at=at,
        )
        out.append(row)
    return out


# ---------------------------------------------------------------------------------------------------- the lander
def enter(batch: Batch, ctx: Ctx) -> Outcome:
    """Run the steps for one batch and record the outcome. Never raises for a bad batch: it is held and logged."""
    spec, key = ctx.spec, content_key(batch.data)
    before = None if ctx.rewrite else ctx.store.batch_status(spec.source, spec.dataset, key)
    if before is None and ctx.pending is not None and any(o.batch_key == key and o.status == "loaded"
                                                          for o, _ in ctx.pending):
        before = "loaded"  # the same bytes earlier in this run, waiting for the commit
    if before == "loaded":
        return _log(ctx, Outcome(batch, key, "skipped_duplicate", 0, f"same bytes as batch {key}, already loaded"))
    if before == "void":  # ADR-025: a voided batch sent again stays out; only a reload may write it to bronze again
        return _log(ctx, Outcome(batch, key, "skipped_duplicate", 0, f"same bytes as batch {key}, which was voided"))
    try:
        # a copy: the steps change values in place and must never change what a source handed over
        rows = [dict(r) for r in batch.rows] if batch.rows is not None else parse(batch.data, spec.settings)
    except (ValueError, CastError) as exc:
        return _log(ctx, Outcome(batch, key, "rejected_contract", 0, f"cannot be read: {exc}"))
    reason, extra = check_columns(spec, rows)
    if reason:
        return _log(ctx, Outcome(batch, key, "rejected_contract", 0, reason))
    if "file_name" in {c.name for c in spec.columns}:
        for r in rows:
            r.setdefault("file_name", batch.name)
    reason = cast_rows([(c.name, c.logical_type) for c in spec.columns] + POSITION_COLUMNS.get(spec.read, []), rows)
    if reason:
        return _log(ctx, Outcome(batch, key, "rejected_contract", 0, reason))
    reason = volume_problem(spec, batch, len(rows))
    if reason:
        return _log(ctx, Outcome(batch, key, "rejected_volume", 0, reason))
    reason = mass_delete_problem(spec, rows, ctx.previous_keys, ctx.tokenizer)
    if reason:
        return _log(ctx, Outcome(batch, key, "held_mass_delete", 0, reason))
    tokens = protect(spec, rows, ctx.tokenizer)
    profile = profile_rows(rows, [(c.name, c.logical_type) for c in spec.columns])  # after tokenising (B9)
    stamped = stamp(spec, rows, key, ctx.run_id, batch, extra, ctx.now())
    notes = [f"new column(s) kept in bronze only: {', '.join(extra)} (SCH-01)"] if extra else []
    notes += [f"{tokens} personal value(s) tokenised"] if tokens else []
    if ctx.pending is not None and not ctx.rewrite:  # written with the run's other new batches in one commit
        o = Outcome(batch, key, "loaded", len(stamped), "; ".join(notes), notes, feed_marks(spec, rows), profile)
        ctx.pending.append((o, stamped))
        return o
    n = ctx.writer.write(spec.table, key, stamped, spec.bronze_columns, replace=ctx.rewrite)
    return _log(ctx, Outcome(batch, key, "loaded", n, "; ".join(notes), notes, feed_marks(spec, rows), profile))


def flush(ctx: Ctx) -> int:
    """Write the run's pending new batches in one commit, then log each as loaded: the file log never says loaded
    before bronze holds the rows. If the write fails nothing is logged and the next run takes the batches again."""
    if not ctx.pending:
        return 0
    new = [(o.batch_key, rows) for o, rows in ctx.pending if rows is not None]
    n = ctx.writer.write_many(ctx.spec.table, new, ctx.spec.bronze_columns) if new else 0
    for o, _ in ctx.pending:
        _record(ctx, o)
    ctx.pending.clear()
    return n


def _log(ctx: Ctx, o: Outcome) -> Outcome:
    if ctx.pending is not None and not ctx.rewrite:  # logged in order at flush, after the bronze commit
        ctx.pending.append((o, None))
        return o
    return _record(ctx, o)


def _record(ctx: Ctx, o: Outcome) -> Outcome:
    s = ctx.spec
    ctx.store.log_file(
        FileRecord(
            ctx.run_id,
            s.source,
            s.dataset,
            o.batch_key,
            o.batch.name,
            o.batch.window,
            hashlib.sha256(o.batch.data).hexdigest(),
            o.batch.declared_rows,
            o.rows,
            o.status,
            o.reason,
            "full" if s.read == "full_copy" else "incremental",  # a full copy is a rebase point (B8)
        )
    )
    return o
