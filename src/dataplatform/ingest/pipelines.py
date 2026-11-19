"""One ingestion pipeline per source type (ADR-002, D11). Each picks its batches, hands every one to the lander steps
(steps.enter) and reports bookmarks, a watermark and what it noticed. They differ only in where batches come from:

    push   change_feed  landing deliveries the change-capture mover wrote (JSON lines of feed rows)
    push   files        landing deliveries the file mover copied from the drop zone, bytes as delivered
    pull   full_copy    the newest full copy the source published; the mass-delete guard compares it with silver
    pull   pages        an interface read page by page from the last cursor
    stream events       every shard from its checkpoint to the end, then stop (trigger-once; never continuous)

Where a run starts: the bookmarks of the last succeeded run, or further on where the file log shows later batches
loaded (a batch is committed by its file record), so a failed run never makes the next one read a batch twice.
Run types (HIST-04): normal reads from there; reload rewrites every batch recorded for the given windows (the write
replaces by batch key, so a rerun changes nothing, DUP-02); backfill enters the batches of those windows that are
not loaded yet (held ones are checked again).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from fnmatch import fnmatch
from typing import Any, TypeVar
from zoneinfo import ZoneInfo

from dataplatform.ingest.ports import Landing, Row
from dataplatform.ingest.steps import Batch, Ctx, Outcome, enter, parse
from dataplatform.meta.port import FileRecord
from dataplatform.resilience import RetryPolicy, SourceGuard
from dataplatform.source_ports import EventStream, RecordInterface, SourceDatabase, TableSpec

T = TypeVar("T")
FEED_COLUMNS = {"op": "_op", "feed_pos": "_feed_pos", "committed_at": "_committed_at"}
PAGE_SIZE = 200
STREAM_CHUNK = 200_000
LATE_ALLOWANCE_S = 5  # a reading more than this behind the newest one already read counts as out of order


@dataclass
class Plan:
    """What a run does for one dataset: the run type and, for reload and backfill, the windows."""

    run_type: str = "normal"
    window: str = ""  # the business date of a normal run (stamped on pulled batches)
    windows: tuple[str, ...] = ()


@dataclass
class Sources:
    """The source side as the binding hands it over: landing for push, the source systems for pull and stream.
    `systems` maps a manifest source_name to the system that holds it (erp -> the ERP database)."""

    landing: Landing
    db: SourceDatabase
    api: RecordInterface
    stream: EventStream
    systems: dict[str, str]
    business_tz: str
    guard: SourceGuard | None = None  # retry, circuit breaker and connection cap for source calls (B7); None = direct


def _read(ctx: Ctx, src: Sources, fn: Callable[[], T]) -> T:
    """One call to a source through the run's guard: retried on temporary errors per the dataset's retry settings,
    refused while the source's circuit is open, limited by the source's connection cap."""
    if src.guard is None:
        return fn()
    return src.guard.call(ctx.spec.source, fn, RetryPolicy.from_settings(ctx.spec.settings))


@dataclass
class Result:
    outcomes: list[Outcome] = field(default_factory=list)
    bookmarks: dict[str, str] = field(default_factory=dict)
    watermark: str | None = None
    noticed: dict[str, int] = field(default_factory=dict)  # detector -> count (DEL-01, SCD-02, LATE-03, DUP-04)
    notes: list[str] = field(default_factory=list)

    def notice(self, detector: str, n: int) -> None:
        if n:
            self.noticed[detector] = self.noticed.get(detector, 0) + n


# ---------------------------------------------------------------------------------------------------- helpers
def _window_of(ts: str, tz: str) -> str:
    return datetime.fromisoformat(ts).astimezone(ZoneInfo(tz)).date().isoformat()


def _table_spec(ctx: Ctx, read: str) -> TableSpec:
    s = ctx.spec
    return TableSpec(
        s.dataset,
        tuple((c.name, c.logical_type) for c in s.columns),
        s.primary_key,
        change_feed=read == "change_feed",
        full_copy=read == "full_copy",
    )


def _pick(files: list[FileRecord]) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """(window, name) of batches recorded at all, and of batches loaded."""
    return (
        {(f.window, f.file_name) for f in files},
        {(f.window, f.file_name) for f in files if f.status in ("loaded", "skipped_duplicate")},
    )


def _replay_names(files: list[FileRecord], plan: Plan) -> list[FileRecord]:
    """The recorded batches a reload (all) or backfill (not loaded) re-enters, latest record per batch."""
    latest: dict[tuple[str, str], FileRecord] = {}
    for f in files:
        if f.window in plan.windows:
            latest[(f.window, f.file_name)] = f
    keep = [f for f in latest.values() if plan.run_type == "reload" or f.status not in ("loaded", "skipped_duplicate")]
    return sorted(keep, key=lambda f: (f.window, f.file_name))


def _max_mark(old: str | None, values: list[Any]) -> str | None:
    """The larger of a stored mark and new values: whole numbers compare as numbers, everything else as ISO text."""
    vals = [v.isoformat() if isinstance(v, datetime) else v for v in values if v is not None]
    if not vals:
        return old
    if all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
        top = max(vals)
        return str(top if old is None else max(top, int(old)))
    text = max(str(v) for v in vals)
    return text if old is None else max(text, old)


# ---------------------------------------------------------------------------------------------------- push
def push(ctx: Ctx, src: Sources, plan: Plan) -> Result:
    s, res = ctx.spec, Result()
    files = ctx.store.dataset_files(s.source, s.dataset)
    seen, loaded = _pick(files)
    pattern = s.settings.get("file_pattern")
    for d in sorted(src.landing.deliveries(s.source, s.dataset), key=lambda d: (d.window, d.name)):
        if pattern and s.read == "files" and not fnmatch(d.name, pattern):
            continue
        if plan.run_type == "normal" and (d.window, d.name) in seen:
            continue
        if plan.run_type != "normal" and (
            d.window not in plan.windows or (plan.run_type == "backfill" and (d.window, d.name) in loaded)
        ):
            continue
        data = src.landing.read_bytes(d)
        rows: list[Row] | None = None
        if s.read == "change_feed":
            names = feed_columns(s.settings)
            rows = [{names.get(k, k): _op_value(k, v, names) for k, v in r.items()} for r in parse(data, s.settings)]
            _feed_notices(ctx, rows, res)
        ctx.rewrite = plan.run_type == "reload"
        o = enter(Batch(d.window, d.name, data, rows, d.declared_rows), ctx)
        res.outcomes.append(o)
        if o.status == "loaded" and rows and all(isinstance(r.get("_feed_pos"), int) for r in rows):
            res.bookmarks["feed_pos"] = (
                _max_mark(
                    res.bookmarks.get("feed_pos") or ctx.store.bookmarks(s.source, s.dataset).get("feed_pos"),
                    [r["_feed_pos"] for r in rows],
                )
                or ""
            )
            res.watermark = _watermark(ctx, rows, res.watermark)
        elif o.status == "loaded" and rows:  # a change feed with no integer position (a change-capture service)
            res.bookmarks["last_file"] = d.name
            res.watermark = _watermark(ctx, rows, res.watermark)
        elif o.status == "loaded":
            res.bookmarks["last_file"] = d.name
            _feed_bookmarks(ctx, src, o.feeds, res)
    return res


def feed_columns(settings: dict[str, str]) -> dict[str, str]:
    """Delivery column -> standard feed column. A change-capture service names its columns its own way (ADR-031):
    cdc_op_column (default op), cdc_position_column (default feed_pos; empty when the service has none) and
    cdc_commit_column (default committed_at). Unset keys keep the defaults W1's own feed uses."""
    out = {}
    for key, default, std in (("cdc_op_column", "op", "_op"), ("cdc_position_column", "feed_pos", "_feed_pos"),
                              ("cdc_commit_column", "committed_at", "_committed_at")):
        name = settings.get(key, default)
        if name:
            out[name] = std
    return out


def _op_value(column: str, value: Any, names: dict[str, str]) -> Any:
    """Operation letters upper-cased (I, U, D); a full-load row with no operation stays None (history reads it as U)."""
    if names.get(column) == "_op" and isinstance(value, str):
        return value.strip().upper()[:1] or None
    return value


def _feed_bookmarks(ctx: Ctx, src: Sources, feeds: dict[str, Any], res: Result) -> None:
    """One bookmark per feed (a dealer), holding its newest data time as an instant in the business time zone; it
    only moves forward. Freshness compares feeds with their siblings; gold reads them for completeness (ADR-018)."""
    if not feeds:
        return
    stored = ctx.store.bookmarks(ctx.spec.source, ctx.spec.dataset)
    for feed, value in feeds.items():
        if isinstance(value, datetime):
            t = value if value.tzinfo else value.replace(tzinfo=ZoneInfo(src.business_tz))
        else:
            t = datetime(value.year, value.month, value.day, tzinfo=ZoneInfo(src.business_tz))
        old = res.bookmarks.get(feed) or stored.get(feed)
        if not old or datetime.fromisoformat(old) < t:
            res.bookmarks[feed] = t.isoformat()


def _feed_notices(ctx: Ctx, rows: list[Row], res: Result) -> None:
    """Deletes arrive as op D (DEL-01). A change whose sequence is older than one already seen for its key in the
    delivery arrived out of order (SCD-02); silver orders history by the sequence (W1c)."""
    res.notice("DEL-01", sum(r.get("_op") == "D" for r in rows))
    order = ctx.spec.load.get("history_order_by")
    if not order:
        return
    newest: dict[tuple[Any, ...], Any] = {}
    late = 0
    for r in rows:
        k = tuple(r.get(c) for c in ctx.spec.primary_key)
        if k in newest and r.get(order) is not None and int(r[order]) < int(newest[k]):
            late += 1
        else:
            newest[k] = r.get(order)
    res.notice("SCD-02", late)


def _watermark(ctx: Ctx, rows: list[Row], current: str | None) -> str | None:
    """The dataset's watermark column, moved forward to the largest value loaded (never back)."""
    col = ctx.spec.settings.get("watermark_column")
    if not col:
        return current
    return _max_mark(current or ctx.store.watermark(ctx.spec.source, ctx.spec.dataset), [r.get(col) for r in rows])


# ---------------------------------------------------------------------------------------------------- pull
def _guard_keys(ctx: Ctx, last_ok: str) -> set[tuple[Any, ...]] | None:
    """What the mass-delete guard compares a full copy with: silver's current keys (DPC-0054), or the last accepted
    copy in bronze while silver has never been written (the first days, or a binding without silver)."""
    s = ctx.spec
    if ctx.silver is not None:
        keys = ctx.silver.current_keys(s.source, s.dataset, s.primary_key)
        if keys is not None:
            return keys
    return ctx.writer.keys(s.table, s.primary_key, {"_batch_key": last_ok}) if last_ok else None


def pull_copy(ctx: Ctx, src: Sources, plan: Plan) -> Result:
    """The newest full copy only: a newer copy supersedes an older one (and a held one)."""
    s, res = ctx.spec, Result()
    system, table = src.systems[s.source], _table_spec(ctx, "full_copy")
    marks = ctx.store.bookmarks(s.source, s.dataset)
    files = ctx.store.dataset_files(s.source, s.dataset)
    if plan.run_type == "normal":
        done = {f.file_name for f in files}
        new = [
            at
            for at in _read(ctx, src, lambda: src.db.copies(system, table))
            if f"copy:{at}" not in done and at > marks.get("copy_extracted_at", "")
        ]
        todo = [(_window_of(new[-1], src.business_tz), new[-1])] if new else []
    else:
        todo = [(f.window, f.file_name.split(":", 1)[1]) for f in _replay_names(files, plan)]
    last_ok = marks.get("copy_batch") or next((f.batch_key for f in reversed(files) if f.status == "loaded"), "")
    for window, at in todo:
        ctx.previous_keys = _guard_keys(ctx, last_ok)
        ctx.rewrite = plan.run_type == "reload"
        name = f"copy:{at}"
        rows = _read(ctx, src, lambda at=at: src.db.copy(system, table, at))
        o = enter(Batch(window, name, f"{s.table}/{name}".encode(), rows), ctx)
        res.outcomes.append(o)
        if o.status == "loaded" and plan.run_type == "normal":
            res.bookmarks.update(copy_extracted_at=at, copy_batch=o.batch_key)
    return res


def pull_pages(ctx: Ctx, src: Sources, plan: Plan) -> Result:
    """Pages since the cursor, one batch per page; the batch name holds the cursor range so a reload can re-read it."""
    s, res = ctx.spec, Result()
    system = src.systems[s.source]
    files = ctx.store.dataset_files(s.source, s.dataset)
    if plan.run_type == "reload":
        res.notes.append(
            "an interface serves only current records, so a page cannot be read again as it was: "
            "bronze is this dataset's replay point (ADR-002); nothing re-read"
        )
        return res
    if plan.run_type == "backfill":  # pages held earlier: read again, keeping only records up to the page's end
        for f in _replay_names(files, plan):
            start, end = f.file_name.removeprefix("page:").split(">", 1)
            items, _ = _read(ctx, src, lambda start=start: src.api.page(system, s.dataset, start, PAGE_SIZE))
            keep = [dict(r) for r in items if _cursor_of(r, s) <= end]
            res.outcomes.append(enter(Batch(f.window, f.file_name, f"{s.table}/{f.file_name}".encode(), keep), ctx))
        return res
    cursor = _resume_cursor(ctx.store.bookmarks(s.source, s.dataset).get("cursor", ""), files)
    while True:
        items, nxt = _read(ctx, src, lambda cursor=cursor: src.api.page(system, s.dataset, cursor, PAGE_SIZE))
        if not items:
            break
        name = f"page:{cursor}>{nxt}"
        o = enter(Batch(plan.window, name, f"{s.table}/{name}".encode(), [dict(r) for r in items]), ctx)
        res.outcomes.append(o)
        if o.status not in ("loaded", "skipped_duplicate"):
            break  # held: the next run starts again at this page
        res.watermark = _watermark(ctx, [dict(r) for r in items], res.watermark)
        cursor = res.bookmarks["cursor"] = nxt
    return res


def _cursor_of(record: Row, spec: Any) -> str:
    """A record's position in the interface's order: '<updated_at>|<key>' (adapters/http_api.py)."""
    return f"{record.get(spec.settings.get('watermark_column', 'updated_at'))}|{record.get(spec.primary_key[0])}"


def _resume_cursor(bookmark: str, files: list[FileRecord]) -> str:
    """The cursor after the last page loaded, if a failed run loaded pages its bookmark does not show."""
    ends = [f.file_name.split(">", 1)[1] for f in files if f.status == "loaded" and f.file_name.startswith("page:")]
    return max([bookmark, *ends])


# ---------------------------------------------------------------------------------------------------- stream
def stream_events(ctx: Ctx, src: Sources, plan: Plan) -> Result:
    """Trigger-once: each shard from its checkpoint to the end, one batch per shard, then stop (BP-044). Bronze keeps
    every reading as delivered (RAW-01); replays and late readings are counted here and dropped or placed by event
    time in silver (DUP-04, LATE-03; W1e)."""
    s, res = ctx.spec, Result()
    files = ctx.store.dataset_files(s.source, s.dataset)
    if plan.run_type != "normal":
        for f in _replay_names(files, plan):
            shard, rng = f.file_name.rsplit(":", 1)
            first, last = (int(x) for x in rng.split("-"))
            recs = src.stream.read(s.dataset, shard, first - 1, last - first + 1)
            ctx.rewrite = plan.run_type == "reload"
            res.outcomes.append(
                enter(Batch(f.window, f.file_name, f"{s.table}/{f.file_name}".encode(), _stream_rows(shard, recs)), ctx)
            )
        return res
    marks = ctx.store.bookmarks(s.source, s.dataset)
    loaded_to: dict[str, int] = {}
    for f in files:
        if f.status == "loaded" and f.file_name.startswith("shard-"):
            shard, rng = f.file_name.rsplit(":", 1)
            loaded_to[shard] = max(loaded_to.get(shard, 0), int(rng.split("-")[1]))
    seen: set[str] = set()
    for shard in src.stream.shards(s.dataset):
        after = max(int(marks.get(shard, "0")), loaded_to.get(shard, 0))
        recs = []
        while chunk := src.stream.read(s.dataset, shard, after, STREAM_CHUNK):
            recs += chunk
            after = chunk[-1].seq
        if not recs:
            continue
        rows = _stream_rows(shard, recs)
        _stream_notices(rows, seen, res)
        name = f"{shard}:{recs[0].seq}-{recs[-1].seq}"
        o = enter(Batch(plan.window, name, f"{s.table}/{name}".encode(), rows), ctx)
        res.outcomes.append(o)
        if o.status == "loaded":
            res.bookmarks[shard] = str(recs[-1].seq)
            res.watermark = _watermark(ctx, rows, res.watermark)
    return res


def _stream_rows(shard: str, recs: list[Any]) -> list[Row]:
    return [{**r.data, "_shard": shard, "_seq": r.seq, "_arrived_at": r.arrived_at} for r in recs]


def _stream_notices(rows: list[Row], seen: set[str], res: Result) -> None:
    """DUP-04: a reading id read before in this run is a replay. LATE-03: a reading whose event time is more than the
    allowance behind the newest already read on its shard arrived out of order."""
    newest, replays, late = "", 0, 0
    for r in rows:
        rid, ts = str(r.get("reading_id")), str(r.get("event_ts"))
        if rid in seen:
            replays += 1
            continue
        seen.add(rid)
        if (
            newest
            and ts < newest
            and (datetime.fromisoformat(newest) - datetime.fromisoformat(ts)).total_seconds() > LATE_ALLOWANCE_S
        ):
            late += 1
        newest = max(newest, ts)
    res.notice("DUP-04", replays)
    res.notice("LATE-03", late)


PIPELINES: dict[tuple[str, str], Callable[[Ctx, Sources, Plan], Result]] = {
    ("push", "change_feed"): push,
    ("push", "files"): push,
    ("pull", "full_copy"): pull_copy,
    ("pull", "pages"): pull_pages,
    ("stream", "events"): stream_events,
}
