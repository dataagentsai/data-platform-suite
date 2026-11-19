"""The gold harness (ADR-003): one run builds the domain's gold tables, dimensions first, then facts.

    dimension (one_to_one)  silver -> common pipeline -> key unique (KEY-01) -> replace the dimension
    fact (modelled)         input watermarks (ADR-018) -> active corrections laid over silver (ADR-017) -> build()
                            -> surrogate keys as of the event (ADR-004, -005; no row dropped) -> the recompute range
                            (lookback, the correction plans' windows, windows still on the unknown member) -> harness
                            columns -> checks (grain unique, orphan rate, reconcile) -> replace the changed windows
                            (ADR-006) -> restatements logged

Every step is one row in the run log (ADR-024 step format: `detect.<DAC id>` for what a detector met, `gold.*` for
pipeline steps). The gold dataset run's bookmarks record each input's watermark in source time (`input.<dataset>`),
how far gold has published (`published_through`) and each correction plan applied (`plan.<id>`); they move only when
the dataset run succeeds. A failed check holds the last good windows: nothing is written (ADR-006).

Domain-free and product-free (R1): the harness knows manifests and frames, not tyres and not the engine.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from dataplatform.gold import windows as W
from dataplatform.gold.plan import GoldPlan, GoldPlanError, order, plan_from_view
from dataplatform.gold.ports import Frame, GoldEngine, GoldTables
from dataplatform.leases import LEASE_SECONDS, busy
from dataplatform.meta.port import GOLD_KINDS, BackfillPlan, Correction, MetadataStore, StepRecord

SILVER_BOOKMARK = "silver.files"
NOT_FEEDS = ("last_file", "feed_pos", "cursor", "copy_extracted_at", "copy_batch")  # ingestion's own bookmarks
AFTER_ALL = "￿"  # a first window later than every window: only the named ones
EVERY_WINDOW = "*"  # a backfill plan's window list meaning every window (VOID-01: a withdrawn batch, recovery.py)
BEFORE_ALL = ""  # a first window earlier than every window


@dataclass
class Restatement:
    table: str
    window: str
    detector: str  # LATE-01 (late data) | HIST-05 (a correction plan)
    before: dict[str, float]
    after: dict[str, float]


@dataclass
class GoldTable:
    name: str
    status: str  # succeeded | failed | skipped
    counts: dict[str, int] = field(default_factory=dict)
    replaced: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)
    restated: list[Restatement] = field(default_factory=list)
    plans_applied: list[str] = field(default_factory=list)
    reason: str = ""
    seconds: float = 0.0


@dataclass
class GoldReport:
    run_id: str
    as_of: str
    status: str
    tables: list[GoldTable]

    def by_name(self) -> dict[str, GoldTable]:
        return {t.name: t for t in self.tables}


@dataclass
class Ctx:
    store: MetadataStore
    tables: GoldTables
    engine: GoldEngine
    run_id: str
    as_of: str
    tz: str
    params: dict[str, str]
    plans: dict[str, GoldPlan]


class CheckFailed(RuntimeError):
    """A check before publish failed: the last good windows stay (ADR-006)."""


# ---------------------------------------------------------------------------------------------------- plans
def plans(store: MetadataStore, domain: str) -> list[GoldPlan]:
    """Every enabled gold dataset's plan, from the store; dimensions first."""
    out = []
    for ref in store.datasets(domain):
        if ref.kind not in GOLD_KINDS or not ref.is_enabled:
            continue
        view = store.dataset_view(ref.source, ref.dataset)
        drivers = [str(i["dataset"]) for i in view["manifest"].get("inputs") or ()]
        keys = {d: store.business_keys(*d.split(".", 1)) for d in drivers}
        out.append(plan_from_view(view, keys))
    return order(out)


# ---------------------------------------------------------------------------------------------------- watermarks
def silver_watermark(store: MetadataStore, dataset: str, tz: str) -> datetime | None:
    """How far an input has loaded in source time (ADR-018): the end of the newest delivery window silver has taken
    (silver's bookmark counts the file records it took; an empty delivery still moves it)."""
    source, name = dataset.split(".", 1)
    taken = int(store.bookmarks(source, name).get(SILVER_BOOKMARK, "0"))
    days = [f.window for f in store.dataset_files(source, name)[:taken] if f.status == "loaded" and f.window]
    return W.day_end(max(days), tz) if days else None


def _time(value: str) -> datetime | None:
    try:
        t = datetime.fromisoformat(value)
    except ValueError:
        return None
    return t if t.tzinfo else None


def feed_marks(store: MetadataStore, dataset: str) -> dict[str, datetime | None]:
    """An input fed per feed (one file per dealer): ingestion's per-feed bookmarks, each the feed's newest data time."""
    source, name = dataset.split(".", 1)
    marks = store.bookmarks(source, name)
    return {k: _time(v) for k, v in marks.items() if k not in NOT_FEEDS and "." not in k and _time(v) is not None}


# ---------------------------------------------------------------------------------------------------- the run
def run_gold(store: MetadataStore, tables: GoldTables, engine: GoldEngine, domain: str, env: str, run_id: str,
             as_of: str, tz: str, params: dict[str, str] | None = None, only: set[str] | None = None) -> GoldReport:
    """Build every gold table of the domain for business date `as_of` (or only the named tables)."""
    ps = plans(store, domain)
    ctx = Ctx(store, tables, engine, run_id, as_of, tz, {"business_tz": tz, **(params or {})}, {p.name: p for p in ps})
    store.start_run(run_id, domain, env, "gold")
    out = []
    for p in ps:
        if only is not None and p.name not in only:
            continue
        t0 = time.monotonic()
        view = store.dataset_view(p.schema, p.name)
        store.start_dataset_run(run_id, p.schema, p.name, view["fingerprint"], view)
        lease = f"gold:{p.schema}.{p.name}"
        holder = store.acquire_lease(lease, run_id, LEASE_SECONDS)
        if holder is not None:  # ORCH-04: another run writes this table now
            one = GoldTable(p.name, "skipped", reason=busy(store, run_id, p.schema, p.name, lease, holder))
            out.append(one)
            continue
        try:
            one = _dimension(ctx, p) if p.kind == "gold_dimension" else _fact(ctx, p)
        except (CheckFailed, GoldPlanError) as exc:
            one = _fail(ctx, p, "gold.checks" if isinstance(exc, CheckFailed) else "gold.plan", exc)
        except Exception as exc:  # an engine or edge error: nothing published, the next run takes the window again
            one = _fail(ctx, p, "gold.build", exc)
        finally:
            store.release_lease(lease, run_id)
        one.seconds = round(time.monotonic() - t0, 1)
        out.append(one)
    return GoldReport(run_id, as_of, store.finish_run(run_id), out)


def _columns(ctx: Ctx, dataset: str) -> list[tuple[str, str]]:
    """An input's contract columns as (name, engine type), for an input silver has not written yet."""
    from dataplatform.contract import TYPES

    return [(c.name, TYPES[c.logical_type]) for c in ctx.store.columns(*dataset.split(".", 1))]


def _step(ctx: Ctx, p: GoldPlan, step: str, status: str, counts: dict[str, int] | None = None,
          message: str = "") -> None:
    ctx.store.log_step(StepRecord(ctx.run_id, p.schema, p.name, step, status, dict(counts or {}), message[:4000]))


def _fail(ctx: Ctx, p: GoldPlan, step: str, exc: Exception) -> GoldTable:
    msg = f"{type(exc).__name__}: {str(exc)[:1500]}"
    ctx.store.log_error(ctx.run_id, p.schema, p.name, step, type(exc).__name__, msg)
    ctx.store.finish_dataset_run(ctx.run_id, p.schema, p.name, "failed", message=msg)
    return GoldTable(p.name, "failed", reason=msg)


def _dimension(ctx: Ctx, p: GoldPlan) -> GoldTable:
    silver = ctx.tables.silver(p.inputs[0].dataset, _columns(ctx, p.inputs[0].dataset))
    rows = ctx.engine.dimension(p, silver, ctx.run_id)
    n, dup = ctx.engine.count(rows), ctx.engine.duplicates(rows, (p.surrogate,))
    _step(ctx, p, "detect.KEY-01", "failed" if dup else "succeeded", {"members": n, "duplicate_keys": dup},
          f"{dup} surrogate keys repeat in {p.name}" if dup else "")
    if dup:
        raise CheckFailed(f"{p.name}: {dup} surrogate keys repeat (hash collision or a broken history)")
    written = ctx.tables.write_dimension(p, rows)
    counts = {"members": n, "rows_written": written}
    _step(ctx, p, "gold.write", "succeeded", counts)
    ctx.store.finish_dataset_run(ctx.run_id, p.schema, p.name, "succeeded", counts)
    return GoldTable(p.name, "succeeded", counts)


# ---------------------------------------------------------------------------------------------------- facts
@dataclass
class _Inputs:
    frames: dict[str, Frame]
    marks: dict[str, datetime | None]  # non-feed inputs: watermark in source time
    feeds: dict[str, tuple[dict[str, datetime | None], dict[str, datetime]]]  # input -> (feed marks, expected)
    applied: list[BackfillPlan]  # correction plans this run applies for the first time
    forced: set[str]  # windows the active plans name (rebuilt whatever the lookback)
    corrections: int = 0
    skipped_corrections: int = 0
    versions: dict[str, int | None] = field(default_factory=dict)  # input -> silver version read (INSP-01)


def _active_plans(ctx: Ctx, dataset: str) -> list[BackfillPlan]:
    source, name = dataset.split(".", 1)
    return [b for b in ctx.store.backfill_plans(source, name) if b.activate_on <= ctx.as_of]


def _corrections(ctx: Ctx, p: GoldPlan, dataset: str, active: list[BackfillPlan]) -> tuple[list[Correction], int]:
    """Active corrections for one input. When a correction moves a row between windows (it corrects the fact's own
    window column), it is applied only if its plan names both windows: a correction never restates a window by
    surprise (ADR-017). Those it cannot apply are counted."""
    source, name = dataset.split(".", 1)
    named = {b.plan_id: set(b.windows) for b in active}
    out, skipped = [], 0
    for c in ctx.store.corrections(source, name, ctx.as_of):
        if c.status != "active" or c.plan_id not in named:
            continue
        moves = p.window is not None and c.field == p.window.column
        if moves and not {str(c.was), str(c.value)} <= named[c.plan_id]:
            skipped += 1
            continue
        out.append(c)
    return out, skipped


def _inputs(ctx: Ctx, p: GoldPlan, done: dict[str, str]) -> _Inputs:
    got = _Inputs({}, {}, {}, [], set())
    for i in p.inputs:
        got.versions[i.dataset] = ctx.tables.silver_version(i.dataset)
        frame = ctx.tables.silver(i.dataset, _columns(ctx, i.dataset))
        active = _active_plans(ctx, i.dataset)
        withdrawn = [b for b in active if EVERY_WINDOW in b.windows]  # VOID-01: every fact on the input reruns
        got.applied += [b for b in withdrawn if f"plan.{b.plan_id}" not in done]
        got.forced |= {EVERY_WINDOW for b in withdrawn if f"plan.{b.plan_id}" not in done}
        if p.corrections:
            active = [b for b in active if b not in withdrawn]
            got.applied += [b for b in active if f"plan.{b.plan_id}" not in done]
            got.forced |= {w for b in active for w in b.windows}
            fixes, skipped = _corrections(ctx, p, i.dataset, active)
            got.corrections, got.skipped_corrections = got.corrections + len(fixes), got.skipped_corrections + skipped
            if fixes:
                frame = ctx.engine.overlay(frame, fixes, tuple(ctx.store.business_keys(i.source, i.name)))
        got.frames[f"{i.source}__{i.name}"] = frame
        feeds = feed_marks(ctx.store, i.dataset)
        if feeds:
            got.feeds[i.dataset] = (feeds, _expected_feeds(ctx, p, feeds))
        else:
            got.marks[i.dataset] = silver_watermark(ctx.store, i.dataset, ctx.tz)
    _trace(ctx, p, got)
    return got


def _expected_feeds(ctx: Ctx, p: GoldPlan, feeds: dict[str, datetime | None]) -> dict[str, datetime]:
    """The members a per-feed input owes a file for: those of the fact's dimension the feed keys belong to, from
    their first version in the source (a dealer that joined later owes nothing for earlier months). Without such a
    dimension, the feeds seen so far."""
    for d in p.dimensions:
        dim = ctx.tables.gold(ctx.plans[d.name])
        if dim is None or len(d.business_key) != 1:
            continue
        seen = ctx.engine.first_seen(dim, d.business_key[0])
        if set(feeds) <= set(seen):
            return seen
    return {}


def _complete(ctx: Ctx, p: GoldPlan, window: str, got: _Inputs) -> tuple[bool, list[str]]:
    """ADR-018: every input caught up past the window's end (per-feed inputs: every expected feed delivered it).
    A row the build says still waits for an input (`_input_complete` false) is flagged on its own (finish)."""
    assert p.window is not None
    unit, waits = p.window.unit, []
    for ds, mark in got.marks.items():
        if not W.caught_up(window, unit, ctx.tz, mark):
            waits.append(ds)
    for ds, (feeds, expected) in got.feeds.items():
        owed = {m for m, first in expected.items() if first < W.end(window, unit, ctx.tz)} if expected else set(feeds)
        ok, missing = W.feeds_caught_up(window, unit, ctx.tz, feeds, owed)
        if not ok:
            waits.append(f"{ds} ({len(missing)} feeds, e.g. {', '.join(missing[:3])})")
    return not waits, waits


def _fact(ctx: Ctx, p: GoldPlan) -> GoldTable:
    assert p.window is not None
    unit, tz = p.window.unit, ctx.tz
    done = ctx.store.bookmarks(p.schema, p.name)
    got = _inputs(ctx, p, done)
    built = ctx.engine.build(p, got.frames, ctx.params)
    dims = {d.name: ctx.tables.gold(ctx.plans[d.name]) for d in p.dimensions}
    missing = [n for n, f in dims.items() if f is None]
    if missing:
        raise CheckFailed(f"{p.name}: dimensions {missing} are not built")
    built = ctx.engine.hold(built)
    keyed = _key_lookup(ctx, p, got, built, dims)
    published = ctx.tables.gold(p)
    first, also = _range(ctx, p, got, published)
    candidate = ctx.engine.window_numbers(p, keyed, first, also)
    waiting = ctx.engine.rows_incomplete(p, keyed)
    complete, why = {}, {}
    for w in candidate:
        complete[w], why[w] = _complete(ctx, p, w, got)
        if w in waiting:
            why[w].append("rows waiting for an input")
    rows = ctx.engine.hold(ctx.engine.finish(p, ctx.engine.only_windows(p, keyed, first, also), complete, ctx.run_id))
    try:
        counts = _checks(ctx, p, rows, built, first, also)
    except CheckFailed:
        ctx.engine.release(built, keyed, rows)
        raise
    before = ctx.engine.window_numbers(p, published, first, also) if published is not None else {}
    after = ctx.engine.window_numbers(p, rows, first, also)
    changed = sorted(w for w in set(before) | set(after) if before.get(w, (None, ""))[1] != after.get(w, (None, ""))[1])
    if p.on_incomplete == "hold":
        changed = [w for w in changed if complete.get(w, True) and w not in waiting]
    written = ctx.tables.replace_windows(p, ctx.engine.only_windows(p, rows, AFTER_ALL, set(changed)), changed) \
        if changed else 0
    ctx.engine.release(built, keyed, rows)
    last = _time(done.get("published_through", ""))
    waited = set(filter(None, done.get("feeds_waiting", "").split(",")))
    applied_windows = {w for b in got.applied for w in b.windows}
    if EVERY_WINDOW in applied_windows:
        applied_windows |= set(changed)
    restated = []
    for w in changed:
        b, a = before.get(w, (None, ""))[0], after.get(w, (None, ""))[0]
        if W.is_restatement(b, a, W.end(w, unit, tz), last, w in waited):
            restated.append(Restatement(p.name, w, "HIST-05" if w in applied_windows else "LATE-01",
                                        _numbers(b), _numbers(a)))
    incomplete = sorted(w for w, ok in complete.items() if not ok or w in waiting)
    counts.update(windows_rebuilt=len(after), windows_replaced=len(changed), rows_written=written,
                  windows_incomplete=len(incomplete), restatements=len(restated), corrections=got.corrections,
                  corrections_not_applied=got.skipped_corrections)
    _record(ctx, p, got, restated, incomplete, why, counts)
    marks = {f"input.{ds}": m.isoformat() for ds, m in got.marks.items() if m is not None}
    marks |= {f"input.{ds}": min(t for t in f.values() if t is not None).isoformat() for ds, (f, _) in got.feeds.items()}
    marks |= {f"plan.{b.plan_id}": ctx.as_of for b in got.applied}
    marks["published_through"] = W.day_end(ctx.as_of, tz).isoformat()
    marks["feeds_waiting"] = ",".join(sorted(w for w in incomplete if any(" feeds, " in x for x in why[w])))
    ctx.store.finish_dataset_run(ctx.run_id, p.schema, p.name, "succeeded", counts, bookmarks=marks)
    return GoldTable(p.name, "succeeded", counts, changed, incomplete, restated, [b.plan_id for b in got.applied])


def _numbers(n: W.Numbers | None) -> dict[str, float]:
    return {"rows": float(n.rows), **dict(n.sums)} if n else {}


LEDGER_STEP = "gold.ledger"
INPUTS_STEP, QUERY_STEP = "gold.inputs", "gold.query"


def _trace(ctx: Ctx, p: GoldPlan, got: _Inputs) -> None:
    """INSP-01: the exact inputs (silver table and version) and the query this run built from, so one window can
    be rebuilt later from the run log alone (gold/inspect.py). An unknown version is recorded as -1."""
    versions = {f"version.{ds}": (-1 if v is None else v) for ds, v in sorted(got.versions.items())}
    named = ", ".join(f"{ds}@{v}" for ds, v in sorted(got.versions.items()))
    _step(ctx, p, INPUTS_STEP, "succeeded", versions, f"as_of={ctx.as_of}; inputs={named}")
    _step(ctx, p, QUERY_STEP, "succeeded", {}, ctx.engine.query_text(p, ctx.params))
RESTATEMENT_STEP = "gold.restatement"


def _range(ctx: Ctx, p: GoldPlan, got: _Inputs, published: Frame | None) -> tuple[str, set[str]]:
    """The windows this run recomputes: the lookback from as_of, plus the windows active plans name and those with
    rows on the unknown member; every window when a withdrawn batch's plan says so (VOID-01)."""
    if p.window is None:
        raise GoldPlanError(f"{p.name}: a fact needs a window")
    first = W.range_start(ctx.as_of, p.window.unit, p.window.lookback, ctx.tz)
    if EVERY_WINDOW in got.forced:  # a withdrawn batch: recompute every window, replace only those that changed
        first = BEFORE_ALL
    return first, (got.forced - {EVERY_WINDOW}) | ctx.engine.windows_with_unknown(p, published)


def _key_lookup(ctx: Ctx, p: GoldPlan, got: _Inputs, built: Frame, dims: dict[str, Frame]) -> Frame:
    """Surrogate keys by the as-of joins (ADR-005), the rows-in = rows-out check and the row ledger (LEDG-01)."""
    keyed = ctx.engine.hold(ctx.engine.add_keys(p, built, dims))
    rows_in, rows_out = ctx.engine.count(built), ctx.engine.count(keyed)
    culprit = "" if rows_in == rows_out else _which_join(ctx, p, built, dims, rows_in)
    _step(ctx, p, "gold.keys", "succeeded" if rows_in == rows_out else "failed",
          {"rows_in": rows_in, "rows_out": rows_out}, culprit)
    _ledger(ctx, p, got, (rows_in, rows_out), keyed)
    if rows_in != rows_out:  # a fact row lost or multiplied by a join: never published
        raise CheckFailed(f"{p.name}: {rows_in} rows built, {rows_out} after the key lookup; {culprit}")
    return keyed


def _which_join(ctx: Ctx, p: GoldPlan, built: Frame, dims: dict[str, Frame], rows_in: int) -> str:
    """The first dimension join that changed the row count (run only when the total moved): names the step without
    a rerun by hand (LEDG-01)."""
    for i, d in enumerate(p.dimensions):
        n = ctx.engine.count(ctx.engine.add_keys(replace(p, dimensions=p.dimensions[: i + 1]), built, dims))
        if n != rows_in:
            return (f"the join to {d.name} on {list(d.business_key)} as of {d.as_of} turned {rows_in} rows into {n}: "
                    f"its members overlap in time or repeat a business key")
    return "the key lookup changed the rows"


def _ledger(ctx: Ctx, p: GoldPlan, got: _Inputs, sizes: tuple[int, int], keyed: Frame) -> dict[str, int]:
    """LEDG-01: rows read per input, rows the build gave, and per dimension join rows matched, on the unknown member
    and not applicable. Each join's three counts must add up to the rows after the lookup, else the run fails."""
    rows_in, rows_out = sizes
    counts = {f"read.{ds}": ctx.engine.count(f) for ds, f in sorted(got.frames.items()) if f is not None}
    counts.update(built=rows_in, after_keys=rows_out)
    off = []
    for key, t in ctx.engine.key_tally(p, keyed).items():
        counts.update({f"{key}.{w}": n for w, n in t.items()})
        if sum(t.values()) != rows_out:
            off.append(f"{key}: {sum(t.values())} of {rows_out}")
    balanced = not off and rows_in == rows_out
    _step(ctx, p, LEDGER_STEP, "succeeded" if balanced else "failed", counts,
          "" if balanced else f"the ledger does not balance: {'; '.join(off) or f'{rows_in} built, {rows_out} keyed'}")
    if off:
        raise CheckFailed(f"{p.name}: the ledger does not balance: {'; '.join(off)}")
    return counts


def _checks(ctx: Ctx, p: GoldPlan, rows: Frame, built: Frame, first: str, also: set[str]) -> dict[str, int]:
    """Checks before publish (ADR-006): grain unique (KEY-01), orphan rate (LATE-02), reconcile with what the build
    gave (no row or quantity lost by the harness). A failure raises: nothing is written."""
    n = ctx.engine.count(rows)
    counts = {"rows": n}
    if p.grain_unique:
        dup = ctx.engine.duplicates(rows, p.grain)
        _step(ctx, p, "detect.KEY-01", "failed" if dup else "succeeded", {"rows": n, "duplicate_grain": dup},
              f"{dup} rows repeat the grain {list(p.grain)}" if dup else "")
        if dup:
            raise CheckFailed(f"{p.name}: {dup} rows repeat the grain")
    unknown = ctx.engine.unknown_rows(p, rows)
    orphans = max(unknown.values(), default=0)
    counts["orphan_rows"] = orphans
    if orphans:
        rate = orphans / n if n else 0.0
        over = p.orphan_rate_max is not None and rate > p.orphan_rate_max
        _step(ctx, p, "detect.LATE-02", "failed" if over else "warned",
              {"orphan_rows": orphans, "rows": n, **{f"unknown_{k}": v for k, v in unknown.items()}},
              f"{orphans} rows on the unknown member ({rate:.4%}; limit {p.orphan_rate_max}); re-keyed when the "
              f"member arrives")
        if over:
            raise CheckFailed(f"{p.name}: orphan rate {rate:.4%} above {p.orphan_rate_max}")
    if p.reconcile:
        cols = tuple(c for _, c in p.reconcile.measures)
        want = ctx.engine.totals(ctx.engine.only_windows(p, built, first, also), cols)
        have = ctx.engine.totals(rows, cols)
        bad = {c: (want[c], have[c]) for c in cols if abs(want[c] - have[c]) > p.reconcile.tolerance.get(c, 0.0)}
        _step(ctx, p, "gold.reconcile", "failed" if bad else "succeeded",
              {f"{c}_total": int(round(have[c])) for c in cols}, f"differs from the build: {bad}" if bad else "")
        if bad:
            raise CheckFailed(f"{p.name}: totals differ from the build: {bad}")
    return counts


def _record(ctx: Ctx, p: GoldPlan, got: _Inputs, restated: list[Restatement], incomplete: list[str],
            why: dict[str, list[str]], counts: dict[str, int]) -> None:
    for b in got.applied:
        _step(ctx, p, "detect.HIST-05", "warned", {"windows": len(b.windows), "corrections": got.corrections},
              f"correction plan {b.plan_id} applied from {b.activate_on}: windows {', '.join(b.windows)} rebuilt "
              f"({b.reason[:200]}; recorded by {b.recorded_by})")
    plans = ", ".join(b.plan_id for b in got.applied)
    for r in restated:  # RSTM-01: one structured record per changed window, read by ops_restatements
        reason = f"correction plan {plans}" if r.detector == "HIST-05" else "late data"
        nums = {f"{k}.before": v for k, v in r.before.items()} | {f"{k}.after": v for k, v in r.after.items()}
        _step(ctx, p, RESTATEMENT_STEP, "warned", nums, f"window={r.window}; reason={reason}; detector={r.detector}")
    for det in ("LATE-01", "HIST-05"):
        mine = [r for r in restated if r.detector == det]
        if mine:
            _step(ctx, p, f"detect.{det}", "warned", {"windows_restated": len(mine)},
                  "restated: " + "; ".join(f"{r.window} {r.before} -> {r.after}" for r in mine)[:3900])
    waits = sorted({w for ws in why.values() for w in ws})
    _step(ctx, p, "gold.completeness", "succeeded", {"windows_incomplete": len(incomplete)},
          (f"flagged incomplete: {', '.join(incomplete[:12])}{' ...' if len(incomplete) > 12 else ''}; waiting for "
           f"{'; '.join(waits)[:1500]}") if incomplete else "every window complete")
    _step(ctx, p, "gold.write", "succeeded", counts)


def summary(report: GoldReport) -> dict[str, Any]:
    """The run in plain numbers (for reports)."""
    return {t.name: {"status": t.status, "counts": t.counts, "replaced": len(t.replaced),
                     "restated": [(r.window, r.detector) for r in t.restated], "seconds": t.seconds}
            for t in report.tables}
