---
title: "W1d gold harness: the two pipeline kinds, the tyre facts, incidents, decisions, spec gaps, notes for W1e"
audience: [architect, engineer]
purpose: "What session W1d built (silver to gold), the decisions taken without the owner, and what W1e inherits"
status: draft
last-reviewed: 2026-11-05
related:
  - adoption.md
  - W1C-SILVER.md
  - W5-DECISIONS.md
  - gold_layer_design.md
---

# W1d gold harness

Branch `w1-core-engine`. Specs: ADR-003, -004, -005, -006, -017, -018; D18, R3; DPC-0046, -0047, -0059, -0079…0084;
DAC LATE-01, LATE-02, HIST-05, KEY-01, DUP-02, SERV-02; the tyre slice's `manifest/gold/`, `manifest/metrics/` and the
contracts `fact_otif`, `fact_sales`, `fact_scrap`, `dim_dealer`, `dim_material`, `dim_press`.

## The shape

```
metadata store ── gold manifests (loader.load_gold: source gold_<domain>) ──▶ gold/plan.py (kernel: the linter rules)
                                                                                 │
silver (Delta) ──read edge──▶ gold/harness.py (kernel) ──────────────────────────┤
  DeltaGoldTables.silver      dimensions:  engine.dimension (the common one-to-one pipeline) ─▶ KEY-01 ─▶ replace whole
                              facts:       input watermarks ─▶ corrections overlay ─▶ build() ─▶ keys as of the event
                                           ─▶ recompute range ─▶ _is_complete, _run_id ─▶ checks ─▶ replace changed
                                           windows ─▶ restatements logged ─▶ bookmarks
gold (Delta) ◀──write edge── DeltaGoldTables.write_dimension / replace_windows (replaceWhere on the window column)
gold/daybyday.py (kernel) + world/domains/tyre_gold.py (the judge): gold per world day against the oracle
semantic/answers.py: the metric views' known answers asked of gold (DP_GOLD_METRICS)
```

The kernel (`src/dataplatform/gold/`) names no engine and no product (R1, tested). The engine operations
(`GoldEngine`) and the edges (`GoldTables`, `GoldReader`) are ports (`gold/ports.py`); the PySpark and Delta adapter
is `adapters/spark_gold.py`. The only hand-written per-fact code is the build (ADR-003, R3):

| Fact | Build (D18) | Grain | Window (lookback) | Dimensions as of | Corrections |
|---|---|---|---|---|---|
| `fact_otif` | SQL `models/tyre/gold/sales/fact_otif.sql` | order line | promised date, day (35) | dealer, material at `order_ts` | yes |
| `fact_sales` | SQL `models/tyre/gold/sales/fact_sales.sql` | dealer, material, month | sale month (3) | dealer, material at `sale_month` | yes (none planned) |
| `fact_scrap` | dataframe `models/tyre/gold/manufacturing/scrap.py:build` | press, material, hour | cure hour (48) | press, material at `cure_hour` | no |

Dimensions `dim_dealer`, `dim_material`, `dim_press` go through the common pipeline with no code.

## What the harness does, in order

1. **Plans** from the store (`harness.plans`): every gold dataset's manifest and contract columns; a dimension's
   business key is its input's primary key in the store. The linter rules of ADR-003 and ADR-006 refuse: a build on a
   one-to-one table, a modelled fact without a build or a window, an unknown build kind or window unit, a window or
   key column not in the contract, a materialised view with two inputs or corrections.
2. **Dimensions** (one-to-one): every silver version becomes a member; surrogate key = the first 15 hex digits of
   SHA-256 of `"<source.dataset>|<business key>|<version start, UTC>"` (`gold/keys.py`; the Spark expression gives the
   same number, tested); the first version is valid from 1900-01-01, the current one to 9999-12-31; reserved members
   -1 `unknown` and -2 `not applicable`; `_source_valid_from`, `_is_deleted`, `_run_id` kept beside the contract
   columns. Key unique per dimension (`detect.KEY-01`, failed blocks the write). Replaced whole each run.
3. **Input watermarks** (ADR-018): an input has loaded, in source time, up to the end of the newest delivery window
   silver has taken (its bookmark `silver.files` counts the file records it took). A per-feed input (one file per
   dealer) is caught up for a window when every member that owes a file has delivered it: the members are those of the
   fact's dimension whose business key the feed bookmarks name, and a member owes months from its first version on.
4. **Corrections** (ADR-017): `store.corrections(source, dataset, as_of)` with status active; laid over every silver
   version of their key before the build (silver itself never changes). A correction that moves a row between windows
   is applied only when its plan names both windows. The windows of active plans are rebuilt whatever the lookback.
   The first run on or after a plan's date logs `detect.HIST-05` (warned) and records `plan.<id>` in the bookmarks.
5. **Build**: SQL builds read their inputs as views `<source>__<dataset>` and take `:business_tz`; a dataframe build
   is `build(inputs, params)`. No IO inside (purity checked by shift-left).
6. **Keys** as of the event (ADR-005): left join on the business key with `valid_from <= t < valid_to`; no member ->
   -1, no business key -> -2. Rows in must equal rows out (`gold.keys`), or nothing is published.
7. **Recompute range**: windows from `lookback` units back on, plus the active plans' windows, plus published
   windows still holding rows on -1 (so a late member is re-keyed even outside the lookback, ADR-004).
8. **Completeness** per window: every input caught up past its end and every expected feed delivered it. A row the
   build says still waits for its own input (`_input_complete`; scrap does, for inspections not yet filed) is flagged
   on its own, so one plant's missing lab file does not flag the other plant's rows of the same hour.
   `on_incomplete: flag` publishes with `_is_complete = false`; `hold` would keep such windows out.
9. **Checks before publish**: grain unique (`detect.KEY-01`), rows on the unknown member (`detect.LATE-02`: warned,
   failed above `orphan_rate_max`), totals equal the build's (`gold.reconcile`). A failure publishes nothing.
10. **Replace** (ADR-006): per window a digest of every column but `_run_id`; only windows whose digest changed are
    replaced, in one Delta commit (`replaceWhere` on the window column); a rerun writes nothing.
11. **Restatements**: logged as `detect.LATE-01` (late data) or `detect.HIST-05` (a correction plan's windows), with
    each window's numbers before and after (rows and the reconcile measures). Rule in decision 6.
12. **Bookmarks** on success: `input.<dataset>` (the watermarks), `published_through`, `feeds_waiting`, `plan.<id>`.

## Gold against the oracle

### Full scale (`SCALE=1.0`), 4 Nov 2026: days 1 to 11, then out of memory

Local PostgreSQL (`dp_meta_w1d`, `tyre_world_w1d`), one Spark session (`local[2]`, 1 GB driver) for all days. Gold
took 20 to 40 s a day. Gold equalled the oracle on days 1 to 7; on days 8 to 11 one business day and plant differed
(2026-11-15 P-CHN): numbers equal, only the completeness flag, because P-HUN's rejected lab files (SCH-02) flagged
the whole hour, P-CHN rows included. Fixed afterwards (a row waiting for its own input is flagged alone; step 8) and
confirmed at scale 0.1 below. On day 12 the 1 GB driver ran out of heap during silver (the Mac had 2.7 to 3.4 GB of
swap in use), so the run stopped before day 14 and wrote no report. `run_platform.py` now starts a fresh Spark
session each day (`--keep-spark` keeps one); every table is on disk, so nothing is lost.

| Day | Gold at full scale | Incident judged on gold |
|---|---|---|
| 1-3 | = oracle | |
| 4 | = oracle (fact_otif 32,135 rows) | DEL-01 handled: 0 of 5 deleted lines in fact_otif |
| 5 | = oracle | LATE-01 handled: October flagged on days 1-4, complete and restated on day 5 (7,392 -> 9,404 rows, 602,101 -> 765,689 units out) |
| 6 | = oracle | DUP-01 handled: sell-out unchanged, no restatement |
| 7 | = oracle (12 scrap hours flagged: P-HUN's held lab files) | |
| 8-10 | 1 day-plant flag differs (fixed since) | |
| 11 | flag as above; every number equal | HIST-05 handled: plan applied, only its windows restated (2026-11-13 .. 11-18 among them); LATE-02 flagged: 114 D-9001 lines on -1 = oracle 114, 87,946 rows = oracle |

### Scale 0.1, 14 days (fresh Spark session each day)

`make run DAYS=14 SCALE=0.1` with `DP_RUN_META_DSN=dbname=dp_meta_w1d WORLD_PG_DSN=dbname=tyre_world_w1d`, 5 Nov 2026:
1493 s in all, silver 890 s, gold 357 s (18 to 35 s a day). Bronze, silver and gold equal the oracle on
every one of the 14 days (`build/run/report.json`; every gold measure, every promised-date window, every business
day and plant, every month and region, October's completeness and the restatements of the day). An earlier attempt
at this scale with one Spark session for all days stalled on day 13 (driver heap full, 3.5 GB of swap in use); the
fresh session per day fixed it.

| Day | Date | Gold s | OTIF rows | OTIF windows replaced / incomplete | Lines on -1 | Sales months replaced / incomplete | Sell-out units | Scrap rows | Tyres scrapped | Restated | = oracle |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2026-11-09 | 17.9 | 578 | 8 / 8 | 0 | 2 / 2 | 156,993 | 1,536 | 637 | - | yes |
| 2 | 2026-11-10 | 31.3 | 1,254 | 9 / 9 | 0 | 2 / 2 | 307,796 | 3,072 | 1,427 | - | yes |
| 3 | 2026-11-11 | 33.2 | 2,181 | 10 / 10 | 0 | 2 / 2 | 461,668 | 4,608 | 2,178 | - | yes |
| 4 | 2026-11-12 | 18.3 | 3,006 | 11 / 10 | 0 | 2 / 2 | 602,101 | 6,144 | 2,938 | - | yes |
| 5 | 2026-11-13 | 21.9 | 3,747 | 12 / 10 | 0 | 2 / 1 | 765,689 | 7,680 | 3,705 | 2026-10-01 (LATE-01) | yes |
| 6 | 2026-11-14 | 25.2 | 4,378 | 12 / 10 | 0 | 1 / 1 | 765,689 | 9,216 | 4,425 | - | yes |
| 7 | 2026-11-15 | 24.9 | 4,938 | 14 / 10 | 0 | 1 / 1 | 765,689 | 10,752 | 5,047 | - | yes |
| 8 | 2026-11-16 | 22.4 | 5,968 | 15 / 10 | 0 | 1 / 1 | 765,689 | 12,288 | 5,800 | - | yes |
| 9 | 2026-11-17 | 23.0 | 6,587 | 15 / 10 | 0 | 1 / 1 | 765,689 | 13,824 | 6,541 | - | yes |
| 10 | 2026-11-18 | 21.7 | 7,380 | 15 / 10 | 0 | 1 / 1 | 765,689 | 15,360 | 7,312 | - | yes |
| 11 | 2026-11-19 | 28.5 | 8,158 | 17 / 10 | 6 | 1 / 1 | 765,689 | 16,896 | 8,065 | 2026-11-13, 2026-11-14, 2026-11-15, 2026-11-16, 2026-11-17, 2026-11-18 (HIST-05) | yes |
| 12 | 2026-11-20 | 24.8 | 8,984 | 15 / 10 | 10 | 1 / 1 | 765,689 | 18,432 | 8,806 | - | yes |
| 13 | 2026-11-21 | 35.3 | 9,525 | 15 / 10 | 0 | 1 / 1 | 765,689 | 19,968 | 9,570 | - | yes |
| 14 | 2026-11-22 | 28.2 | 10,516 | 15 / 10 | 0 | 1 / 1 | 765,689 | 21,504 | 10,339 | - | yes |

On days 11 and 12 the sales cells `2026-11/west` and `2026-11/unknown` differ by region only, and are reported as
explained: D-9001's sell-in sits on the unknown member until its record arrives (the oracle knows its region is
west); the `all` cells equal the oracle.

### Full scale is owed

Full scale (`SCALE=1.0`) was not completed on this laptop (8 GB, 2.7 to 3.5 GB of swap in use by the time silver
reached day 12): the attempt above ran from 21:58 to about 23:08 on 4 Nov, equalled the oracle through day 7 (and in
every number through day 11), then the 1 GB driver ran out of Java heap (`java.lang.OutOfMemoryError: Java heap
space`) in day 12's silver run, and no report was written. The full-scale 14-day check (and the full-scale known
answers through `make gold-known-answers`) is owed on Databricks Free Edition, where memory is not the limit.


## Incidents gold owns

Scale 0.1 run (the same verdicts on the full-scale attempt through day 11, with its own counts):

| Day | Incident | Detector | Gold verdict | Evidence |
|---|---|---|---|---|
| 3 | readings-out-of-order | LATE-03 | gold = oracle | scrap per business day and plant equals the oracle (twin check `totals` day 3) |
| 4 | order-lines-deleted | DEL-01 | handled | 0 of 5 deleted lines in fact_otif; fact_otif rows 3,006 = oracle |
| 5 | sellout-file-late | LATE-01 | handled | October flagged incomplete on days 1-4, complete on day 5 and restated (`detect.LATE-01`: 7,392 -> 9,404 rows, 602,101 -> 765,689 units out) |
| 6 | sellout-file-resent | DUP-01 | handled | sell-out per month unchanged (October 765,689), no restatement |
| 7 | lab-column-renamed | SCH-02 | gold = oracle | P-HUN's held hours flagged incomplete (12 hours); P-CHN's rows of the same hours complete |
| 11 | promised-date-corrected | HIST-05 | handled | plan applied on its date; restated windows 2026-11-13 .. 11-18, none outside the plan; OTIF windows equal the corrected truth |
| 11, 12 | new-dealer-late | LATE-02 | flagged | 6 then 10 D-9001 lines on -1 (= oracle); no row dropped (8,158 and 8,984 rows = oracle) |
| 13 | new-dealer-late | LATE-02 | handled | D-9001 arrived: 0 lines on -1 (were 6, 10); rows 9,525 = oracle |
| 13 | stream-replayed | DUP-04 | gold = oracle | scrap totals equal the oracle on day 13 |
| every day | gold windows waiting | SERV-02 (ADR-018) | flagged | OTIF promised dates after today, November sales (no November files in the clock), scrap hours whose lab files have not arrived; `gold.completeness` names what each waits for |


## Known answers (the metric views asked of gold, `build/run/gold_metrics.json`)

| Known answer (manifest) | Spec | Gold at scale 0.1 (oracle of the same run) | Gold at full scale |
|---|---|---|---|
| OTIF %, south, November | 0.8961 | 0.8851 (= oracle 0.8851) | 0.8961 over complete windows on 4 Nov (gold built once over W1c's full-scale day-14 silver); the 14-day full-scale run is owed |
| Order lines, November | 107,188 | 10,216 (= oracle) | 107,188 (same one-off build) |
| Sell-in units, north, November | 1,279,642 | 115,637 (= oracle) | 1,279,642 (same one-off build) |
| Sell-out units, north, October | 150,803 | 150,803 (match: dealer files do not scale) | 150,803 |
| Scrap rate %, P-CHN, 12 Nov | 0.0180 | 0.017981 (match: presses do not scale) | 0.017981 at every scale |

`make gold-known-answers DAYS=14` on this run: 12 passed, 4 failed. The 8 "view equals the oracle of the same run"
cases pass; of the 8 "view equals the manifest's known answer" cases, sell-out and scrap (certified and preview) pass
and OTIF %, Order lines and Sell-in (certified and preview) fail only because they depend on scale (their values
hold at 1.0). OTIF % asked of gold equals the oracle because the catalog fixed gap 1 during the session (catalog
afc7078, `FILTER (WHERE _is_complete)`); before that fix the view gave 0.6706 at full scale (0.3655 on day 6 at
scale 0.05). W5's generated `mv_otif.sql` was regenerated from it.


## Tests (table-driven; local Spark shared by the session)

| File | Cases | What |
|---|---|---|
| `test_gold_plan.py` | 58 | the linter's refusals; plans from the tyre manifests; R1 for the gold kernel; surrogate keys (determinism, zones, reserved members); window arithmetic; completeness from watermarks and feeds; the restatement rule |
| `test_gold_engine.py` | 40 | a table of cases per build: `fact_otif` (11), `fact_sales` (5), `fact_scrap` (6); the common one-to-one pipeline (3 cases: keys equal the Python recipe, validity; reserved members; a rebuild gives the same keys); keys as of the event (6 cases); no row dropped or multiplied (400 rows of every kind); corrections overlay; window digests; row completeness flags (4 cases) |
| `test_gold_harness.py` | 10 | no fact row dropped; replace never append (rerun writes nothing, changed windows replaced whole, others untouched); late member re-keyed outside the lookback; corrections only for the plan's windows (applied and not); completeness and the input watermark bookmark; a failed check holds the last good windows; the step format; a late member file restates the month (LATE-01) and a resend changes nothing; a member that joined later owes nothing |
| `test_gold_ports.py` | 10 | M5 contract tables: `GoldTables` (first write, only named windows change, an empty named window is emptied, idempotent, hour windows, a non-window name refused, dimension replaced whole, unwritten silver is empty with its columns) and `GoldReader` (time zone) |
| `test_obs_steps.py` | +1 | the gold harness's step rows keep the ADR-024 step format |
| `test_twin_gate.py` | +5 | the gate's gold specs from each fact manifest; a planted bad OTIF change (cancelled lines counted) blocks |
| `test_twin_engine.py` (slow) | gold attached | the scenarios' gold checks run on the engine (14 days at scale 0.02) |
| `test_twin_scenarios.py` | updated | `sellout-file-late` no longer waits for DES (gold logs the restatement) |

Runs: `test_gold_*`, `test_obs_steps.py`: 77 passed, 2 skipped (the silver and DES step-format rows, pending as
before), local Spark, 2 min. Kernel and realm suite without Spark (`-m "not slow"`): 1,060 passed, 67 skipped; then
`test_twin_scenarios.py` fixed (25 passed). `test_twin_gate.py` 36 passed, 6 skipped (the workspace rows).
`make gold-known-answers`: above. Not run in this session: `test_twin_engine.py` (slow; 14 world days at scale 0.02
through silver and gold on one Spark) because the laptop's memory was taken by the 14-day runs; its gold checks are
the same verdicts and totals the scale-0.1 run judged. `mypy --strict` clean on the new files (`dataplatform.gold`,
`adapters/spark_gold.py`, `semantic/answers.py`, `world/domains/tyre_gold.py`, `twin/gate.py`, the scrap build;
`contract.py`, `dwd.py`, `tyre.py`, `tyre_oracle.py` keep their earlier errors). shift-left `scan --stage pr`: 0
blocking in `dataplatform/gold`, `semantic`, `world/domains`, `ingest`, `models`; in `adapters` the blocking
findings are all in older files (`sql_source.py`, `spark_delta.py`, `spark_silver.py`), none in `spark_gold.py`;
intended exceptions are marked `shiftleft: allow` with the reason.

## Decisions taken (the owner was not asked)

1. **Gold datasets live in the metadata store** beside the source datasets (`meta.loader.load_gold`), as source
   `gold_<domain>` (the gold schema of ADR-012), grain as primary key, no settings. Their runs, steps and bookmarks
   then sit in the same run log. Ingestion and silver skip kinds `gold_dimension` / `gold_fact`.
2. **Builds live in `models/<domain>/`**; the manifest's `build.ref` is relative to it (a SQL file, or
   `module.path:function`). Domain logic stays out of the kernel and out of the adapter.
3. **Surrogate key recipe**: SHA-256 of a plain text, first 15 hex digits (60 bits, positive in any 64-bit integer),
   so Python and every SQL engine compute the same key. Version start rendered `yyyy-MM-ddTHH:mm:ss.ffffffZ` in UTC.
4. **An event before a member's first version joins that first version** (the first version is valid from
   1900-01-01). Without it, an order placed before the dealer's first change record (or a day-1 order before the
   material copy at 01:00) would sit on -1 forever, and LATE-02's "re-keyed on day 13" could never happen, since
   D-9001's orders predate its first record. `_source_valid_from` keeps the real start for the key and for feeds.
5. **The date first promised** is the promised date of the line's first version in the source order (`_seq_from`), with
   corrections laid over every version; OTIF uses it as its window (contract: "by the date first promised").
6. **When a rebuild is a restatement.** Numbers readers were entitled to treat as final change: the window was
   published complete and its rows or reconcile measures change, or the window was already over when published but
   flagged incomplete because a member's feed had not delivered (`feeds_waiting`), and the rebuild that completes it
   changes its numbers. A provisional number waiting for an input's ordinary lag (the next hour's lab file) is not a
   restatement, nor a period still open. This gives exactly the oracle's restatements: October on day 5 (LATE-01) and
   the corrected promised dates on day 11 (HIST-05), nothing on the other days.
7. **Per-dealer feed bookmarks** (W3's ask of W1b): a file whose rows share one value of a text key column and whose
   key holds a date or time column feeds that value; ingestion keeps one bookmark per feed whose value is the newest
   data time in the file as an instant in the business time zone (`2026-10-01T00:00:00+05:30` for October). The
   scenario's `bookmark_moves` check is on (no longer pending).
8. **Window replace by digest**: a window is replaced only when its content (every column but `_run_id`) changed, so
   a rerun writes nothing and the `_run_id` of a window says which run last changed it.
9. **`cure_hour` is the UTC hour of `cured_ts`** (an instant). The plants' time zones are declared nowhere the
   platform reads (gap 3); the UTC hour gives every row the same business day the world gives it.
10. **Reconcile against the build**, not silver: the harness proves it lost no row or quantity between the build and
    the write; gold against silver-side filters is gap 6. Gold against the world is the day-by-day comparison.
11. **Completeness is window-level and flags** (`on_incomplete: flag`): October is incomplete until the last dealer's
    file (day 5 in the world: D-0042 and the dealers due that day); OTIF days are complete once both feeds have
    loaded past the day; scrap hours also wait for their inspection results.
12. **Metric views are read in the business time zone** (`semantic/answers.py` sets the session zone), so `Day` in
    `mv_scrap` is a business day. On the engine, the warehouse's `TIMEZONE` must be the business zone (gap 2).
13. **The restatement log is the run log** (`detect.LATE-01` / `detect.HIST-05` rows with the numbers before and
    after). Scenario `sellout-file-late`'s "logged as a restatement" check moved from the DES layer to gold.
14. **Cost (4 Nov decision)**: W5's generated `cost/` files, their renderers and `shares()` are removed; the job tag
    convention (`COMPUTE_TAGS`, `compute_tags()`) stays; golden files regenerated; `W5-DECISIONS.md` updated.

## Spec gaps (for the TODO; not invented silently)

1. **`mv_otif` OTIF % counts incomplete promised dates.** Its expression `SUM(on_time_in_full) / COUNT(1)` runs over
   every row of the month, while the known answer (0.8961) and the oracle count promised dates up to today only. Gold
   over complete windows gives 0.8961; the view as written gives a lower number (lines not yet due count as missed).
   Fix in the catalog: `SUM(on_time_in_full) FILTER (WHERE _is_complete) / COUNT(1) FILTER (WHERE _is_complete)`
   (or a view filter), keeping `Order lines` over all rows (its known answer counts every line). Until then the OTIF
   case of W5's pending known-answer tests fails on gold, as it should. *Status 5 Nov: fixed in the catalog
   (afc7078) during the session; regenerated, and the view now equals the oracle on gold.*
2. **Time zone of metric-view dimensions.** `DATE_TRUNC('DAY', cure_hour)` depends on the session zone; the business
   day needs the business zone (warehouse `TIMEZONE` or `from_utc_timestamp` in the expression).
3. **Plant time zones** are only in `world.yaml`; no contract or manifest the platform reads declares them (a plant
   dimension or `mes.press.plant_timezone`). Needed for plant-local hours and shifts.
4. **Per-feed inputs.** DPM cannot say that an input is fed per member (which key column, which dimension says who
   owes a file, from when). The harness infers it from the per-feed bookmarks and the fact's dimensions.
5. **Restatement rule.** ADR-017 says restatements are logged; no spec says when a rebuild is one (decision 6), nor
   where the log lives (run log today; a DES restatement table in W1e?).
6. **`checks.reconcile.against: silver`** does not say the silver-side filter (OTIF: open lines only; the manifest
   says so in a comment). Gold reconciles with its build (decision 10).
7. **Gold has no settings in the registry** (`orphan_rate_max`, lookbacks, `on_incomplete` live in the manifest only)
   and DPM does not say gold manifests are loaded into the store (decision 1).
8. **ADR-005 does not say how an event before a member's first version joins** (decision 4).
9. **The world's due day per dealer** (D-0042 due on day 1) is knowable to the oracle only; on the platform every
   dealer's October is due in the first five days, so October as a whole is flagged until day 5. W3's suggested
   `visible_from` for late-delivery incidents stands.
10. **Dimension contracts** have no column for the source version start or the deleted flag gold keeps beside them
    (`_source_valid_from`, `_is_deleted`).

## Notes for W1e (data trust + streaming)

- Gold writes `detect.KEY-01`, `detect.LATE-02`, `detect.LATE-01`, `detect.HIST-05` and `gold.keys`,
  `gold.reconcile`, `gold.completeness`, `gold.write`; DES's `quality.*` / `recon.*` rows plug in beside them. The
  reconciliation "gold against silver" entries (gap 6) are DES's; the harness's own reconcile stays as the no-loss check.
- Restatements are run-log rows with before and after numbers per window; a DES restatement table and screen can read
  them (decision 13).
- REC-05 (day 12, 7 deliveries lost at the source) shows in gold as OTIF lines not delivered; the oracle's
  `otif_lines_truth` is the number DES's live-source entry must explain.
- Gold input watermarks are in the gold dataset's bookmarks (`input.<dataset>`); a freshness screen for gold can read
  them like the source bookmarks.
- The stream (readings) is no gold input today; a press-hour fact from readings would be a fourth build with an hour
  window and the same harness.
- On Databricks: gold runs where silver runs (`adapters/spark_gold.py` on the cluster session); the bundle needs a gold
  task after silver and the models folder deployed with it; the gold schemas follow ADR-012.
