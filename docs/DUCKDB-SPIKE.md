---
title: "DuckDB spike: the same silver stages and gold builds on DuckDB (SQLFrame) and on Spark"
audience: [architect, engineer]
purpose: "Can one stage logic run on DuckDB for laptop and CI runs while Databricks keeps Spark and Photon? Results, gaps, timing, recommendation, ADR draft"
status: draft
last-reviewed: 2026-11-05
related:
  - W1C-SILVER.md
  - W1D-GOLD.md
  - adoption.md
---

# DuckDB spike: one logic, two engines

Branch `w1-core-engine`. Question (TODO "local DuckDB spike", 5 Nov): can the same pure stage logic (the 13 silver
stages, the gold builds) run on DuckDB for fast laptop and CI runs, while Databricks keeps Spark and Photon?
Approach 1: SQLFrame, which offers the PySpark DataFrame API and compiles it to SQL (through sqlglot) for DuckDB.

**Short answer:** yes. The same stage and build code runs on both engines and gives the same answers: the 143
stage and gold-build test cases pass on Spark and on DuckDB from one set of test tables, and the 14-day world at scale
0.1 equals the oracle on every day in bronze, silver and gold on both engines, with the same known answers. It took 13
rewrites into forms both engines read the same way (none changes what Spark computes) and a DuckDB adapter for the
engine-only pieces. **Speed is mixed**: DuckDB has no JVM and gold is three to four times faster, but SQLFrame spends
its time in Python building SQL text, so the stage tests take 133 s against Spark's 74 s. Recommendation: **adopt
partially** — DuckDB as a second engine in CI (done: the stage and build tests, which CI skipped because it has no
Spark, now run there) and as an option for laptop runs (`make run ENGINE=duckdb`); Spark stays the default locally and
the only engine on Databricks.

## What SQLFrame is

| | |
|---|---|
| Package | `sqlframe` 4.5.0 (MIT licence; sqlglot 30.x underneath, MIT), PyPI, released 14 Oct 2026. The newest, 4.10.0, came out today; 4.5.0 is pinned because it is the newest release older than two weeks. |
| DuckDB | `duckdb` 1.5.6 (MIT), 28 Sep 2026 (2.0.0 is three weeks old but SQLFrame 4.5.0 asks for `duckdb<1.6`). DuckDB needs `pytz` to hand back zoned timestamps. |
| How it plugs in | `sqlframe.activate(engine="duckdb", conn=...)` puts SQLFrame in place of every later `import pyspark...` in the process, so `dataplatform.stages` and `models/` run unchanged. Because it replaces modules for the whole process, **the engine is chosen per Python process**, never per test. |
| Delta | SQLFrame does not read or write Delta. The local Delta tables are read and written by delta-rs (`deltalake` 1.6.6, already used for bronze); DuckDB holds the rows in memory while the stages run. No DuckDB extension is downloaded. |

## What was built

| Piece | Where | What it does |
|---|---|---|
| Engine switch for the tests | `tests/dataplatform/conftest.py` | `--engine spark` (default) or `--engine duckdb` (or `DP_ENGINE`); the `spark` fixture yields the engine's session, so the **same test tables** run on both. `make engine-conformance` runs the two pytest processes. |
| DuckDB session and checkpoint | `adapters/duckdb_frames.py` | `activate()`, a small session (2 threads, 1 GB, UTC), `checkpoint()` (the query run once into a DuckDB table: SQLFrame has no `localCheckpoint`) |
| Silver on DuckDB | `adapters/duckdb_silver.py` | `DuckStageRunner` (the same `run_stages`), `DeltaRsSilverTables` (read by delta-rs; append with an app transaction so a repeated write is skipped; MERGE on `_row_key` by delta-rs) |
| Gold on DuckDB | `adapters/duckdb_gold.py` | `DuckGoldEngine` (the Spark gold engine with five methods replaced), `DeltaRsGoldTables` (dimension overwrite, window replace with a predicate), `DuckGoldReader` (the judge's and the metric views' Spark SQL transpiled to DuckDB by sqlglot) |
| End to end | `scripts/run_platform.py --engine duckdb`, `make run ENGINE=duckdb` | world, ingestion, silver and gold day by day against the oracle, exactly like the Spark run; no Java needed |

The kernel (`silver/`, `gold/`) is unchanged and names neither engine (R1). The stages and the dataframe build import
only `pyspark`; DuckDB and SQLFrame appear only in `adapters/duckdb_*.py`, the test fixture, the run script and the
Makefile. Nothing here can write a governed table on Databricks: the realm bundles and jobs still name the Spark
adapters only. CI (`.github/workflows/aws-databricks.yml`, test job, and `make ci-test`) now also runs the stage and
gold-build tests on DuckDB.

## Conformance per stage and build

The existing table-driven tests, unchanged in what they assert, run once per engine (`make engine-conformance`).
Added: 12 cases that pin exact values across engines (hashes against Python's SHA-256, row keys, record hash, null
placement in window orders) and 4 that pin the known differences per engine. Times are the sum of each group's test
times in one pytest process per engine (`--durations=0`), 5 Nov, nothing else heavy running.

| Stage or build | Cases | Spark | DuckDB | Result | Spark s | DuckDB s |
|---|---|---|---|---|---|---|
| 1 contract check | 5 | pass | pass | equal | 6.1 (JVM start in the first test) | 0.8 |
| 2 projection, extract, rename | 6 | pass | pass | equal (after: empty `F.array()`) | 0.4 | 0.3 |
| 3 hygiene | 9 | pass | pass | equal (after: `dtypes`) | 3.2 | 11.4 |
| 4 parse and type | 10 | pass | pass | equal (after: `try_cast`, `try_to_timestamp`) | 3.0 | 2.4 |
| 5 time | 3 | pass | pass | equal (after: `convert_timezone`, pattern pieces) | 0.6 | 0.3 |
| 6 standardise | 6 | pass | pass | equal | 0.7 | 0.4 |
| 7 personal data | 4 | pass | pass | equal | 0.7 | 0.4 |
| 8 row filter | 4 | pass | pass | equal | 0.7 | 0.4 |
| 9 quality | 3 | pass | pass | equal | 0.5 | 0.5 |
| 10 duplicates | 8 | pass | pass | equal | 2.8 | 1.8 |
| 11 keys (and gold keys as of the event) | 9 | pass | pass | equal; SHA-256 key hashes byte for byte (after: `chr(0)`) | 1.4 | 0.8 |
| 12 history (type 2, full-copy deletes, type 1, snapshot, append, late readings) | 23 | pass | pass | equal (after: `last_value`, null order, `withColumnRenamed`) | 38.0 | 93.8 |
| 13 provenance and record hash | 7 | pass | pass | equal; `_record_hash` equal when no value is null | 1.8 | 9.1 |
| exact values on every engine | 8 | pass | pass | equal | 1.7 | 2.9 |
| known engine differences | 4 | pass | pass | differ as pinned (to_json nulls, timestamp text, default null order) | 1.0 | 1.4 |
| gold fact_otif (SQL) | 11 | pass | pass | equal (India local date both ways) | 4.7 | 1.4 |
| gold fact_sales (SQL) | 5 | pass | pass | equal | 1.3 | 0.5 |
| gold fact_scrap (dataframe) | 6 | pass | pass | equal | 1.7 | 0.6 |
| gold common pipeline: surrogate keys, validity, reserved members, rebuild | 5 | pass | pass | **SHA-256 surrogate keys equal the Python recipe on both** (after: pattern pieces; DuckDB adds hex digits instead of `conv`) | 0.6 | 1.3 |
| gold: no row dropped, corrections, digests, completeness flags | 7 | pass | pass | equal (after: backticks, `StructType[name]`) | 2.8 | 2.4 |
| **all** | **143** | **143 pass** | **143 pass** | | **74 s** (75 s wall) | **133 s** (134 s wall) |

Where DuckDB loses time: the history tests build each frame on top of the last over several batches, and SQLFrame
copies the whole growing sqlglot tree at every DataFrame call (a profile of the slow tests: about two thirds of the
time in `copy.deepcopy` of sqlglot expressions, a quarter in DuckDB itself). Holding each stage's output in a DuckDB
table (`run_stages(..., hold=)`, what `DuckStageRunner` does) halved it (194 s to 92 s); without any hold a three-batch
history test did not finish in five minutes. Hygiene and provenance are slow for the same reason (many `withColumn`
calls on one frame).

## The 14-day run: Spark against DuckDB (scale 0.1)

Same command shape as W1d (`make run DAYS=14 SCALE=0.1`, local PostgreSQL for the sources and the metadata store,
`nice`, nothing else heavy running), 5 Nov 2026, the code as committed:

| | Spark (`ENGINE=spark`, local[2], 1 GB, fresh session per day) | DuckDB (`ENGINE=duckdb`, 2 threads, 1 GB) |
|---|---|---|
| Whole run | 1,272 s | 1,225 s (of which one unexplained 509 s pause, see below) |
| Silver, 14 days | 765 s (41 to 61 s a day) | 969 s; **13 to 39 s a day** except day 5 (543 s) |
| Gold, 14 days | 282 s (14 to 26 s a day) | **96 s** (5 to 10 s a day) |
| Ingestion and checks (the rest; no engine) | 225 s | 160 s |
| Bronze / silver / gold = oracle every day | yes / yes / yes | yes / yes / yes |
| Incidents (DEL-01, SCD-02, LATE-03, DUP-04 in silver; LATE-01, DUP-01, HIST-05, LATE-02 in gold) | handled | handled, same counts |
| Known answers asked of gold | OTIF south Nov 0.8851, order lines 10,216, sell-in north 115,637, sell-out north Oct 150,803, scrap P-CHN 0.017981 | identical to the last digit |
| `make gold-known-answers DAYS=14` | 12 pass, 4 fail (scale-dependent, as in W1d) | 12 pass, 4 fail (the same 4) |
| Needs | Java 17, pyspark, delta-spark | Python only (sqlframe, duckdb, deltalake) |

Day 5 on DuckDB: the nine dataset runs took 36 s together, so the other 507 s were spent outside the stages (the
silver comparison reads, or the Mac pausing; the earlier DuckDB run's day 5 took 75 s with the same data). Not
explained; without it the DuckDB run is about 715 s, silver about 460 s. An earlier DuckDB run without the per-stage
hold took 1,193 s (silver 956 s, 59 to 79 s a day; gold 75 s). W1d's Spark run (4 Nov code) took 1,493 s.

Reading the numbers: on the laptop at this scale DuckDB is a bit faster end to end and much faster for gold; it is
slower in the test suite, where SQLFrame's frame building dominates and the data is tiny. Neither engine is near its
data limit at scale 0.1; full scale was not tried on DuckDB (silver holds every table in memory while it runs, as
read by delta-rs, so the readings table at full scale, 8.7 million rows, would be the test).

## Recommendation: adopt partially, as an adapter

| Use | Verdict | Why |
|---|---|---|
| CI: the stage and gold-build tests on DuckDB | **Adopt (done)** | CI has no Spark, so all 143 stage and build tests were skipped there. They now run on every push in about 2 minutes, Python only; a stage change that breaks the logic is caught in CI, not only on a laptop with Java. |
| CI: a second engine as a portability check | **Adopt (same step)** | A Spark-only or silently different feature shows up at once (the 20 rows above were all found this way). |
| Laptop: `make run ENGINE=duckdb` | **Adopt as an option** | No Java, gold 3x faster, silver about the same or faster once warm; same answers. Spark stays the default because it is the engine Databricks runs. |
| Laptop: the day-to-day test suite on DuckDB | **Drop for now** | 133 s against Spark's 74 s: SQLFrame's frame building costs more than Spark's start-up at test-table sizes. Revisit with a faster SQLFrame. |
| Anything on Databricks, any governed table | **Never** | Spark + Photon there; one table, one engine (record hashes and digests are engine-local). |
| Full scale on the laptop on DuckDB | **Not tried** | Owed with the full-scale Spark check; silver holds each table in memory while it runs. |

Kept regardless of the verdict: the 13 portable rewrites (they also suit Spark 4 and Databricks with ANSI on), the
`hold` hook in `run_stages`, the engine switch in the test fixture, and the conformance cases.

Not done in this spike: Approach 2 (Ibis or plain SQL models) was not tried; SQLFrame 4.10.0 (out today) was not
timed; the DuckDB delta extension was not used (delta-rs already in the run); peak memory of the two runs was not
measured.

## What SQLFrame does not cover (every one the stages, builds and gold engine met)

"Silent" means no error: the run goes on with a wrong value. Those are the dangerous ones; each now has a test case
that runs on both engines (`test_same_values_on_every_engine`, `test_known_engine_differences`).

| # | PySpark feature | On SQLFrame + DuckDB | Silent? | Workaround (where) | Status |
|---|---|---|---|---|---|
| 1 | `F.array()` with no elements (empty `_dq_flags`) | `IndexError` while building the frame | no | `F.expr("CAST(array() AS array<string>)")`, `stages/common.py: no_flags` | portable, applied |
| 2 | `DataFrame.dtypes` | returns a Column object, not a list | **yes** (fails later, far away) | the schema's fields, `stages/common.py: column_types`; same in `spark_gold.overlay` | portable, applied |
| 3 | a NUL character in a literal (`F.lit("\u0000")`, the null mark in key hashes) | the SQL text holds `'\0'`, which DuckDB cannot parse | no | `F.expr("chr(0)")`, `common.null_mark`; the hashes are byte for byte the same (tested against Python's SHA-256) | portable, applied |
| 4 | `cast` of text that does not convert (`'n/a'` to a number, `31.02.2026` to a date) | DuckDB raises; Spark (ANSI off) gives null | no | `try_cast` and `F.try_to_timestamp`, `stages/clean.py: cast_to`. Also what Spark 4 / Databricks with ANSI on needs, so it is the better form anyway | portable, applied |
| 5 | `F.from_utc_timestamp` | not a SQLFrame function; through `F.expr` it compiles to `CAST(ts AS TIMESTAMP) AT TIME ZONE tz`, which moves the clock the wrong way (19:00 UTC gave 13:30 instead of 00:30 next day) | **yes** (through `F.expr`) | `F.convert_timezone(lit("UTC"), lit(tz), col)`, `clean.time`. In SQL text (`session.sql`, the SQL models, the judge's queries through sqlglot) it is right: the fact_otif and fact_sales India-local-date cases pass | portable, applied |
| 6 | quoted letters in a `date_format` pattern (`yyyy-MM-dd'T'HH:mm:ss'Z'`) | the quotes are copied into the text (`2026-11-09'T'00:00:00'+00'`) | **yes**: the gold surrogate keys would all differ | the text built from pieces around literal `T` / `Z`, `spark_gold.as_text` (key text, hour windows, `first_seen`) and `clean.time` | portable, applied |
| 7 | `F.last(col, ignorenulls=True)` over a window (type 2 version detection) | the null skipping is dropped (or a `FILTER` clause DuckDB rejects) | **yes** | `F.last_value(col, F.lit(True))`, `stages/history.scd2` | portable, applied |
| 8 | ascending window order with nulls (Spark: nulls first) | SQLFrame writes no `NULLS FIRST`; DuckDB puts nulls last | **yes** | state it: `asc_nulls_first()` / `desc_nulls_last()` in every window order (`history.py`, `spark_gold.dimension`). SQL text is fine: sqlglot adds `NULLS FIRST` | portable, applied |
| 9 | `F.broadcast` | not a SQLFrame function | no | `df.hint("broadcast")` (Spark: the same plan; DuckDB ignores it) | portable, applied |
| 10 | `withColumnRenamed` of a column that is not there | raises (Spark: does nothing) | no | rename only when present, `history.working` | portable, applied |
| 11 | backticks in a DDL schema string (`` "`id` string" ``) | kept as part of the name; a later `withColumn` after a join then keeps the **old** value | **yes** | plain names (`gold_helpers.silver`, `DeltaRsGoldTables.silver`) | portable, applied |
| 12 | `StructType[name]` | not subscriptable | no | a dict of the schema's fields (`spark_gold.overlay`) | portable, applied |
| 13 | `DataFrame.selectExpr` | not there (a Column comes back) | no | `select(...)` with `F.date_format` (tests only) | portable, applied |
| 14 | `localCheckpoint`, `unpersist` | not there | no | `duckdb_frames.checkpoint` (the query into a DuckDB table); the tests' `hold()` picks it per engine | adapter only |
| 15 | `F.conv(hex, 16, 10)` (gold surrogate key) | not a SQLFrame function | no | the 15 hex digits added up from their character codes, `duckdb_gold.surrogate`; keys equal `keys.surrogate_key` (tested) | adapter only (Spark keeps `conv`) |
| 16 | `F.xxhash64` (window digests) | not a SQLFrame function | no | DuckDB's `hash`, `DuckGoldEngine.row_hash`; digests are compared within one engine's runs only | adapter only |
| 17 | `spark.sql(text, args=...)` named parameters | not supported | no | parameters bound into the parsed query by sqlglot as quoted literals, `duckdb_gold.bind` | adapter only |
| 18 | `to_json(struct(...))` (`_record_hash`, the quarantine `_record`) | null fields kept (Spark leaves them out); timestamps written `2026-11-09 00:00:00+00` (Spark `2026-11-09T00:00:00.000Z`) | **yes** | none needed: a record hash is compared only with the same engine's rows. **Rule: one table, one engine** | keep engine-specific, tested as a known difference |
| 19 | a timestamp cast to text inside a key hash | DuckDB adds the offset (`... 00:00:00+00`) | **yes** | no tyre silver key has a timestamp column (checked: all 15 contracts); if one ever does, build its text with `as_text` | known difference, tested |
| 20 | Delta read and write, MERGE, `replaceWhere`, `txnAppId` | SQLFrame has no Delta | — | delta-rs in the DuckDB adapters | adapter only |

What stays Spark-only: nothing in the stages or the builds. The Delta edges, `conv`, `xxhash64`, checkpointing and
parameter binding have DuckDB versions in the adapter; the Databricks realm uses none of them.

Things checked and equal on both engines without change: SHA-256 (`sha2`), `concat_ws` with the unit separator,
`get_json_object`, `rlike`, the hygiene regular expressions (byte-order mark, control characters), `create_map`
lookups, `F.filter` with a lambda, `array_compact`, `row_number`/`lead`/`min`/`max` over windows, struct comparison,
`date_trunc`, `unix_timestamp`/`timestamp_seconds`, date keys, decimals in the window digest (`decimal(38,0)`),
`ROUND(..., 2)` on `net_value`, the full outer join of fact_sales.

## ADR draft: "One logic, several engines" (for the owner to accept)

> **ADR-0xx — One logic, several engines**
>
> **Status:** proposed (DuckDB spike, 5 Nov 2026).
>
> **Context.** The silver stages (`dataplatform.stages`) and the dataframe gold builds (`models/`) are pure functions
> written against the PySpark DataFrame API; the SQL gold builds are Spark SQL text. Databricks runs them on Spark
> (and Photon). On the laptop and in CI, Spark costs a JVM, a 1 GB driver and seconds per action. SQLFrame offers the
> same API on DuckDB. The spike ran the same test tables and the same 14-day world on both engines: equal answers
> after 13 portable rewrites; DuckDB faster for gold and end to end, slower for the
> test suite (SQLFrame builds SQL in Python).
>
> **Decision.**
> 1. Stage and build logic is written once, against the PySpark DataFrame API (or Spark SQL text), in the portable
>    subset: no feature listed in docs/DUCKDB-SPIKE.md as uncovered or silent; window orders state where nulls go;
>    casts that may fail use `try_cast` / `try_to_timestamp`; timestamps as text are built from pattern pieces, never
>    with quoted letters; no `F.lit` holding a NUL.
> 2. An engine is an adapter: the stage runner, the gold engine and the read and write edges per engine. Engine-only
>    operations (checkpointing, `conv`, `xxhash64`, parameter binding, Delta IO) live in the adapter, never in a stage
>    or a build.
> 3. Every stage and build test table runs on every supported engine (`make engine-conformance`). A value an engine
>    computes differently is either made equal or recorded as a known difference with a test that pins it.
> 4. Production and anything that writes a governed table runs on the platform's engine only (Spark + Photon on
>    Databricks). A second engine is for development and CI; it never writes to a governed catalog, and one table is
>    always written by one engine (record hashes and digests are engine-local).
> 5. Supported engines today: Spark (production, laptop, CI where Java is present) and DuckDB through SQLFrame
>    (CI and optional laptop runs).
>
> **Consequences.** Stage authors keep one language. A Spark-only feature can still be used, but only in an adapter
> or with the second engine's version beside it, and the conformance run shows it at once. The portable forms are
> also what Spark 4 with ANSI on and Fabric's engines need. Cost: a second pytest process in CI (about two minutes
> today) and pinning SQLFrame.
>
> **Revisit when** SQLFrame's frame building gets cheaper (its time goes into copying sqlglot trees, not into DuckDB),
> or when a binding needs a Spark-free engine for real (Fabric's Python notebooks, see the TODO's Fabric note).
