---
title: "W1c silver engine: the thirteen stages, where each lives, decisions, spec gaps, notes for gold"
audience: [architect, engineer]
purpose: "What session W1c built (bronze to silver), the decisions taken without the owner, and what W1d (gold) inherits"
status: draft
last-reviewed: 2026-11-04
related:
  - adoption.md
  - silver_layer_design.md
  - W5-DECISIONS.md
---

# W1c silver engine

Branch `w1-core-engine`. Specs: ADR-001, -007, -008, -013, -016, -017; DPC-0008, -0017, -0045, -0054, -0070, -0085;
port `stage_runner` (dpc/ports.yaml); binding `dbs/engine/databricks.yaml` (`gap_filled_by: dataplatform.stages`).

## The shape

```
metadata store ──view──▶ silver/plan.py (kernel: the 13 stages, their order, every setting; refuses others)
                              │
bronze (Delta) ──read edge──▶ stages/ (PySpark, pure: DataFrames in, DataFrames out) ──write edge──▶ silver (Delta)
                 adapters/spark_silver.py: DeltaSilverTables.bronze / current        DeltaSilverTables.write (MERGE
                                                                                      on _row_key, or idempotent append)
silver/runner.py (kernel): new batches from the file log (bookmark silver.files) → stage runner → write → run log
silver/daybyday.py (kernel): silver per world day against the oracle; DEL-01, SCD-02, LATE-03, DUP-04 judged
```

The kernel (`src/dataplatform/silver/`) names no product (R1, tested). The stages (`src/dataplatform/stages/`) are the
in-house part of the stage_runner port and are PySpark; they never read or write (ADR-013): `current` (silver as it
stands) is handed in by the read edge, and the load time is passed in.

## The thirteen stages and where each lives

| # | Stage | Function | What it does | Settings it reads |
|---|---|---|---|---|
| 1 | contract check | `stages/clean.py: contract_check` | every kept contract column is in the batch, else the batch stops naming it (DPC-0008) | contract columns, `from`, `path`, `exclude` |
| 2 | projection, extract, rename | `clean.py: project` | all columns by default (ADR-008); rename from a source name; extract from the held `_extra` JSON; exclude | `projection`, `from`, `path`, `exclude` |
| 3 | hygiene | `clean.py: hygiene` / `clean_text` | byte-order mark, control characters, line endings, trim, runs of spaces, empty → null | `hygiene` per column, `processor_groups.hygiene` |
| 4 | parse and type | `clean.py: parse_type` / `cast_to` | cast to the contract's logical type; a fraction never truncated; a failed cast is quarantined, or emptied + flagged + counted (DQ-02) | `on_cast_failure`, `format`, `processor_groups.type_safety` |
| 5 | time | `clean.py: time` | timestamps stay UTC instants; `<col>_local` wall time when asked | `local_tz`, `processor_groups.timestamp_normalize` |
| 6 | standardise values | `clean.py: standardise` | declared value maps, letter case | `values`, `case`, `processor_groups.standardization` |
| 7 | personal data | `clean.py: personal_data` | a restricted column must hold tokens (DPC-0070); a plain value is quarantined | contract `classification`, the binding's token format, `processor_groups.pii` |
| 8 | row filter | `clean.py: row_filter` | declared filters drop rows, counted | plan `row_filters` |
| 9 | quality | `clean.py: quality` | row rules; severity sets the action (ADR-009): quarantine or flag in `_dq_flags` | required columns (critical), `quality.actions` |
| 10 | duplicates | `stages/rows.py: duplicates` | first / latest / full row; type 2 keeps every distinct change; append kinds drop keys already in silver within the late lookback (DUP-04) | `dedup.*`, `load.late_lookback_days`, `processor_groups.dedup` |
| 11 | keys | `rows.py: keys` | business key present, else quarantined; `_key_hash` | contract primary key |
| 12 | history and deletes | `stages/history.py: history` (`scd2`, `scd1`, `append`, `snapshot`) | by dataset kind (ADR-016); type 2 ordered by the source sequence (SCD-02); soft deletes from op D or a key missing from a full copy (DEL-01, DPC-0017); snapshots replace their window | `history.*`, `load.history_order_by`, `load.deletes` |
| 13 | provenance | `rows.py: provenance` | `_batch_key`, `_source_file`, `_window`, `_bronze_run_id`, `_silver_run_id`, `_silver_loaded_at`, `_record_hash`; fixed column order | — |

Order and the stage list: `silver/plan.py: STAGES`, checked at import by `check_stage_order` (CFG-01). Runner:
`stages/__init__.py: run_stages` (skips a stage only when its `processor_groups` key says `disabled`).

## Silver columns

Business columns (contract first, then any other source column), then `_extra` (held new columns, kept so nothing is
lost), the transport columns bronze had (`_op`, `_feed_pos`, `_committed_at`, `_shard`, `_seq`, `_arrived_at`), and
silver's own: `_key_hash`, `_record_hash`, `_seq_from` (type 2: the order value the version starts at), `_valid_from`,
`_valid_to`, `_is_current`, `_is_deleted`, `_deleted_at`, `_dq_flags`, `_row_key`, `_bronze_run_id`,
`_silver_run_id`, `_silver_loaded_at`. Current rows are `_is_current AND NOT _is_deleted`.

## Silver against the oracle: `make run DAYS=14` (full scale, 4 Nov 2026)

Local PostgreSQL for the sources and the metadata store, bronze by delta-rs, silver on local Spark (`local[2]`, 1 GB).
294 numbers compared over 14 days (current rows, versions, deletes marked, each dataset's reconciliation measures,
readings per event hour): all equal. Bronze equals the oracle every day; the five known answers match on day 14.
Run time 2,539 s, of which silver 1,711 s (89 to 153 s a day; the readings table grows by 622,080 rows a day).

| Day | Date | Silver seconds | Current rows | Order lines current / versions / deleted | Material versions | Dealer versions | Readings | Sell-out rows | = oracle |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 2026-11-09 | 89.7 | 635,048 | 5,676 / 5,676 / 0 | 1,800 | 600 | 622,080 | 1,912 | yes |
| 2 | 2026-11-10 | 123.9 | 1,273,027 | 15,681 / 15,924 / 0 | 1,802 | 600 | 1,244,160 | 3,790 | yes |
| 3 | 2026-11-11 | 118.5 | 1,910,865 | 23,879 / 24,711 / 0 | 1,804 | 601 | 1,866,240 | 5,684 | yes |
| 4 | 2026-11-12 | 114.1 | 2,550,357 | 32,710 / 34,274 / 5 | 1,806 | 602 | 2,488,320 | 7,392 | yes |
| 5 | 2026-11-13 | 123.7 | 3,191,227 | 41,273 / 43,497 / 5 | 1,808 | 602 | 3,110,400 | 9,404 | yes |
| 6 | 2026-11-14 | 113.4 | 3,828,346 | 47,313 / 50,213 / 5 | 1,810 | 603 | 3,732,480 | 9,404 | yes |
| 7 | 2026-11-15 | 109.1 | 4,467,345 | 55,031 / 58,521 / 5 | 1,812 | 603 | 4,354,560 | 9,404 | yes |
| 8 | 2026-11-16 | 115.0 | 5,105,721 | 61,604 / 65,644 / 5 | 1,814 | 603 | 4,976,640 | 9,404 | yes |
| 9 | 2026-11-17 | 125.3 | 5,747,867 | 71,948 / 76,541 / 5 | 1,814 | 603 | 5,598,720 | 9,404 | yes |
| 10 | 2026-11-18 | 133.1 | 6,388,681 | 80,096 / 85,360 / 5 | 1,818 | 604 | 6,220,800 | 9,404 | yes |
| 11 | 2026-11-19 | 141.2 | 7,031,674 | 90,198 / 96,202 / 5 | 1,820 | 604 | 6,842,880 | 9,404 | yes |
| 12 | 2026-11-20 | 121.8 | 7,671,136 | 96,379 / 103,073 / 5 | 1,822 | 605 | 7,464,960 | 9,404 | yes |
| 13 | 2026-11-21 | 153.2 | 8,311,834 | 104,102 / 111,448 / 5 | 1,824 | 606 | 8,087,040 | 9,404 | yes |
| 14 | 2026-11-22 | 129.0 | 8,954,120 | 113,457 / 121,322 / 5 | 1,826 | 606 | 8,709,120 | 9,404 | yes |

Incidents silver owns, judged on what silver holds:

| Day | Incident | Detector | Silver verdict | Evidence |
|---|---|---|---|---|
| 3 | readings-out-of-order | LATE-03 | handled | 188 late readings; silver per event hour equals the oracle in all 24 hours |
| 4 | order-lines-deleted | DEL-01 | handled | 5 of 5 deleted keys kept in silver and marked deleted |
| 8 | promised-date-out-of-order | SCD-02 | handled | 3 versions; the late change (seq 87216) sits in history, not current; current promised date 2026-11-20 |
| 9 | material-copy-short | DEL-04 | handled (ingestion) | the guard now compares with silver's current keys: 720 of 1,800 missing (40%) held |
| 13 | stream-replayed | DUP-04 | handled | 19,440 replayed rows dropped; silver 8,087,040 readings = oracle |

Left to their layers, as before: LATE-01, HIST-05, LATE-02 (gold, W1d); REC-05 (reconciliation, W1e); DUP-01 and
SCH-02 handled by ingestion.

Note on the machine: an earlier attempt stalled for 15 minutes on day 8 while the Mac (8 GB) was swapping (2.5 GB of
swap used by other sessions); the same day's stages take about 20 s for the readings when memory is free.

## Tests (table-driven; one local Spark shared by the session, `tests/dataplatform/conftest.py`)

| File | Cases | What |
|---|---|---|
| `test_silver_plan.py` | 44 | the plan from the store's view; refusals (settings outside the stage list, ADR-007); the stage order; every tyre dataset's plan; R1 for the silver kernel (no Spark) |
| `test_silver_stages.py` | 91 | one table per stage, 13 stages; history: type 2 (late change, deletes, idempotent rerun, non-overlapping validity), full-copy key compare, type 1, snapshot, append, late readings |
| `test_silver_ports.py` | 20 | M5 contract tables for the silver tables (write, measure, idempotent append, stale-row removal, new column, quarantine, current keys, hourly counts, bronze read) and the stage runner (counts, contract stop) |
| `test_silver_runner.py` | 19 | the kernel run with fakes behind the ports: which batches, bookmark, failures logged, steps per stage; the incident judges |
| `test_ingest_pipelines.py` | +4 | the mass-delete guard compares with silver's current keys (CurrentKeys port) |
| `test_silver_world.py` (slow) | 19 | 14 world days at scale 0.05 through ingestion and silver: silver = oracle every day; DEL-01, SCD-02, LATE-03, DUP-04 handled; DEL-04 still held (936 s) |

Kernel and realm suite with Spark (`-m "not slow"`, the session's command plus `--with 'pyspark==3.5.9' --with
'delta-spark==3.2.0'`): 839 passed, 29 skipped (was 660 passed, 1 failed). Without pyspark the Spark tests skip.
`mypy --strict` clean on `dataplatform.silver`, `dataplatform.stages`, `adapters/spark_silver.py`, `dataplatform.ingest`;
shift-left (`shiftleft scan --stage pr`) passes with 0 blocking; the IO warnings on the adapter's edge methods are
marked `shiftleft: allow` with the reason.

## Decisions taken (the owner was not asked)

1. **Engine and binding.** One engine: PySpark 3.5.9 + delta-spark 3.2.0 (the repo pinned no Spark; Delta 3.2 is
   the release for Spark 3.5 and its jars were already in the local Ivy cache), Java 17, `local[2]`, 1 GB driver, UI and progress bars off. The session factory sets SPARK_HOME to the
   `pyspark` package's own Spark, because this Mac's environment points SPARK_HOME at Homebrew Spark 4.1.1, which does
   not match the Python package. Bronze stays delta-rs (W1b); Spark reads it as it is.
2. **Kernel / engine split.** The kernel (`silver/`) owns the stage list, order, settings and the run; the stages are
   PySpark functions in `stages/` (the binding names `dataplatform.stages` as the in-house stage runner), never
   doing IO: `current` silver is handed in by the read edge, the load time is passed in.
3. **Which batches.** Silver takes the file log's `loaded` batches after its bookmark `silver.files` (the number of
   file records already taken). The bookmark moves only when the dataset run succeeds; a failed run is taken again.
   When only held batches are new, the dataset run succeeds with 0 batches so the bookmark moves past them.
4. **Idempotency.** Every output row has a `_row_key` computed from the data (type 2: key + version start; type 1 and
   append: key; snapshot: key + batch). Non-append kinds write by MERGE on `_row_key`; append kinds append with
   `txnAppId = silver:<table>`, `txnVersion = <file-log mark>`, so a write repeated after a crash is skipped.
5. **Type 2.** History is ordered by `load.history_order_by` (change_seq, extracted_at, updated_at), never by
   arrival. Only `history.track_columns` open a version; other changes update the version they fall in. A delete
   event never opens a version, it marks the one it falls in. Validity (`_valid_from/_valid_to`) runs on the effective
   time (`_committed_at` for change feeds, else a timestamp order column) and is made monotonic: a version never
   starts after a later-ordered one (the late change committed six hours after the newer one gets a zero-length
   validity, so an as-of join never picks it). When a late change moves a version's start, the old row is deleted
   by the merge (`_action = delete`).
6. **Deletes are soft** (ADR-016): op D marks the current version deleted; a key missing from a full copy is a delete
   only when `deletes.detect: key_compare` and the read is a full copy (DPC-0017); a key that comes back is live again
   in the same version. `deletes.apply: remove` is supported (rows removed) though no tyre dataset uses it.
7. **Duplicates.** For a type 2 dataset, `dedup.mode: business_key_latest` would collapse distinct changes; the stage
   keeps every distinct (key, order value) instead. For append kinds a key already in silver within
   `late_lookback_days` (all of silver when none is declared) is a replay and dropped (DUP-04).
8. **Snapshot** (dealer sell-out): the window is the business key minus its last column (dealer, month); the newest
   batch for a window (by `dedup.order_by`, then batch key) becomes current; the window's other rows stay, not
   current. An older file sent again does not win.
9. **Projection all** (ADR-008): every bronze column passes; contract columns under contract names; `_extra` (columns
   held by `new_columns: hold_in_bronze`) is carried to silver untouched so nothing is lost; a merge adds a new
   column to the table first.
10. **Quality in W1c** applies the rules the plan holds: required columns present (critical, quarantine unless the
    dataset's `quality.actions` says otherwise). The contract's other row rules arrive with W1e's DQ engine; stage 9 is
    where they plug in.
11. **Personal data.** Restricted columns must look like tokens (the binding passes the tokenizer's format,
    `KeyedTokenizer.PATTERN`); a plain value is quarantined. A restricted column with no token format bound stops the
    batch.
12. **Mass-delete guard** (DPC-0054) now compares a full copy with silver's current keys (`ingest.ports.CurrentKeys`,
    passed to `ingest()` / `run_days()`), and falls back to the last accepted bronze copy before silver exists.
13. **Corrections** never reach silver (ADR-017); silver keeps what the source said. W1d reads them through
    `store.corrections(..., as_of)`.
14. **Catalog fixes (data-platform-catalog 58e6d97):** partner manifest `batch_key: [content_checksum]`;
    `dealer_sellout.file_name` described as filled by the lander; `fact_sales` contract and manifest say cancelled and
    rejected lines are left out of sell-in (as fact_otif). The W5 generated catalog sync files were regenerated.

## Spec gaps (for the TODO; not invented silently)

1. **No home for per-column silver settings in DPM.** The binding names `silver.columns`, `processor_groups.*` and
   `on_cast_failure` as metadata keys; DPM's dataset schema has only an untyped `columns` array (LIF's
   dataset_config_columns). The engine reads per-column settings from that array; DPM should type its items.
2. **No key for the projection mode** (ADR-008 "declared available per dataset"): the plan supports `declared`, DPM
   has nowhere to say it.
3. **No key for a snapshot's window.** ADR-016 says "snapshots append by date"; the tyre contract says "a re-sent or
   corrected file replaces that dealer's month". The engine defaults the window to the key minus its last column; DPM
   should declare it (e.g. `history.snapshot_by`).
4. **Domain quality defaults are not in the dataset view.** `platform.yaml` has `quality_defaults`; the store's
   merged view does not carry them, so the plan uses ADR-009's defaults (equal today) overridden by the dataset's
   `quality.actions`.
5. **`new_columns` has no "load" value.** ADR-008 says a new column on a non-personal source is "noted and loaded";
   DPM offers only `stop` and `hold_in_bronze`. Silver carries `_extra` through; promoting a held column to a real
   silver column needs a contract change (or a `load` value).
6. **Local time beside timestamps.** `sales_order_line.order_ts` says "the plant's local time is added beside it";
   the plant's zone is another source's fact (conforming, gold per ADR-001) and no per-column zone is declared. The
   time stage can add `<col>_local` for a declared fixed zone (`local_tz`); per-row zones belong in gold.
7. **Contract physical names vs ADR-012.** Contracts say `silver_erp.sales_order_line`; ADR-012 says one catalog per
   environment and layer (`<env>_silver.<source>.<dataset>`). Locally silver is a folder per table; the realm must
   pick one.
8. **lims.inspection_result batch key** is still `[file_name, content_checksum]`; the partner fix (a renamed resend is
   the same delivery) applies to lab files as well. Left for the owner: lab files are never resent in the world.
9. **Unicode normalisation** (LIF hygiene default NFC) has no Spark built-in; not applied.

## Notes for W1d (gold)

- Read silver current rows as `_is_current AND NOT _is_deleted`; deleted members stay joinable (`_is_deleted`) so
  facts can still find them (DPC-0053 needs gold to stop counting them).
- Type 2 dimensions (`erp.material`, `mes.press`, `dealer_api.dealer`, and `erp.sales_order_line` itself) carry
  `_valid_from` / `_valid_to` (half open, `_valid_to` null for the current version) on the effective time; join as of
  the event with `_valid_from <= t AND (t < _valid_to OR _valid_to IS NULL)` (ADR-005). Zero-length versions (a late
  change) are never selected by that predicate, which is intended.
- `_seq_from` is the version's start in the source order; `change_seq` on the row is the last change applied.
- Order lines keep cancelled and rejected lines (with qty and value); OTIF and sell-in filter `line_status = 'open'`
  (contracts updated). Silver row counts include them.
- Dealer sell-out: current rows are the newest file per dealer and month; earlier files' rows are `_is_current =
  false`. A late file (LATE-01) arrives as a new batch; gold rebuilds the month (window recompute).
- Corrections: `store.corrections(source, dataset, as_of)`, laid over silver by the window recompute (ADR-017).
- Silver tables locally: `build/run/silver/<source>__<dataset>` (Delta); quarantine beside them
  `<source>__<dataset>__quarantine`. `DeltaSilverTables.frame(source, dataset)` gives a frame.
- Readings: `_valid_from` holds the event time for append kinds (the watermark column when it is a timestamp).
