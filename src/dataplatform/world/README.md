# The tyre world (goal G4)

A pretend tyre company that runs for simulated days and writes its data into four mock source systems, so the
platform has something realistic to ingest. It is built from the spec in
`~/data-platform-catalog/slices/02-tyre/` (the DWD `world.yaml`, the dataset manifests and their contracts).
Each day it also writes down what a correct platform should show at that point. These expected totals are the
**oracle**.

## Run it

```bash
make world DAYS=7                  # 7 days at the DWD's volumes into the local Postgres (~90 s, ~120 MB of stream)
make world DAYS=1                  # one day
make world DAYS=14 SCALE=0.05      # all 14 days, small and quick (~10 s)
make world WORLD_DB=sqlite         # no Postgres needed
make world-api                     # serve the dealer interface of the last run on http://127.0.0.1:8765
make world-test                    # table-driven tests: the generator, every incident, every port's contract
```

Postgres: the run creates the database `tyre_world` if missing (`WORLD_PG_DSN` overrides the connection;
`brew services start postgresql@16` locally). Each run starts the sources fresh.

## Where things land

| Source system (DWD) | Kind | Port | Local adapter | Where |
|---|---|---|---|---|
| `erp_db`, `mes_db` | database with a change feed | `SourceDatabase` | `postgres_source.py` (or `sqlite_source.py`) | schemas `erp_db`, `mes_db`: `<table>` live, `<table>__changes` the feed, `<table>__copy` nightly full copies |
| `lims_drop`, `partner_drop` | file drop | `FileDrop` | `file_drop.py` | `build/world/sources/drops/<system>/<dataset>/`, arrival time = file modified time |
| `dealer_api` | request-response interface | `RecordInterface` | `http_api.py` | `build/world/sources/dealer_api.sqlite`, served at `/dealer_api/dealer?cursor=&limit=200` |
| `press_stream` | event stream | `EventStream` | `local_stream.py` | `build/world/sources/stream/press_reading/shard-NNN.jsonl.gz` |

## Where the oracle totals are

`build/world/oracle/day-NN.json`, one file per simulated day:

| Key | What it says |
|---|---|
| `sources` | rows each source produced that day |
| `bronze` | rows a correct platform appends to bronze that day (a resent file adds 0; a short full copy adds 0, because it is blocked; renamed-column files are stopped) |
| `silver` | current rows and the manifest's own reconciliation measures per dataset at the end of the day, plus versions for the history tables |
| `gold` | `fact_otif` per promised date, `fact_scrap` per day and plant, `fact_sales` per month and region, with completeness flags |
| `metrics` | OTIF %, order lines, scrap rate %, tyres cured, sell-in and sell-out units, as the metric views define them |
| `findings` | the incidents planted that day and the detector each must fire. A day with none is `clean_day: true` |
| `known_answers` | every known answer in the metric view files (`manifest/metrics/`), beside what the world produced and whether they match |

Where what the platform can see differs from what happened at the source (the 7 rows the change feed skipped, the
lab files stopped for a renamed column), the oracle gives both an `expected` and a `truth` value. The gap is what
reconciliation must find. `oracle/incidents.json` lists every planted incident with its keys.
The day-10 correction is not a file: the world records its 12 rows and their backfill plan in the metadata
store's corrections table (ADR-017), through the store's `record_corrections` procedure.

## Plant an incident

Incidents come from the DWD calendar (`world.yaml`, `calendar:`). To move one, change its `day`. To tune one, change its
`params:` block. For example, to keep the lab's renamed column for three days:

```yaml
  - id: lab-column-renamed
    day: 7
    kind: renamed_column
    params: {plant: P-HUN, from_local_hour: 10, column: defect_code, renamed_to: defect, until_day: 9}
```

Every calendar entry's exact values (dealer D-0042, 40% short, 90 seconds) are in its `params:` block in the DWD;
the world keeps no table of its own. A new incident `kind` needs a handler in the domain model. Until it has one, the
engine refuses to start and names the kind.

## How it is built

- `engine.py`, `clock.py`, `dwd.py`: domain-free. They run the clock and route each day's output to the port that
  matches the system's kind in the DWD, refusing writes to systems the DWD does not declare.
- `domains/tyre.py`: the only code that knows tyres. It is deterministic (seed 11), with one random stream per
  concern. `domains/tyre_oracle.py` computes the oracle from the world's own record, never from the platform.
- `../source_ports.py` and `../adapters/`: one adapter per product, each tested against the same contract table
  (`tests/dataplatform/test_source_ports.py`). Adapters are the only files that name a product (R1, tested).

## Ingest it (W1b)

```bash
make run DAYS=14                   # world + ingestion day by day: sources -> landing -> bronze (build/run/)
make run DAYS=3 SCALE=0.05 WORLD_DB=sqlite DP_RUN_META=memory   # quick, nothing but Python packages
```

Each world day is followed by one ingestion run (`src/dataplatform/ingest/`): the push movers land the change feeds
and the dropped files, the pull pipelines read the full copies and the dealer interface, the stream is read
trigger-once from its checkpoints, and every batch passes the same lander steps into bronze (Delta tables under
`build/run/bronze/`). Bronze rows per dataset are compared with the oracle's `bronze` totals for the day, and each
planted incident gets the verdict ingestion owes it (handled, flagged, or deferred to silver, gold or
reconciliation). Report: `build/run/report.json`. The metadata store is the only config source: the manifest is
loaded into it first (`DP_RUN_META_DSN`, default `dbname=dp_meta_run`, recreated each run).
