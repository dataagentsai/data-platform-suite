# Observability and DataTwin (session W3, goals G8 and G10; ADR-024 applied)

W3 was built on W1a only. Since the merge into the core engine branch, ingestion (W1b), silver (W1c) and gold (W1d)
exist; the data expectation services (W1e) do not. Observability's own screens still run on a clearly marked
fixture run log; the scenarios' ingestion and silver checks run on the engine (see "Scenarios" below).

## Three sets (ADR-024 rule 1)

| Set | What | Where |
|---|---|---|
| (a) Platform-native, adopted | the jobs monitoring screen (job runs, durations, retries), job failure notifications, lineage and audit views | `obs/native.py` (product-free list); `adapters/databricks_native.py` -> `realms/databricks/dashboards/native.yml` (generated). Referenced or imported, never rebuilt |
| (b) Observability, ours | runs and promises: run health per dataset and batch, freshness against the manifest's targets, held and duplicate deliveries with the reason, alert routing and noise, the step-severity summary with links | `obs/`; five dashboards: Platform health, Freshness and SLOs, Performance, Sources and ingestion, Usage and adoption |
| (c) Data expectation services (DES), ours | whether the data is right: quality, reconciliation, profiling, quarantine and release | `src/dataplatform/des/` (W1e fills it); the DES screen `des_data_expectations` moved here from observability's former "Data trust" dashboard |

Observability never judges data content. It reads each owner's step row (the one step format) to alert and to mark a
run passed, warned or blocked, and links to the owner's screen.

## What is here

| Part | Where | Status |
|---|---|---|
| Run recorder, run id = trace id, push hooks | `obs/recorder.py` (`on_finish`), `obs/trace.py`, `adapters/otel_tracer.py` | done |
| One step format (`detect.*`, `quality.*`, `recon.*`, `profile.*`, severity, owner, screen) | `obs/steps.py`; contract test `test_obs_steps.py` over every owner | done; recorder, ingestion, fixture and gold rows checked; silver and DES rows pending |
| Run log read back through its port (three customer levels) | `obs/runlog.py`; `adapters/memory_runlog.py`, `adapters/postgres_runlog.py`, `adapters/null_runlog.py` (no log), `adapters/customer_runlog.py` (example skeleton) | memory and PostgreSQL in the contract table; the customer row pending |
| Platform telemetry (jobs with tags, lineage, access), copied once a day | `obs/system.py` (`DailyCopy`, `on_demand`); `adapters/memory_system.py`, `adapters/databricks_system.py` | done; no billing read |
| Declarative pipelines report themselves | `obs/pipeline_events.py`; `adapters/memory_pipeline_events.py`, `adapters/databricks_pipeline_events.py` (stub) | port and contract table done; used once silver or gold runs as a declarative pipeline |
| Ops builder (14 compact ops tables, joined by run id, `origin` per row) | `obs/ops.py` | done |
| Ops metric views (certified definitions) | `obs/views.py`; `adapters/databricks_ops_views.py` -> `realms/databricks/dashboards/views/` | done; generated and drift-checked |
| Freshness and the next due lateness check | `obs/freshness.py` (`next_due`) | done |
| Alerts as config with the OPS-06 noise controls, scoped push evaluation | `obs/alerts.py` (`Scope`), `obs/rules/alerts.yaml` | done |
| Delivery through the platform's notification destinations | `obs/monitor.py` (`Notifier`), `adapters/databricks_notify.py` | done; real workspace send pending |
| Monitor: pushed by runs, lateness on wake-up, daily copy | `obs/monitor.py` | done |
| Dashboards: observability's five and DES's one | `obs/dashboards.py`, `des/dashboards.py`, `adapters/lakeview.py`, `realms/databricks/dashboards/` (generated) | done; not yet deployed |
| Fixture runner | `obs/fixture.py` | FIXTURE |
| Scenario file, runner, oracle and engine checks, 11 scenarios | `twin/scenario.py`, `twin/runner.py`, `twin/engine.py`, `twin/scenarios/` | world and obs checks run on the fixture; ingestion and silver checks run on the engine (`test_twin_engine.py`, slow); gold and DES pending |
| Learning loop | `twin/learning.py` | done |
| Release gate | `twin/gate.py`; `adapters/memory_tables.py`, `adapters/databricks_clone.py` | logic done; shallow-clone contract rows pending a workspace |

## Decisions (made while the owner was away; each is easy to reverse)

1. **The run id is the trace id.** A root span opens the run; its 32-hex W3C trace id is the run id in the
   store. The same id goes on compute as the tag `dp_run_id` (and as a job parameter), on lineage as a UUID of
   the same 128 bits, and on every log line. With only the tracing API installed (no SDK), the adapter makes
   the ids itself, so nothing depends on an exporter being configured.
2. **The run log is read through a new read-only port, not a change to W1a's port.** W1a's `run_report`
   answers one run and keeps no times in memory. Operations needs a window with times, so `RunLogReader`
   is new. The memory adapter wraps any store and stamps each write with the clock port's time (so a
   simulated week runs in seconds). The PostgreSQL adapter adds one function in a new file
   (`postgres_obs.sql`). W1a's port, schema and procedures are unchanged.
3. **Freshness has two measures.** Dataset freshness is the time since the last *successful* dataset run, even
   one that found nothing new: the platform looked and is current. Feed freshness compares each bookmark whose
   value is a time with the newest bookmark of the same dataset. One dealer's file missing while the others
   arrived is a lagging bookmark. Both use `sla_freshness_hours` (warning) and `sla_freshness_error_hours`
   (error) from the store's merged view, so from the manifest. Only an error raises an alert.
4. **Tier.** The manifests declare no tier. Tier comes from `tiers:` in the alert config, otherwise from the
   error target: 4 hours or less is tier 1 (page the rota), 48 hours or less is tier 2 (ticket to the rota),
   longer is tier 3 (digest to the dataset owner). Rota and owner come from the manifest's `owners`.
5. **"One per rule per day" means one message per rule, per route, per business day.** Two tiers going to
   two different people are two messages. Days are counted in the domain's business time zone. Resolutions are
   sent, but never more than one per subject per recovery.
6. **Snoozes** need a rule, a subject (a pattern), a reason of at least 10 characters (the same bar as a
   watermark reset), an owner and an expiry with a time zone, no more than 30 days ahead. After the expiry, a
   subject that is still failing alerts again.
7. **Step naming is the contract between the engine and the ops builder:** `quality.*`, `recon.*`,
   `profile.*` and `detect.<DAC id>`. A detector that handled an incident is `warned`; one that stopped the load
   is `failed`. Since ADR-024 this is the one step format with its contract test (decision 18).
8. **Dashboards are product-free definitions** with small queries over the ops tables (aggregates through the
   ops metric views since ADR-024, decision 17). They are compiled to
   AI/BI dashboard files by an adapter and evaluated in memory by the tests. The files are generated and
   committed, and a test checks them for drift (`make dashboards`). The catalog and schema default to
   `tyre_ops.ops`. `resources.yml` must be included from the bundle; that is not done yet, so W3 does not touch
   the shared `databricks.yml`.
9. **Scenarios follow AgentTwin's scenario-file shape** (`dwd-scenario/v0`, judged by world state). AgentTwin
   is not imported: its runner drives an agent, and this one drives a data platform. Layers are `world`, `obs`,
   `ingestion` (W1b), `silver` (W1c), `gold` (W1d) and `trust` (W1e). A check on a missing layer is
   *pending*, never failing.
10. **One alert per incident in the fixture.** For each incident the fixture records only the one signal the
    DAC detector implies. Most incidents get a detector step. LATE-01 gets a lagging bookmark. REC-05 gets a
    failed reconciliation entry. A blocked load (DEL-04) is a *skipped* dataset run, not a failed one, so it
    raises the detector alert and not also a run-failed alert.
11. **The gate compares measures, not rows.** It compares the row count, the sums of the manifest's measure
    columns and a digest of the business keys against the oracle (within the manifest's tolerance) and against
    prod. A difference the change did not declare blocks the release. Clones are named
    `<table>__gate_<change>` and are always dropped.

### ADR-024 decisions (4 Nov, applied at the W3 merge)

12. **The DES screen moved, observability keeps a summary.** The former "Data trust" dashboard is now
    `des_data_expectations`, owned by `dataplatform.des` and generated beside observability's (resource prefix `des_`).
    Until W1e builds its own result tables it reads DES's step rows from `ops_steps`; W1e replaces its queries.
    Platform health gained two widgets: step results per day by severity, and steps not passed with the owner and the
    owner's screen. Detector rows (`detect.*`, owner `engine`) are listed on Sources and ingestion.
13. **Cost is out (the separate cost tool).** "Cost and performance" became "Performance": time per dataset and per pipeline
    step, slow-downs (a dataset run at least twice its own median so far) and re-runs (attempts after the first the
    same business day). Job-level performance is the native jobs monitoring screen. No billing is read
    (`UsageRow` and the usage query are gone); `run.job_tags(data_product)` puts `dp_run_id`, `dp_domain`, `dp_env`
    and `dp_data_product` on every job so the cost product can attribute spend. The cost files W5 generates under
    `realms/databricks/semantic_governance/generated/<domain>/cost/` were left as W5's (not observability's).
14. **Push, not poll.** The recorder calls `on_finish` hooks; `Monitor.run_finished` evaluates that run's datasets,
    detectors and checks within a `Scope` (subjects of datasets that did not run keep their state), then lateness.
    Lateness has no event: `freshness.next_due` says when the next dataset could pass its error target, and
    `Monitor.wake` runs a lateness check only then (through the run-log port and the store; no engine compute).
    Telemetry is copied once per business day from a watermark (`DailyCopy`); `on_demand` reads past it for a tool
    that asks. The twin steps a day at a time, so its one wake-up per day is at 23:59.
15. **Delivery is the platform's, routing is ours.** `DestinationNotifier` binds each channel (page, ticket, digest)
    to a workspace notification destination and posts one payload shape; an unbound channel is refused, not dropped.
    The transport is injected; sending through a real workspace is pending. Job failure notifications themselves are
    the native set's: `JOB_NOTIFICATIONS` in `adapters/databricks_native.py` is the snippet for every job; merging it
    into the bundle generator is pending (W2b's generator, not touched here).
16. **Three customer levels, one set of screens.** Every run and dataset-run row carries `origin`: `run_log`,
    `pipeline_events` or `telemetry`. A platform job run no run-log run accounts for becomes a telemetry row with the
    job-level columns filled and every log column empty. A test renders every dashboard with `NullRunLog`.
    `CustomerRunLog` is an example skeleton; its contract row is pending.
17. **Ops metric views.** Twelve `mv_ops_*` views over the compact tables (`obs/views.py`), generated like W5's
    business metric views. Every aggregate dashboard query must name a measure the view certifies (same aggregate,
    same column) and group or filter only on its dimensions; the AI/BI files read the views with `MEASURE()`.
    Listings read the compact table (rows are not metrics).
18. **One step format.** `obs/steps.py` holds the contract: `<detect|quality|recon|profile>.<id>`, status
    `succeeded|warned|failed` read as severity `passed|warned|blocked`, integer counts, a message (required when
    blocked, at most 4000 characters). Ingestion now writes one `detect.<DAC id>` row per detector that met a run:
    skipped duplicate DUP-01 (warned), contract SCH-02, volume ING-01 and mass delete DEL-04 (failed), and each
    noticed detector (warned). The volume guard has no DAC id of its own, so it reports as ING-01 (entry steps).
19. **The data-trust layer is `des`.** Scenario layers are `world`, `obs`, `ingestion`, `silver`, `gold`, `des`.
20. **Engine checks.** An engine layer's run of the seeded world (`twin/engine.py`: verdicts per incident, totals per
    day, bookmarks per day) is attached to the harness; an `engine` check says `expect: verdict | totals |
    bookmark_moves`. A check may say `pending: <what the layer owes>`: reported pending, never failed.

## Scenarios now

| Layer | Status |
|---|---|
| world, obs (fixture) | every check passes on the 14-day fortnight; one alert per incident, none on a clean day |
| ingestion (engine) | 5 of 5 checks pass on the engine (DEL-01 flagged, DUP-01, SCH-02, DEL-04 handled; LATE-01: since W1d ingestion keeps one bookmark per dealer holding its newest data time, so D-0042's holds until day 5 and moves on it) |
| silver (engine) | 7 checks wired (DEL-01, SCD-02, LATE-03, DUP-04 handled; silver equals the oracle on days 7, 9, 10); run with Spark in `test_twin_engine.py` |
| gold (engine, W1d) | 10 checks wired: LATE-03 and DUP-04 (gold equals the oracle on days 3 and 13), DEL-01, LATE-01 (flagged until day 5, then restated and complete), DUP-01, HIST-05, LATE-02 (flagged on days 11-12, re-keyed on 13) |
| des (W1e) | pending |

## Spec gap found (for the catalog)

`world.yaml` lists LATE-01 (`sellout-file-late`) as a finding only on day 5, when the late file arrives. The
lateness can be seen from day 1, the due day. The freshness alert fires on day 1 and resolves on day 5. The
scenario expects exactly that, and SIM-01 ("no detector on a clean day") holds only if days 1 to 4 count as
part of this incident's window. Suggestion: give late-delivery incidents a `visible_from` day (or the due day)
in the DWD.

## What W3 needs from W1b to W1e to go fully live

- **W1b ingestion:** `detect.<id>` steps are written now (decision 18). Still owed: record runs through
  `RunRecorder` (run id = trace id, `on_finish` push); keep one bookmark per feed whose value is the newest *data
  time* (a dealer's month end, a shard's event time) so LATE-01 is seen on the engine as on the fixture; pass
  `run.job_tags(data_product)` to the job (tags, and `dp_run_id` as a job parameter). Run a heartbeat source
  (DPC-0058).
- **W1c silver and W1d gold:** record `detect.*` for SCD-02, HIST-05, LATE-02 and DUP-04. Write the silver and
  gold tables that the scenarios' `engine` checks will compare with the oracle. The gate's `TableSpec` for each
  table comes from its manifest's keys, measures and tolerance.
- **W1e data expectation services:** write `quality.*`, `recon.*` and `profile.*` steps in the one step format
  (REC-05's entry, with the hop and the keys in the message); replace the DES screen's queries with DES's own
  result tables; turn on DES's row in `test_obs_steps.py`.
- **Then in W3:** drive the obs checks from the engine's run log instead of the fixture once ingestion records
  through `RunRecorder`. Write the ops tables to Delta on the engine (the `on_finish` hook of each run, plus the
  daily copy). Bind a statement-execution runner for `DatabricksSystemTables`, `DatabricksPipelineEvents` and
  `DatabricksTables`, and turn the gate's pending contract rows on (`DP_GATE_WAREHOUSE`). Run `views/*.sql`, include
  `dashboards/resources.yml` in the bundle, merge `JOB_NOTIFICATIONS` into every job, and deploy to Free Edition.
