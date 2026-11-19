# ADR-035 spike, step 1: open-source declarative silver with AUTO CDC (13 Nov 2026)

`run.sh` builds SCD Type 2 silver for a tiny order-line change feed with open-source Spark Declarative Pipelines:
our own stage code (`clean_text`) inside the flow, `create_auto_cdc_flow(stored_as_scd_type=2)` into Iceberg.

## Result: blocked by versions, not by the design

| Fact (checked 13 Nov 2026 against source and package indexes) | |
|---|---|
| AUTO CDC in `pyspark.pipelines` | absent in Spark 4.1.0; Spark 4.2 = SCD Type 1 only (raises on 2); Spark 4.3.0 = SCD Type 1 and 2 |
| AUTO CDC target | must support row-level operations (DSv2), so Iceberg or a similar format, not plain files |
| Iceberg runtime | newest 1.12.0, built for Spark 4.0 and 4.1 only; on Spark 4.3 it fails to load (`IncompatibleClassChangeError: SparkView can not implement ...View`) |
| Delta | delta-spark 4.4.1 supports Spark 4.0.1 to 4.2.0 |

So no open table format runs on the only Spark version with open-source SCD2 AUTO CDC. Rerun `run.sh` when an
Iceberg (or Delta) release supports Spark 4.3: `PY=<pyspark 4.3 python> JAR=<iceberg runtime for 4.3> WORK=<dir> ./run.sh`.
`show.py` checks: our stage ran, three versions in sequence order, the late older change not current, delete closes.

# Step 1b: the same flow on Databricks Lakeflow (Free Edition, serverless), 13-14 Nov 2026

`lakeflow/` (bundle `dp_sdp_silver_spike`, pipeline `dp-dev-sdp-silver-spike`, schema `dev_silver.spike_sdp`):
the change feed uploaded to the volume `dev_ops.spike.changes`, our `clean_text` imported from the synced `src/`,
AUTO CDC SCD Type 2 into two streaming tables, run as two triggered updates (batch 1, then batch 2).

**Result: all five history checks pass on both tables** (our stage ran inside the flow; versions 10, 20, 30; the late
older change is history, not current; the delete closes the key; the qty change opens a version).

Findings:

| Finding | Meaning |
|---|---|
| Streaming tables are Delta; "Iceberg" is Delta with Iceberg metadata (UniForm), and pipelines require Iceberg v3 for it (`Streaming Table and Materialized View must use Iceberg V3 or above`) | Properties: `delta.enableIcebergCompatV3=true`, `delta.universalFormat.enabledFormats=iceberg`, `delta.columnMapping.mode=name`. Native Iceberg (our openness rule for the client) is not what the declarative engine writes |
| IcebergCompatV2 needs column mapping and deletion vectors off; V3 keeps deletion vectors | V3 is the one that works for pipeline tables |
| A property that must be set at creation cannot be fixed by a full refresh | The table keeps its features; only a new table (or a drop) applies them |
| Tables from removed or renamed definitions stay behind, owned by no code | Teardown must drop them (same lesson as slice 1, 2 Nov) |
| Triggered mode: each update reads only new files, then stops | A streaming table in batch mode is incremental batch |
