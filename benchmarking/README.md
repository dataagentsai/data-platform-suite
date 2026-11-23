# Benchmarking: worst case, start to end, ending in Power BI

Requirements as given: [REQUIREMENTS.md](REQUIREMENTS.md). This folder holds the **mechanics**. Nothing here generates
data on this machine; the data is generated and everything is run in the office Databricks and Power BI.

## What is here

| Path | What it is | State |
|---|---|---|
| `scenarios/powerbi_worst_cases.yaml` | 14 Power BI worst cases (PB-01 to PB-14): why each is hard, how the data is shaped, what is measured, when it passes | designed |
| `scenarios/maintenance.yaml` | 9 table-maintenance operations (M-01 to M-09) x the three formats; every cell starts *unverified* | designed |
| `bench/plan.py` | Checks the two files and expands them into one run plan (105 runs for the three formats) | built, tested |
| `bench/maintenance.py` + `databricks/tasks/maintenance_probe.py` | Makes a fragmented table in a format, tries every maintenance statement, records ok or the engine's error | built, tested on a stand-in; **not run on a workspace** |
| `databricks/databricks.yml` | Jobs `bench_maintenance` (serverless, UC formats) and `bench_maintenance_s3tables` (classic cluster with the S3 Tables catalog) | written; not validated against a workspace |
| `bench/dax.py` + `powerbi/run_pack.py` + `powerbi/queries.yaml` | Runs a DAX query pack against a published model through the Power BI REST API, cold and warm, and writes timings | built, tested without a network; **not run against Power BI** |
| `deploy.sh` | `validate`, `deploy`, `maintenance`, `world`, `all` for a Databricks profile | syntax checked only |
| the world | The engine bundle's `engine_days` job now takes `table_format` and `scale` parameters | changed; not run |

## Not built yet (the next step)

The **worst-case data shapers** for the world: skew, very high key cardinality, deep SCD2 history, a security mapping
table, fragmented files, concurrent-user load. They extend the tyre world's generator (reuse first) and are driven by
the `data_shape` of each case. Until they exist, `engine_days` at a larger `scale` is the largest honest load.

## The office run, in order

1. `benchmarking/deploy.sh <profile> validate`: scenario files and both bundles.
2. `benchmarking/deploy.sh <profile> deploy`: copies the tyre slice into the engine bundle and deploys both bundles.
3. `benchmarking/deploy.sh <profile> maintenance`: one probe per format. Results land in the volume
   `<env>_ops.reports.bench/<run id>/maintenance-<format>.json`. Bring them back; the verdict per operation is in them.
4. `benchmarking/deploy.sh <profile> world`: the 3-day world per format (`DAYS=3 SCALE=0.1` by default; raise `SCALE`).
5. In Power BI: publish a semantic model on the gold tables for each format and mode, write a `names.json` mapping the
   pack's names to the model's, then `python powerbi/run_pack.py --workspace <id> --dataset <id> --names names.json
   --label <format>-<mode>`. Bring the `results/powerbi-*.json` files back.

## Things to check before the first run

- S3 Tables: the table-bucket ARN (`--var s3tables_arn=...`), the two library versions in `databricks.yml`
  (`iceberg_runtime`, `s3tables_catalog_jar`), IAM for the cluster, and that the cluster's own Databricks Runtime allows
  the extra Iceberg catalog beside its built-in one.
- The Power BI token: `PBI_TOKEN` from the environment, never a file.
- The DAX templates use `{{names}}` and are written without sight of the published model: adjust before trusting a
  number.
