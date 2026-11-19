# Databricks realm: slice 1

Orders from the pretend shop's database to silver, gold and a metric view, on Databricks Free Edition.
Specs: `data-platform-catalog/slices/01-orders/` (contract, manifest, world, binding).

| Part | Who does it | Where |
|---|---|---|
| Mover (world writes deliveries into a volume) | the world, playing the source and mover | `tasks/main.py` prepare |
| Metadata DB (config, rules, batches, run log, checks, evidence) | ours, on Delta tables | `src/dataplatform/adapters/spark_delta.py` |
| Lander: landing to bronze, batch key, contract check | ours (decision D11) | `src/dataplatform/lander.py` |
| Silver: ordering by change sequence, deletes, SCD type 2, row rules | Databricks (AUTO CDC, expectations), rules read from the metadata DB | `pipelines/silver.py` |
| Quarantine with rule names | Databricks table, our rule naming | `pipelines/silver.py` |
| Gold and metric view | Databricks (table, metric view) | `tasks/main.py` finish |
| Checks: oracle, detectors, reconciliation, contract table rules | ours, keyed by run id | `tasks/checks.py` |
| Orchestration, retries, run history | Databricks Jobs | `databricks.yml` |
| Lineage | Databricks (Unity Catalog) | automatic |

Run: `scripts/run_slice1.sh` (needs `databricks auth login --profile free`). Evidence:
`SELECT * FROM workspace.dp_dev_meta.evidence ORDER BY day`.
